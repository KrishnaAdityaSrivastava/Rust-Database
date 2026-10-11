package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	httpAddr = ":8080"
	rustAddr = "node1:7001"
)

var rustNodes = map[uint64]string{
	1: "node1:7001",
	2: "node2:7002",
	3: "node3:7003",
}

var (
	apiRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kvraft_api_requests_total",
			Help: "Total HTTP requests received by the API gateway.",
		},
		[]string{"method", "route", "status"},
	)

	apiRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kvraft_api_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "route"},
	)
)

func init() {
	prometheus.MustRegister(apiRequests, apiRequestDuration)
}

type WireValue struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

type WireRequest struct {
	Op    string     `json:"op"`
	Key   string     `json:"key,omitempty"`
	Value *WireValue `json:"value,omitempty"`
}

type WireResponse struct {
	Success  bool            `json:"success"`
	Value    json.RawMessage `json:"value"`
	Message  string          `json:"message"`
	LeaderID *uint64         `json:"leader_id"`
	Term     uint64          `json:"term"`
}

func validateValue(v WireValue) error {
	switch v.Type {
	case "string":
		var value string
		if err := json.Unmarshal(v.Value, &value); err != nil {
			return errors.New("string values must be JSON strings")
		}
	case "int":
		var value int64
		if err := json.Unmarshal(v.Value, &value); err != nil {
			return errors.New("int values must be 64-bit integers")
		}
	case "float":
		var value float64
		if err := json.Unmarshal(v.Value, &value); err != nil {
			return errors.New("float values must be numbers")
		}
	default:
		return errors.New("type must be string, int, or float")
	}

	if !json.Valid(v.Value) {
		return errors.New("invalid JSON value")
	}
	return nil
}

func callRust(ctx context.Context, addr string, req WireRequest) (WireResponse, error) {
	var result WireResponse

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return result, fmt.Errorf("connect to Rust service %s: %w", addr, err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	}

	// One newline-delimited JSON request per TCP connection.
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return result, fmt.Errorf("send request to Rust: %w", err)
	}

	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return result, fmt.Errorf("read response from Rust: %w", err)
	}

	if err := json.Unmarshal(bytes.TrimSpace(line), &result); err != nil {
		return result, fmt.Errorf("decode Rust response: %w", err)
	}

	return result, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (sr *statusRecorder) WriteHeader(status int) {
	if sr.status != 0 {
		return
	}
	sr.status = status
	sr.ResponseWriter.WriteHeader(status)
}

func (sr *statusRecorder) Write(b []byte) (int, error) {
	if sr.status == 0 {
		sr.WriteHeader(http.StatusOK)
	}
	return sr.ResponseWriter.Write(b)
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		recorder := &statusRecorder{
			ResponseWriter: w,
		}

		next.ServeHTTP(recorder, r)

		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}

		// Use route labels, not arbitrary URL paths or keys.
		route := "unmatched"
		switch {
		case strings.HasPrefix(r.URL.Path, "/kv/"):
			route = "/kv/{key}"
		case r.URL.Path == "/status":
			route = "/status"
		case r.URL.Path == "/metrics":
			route = "/metrics"
		}

		apiRequests.WithLabelValues(
			r.Method,
			route,
			fmt.Sprintf("%d", status),
		).Inc()

		apiRequestDuration.WithLabelValues(
			r.Method,
			route,
		).Observe(time.Since(start).Seconds())
	})
}

func forwardRust(w http.ResponseWriter, r *http.Request, req WireRequest) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Try the preferred node first, then the remaining nodes.
	candidates := []string{rustAddr}
	for _, addr := range rustNodes {
		if addr != rustAddr {
			candidates = append(candidates, addr)
		}
	}

	tried := make(map[string]bool)
	var lastResp WireResponse
	var lastErr error
	gotResponse := false

	for len(candidates) > 0 {
		if err := ctx.Err(); err != nil {
			break
		}

		target := candidates[0]
		candidates = candidates[1:]

		if tried[target] {
			continue
		}
		tried[target] = true

		resp, err := callRust(ctx, target, req)
		if err != nil {
			lastErr = err
			continue
		}

		lastResp = resp
		gotResponse = true

		// Prefer the known leader if this node identifies one.
		if resp.LeaderID != nil {
			if leaderAddr, ok := rustNodes[*resp.LeaderID]; ok &&
				!tried[leaderAddr] {

				// Prioritize the leader over the remaining candidates.
				candidates = append(
					[]string{leaderAddr},
					candidates...,
				)

				if !resp.Success {
					continue
				}
			}
		}

		// Return a completed operation response.
		if resp.Success {
			writeJSON(w, http.StatusOK, resp)
			return
		}

		// A failure without a known leader may be recoverable
		// by trying another node.
	}

	if gotResponse {
		status := http.StatusConflict
		if ctx.Err() != nil {
			status = http.StatusGatewayTimeout
		}
		writeJSON(w, status, lastResp)
		return
	}

	writeJSON(w, http.StatusBadGateway, map[string]string{
		"error": fmt.Sprintf(
			"no Rust node responded successfully; last error: %v",
			lastErr,
		),
	})
}

func handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" || strings.Contains(key, "/") {
		http.Error(w, "expected a key at /kv/{key}", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		forwardRust(w, r, WireRequest{
			Op:  "GET",
			Key: key,
		})

	case http.MethodPut:
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		defer r.Body.Close()

		var body WireValue
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "expected JSON containing type and value",
			})
			return
		}

		// Reject extra JSON values after the first object.
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "request must contain exactly one JSON object",
			})
			return
		}

		if err := validateValue(body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": err.Error(),
			})
			return
		}

		forwardRust(w, r, WireRequest{
			Op:    "SET",
			Key:   key,
			Value: &body,
		})

	case http.MethodDelete:
		forwardRust(w, r, WireRequest{
			Op:  "DELETE",
			Key: key,
		})

	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	forwardRust(w, r, WireRequest{Op: "STATUS"})
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", handleKV)
	mux.HandleFunc("/status", handleStatus)
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:              httpAddr,
		Handler:           metricsMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("Go REST API listening on %s", httpAddr)
	log.Printf("Forwarding requests to Rust service at %s", rustAddr)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
