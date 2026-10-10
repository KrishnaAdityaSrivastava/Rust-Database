package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	httpAddr = ":8080"
	rustAddr = "127.0.0.1:7001"
)

var rustNodes = map[uint64]string{
    1: "127.0.0.1:7001",
    2: "127.0.0.1:7002",
    3: "127.0.0.1:7003",
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

func callRust(ctx context.Context,addr string, req WireRequest) (WireResponse, error) {
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

func forwardRust(w http.ResponseWriter, r *http.Request, req WireRequest) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := callRust(ctx, addr, req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": err.Error(),
		})
		return
	}

	status := http.StatusOK
	if !resp.Success {
		status = http.StatusConflict
	}

	writeJSON(w, status, resp)
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

	server := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("Go REST API listening on %s", httpAddr)
	log.Printf("Forwarding requests to Rust service at %s", rustAddr)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
