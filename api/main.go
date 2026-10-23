package main

import (
	"bufio"
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	httpAddr = ":8080"
	rustAddr = "node1:7001"
)

// nodeRegistry stores the gateway's current view of node HTTP API addresses.
// Membership changes update it only after the Rust cluster confirms success.

type nodeRegistry struct {
	mu    sync.RWMutex
	nodes map[uint64]string
}

func newNodeRegistry(initial map[uint64]string) *nodeRegistry {
	nodes := make(map[uint64]string, len(initial))
	for id, addr := range initial {
		nodes[id] = addr
	}
	return &nodeRegistry{nodes: nodes}
}

func (nr *nodeRegistry) snapshot() map[uint64]string {
	nr.mu.RLock()
	defer nr.mu.RUnlock()
	result := make(map[uint64]string, len(nr.nodes))
	for id, addr := range nr.nodes {
		result[id] = addr
	}
	return result
}

func (nr *nodeRegistry) get(id uint64) (string, bool) {
	nr.mu.RLock()
	defer nr.mu.RUnlock()
	addr, ok := nr.nodes[id]
	return addr, ok
}

func (nr *nodeRegistry) set(id uint64, addr string) {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	nr.nodes[id] = addr
}

func (nr *nodeRegistry) remove(id uint64) {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	delete(nr.nodes, id)
}

var membershipMu sync.Mutex

var rustNodes = newNodeRegistry(map[uint64]string{
	1: "node1:7001",
	2: "node2:7002",
	3: "node3:7003",
})

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
	Op     string     `json:"op"`
	Key    string     `json:"key,omitempty"`
	Value  *WireValue `json:"value,omitempty"`
	NodeID uint64     `json:"node_id,omitempty"`
	Addr   string     `json:"addr,omitempty"`
}

type WireResponse struct {
	Success  bool            `json:"success"`
	Value    json.RawMessage `json:"value"`
	Message  string          `json:"message"`
	LeaderID *uint64         `json:"leader_id"`
	Term     uint64          `json:"term"`
}

type NodeHealth struct {
	NodeID    uint64  `json:"node_id"`
	Address   string  `json:"address"`
	Reachable bool    `json:"reachable"`
	Healthy   bool    `json:"healthy"`
	LatencyMS int64   `json:"latency_ms"`
	Message   string  `json:"message,omitempty"`
	Error     string  `json:"error,omitempty"`
	LeaderID  *uint64 `json:"leader_id,omitempty"`
	Term      uint64  `json:"term,omitempty"`
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
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&result); err != nil {
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
		case r.URL.Path == "/health/nodes":
			route = "/health/nodes"
		case strings.HasPrefix(r.URL.Path, "/health/node/"):
			route = "/health/node/{id}"
		case r.URL.Path == "/status":
			route = "/status"
		case r.URL.Path == "/metrics":
			route = "/metrics"
		case strings.HasPrefix(r.URL.Path, "/errors/recent"):
			route = "/errors/recent"
		case r.URL.Path == "/diagnose":
			route = "/diagnose"
		case r.URL.Path == "/recovery/recommend":
			route = "/recovery/recommend"
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

func executeRust(ctx context.Context, req WireRequest) (WireResponse, int, error) {
	nodes := rustNodes.snapshot()
	if len(nodes) == 0 {
		return WireResponse{}, http.StatusBadGateway, errors.New("no Rust nodes are registered")
	}
	ids := make([]uint64, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	candidates := make([]string, 0, len(nodes))
	if addr, ok := nodes[1]; ok {
		candidates = append(candidates, addr)
	}
	for _, id := range ids {
		addr := nodes[id]
		if len(candidates) == 0 || addr != candidates[0] {
			candidates = append(candidates, addr)
		}
	}
	tried := make(map[string]bool, len(candidates))
	var lastResp WireResponse
	var lastErr error
	gotResponse := false
	for len(candidates) > 0 {
		if err := ctx.Err(); err != nil {
			lastErr = err
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
		lastResp, gotResponse = resp, true
		if resp.LeaderID != nil {
			if leaderAddr, ok := nodes[*resp.LeaderID]; ok && !tried[leaderAddr] {
				candidates = append([]string{leaderAddr}, candidates...)
				if !resp.Success {
					continue
				}
			}
		}
		if resp.Success {
			return resp, http.StatusOK, nil
		}
	}
	if gotResponse {
		status := http.StatusConflict
		if ctx.Err() != nil {
			status = http.StatusGatewayTimeout
		}
		return lastResp, status, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no Rust node responded")
	}
	return WireResponse{}, http.StatusBadGateway, fmt.Errorf("no Rust node responded successfully: %w", lastErr)
}

func forwardRust(w http.ResponseWriter, r *http.Request, req WireRequest) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, status, err := executeRust(ctx, req)
	if err != nil {
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, status, resp)
}

// apiAddressFor derives the HTTP API address from the Raft transport address.
// By convention, Raft port 600N maps to HTTP API port 700N.

func apiAddressFor(raftAddr, explicitAPIAddr string) (string, error) {
	if strings.TrimSpace(explicitAPIAddr) != "" {
		explicitAPIAddr = strings.TrimSpace(explicitAPIAddr)
		if _, _, err := net.SplitHostPort(explicitAPIAddr); err != nil {
			return "", fmt.Errorf("invalid api_addr %q: %w", explicitAPIAddr, err)
		}
		return explicitAPIAddr, nil
	}
	host, portText, err := net.SplitHostPort(strings.TrimSpace(raftAddr))
	if err != nil {
		return "", fmt.Errorf("addr must be host:port (or provide api_addr): %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("invalid Raft port %q", portText)
	}
	if port >= 6000 && port <= 6999 {
		port += 1000
	} else if port < 7000 || port > 7999 {
		return "", errors.New("cannot derive API port: expected Raft port 6000-6999 or explicit api_addr")
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
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

func handleNodeHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Sort IDs so the response is deterministic.
	ids := make([]uint64, 0, len(rustNodes.snapshot()))
	for id := range rustNodes.snapshot() {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	results := make([]NodeHealth, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func(i int, id uint64) {
			defer wg.Done()
			addr, _ := rustNodes.get(id)
			result := NodeHealth{
				NodeID:  id,
				Address: addr,
			}
			start := time.Now()
			ctx, cancel := context.WithTimeout(
				r.Context(),
				2*time.Second,
			)
			defer cancel()
			resp, err := callRust(ctx, addr, WireRequest{
				Op: "STATUS",
			})
			result.LatencyMS = time.Since(start).Milliseconds()
			if err != nil {
				result.Reachable = false
				result.Healthy = false
				result.Error = err.Error()
				result.Message = "Node did not respond to STATUS"
				results[i] = result
				return
			}
			result.Reachable = true
			result.Healthy = resp.Success
			result.Message = resp.Message
			result.LeaderID = resp.LeaderID
			result.Term = resp.Term
			results[i] = result
		}(i, id)
	}
	wg.Wait()
	allHealthy := true
	healthyCount := 0
	for _, node := range results {
		if node.Healthy {
			healthyCount++
		} else {
			allHealthy = false
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":       allHealthy,
		"cluster_size":  len(results),
		"healthy_nodes": healthyCount,
		"message":       fmt.Sprintf("%d/%d nodes responding healthily", healthyCount, len(results)),
		"nodes":         results,
	})
}

// handleNodeDetail serves GET /health/node/{id}.
// It contacts that specific node directly and returns its health.

func handleNodeDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	idStr := strings.TrimPrefix(r.URL.Path, "/health/node/")
	if idStr == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing node_id"})
		return
	}
	id64, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "node_id must be a positive integer"})
		return
	}
	addr, ok := rustNodes.get(id64)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": fmt.Sprintf("unknown node_id %d; configured nodes: %v", id64, rustNodes.snapshot()),
		})
		return
	}
	result := NodeHealth{
		NodeID:  id64,
		Address: addr,
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	resp, err := callRust(ctx, addr, WireRequest{Op: "STATUS"})
	result.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		result.Reachable = false
		result.Healthy = false
		result.Error = err.Error()
		result.Message = "Node did not respond to STATUS"
		writeJSON(w, http.StatusOK, result)
		return
	}
	result.Reachable = true
	result.Healthy = resp.Success
	result.Message = resp.Message
	result.LeaderID = resp.LeaderID
	result.Term = resp.Term
	writeJSON(w, http.StatusOK, result)
}

// handleRecentErrors serves GET /errors/recent[?limit=N].
// It queries every node in parallel and collects those that are
// unhealthy or unreachable, returning them as a structured error list.

func handleRecentErrors(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	limit := 0
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		l, err := strconv.Atoi(lStr)
		if err != nil || l < 1 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be a positive integer"})
			return
		}
		limit = l
	}
	ids := make([]uint64, 0, len(rustNodes.snapshot()))
	for id := range rustNodes.snapshot() {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	type nodeError struct {
		NodeID    uint64 `json:"node_id"`
		Address   string `json:"address"`
		Reachable bool   `json:"reachable"`
		Healthy   bool   `json:"healthy"`
		Error     string `json:"error,omitempty"`
		Message   string `json:"message,omitempty"`
		LatencyMS int64  `json:"latency_ms"`
	}
	results := make([]nodeError, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func(i int, id uint64) {
			defer wg.Done()
			addr, _ := rustNodes.get(id)
			ne := nodeError{NodeID: id, Address: addr}
			start := time.Now()
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			resp, err := callRust(ctx, addr, WireRequest{Op: "STATUS"})
			ne.LatencyMS = time.Since(start).Milliseconds()
			if err != nil {
				ne.Reachable = false
				ne.Healthy = false
				ne.Error = err.Error()
				ne.Message = "Node did not respond to STATUS"
			} else {
				ne.Reachable = true
				ne.Healthy = resp.Success
				ne.Message = resp.Message
				if !resp.Success {
					ne.Error = resp.Message
				}
			}
			results[i] = ne
		}(i, id)
	}
	wg.Wait()
	// Collect only unhealthy / unreachable nodes as "errors".
	var errs []nodeError
	for _, ne := range results {
		if !ne.Healthy {
			errs = append(errs, ne)
		}
	}
	if limit > 0 && len(errs) > limit {
		errs = errs[:limit]
	}
	if errs == nil {
		errs = []nodeError{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("%d error(s) found across %d node(s)", len(errs), len(ids)),
		"errors":  errs,
	})
}

// handleDiagnose serves GET /diagnose.
// It aggregates the cluster status and per-node health into a single
// structured diagnostic report.

func handleDiagnose(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ids := make([]uint64, 0, len(rustNodes.snapshot()))
	for id := range rustNodes.snapshot() {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	nodes := make([]NodeHealth, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func(i int, id uint64) {
			defer wg.Done()
			addr, _ := rustNodes.get(id)
			nh := NodeHealth{NodeID: id, Address: addr}
			start := time.Now()
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			resp, err := callRust(ctx, addr, WireRequest{Op: "STATUS"})
			nh.LatencyMS = time.Since(start).Milliseconds()
			if err != nil {
				nh.Reachable = false
				nh.Healthy = false
				nh.Error = err.Error()
				nh.Message = "Node did not respond"
			} else {
				nh.Reachable = true
				nh.Healthy = resp.Success
				nh.Message = resp.Message
				nh.LeaderID = resp.LeaderID
				nh.Term = resp.Term
			}
			nodes[i] = nh
		}(i, id)
	}
	wg.Wait()
	healthyCount := 0
	reachableCount := 0
	var leaderID *uint64
	var term uint64
	var issues []string
	for _, n := range nodes {
		if n.Reachable {
			reachableCount++
		} else {
			issues = append(issues, fmt.Sprintf("node %d (%s) is unreachable: %s", n.NodeID, n.Address, n.Error))
		}
		if n.Healthy {
			healthyCount++
			if n.LeaderID != nil {
				leaderID = n.LeaderID
			}
			if n.Term > term {
				term = n.Term
			}
		} else if n.Reachable {
			issues = append(issues, fmt.Sprintf("node %d (%s) is reachable but unhealthy: %s", n.NodeID, n.Address, n.Message))
		}
	}
	clusterOK := healthyCount > len(ids)/2
	summary := fmt.Sprintf("%d/%d nodes healthy, %d/%d reachable", healthyCount, len(ids), reachableCount, len(ids))
	if !clusterOK {
		summary = "DEGRADED: " + summary + " — quorum may be lost"
	}
	if issues == nil {
		issues = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":         clusterOK,
		"message":         summary,
		"cluster_size":    len(ids),
		"healthy_nodes":   healthyCount,
		"reachable_nodes": reachableCount,
		"leader_id":       leaderID,
		"term":            term,
		"nodes":           nodes,
		"issues":          issues,
	})
}

// handleRecoveryRecommend serves POST /recovery/recommend.
// It accepts a { "summary": "..." } body and applies rule-based
// heuristics to return structured recovery suggestions.

func handleRecoveryRecommend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	defer r.Body.Close()
	var body struct {
		Summary string `json:"summary"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "expected JSON with a \"summary\" field"})
		return
	}
	if strings.TrimSpace(body.Summary) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "summary must not be empty"})
		return
	}
	summaryLower := strings.ToLower(body.Summary)
	type step struct {
		Order       int    `json:"order"`
		Action      string `json:"action"`
		Description string `json:"description"`
		Risk        string `json:"risk"`
	}
	var steps []step
	var diagnosis string
	switch {
	case strings.Contains(summaryLower, "leader") && (strings.Contains(summaryLower, "lost") || strings.Contains(summaryLower, "no leader") || strings.Contains(summaryLower, "election")):
		diagnosis = "Leader loss / election failure"
		steps = []step{
			{1, "diagnose_nodes", "Run diagnose_nodes to confirm which nodes are reachable and identify the current election state.", "none"},
			{2, "wait_election", "Allow up to 10 seconds for a new leader election to complete before intervening.", "low"},
			{3, "restart_isolated_node", "If a node is isolated (reachable = false), restart that node container to re-join the cluster.", "medium — causes brief unavailability on that node"},
			{4, "verify_quorum", "Re-run diagnose_nodes and confirm healthy_nodes > cluster_size / 2.", "none"},
		}
	case strings.Contains(summaryLower, "quorum") || strings.Contains(summaryLower, "majority") || strings.Contains(summaryLower, "unavailable"):
		diagnosis = "Quorum loss / majority of nodes down"
		steps = []step{
			{1, "diagnose_nodes", "Identify which nodes are down.", "none"},
			{2, "restore_nodes", "Bring enough downed nodes back online to restore quorum (need > N/2 healthy).", "medium"},
			{3, "verify_leader", "Confirm a new leader is elected via get_cluster_status.", "none"},
			{4, "replay_pending_ops", "If writes were rejected during the outage, retry them after quorum is restored.", "low — idempotent ops are safe to retry"},
		}
	case strings.Contains(summaryLower, "network") || strings.Contains(summaryLower, "partition") || strings.Contains(summaryLower, "split"):
		diagnosis = "Network partition"
		steps = []step{
			{1, "identify_partition", "Use diagnose_nodes to find which nodes can reach each other.", "none"},
			{2, "restore_connectivity", "Restore network connectivity between partitioned segments.", "none"},
			{3, "wait_convergence", "Allow Raft to re-synchronise logs; monitor term and leader_id stabilising.", "low"},
			{4, "check_stale_reads", "Clients that read from a minority partition may have stale data — re-read critical keys.", "low"},
		}
	case strings.Contains(summaryLower, "slow") || strings.Contains(summaryLower, "latency") || strings.Contains(summaryLower, "timeout"):
		diagnosis = "High latency / timeouts"
		steps = []step{
			{1, "check_metrics", "Run get_cluster_metrics to inspect request latency percentiles and error rates.", "none"},
			{2, "diagnose_nodes", "Check per-node latency via diagnose_nodes.", "none"},
			{3, "investigate_load", "Examine host CPU/memory/disk I/O on slow nodes.", "none"},
			{4, "reduce_traffic", "Temporarily reduce write throughput to allow Raft log catchup if a follower is lagging.", "medium — impacts write availability"},
		}
	default:
		diagnosis = "General cluster issue"
		steps = []step{
			{1, "diagnose_nodes", "Start with a full node health check to gather evidence.", "none"},
			{2, "get_recent_errors", "Retrieve recent node errors for more detail.", "none"},
			{3, "get_cluster_status", "Confirm current leader and term.", "none"},
			{4, "escalate", "If the above tools do not pinpoint the issue, review node logs directly.", "none"},
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":         true,
		"failure_summary": body.Summary,
		"diagnosis":       diagnosis,
		"recovery_steps":  steps,
		"disclaimer":      "These are heuristic recommendations. Always validate against your specific cluster state before acting.",
	})
}

func handleAddNode(w http.ResponseWriter, r *http.Request) {
	membershipMu.Lock()
	defer membershipMu.Unlock()
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	defer r.Body.Close()
	var req struct {
		ID      uint64 `json:"id"`
		Addr    string `json:"addr"`               // Raft transport address, e.g. node4:6004
		APIAddr string `json:"api_addr,omitempty"` // optional HTTP API address, e.g. node4:7004
	}
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "expected JSON with id and addr"})
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request must contain exactly one JSON object"})
		return
	}
	req.Addr = strings.TrimSpace(req.Addr)
	if req.ID == 0 || req.Addr == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id and addr are required"})
		return
	}
	if _, exists := rustNodes.get(req.ID); exists {
		writeJSON(w, http.StatusConflict, map[string]string{"error": fmt.Sprintf("node %d is already registered", req.ID)})
		return
	}
	apiAddr, err := apiAddressFor(req.Addr, req.APIAddr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, status, callErr := executeRust(ctx, WireRequest{Op: "ADDNODE", NodeID: req.ID, Addr: req.Addr})
	if callErr != nil {
		writeJSON(w, status, map[string]string{"error": callErr.Error()})
		return
	}
	if status == http.StatusOK && resp.Success {
		rustNodes.set(req.ID, apiAddr)
	}
	writeJSON(w, status, resp)
}

func handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	membershipMu.Lock()
	defer membershipMu.Unlock()
	if r.Method != http.MethodDelete {
		w.Header().Set("Allow", "DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id, err := strconv.ParseUint(r.URL.Query().Get("id"), 10, 64)
	if err != nil || id == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "a valid node id is required"})
		return
	}
	if _, exists := rustNodes.get(id); !exists {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("node %d is not registered", id)})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, status, callErr := executeRust(ctx, WireRequest{Op: "REMOVENODE", NodeID: id})
	if callErr != nil {
		writeJSON(w, status, map[string]string{"error": callErr.Error()})
		return
	}
	if status == http.StatusOK && resp.Success {
		rustNodes.remove(id)
	}
	writeJSON(w, status, resp)
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", handleKV)
	mux.HandleFunc("/status", handleStatus)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health/nodes", handleNodeHealth)
	mux.HandleFunc("/health/node/", handleNodeDetail)
	mux.HandleFunc("/errors/recent", handleRecentErrors)
	mux.HandleFunc("/diagnose", handleDiagnose)
	mux.HandleFunc("/recovery/recommend", handleRecoveryRecommend)
	mux.HandleFunc("/nodes/add", handleAddNode)
	mux.HandleFunc("/nodes/remove", handleRemoveNode)
	server := &http.Server{
		Addr:              httpAddr,
		Handler:           metricsMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("Go REST API listening on %s", httpAddr)
	log.Printf("Forwarding requests to Rust cluster; preferred node: %s", rustAddr)
	if err := server.ListenAndServe(); err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
