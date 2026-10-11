#!/usr/bin/env python3
"""
KV-Store Prometheus Performance Evaluation Tool
Queries Prometheus metrics (or the Go API /metrics exposition) and computes:
- Request rates and status code distributions (2xx, 4xx, 5xx)
- Request latency percentiles (p50, p90, p95, p99) from Prometheus histogram buckets
- Average request duration and per-route breakdown
"""

import argparse
import json
import math
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BOLD = "\033[1m"
CYAN = "\033[0;36m"
GREEN = "\033[0;32m"
YELLOW = "\033[1;33m"
RED = "\033[0;31m"
RESET = "\033[0m"


def fetch_url(url, timeout=3.0):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "kv-eval/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8")
    except Exception as e:
        return 0, str(e)


def send_http(method, url, data=None, timeout=2.0):
    try:
        req = urllib.request.Request(
            url,
            data=data.encode("utf-8") if data else None,
            headers={"Content-Type": "application/json"} if data else {},
            method=method,
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0


def generate_load(api_url, duration_sec, concurrency, num_keys):
    print(f"\n{BOLD}{CYAN}>>> Generating HTTP workload against {api_url}...{RESET}")
    print(f"    Duration: {duration_sec}s | Concurrency: {concurrency} workers | Keys: {num_keys}")

    stop_time = time.time() + duration_sec
    op_counts = {"PUT": 0, "GET": 0, "STATUS": 0, "DELETE": 0}

    def worker(worker_id):
        nonlocal op_counts
        local_counts = {"PUT": 0, "GET": 0, "STATUS": 0, "DELETE": 0}
        rng = random.Random(worker_id * 1000 + int(time.time()))

        while time.time() < stop_time:
            key = f"prom_key_{rng.randint(0, num_keys - 1):06d}"
            roll = rng.random()

            if roll < 0.50:
                # PUT
                payload = json.dumps({"type": "string", "value": f"val_{rng.randint(0, 99999)}"})
                send_http("PUT", f"{api_url}/kv/{key}", data=payload)
                local_counts["PUT"] += 1
            elif roll < 0.85:
                # GET
                send_http("GET", f"{api_url}/kv/{key}")
                local_counts["GET"] += 1
            elif roll < 0.95:
                # STATUS
                send_http("GET", f"{api_url}/status")
                local_counts["STATUS"] += 1
            else:
                # DELETE
                send_http("DELETE", f"{api_url}/kv/{key}")
                local_counts["DELETE"] += 1

        return local_counts

    start = time.time()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker, i) for i in range(concurrency)]
        for f in futures:
            res = f.result()
            for k in op_counts:
                op_counts[k] += res[k]

    elapsed = time.time() - start
    total_ops = sum(op_counts.values())
    print(f"    Completed {total_ops} requests in {elapsed:.2f}s ({total_ops / elapsed:.1f} req/s).")
    return total_ops


def parse_prometheus_text(text):
    """
    Parses Prometheus text exposition format into structured data:
    - counters: {(metric, labels_tuple): value}
    - histograms: {(metric, labels_tuple): {"buckets": [(le, count)], "sum": float, "count": float}}
    """
    counters = {}
    histograms = {}

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        # Match metric_name{labels} value or metric_name value
        m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([0-9.eE+-]+|NaN|\+Inf|-Inf)$", line)
        if not m:
            continue

        name, label_str, val_str = m.groups()
        try:
            val = float(val_str)
        except ValueError:
            continue

        labels = {}
        if label_str:
            for pair in re.finditer(r'([a-zA-Z_0-9]+)="([^"]*)"', label_str):
                labels[pair.group(1)] = pair.group(2)

        if name == "kvraft_api_requests_total":
            key = (
                labels.get("method", "UNKNOWN"),
                labels.get("route", "UNKNOWN"),
                labels.get("status", "0"),
            )
            counters[key] = val

        elif name.startswith("kvraft_api_request_duration_seconds"):
            route_key = (
                labels.get("method", "ALL"),
                labels.get("route", "ALL"),
            )
            if route_key not in histograms:
                histograms[route_key] = {"buckets": [], "sum": 0.0, "count": 0.0}

            if name.endswith("_bucket"):
                le_str = labels.get("le", "Inf")
                le = float("inf") if le_str == "+Inf" else float(le_str)
                histograms[route_key]["buckets"].append((le, val))
            elif name.endswith("_sum"):
                histograms[route_key]["sum"] = val
            elif name.endswith("_count"):
                histograms[route_key]["count"] = val

    # Sort histogram buckets by le
    for key in histograms:
        histograms[key]["buckets"].sort(key=lambda x: x[0])

    return counters, histograms


def calculate_quantile_from_buckets(buckets, q):
    """
    Implements Prometheus linear interpolation for histogram_quantile(q, buckets).
    buckets: list of (le, count) sorted by le.
    """
    if not buckets:
        return 0.0

    total_count = buckets[-1][1]
    if total_count <= 0:
        return 0.0

    rank = q * total_count

    # Find the bucket containing the quantile rank
    prev_le = 0.0
    prev_count = 0.0

    for le, count in buckets:
        if count >= rank:
            if math.isinf(le):
                return prev_le
            # Linear interpolation inside bucket
            bucket_count = count - prev_count
            if bucket_count <= 0:
                return prev_le
            fraction = (rank - prev_count) / bucket_count
            return prev_le + fraction * (le - prev_le)
        prev_le = le
        prev_count = count

    return buckets[-1][0] if not math.isinf(buckets[-1][0]) else prev_le


def query_prometheus_api(prom_url, query):
    url = f"{prom_url}/api/v1/query?" + urllib.parse.urlencode({"query": query})
    status, body = fetch_url(url, timeout=3.0)
    if status == 200:
        try:
            data = json.loads(body)
            if data.get("status") == "success":
                return data.get("data", {}).get("result", [])
        except Exception:
            pass
    return None


def run_evaluation(api_url, prom_url, generate_traffic, duration, concurrency, num_keys):
    print(f"{BOLD}=========================================================================================={RESET}")
    print(f"{BOLD}           KV-STORE PROMETHEUS PERFORMANCE EVALUATION DASHBOARD                           {RESET}")
    print(f"{BOLD}=========================================================================================={RESET}")

    if generate_traffic:
        generate_load(api_url, duration, concurrency, num_keys)
        time.sleep(1.0)  # brief pause for metrics collection

    # 1. Fetch metrics from API /metrics endpoint
    print(f"\n{BOLD}[1/2] Scraping Prometheus Metrics from API Gateway ({api_url}/metrics)...{RESET}")
    code, raw_metrics = fetch_url(f"{api_url}/metrics")
    if code != 200:
        print(f"{RED}Error: Failed to reach API metrics at {api_url}/metrics (HTTP {code}).{RESET}")
        print("Ensure the Go API service is running (e.g. via 'docker compose up' or local binary).")
        sys.exit(1)

    counters, histograms = parse_prometheus_text(raw_metrics)

    # 2. Check Prometheus Server connectivity
    print(f"{BOLD}[2/2] Checking Prometheus Server Status ({prom_url})...{RESET}")
    prom_available = False
    prom_p99 = None
    if prom_url:
        p99_result = query_prometheus_api(
            prom_url,
            'histogram_quantile(0.99, sum(rate(kvraft_api_request_duration_seconds_bucket[1m])) by (le))',
        )
        if p99_result is not None:
            prom_available = True
            print(f"    {GREEN}Connected to Prometheus server successfully.{RESET}")
            if p99_result and len(p99_result) > 0:
                try:
                    prom_p99 = float(p99_result[0]["value"][1])
                except (IndexError, ValueError, KeyError):
                    pass
        else:
            print(f"    {YELLOW}Prometheus server at {prom_url} not reachable; evaluating using direct metrics stream.{RESET}")

    # 3. Aggregate Stats
    total_requests = 0
    status_counts = {"2xx": 0, "4xx": 0, "5xx": 0, "other": 0}
    route_stats = {}

    for (method, route, status_code), count in counters.items():
        total_requests += int(count)
        sc = int(status_code)
        if 200 <= sc < 300:
            status_counts["2xx"] += int(count)
        elif 400 <= sc < 500:
            status_counts["4xx"] += int(count)
        elif 500 <= sc < 600:
            status_counts["5xx"] += int(count)
        else:
            status_counts["other"] += int(count)

        r_key = f"{method} {route}"
        route_stats[r_key] = route_stats.get(r_key, 0) + int(count)

    # Global histogram calculation (sum all buckets across routes)
    combined_buckets_map = {}
    total_duration_sum = 0.0
    total_duration_count = 0.0

    for route_key, hist in histograms.items():
        total_duration_sum += hist["sum"]
        total_duration_count += hist["count"]
        for le, cnt in hist["buckets"]:
            combined_buckets_map[le] = combined_buckets_map.get(le, 0.0) + cnt

    combined_buckets = sorted(combined_buckets_map.items(), key=lambda x: x[0])

    p50_ms = calculate_quantile_from_buckets(combined_buckets, 0.50) * 1000.0
    p90_ms = calculate_quantile_from_buckets(combined_buckets, 0.90) * 1000.0
    p95_ms = calculate_quantile_from_buckets(combined_buckets, 0.95) * 1000.0
    p99_ms = calculate_quantile_from_buckets(combined_buckets, 0.99) * 1000.0
    avg_ms = (total_duration_sum / total_duration_count * 1000.0) if total_duration_count > 0 else 0.0

    # 4. Display Report
    success_rate = (status_counts["2xx"] / total_requests * 100.0) if total_requests > 0 else 0.0

    print(f"\n{BOLD}=========================================================================================={RESET}")
    print(f"{BOLD}                        PROMETHEUS PERFORMANCE REPORT                                     {RESET}")
    print(f"{BOLD}=========================================================================================={RESET}")
    print(f"  Total Requests Recorded:  {BOLD}{total_requests:,}{RESET}")
    print(f"  Success Rate (HTTP 2xx):  {BOLD}{GREEN}{success_rate:.2f}%{RESET} ({status_counts['2xx']:,} ok)")
    print(f"  Client Errors (HTTP 4xx): {YELLOW}{status_counts['4xx']:,}{RESET}")
    print(f"  Server Errors (HTTP 5xx): {RED}{status_counts['5xx']:,}{RESET}")
    print(f"  Average Request Latency:  {BOLD}{avg_ms:.2f} ms{RESET}")
    print("------------------------------------------------------------------------------------------")
    print(f"  {BOLD}Prometheus Histogram Latency Quantiles:{RESET}")
    print(f"    p50 (Median) Latency:   {BOLD}{p50_ms:.2f} ms{RESET}")
    print(f"    p90 Latency:            {BOLD}{p90_ms:.2f} ms{RESET}")
    print(f"    p95 Latency:            {BOLD}{p95_ms:.2f} ms{RESET}")
    print(f"    p99 Latency:            {BOLD}{p99_ms:.2f} ms{RESET}")
    if prom_p99 is not None:
        print(f"    Prometheus Server p99:  {BOLD}{prom_p99 * 1000.0:.2f} ms{RESET}")
    print("------------------------------------------------------------------------------------------")
    print(f"  {BOLD}Traffic Breakdown by Route & Method:{RESET}")
    print(f"  {BOLD}{'Route / Endpoint':<24} | {'Total Requests':<16} | {'Traffic Share':<14}{RESET}")
    print("  ----------------------------------------------------------------------------------------")
    for r_key, cnt in sorted(route_stats.items(), key=lambda x: x[1], reverse=True):
        share = (cnt / total_requests * 100.0) if total_requests > 0 else 0.0
        print(f"  {r_key:<24} | {cnt:<16,} | {share:.1f}%")

    print(f"{BOLD}=========================================================================================={RESET}\n")


def main():
    parser = argparse.ArgumentParser(description="Evaluate KV-Store performance using Prometheus metrics")
    parser.add_argument("--api-url", default="http://localhost:8080", help="API Gateway URL")
    parser.add_argument("--prom-url", default="http://localhost:9090", help="Prometheus server URL")
    parser.add_argument("--duration", type=int, default=5, help="Workload duration in seconds (default: 5)")
    parser.add_argument("--concurrency", type=int, default=4, help="Concurrent client workers (default: 4)")
    parser.add_argument("--keys", type=int, default=1000, help="Keyspace size (default: 1000)")
    parser.add_argument("--skip-load", action="store_true", help="Skip load generation and query current metrics")

    args = parser.parse_args()

    run_evaluation(
        api_url=args.api_url.rstrip("/"),
        prom_url=args.prom_url.rstrip("/"),
        generate_traffic=not args.skip_load,
        duration=args.duration,
        concurrency=args.concurrency,
        num_keys=args.keys,
    )


if __name__ == "__main__":
    main()
