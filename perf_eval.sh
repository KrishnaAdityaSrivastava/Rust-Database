#!/usr/bin/env bash
set -e

BOLD='\033[1m'
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

show_usage() {
    echo -e "${BOLD}Usage:${NC} $0 [local|prometheus|all] [options]"
    echo ""
    echo -e "  ${BOLD}local${NC}       - Run local LSM engine and Raft cluster benchmarks via Rust benchmark binary"
    echo -e "  ${BOLD}prometheus${NC}  - Run HTTP workload and evaluate live performance metrics from Prometheus"
    echo -e "  ${BOLD}all${NC}         - Run both local benchmarks and Prometheus performance evaluation"
    echo ""
    echo -e "${BOLD}Examples:${NC}"
    echo "  $0 local"
    echo "  $0 prometheus --api-url http://localhost:8080 --duration 10"
    echo "  $0 all"
    exit 0
}

MODE="${1:-local}"
shift || true

case "$MODE" in
    local|bench)
        echo -e "${BOLD}${CYAN}>>> Executing Local Performance Evaluation Benchmark...${NC}"
        cargo run --release --bin bench
        ;;
    prometheus|prom)
        echo -e "${BOLD}${CYAN}>>> Executing Prometheus-based Performance Evaluation...${NC}"
        python3 scripts/prometheus_eval.py "$@"
        ;;
    all)
        echo -e "${BOLD}${CYAN}>>> Executing Full Performance Evaluation (Local + Prometheus)...${NC}"
        cargo run --release --bin bench
        echo ""
        python3 scripts/prometheus_eval.py "$@"
        ;;
    -h|--help|help)
        show_usage
        ;;
    *)
        echo -e "${RED}Unknown mode: '$MODE'${NC}"
        show_usage
        ;;
esac
