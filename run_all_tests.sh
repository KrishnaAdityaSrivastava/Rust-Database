#!/usr/bin/env bash
set -e

# ANSI Color Codes
BOLD='\033[1m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BOLD}${CYAN}==========================================================================================${NC}"
echo -e "${BOLD}${CYAN}                 KV-STORE AUTOMATED MASTER TEST & BENCHMARK SUITE                         ${NC}"
echo -e "${BOLD}${CYAN}==========================================================================================${NC}"
echo ""

export RUSTFLAGS="-C target-cpu=native"

PASS_COUNT=0
TOTAL_SUITES=4

run_suite() {
    local suite_name="$1"
    local test_target="$2"
    local extra_args="$3"

    echo -e "${BOLD}${YELLOW}[PHASE $((PASS_COUNT + 1))/$TOTAL_SUITES] Running ${suite_name}...${NC}"
    echo -e "Command: cargo test --test ${test_target} ${extra_args}"
    echo "------------------------------------------------------------------------------------------"
    
    if cargo test --test "${test_target}" ${extra_args}; then
        echo -e "${BOLD}${GREEN}✔ ${suite_name}: PASSED${NC}\n"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        echo -e "${BOLD}${RED}✘ ${suite_name}: FAILED${NC}\n"
        exit 1
    fi
}

# Phase 1: Storage Engine Unit Correctness
run_suite "LSM Storage Engine Correctness" "correctness" ""

# Phase 2: Raft Consensus Fault Tolerance & Invariants (13 tests)
run_suite "Raft Consensus Fault Tolerance" "raft_fault_tolerance" ""

# Phase 3: Tokio TCP Networking & Framing
run_suite "Tokio TCP Networking & Serialization" "raft_network" ""

# Phase 4: Unified Performance, Resource Footprint & Cluster Stress Suite
run_suite "Unified Performance & System Stress Suite" "unified_suite" "--release -- --nocapture"

echo -e "${BOLD}${CYAN}==========================================================================================${NC}"
echo -e "${BOLD}${GREEN}                        ALL ${PASS_COUNT}/${TOTAL_SUITES} TEST SUITES PASSED SUCCESSFULLY!                         ${NC}"
echo -e "${BOLD}${CYAN}==========================================================================================${NC}"
