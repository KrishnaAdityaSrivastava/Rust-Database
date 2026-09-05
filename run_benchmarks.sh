#!/usr/bin/env bash
set -e

echo "=========================================================================="
echo " Starting Unified Stress Test, Correctness & Benchmark Suite (1, 3, 5 Nodes)"
echo "=========================================================================="
echo ""

export RUSTFLAGS="-C target-cpu=native"

sudo env HOME="$HOME" RUSTUP_HOME="$RUSTUP_HOME" CARGO_HOME="$CARGO_HOME" \
    nice -n -20 \
    rustup run stable cargo test --test unified_suite --release -- --nocapture