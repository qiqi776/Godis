#!/usr/bin/env bash
set -Eeuo pipefail

# shellcheck source=./common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

fct_execute_this() {
    mkdir -p "${BENCH_PID_DIR}"

    local node_name
    for node_name in "${BENCH_NODES[@]}"; do
        fct_stop_node "${node_name}"
    done
    fct_kill_bench_ports

    printf 'bench cluster stopped\n'
}

fct_execute_this "${@}"
