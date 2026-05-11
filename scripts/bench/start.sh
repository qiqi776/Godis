#!/usr/bin/env bash
set -Eeuo pipefail

# shellcheck source=./common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

fct_execute_this() {
    local profile="${1:-steady}"
    fct_require_profile "${profile}"
    fct_build_bench_binaries

    local node_name
    for node_name in "${BENCH_NODES[@]}"; do
        fct_start_node "${profile}" "${node_name}"
    done

    for node_name in "${BENCH_NODES[@]}"; do
        fct_wait_for_port "$(fct_node_port "${node_name}")"
    done

    printf 'bench cluster started with profile=%s endpoints=%s\n' "${profile}" "${BENCH_ENDPOINTS}"
}

fct_execute_this "${@}"
