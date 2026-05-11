#!/usr/bin/env bash
set -Eeuo pipefail

# shellcheck source=./common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

fct_capture_node() {
    local profile="$1"
    local node_name="$2"
    local target_dir="$3"
    local pprof_seconds="$4"
    local debug_port
    local base_url

    debug_port=$(fct_node_debug_port "${profile}" "${node_name}")
    base_url="http://127.0.0.1:${debug_port}"
    mkdir -p "${target_dir}/${node_name}"

    curl -fsS "${base_url}/metrics" >"${target_dir}/${node_name}/metrics.prom"
    curl -fsS "${base_url}/debug/vars" >"${target_dir}/${node_name}/vars.json"
    curl -fsS "${base_url}/debug/pprof/goroutine?debug=1" >"${target_dir}/${node_name}/goroutine.txt"
    curl -fsS "${base_url}/debug/pprof/heap?debug=1" >"${target_dir}/${node_name}/heap.txt"

    if (( pprof_seconds > 0 )); then
        curl -fsS "${base_url}/debug/pprof/profile?seconds=${pprof_seconds}" >"${target_dir}/${node_name}/cpu.pprof"
        curl -fsS "${base_url}/debug/pprof/trace?seconds=${pprof_seconds}" >"${target_dir}/${node_name}/trace.out"
    fi
}

fct_execute_this() {
    local profile="${1:-steady}"
    local target_dir="${2:-}"
    local pprof_seconds="${3:-0}"
    shift $(( $# >= 3 ? 3 : $# ))

    fct_require_profile "${profile}"
    if [[ -z "${target_dir}" ]]; then
        printf 'usage: %s <profile> <target-dir> [pprof-seconds]\n' "${0}" >&2
        return 1
    fi
    if ! [[ "${pprof_seconds}" =~ ^[0-9]+$ ]]; then
        printf 'pprof-seconds must be an integer: %s\n' "${pprof_seconds}" >&2
        return 1
    fi

    local nodes=("$@")
    if ((${#nodes[@]} == 0)); then
        nodes=("${BENCH_NODES[@]}")
    fi

    local node_name
    for node_name in "${nodes[@]}"; do
        fct_capture_node "${profile}" "${node_name}" "${target_dir}" "${pprof_seconds}"
    done

    printf 'observability captured under %s\n' "${target_dir}"
}

fct_execute_this "${@}"
