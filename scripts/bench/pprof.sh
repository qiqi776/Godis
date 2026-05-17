#!/usr/bin/env bash
set -Eeuo pipefail

# shellcheck source=./common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

fct_run_pprof_tool() {
    if go tool pprof -h >/dev/null 2>&1; then
        go tool pprof "$@"
        return
    fi
    if command -v pprof >/dev/null 2>&1; then
        pprof "$@"
        return
    fi
    if [[ -x /tmp/mini-kv-tools/pprof ]]; then
        /tmp/mini-kv-tools/pprof "$@"
        return
    fi

    printf 'pprof not found. Install it with: env GOBIN=/tmp/mini-kv-tools GOPATH=/tmp/mini-kv-gopath go install github.com/google/pprof@latest\n' >&2
    return 127
}

fct_write_pprof_report() {
    local profile_path="$1"
    local output_path="$2"
    shift 2

    if ! fct_run_pprof_tool -top "$@" "${profile_path}" >"${output_path}"; then
        printf 'failed to analyze %s\n' "${profile_path}" >"${output_path}"
        return 1
    fi
}

fct_capture_node_pprof() {
    local profile="$1"
    local node_name="$2"
    local target_dir="$3"
    local seconds="$4"
    local base_url
    local node_dir

    fct_wait_for_port "$(fct_node_debug_port "${profile}" "${node_name}")"
    base_url=$(fct_node_debug_base_url "${profile}" "${node_name}")
    node_dir="${target_dir}/${node_name}"
    mkdir -p "${node_dir}"

    curl -fsS "${base_url}/debug/pprof/profile?seconds=${seconds}" >"${node_dir}/cpu.pprof"
    curl -fsS "${base_url}/debug/pprof/heap" >"${node_dir}/heap.pprof"
    curl -fsS "${base_url}/debug/pprof/goroutine?debug=2" >"${node_dir}/goroutine.txt"

    fct_write_pprof_report "${node_dir}/cpu.pprof" "${node_dir}/cpu-top.txt" || true
    fct_write_pprof_report "${node_dir}/cpu.pprof" "${node_dir}/cpu-top-cum.txt" -cum || true
    fct_write_pprof_report "${node_dir}/heap.pprof" "${node_dir}/heap-alloc-objects-top.txt" -alloc_objects || true
    fct_write_pprof_report "${node_dir}/heap.pprof" "${node_dir}/heap-alloc-space-top.txt" -alloc_space || true
}

fct_execute_this() {
    local profile="${1:-steady}"
    local target_dir="${2:-}"
    local seconds="${3:-0}"
    shift $(( $# >= 3 ? 3 : $# ))

    fct_require_profile "${profile}"
    if [[ -z "${target_dir}" ]]; then
        printf 'usage: %s <profile> <target-dir> <seconds> [node...]\n' "${0}" >&2
        return 1
    fi
    if ! [[ "${seconds}" =~ ^[0-9]+$ ]] || (( seconds <= 0 )); then
        printf 'seconds must be a positive integer: %s\n' "${seconds}" >&2
        return 1
    fi

    local nodes=("$@")
    if ((${#nodes[@]} == 0)); then
        nodes=("${BENCH_NODES[@]}")
    fi

    local pids=()
    local node_name
    for node_name in "${nodes[@]}"; do
        fct_capture_node_pprof "${profile}" "${node_name}" "${target_dir}" "${seconds}" &
        pids+=("$!")
    done

    local status=0
    local pid
    for pid in "${pids[@]}"; do
        if ! wait "${pid}"; then
            status=1
        fi
    done

    printf 'pprof captured under %s\n' "${target_dir}"
    return "${status}"
}

fct_execute_this "${@}"
