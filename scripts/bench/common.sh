#!/usr/bin/env bash
set -Eeuo pipefail

readonly SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly ROOT_DIR=$(cd -- "${SCRIPT_DIR}/../.." && pwd)
readonly BENCH_TMP_DIR="${ROOT_DIR}/tmp/bench"
readonly BENCH_BIN_DIR="${BENCH_TMP_DIR}/bin"
readonly BENCH_PID_DIR="${BENCH_TMP_DIR}/pids"
readonly BENCH_LOG_DIR="${BENCH_TMP_DIR}/logs"
readonly BENCH_RESULT_DIR="${BENCH_TMP_DIR}/results"
readonly BENCH_ENDPOINTS="127.0.0.1:6380,127.0.0.1:6381,127.0.0.1:6382"
readonly BENCH_NODES=(node1 node2 node3)
readonly BENCH_MATRIX_DIR="${SCRIPT_DIR}/matrix"

fct_require_profile() {
    local profile="${1:-}"
    case "${profile}" in
        steady|snapshot-stress)
            ;;
        *)
            printf 'Unsupported bench profile: %s\n' "${profile}" >&2
            return 1
            ;;
    esac
}

fct_profile_config_dir() {
    local profile="$1"
    printf '%s/configs/bench/%s\n' "${ROOT_DIR}" "${profile}"
}

fct_runtime_log_dir() {
    local profile="$1"
    printf '%s/%s\n' "${BENCH_LOG_DIR}" "${profile}"
}

fct_node_pid_path() {
    local node_name="$1"
    printf '%s/%s.pid\n' "${BENCH_PID_DIR}" "${node_name}"
}

fct_node_log_path() {
    local profile="$1"
    local node_name="$2"
    local log_dir
    log_dir=$(fct_runtime_log_dir "${profile}")
    printf '%s/%s.log\n' "${log_dir}" "${node_name}"
}

fct_node_port() {
    local node_name="$1"
    case "${node_name}" in
        node1) printf '6380\n' ;;
        node2) printf '6381\n' ;;
        node3) printf '6382\n' ;;
        *)
            printf 'unknown node: %s\n' "${node_name}" >&2
            return 1
            ;;
    esac
}

fct_node_debug_port() {
    local profile="$1"
    local node_name="$2"
    case "${profile}:${node_name}" in
        steady:node1) printf '17080\n' ;;
        steady:node2) printf '17081\n' ;;
        steady:node3) printf '17082\n' ;;
        snapshot-stress:node1) printf '17180\n' ;;
        snapshot-stress:node2) printf '17181\n' ;;
        snapshot-stress:node3) printf '17182\n' ;;
        *)
            printf 'unknown debug port for %s %s\n' "${profile}" "${node_name}" >&2
            return 1
            ;;
    esac
}

fct_node_raft_port() {
    local node_name="$1"
    case "${node_name}" in
        node1) printf '16380\n' ;;
        node2) printf '16381\n' ;;
        node3) printf '16382\n' ;;
        *)
            printf 'unknown node: %s\n' "${node_name}" >&2
            return 1
            ;;
    esac
}

fct_node_all_ports() {
    local node_name="$1"
    case "${node_name}" in
        node1) printf '6380\n16380\n17080\n17180\n' ;;
        node2) printf '6381\n16381\n17081\n17181\n' ;;
        node3) printf '6382\n16382\n17082\n17182\n' ;;
        *)
            printf 'unknown node: %s\n' "${node_name}" >&2
            return 1
            ;;
    esac
}

fct_node_debug_base_url() {
    local profile="$1"
    local node_name="$2"
    printf 'http://127.0.0.1:%s\n' "$(fct_node_debug_port "${profile}" "${node_name}")"
}

fct_wait_for_port() {
    local port="$1"
    local attempts=0
    while (( attempts < 40 )); do
        if (echo >"/dev/tcp/127.0.0.1/${port}") >/dev/null 2>&1; then
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 0.5
    done
    printf 'Timed out waiting for port %s\n' "${port}" >&2
    return 1
}

fct_wait_for_port_closed() {
    local port="$1"
    local attempts=0
    while (( attempts < 40 )); do
        if ! (echo >"/dev/tcp/127.0.0.1/${port}") >/dev/null 2>&1; then
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 0.25
    done
    printf 'Timed out waiting for port %s to close\n' "${port}" >&2
    return 1
}

fct_kill_node_ports() {
    local node_name="$1"
    local port
    while IFS= read -r port; do
        fuser -k "${port}/tcp" >/dev/null 2>&1 || true
    done < <(fct_node_all_ports "${node_name}")
}

fct_build_bench_binaries() {
    mkdir -p "${BENCH_BIN_DIR}"
    (
        cd "${ROOT_DIR}"
        go build -o "${BENCH_BIN_DIR}/mini-kv" ./cmd/mini-kv
        go build -o "${BENCH_BIN_DIR}/mini-kv-bench" ./cmd/mini-kv-bench
        go build -o "${BENCH_BIN_DIR}/mini-kv-bench-report" ./cmd/mini-kv-bench-report
    )
}

fct_start_node() {
    local profile="$1"
    local node_name="$2"
    local config_dir
    local config_path
    local log_path
    local pid_path

    config_dir=$(fct_profile_config_dir "${profile}")
    config_path="${config_dir}/${node_name}.yaml"
    log_path=$(fct_node_log_path "${profile}" "${node_name}")
    pid_path=$(fct_node_pid_path "${node_name}")

    mkdir -p "$(dirname "${log_path}")" "${BENCH_PID_DIR}"

    if [[ -f "${pid_path}" ]]; then
        local existing_pid
        existing_pid=$(<"${pid_path}")
        if kill -0 "${existing_pid}" 2>/dev/null; then
            printf '%s already running with pid %s\n' "${node_name}" "${existing_pid}"
            return 0
        fi
        rm -f "${pid_path}"
    fi

    (
        cd "${ROOT_DIR}"
        MINIKV_CONFIG="${config_path}" "${BENCH_BIN_DIR}/mini-kv" >"${log_path}" 2>&1
    ) &

    printf '%s' "$!" >"${pid_path}"
    printf 'started %s with pid %s\n' "${node_name}" "$!"
}

fct_stop_node() {
    local node_name="$1"
    local pid_path
    pid_path=$(fct_node_pid_path "${node_name}")
    if [[ ! -f "${pid_path}" ]]; then
        return 0
    fi

    local pid
    pid=$(<"${pid_path}")
    if kill -0 "${pid}" 2>/dev/null; then
        kill "${pid}"
        local attempts=0
        while kill -0 "${pid}" 2>/dev/null; do
            if (( attempts >= 20 )); then
                kill -9 "${pid}" 2>/dev/null || true
                break
            fi
            attempts=$((attempts + 1))
            sleep 0.5
        done
        wait "${pid}" 2>/dev/null || true
    fi
    rm -f "${pid_path}"
    fct_kill_node_ports "${node_name}"
}

fct_kill_bench_ports() {
    local ports=(
        6380 6381 6382
        16380 16381 16382
        17080 17081 17082
        17180 17181 17182
    )
    local port
    for port in "${ports[@]}"; do
        fuser -k "${port}/tcp" >/dev/null 2>&1 || true
    done
}

fct_fetch_node_vars() {
    local profile="$1"
    local node_name="$2"
    local base_url
    base_url=$(fct_node_debug_base_url "${profile}" "${node_name}")
    curl -fsS "${base_url}/debug/vars"
}

fct_node_json_string_value() {
    local profile="$1"
    local node_name="$2"
    local section="$3"
    local key="$4"
    fct_fetch_node_vars "${profile}" "${node_name}" | awk -v section="${section}" -v key="${key}" '
        $0 ~ "\"" section "\": \\{" { in_section=1; next }
        in_section && $0 ~ /^  },?$/ { in_section=0 }
        in_section && $0 ~ "\"" key "\":" {
            if (match($0, /"[^"]+": "([^"]*)"/, captures)) {
                print captures[1]
                exit
            }
        }
    '
}

fct_node_json_uint_value() {
    local profile="$1"
    local node_name="$2"
    local section="$3"
    local key="$4"
    fct_fetch_node_vars "${profile}" "${node_name}" | awk -v section="${section}" -v key="${key}" '
        $0 ~ "\"" section "\": \\{" { in_section=1; next }
        in_section && $0 ~ /^  },?$/ { in_section=0 }
        in_section && $0 ~ "\"" key "\":" {
            value=$0
            sub(/^[^:]*:/, "", value)
            gsub(/[^0-9]/, "", value)
            print value
            exit
        }
    '
}

fct_current_leader() {
    local profile="$1"
    local node_name
    for node_name in "${BENCH_NODES[@]}"; do
        if [[ "$(fct_node_json_string_value "${profile}" "${node_name}" "raft_state" "${node_name}" 2>/dev/null || true)" == "leader" ]]; then
            printf '%s\n' "${node_name}"
            return 0
        fi
    done
    return 1
}

fct_pick_follower() {
    local leader_node="$1"
    local node_name
    for node_name in "${BENCH_NODES[@]}"; do
        if [[ "${node_name}" != "${leader_node}" ]]; then
            printf '%s\n' "${node_name}"
            return 0
        fi
    done
    return 1
}

fct_wait_for_leader() {
    local profile="$1"
    local timeout_seconds="$2"
    local deadline=$((SECONDS + timeout_seconds))
    while (( SECONDS < deadline )); do
        local leader
        leader=$(fct_current_leader "${profile}" 2>/dev/null || true)
        if [[ -n "${leader}" ]]; then
            printf '%s\n' "${leader}"
            return 0
        fi
        sleep 0.5
    done
    printf 'timed out waiting for leader in profile %s\n' "${profile}" >&2
    return 1
}

fct_wait_for_new_leader() {
    local profile="$1"
    local old_leader="$2"
    local timeout_seconds="$3"
    local deadline=$((SECONDS + timeout_seconds))
    while (( SECONDS < deadline )); do
        local leader
        leader=$(fct_current_leader "${profile}" 2>/dev/null || true)
        if [[ -n "${leader}" && "${leader}" != "${old_leader}" ]]; then
            printf '%s\n' "${leader}"
            return 0
        fi
        sleep 0.5
    done
    printf 'timed out waiting for a new leader in profile %s\n' "${profile}" >&2
    return 1
}

fct_node_commit_index() {
    local profile="$1"
    local node_name="$2"
    fct_node_json_uint_value "${profile}" "${node_name}" "commit_index" "${node_name}"
}

fct_node_applied_index() {
    local profile="$1"
    local node_name="$2"
    fct_node_json_uint_value "${profile}" "${node_name}" "applied_index" "${node_name}"
}

fct_wait_for_applied_at_least() {
    local profile="$1"
    local node_name="$2"
    local target_index="$3"
    local timeout_seconds="$4"
    local deadline=$((SECONDS + timeout_seconds))
    while (( SECONDS < deadline )); do
        local applied
        applied=$(fct_node_applied_index "${profile}" "${node_name}" 2>/dev/null || true)
        if [[ -n "${applied}" ]] && (( applied >= target_index )); then
            printf '%s\n' "${applied}"
            return 0
        fi
        sleep 0.5
    done
    printf 'timed out waiting for %s applied index >= %s in profile %s\n' "${node_name}" "${target_index}" "${profile}" >&2
    return 1
}
