#!/usr/bin/env bash
set -Eeuo pipefail

# shellcheck source=./common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

fct_write_fault_json() {
    local output_path="$1"
    local scenario="$2"
    local profile="$3"
    local target_node="$4"
    local leader_before="$5"
    local leader_after="$6"
    local status="$7"
    local inject_delay="$8"
    local outage="$9"
    local leader_commit="${10}"
    local follower_applied="${11}"
    local snapshot_create_count="${12}"
    local install_snapshot_count="${13}"
    local result_path="${14}"
    local note="${15:-}"

    cat >"${output_path}" <<EOF
{
  "scenario": "${scenario}",
  "profile": "${profile}",
  "target_node": "${target_node}",
  "leader_before": "${leader_before}",
  "leader_after": "${leader_after}",
  "status": "${status}",
  "inject_delay": "${inject_delay}",
  "outage": "${outage}",
  "leader_commit_index": ${leader_commit},
  "follower_applied_index": ${follower_applied},
  "snapshot_create_count": ${snapshot_create_count},
  "install_snapshot_count": ${install_snapshot_count},
  "result_path": "${result_path}",
  "note": "${note}"
}
EOF
}

fct_metric_count_from_file() {
    local metrics_path="$1"
    local operation="$2"
    awk -v operation="${operation}" '
        $1 ~ /^mini_kv_raft_operations_total/ && $0 ~ "operation=\"" operation "\"" && $0 ~ "status=\"ok\"" {
            split($2, parts, ".")
            print parts[1]
            exit
        }
    ' "${metrics_path}"
}

fct_run_background_bench() {
    local scenario_dir="$1"
    local label="$2"
    shift 2

    local report_path="${scenario_dir}/result.json"
    local stdout_path="${scenario_dir}/bench.stdout.txt"
    mkdir -p "${scenario_dir}"

    (
        cd "${ROOT_DIR}"
        "${BENCH_BIN_DIR}/mini-kv-bench" \
            -label "${label}" \
            -endpoints "${BENCH_ENDPOINTS}" \
            -routing leader \
            -report "${report_path}" \
            "$@" >"${stdout_path}" 2>&1
    ) &
    BENCH_LAST_PID="$!"
    BENCH_LAST_STDOUT="${stdout_path}"
}

fct_wait_bench() {
    local bench_pid="$1"
    local stdout_path="$2"
    if ! wait "${bench_pid}"; then
        cat "${stdout_path}" >&2
        return 1
    fi
}

fct_prepare_cluster() {
    local profile="$1"
    "${SCRIPT_DIR}/clean.sh"
    fct_build_bench_binaries

    local node_name
    for node_name in "${BENCH_NODES[@]}"; do
        fct_start_node "${profile}" "${node_name}"
    done
    for node_name in "${BENCH_NODES[@]}"; do
        fct_wait_for_port "$(fct_node_port "${node_name}")"
    done
}

fct_capture_and_write_summary() {
    local profile="$1"
    local scenario_dir="$2"
    local pprof_seconds="$3"

    "${SCRIPT_DIR}/capture.sh" "${profile}" "${scenario_dir}/observability" "${pprof_seconds}"
}

fct_write_summary() {
    local root_dir="$1"
    "${BENCH_BIN_DIR}/mini-kv-bench-report" \
        -input-dir "${root_dir}" \
        -markdown "${root_dir}/summary.md" \
        -json "${root_dir}/summary.json"
}

fct_run_leader_kill() {
    local profile="$1"
    local root_dir="$2"
    local duration="$3"
    local warmup="$4"
    local inject_delay="$5"
    local concurrency="$6"
    local keyspace="$7"
    local value_size="$8"
    local pprof_seconds="$9"
    local scenario_dir="${root_dir}/leader-kill"

    fct_prepare_cluster "${profile}"
    local leader_before
    leader_before=$(fct_wait_for_leader "${profile}" 15)

    fct_run_background_bench "${scenario_dir}" "leader-kill" \
        -mode mixed \
        -duration "${duration}" \
        -warmup "${warmup}" \
        -concurrency "${concurrency}" \
        -keyspace "${keyspace}" \
        -value-size "${value_size}" \
        -preload-keys -1 \
        -read-percent 70 \
        -write-percent 30 \
        -delete-percent 0

    sleep "${inject_delay}"
    fct_stop_node "${leader_before}"
    local leader_after
    leader_after=$(fct_wait_for_new_leader "${profile}" "${leader_before}" 15)

    fct_wait_bench "${BENCH_LAST_PID}" "${BENCH_LAST_STDOUT}"

    local current_leader
    current_leader=$(fct_wait_for_leader "${profile}" 15)
    local leader_commit
    leader_commit=$(fct_node_commit_index "${profile}" "${current_leader}")
    local survivor_node
    survivor_node=""
    local node_name
    for node_name in "${BENCH_NODES[@]}"; do
        if [[ "${node_name}" != "${leader_before}" && "${node_name}" != "${current_leader}" ]]; then
            survivor_node="${node_name}"
            break
        fi
    done

    if [[ -n "${survivor_node}" ]]; then
        "${SCRIPT_DIR}/capture.sh" "${profile}" "${scenario_dir}/observability" "${pprof_seconds}" "${current_leader}" "${survivor_node}"
    else
        "${SCRIPT_DIR}/capture.sh" "${profile}" "${scenario_dir}/observability" "${pprof_seconds}" "${current_leader}"
    fi
    fct_write_fault_json \
        "${scenario_dir}/fault.json" \
        "leader-kill" \
        "${profile}" \
        "${leader_before}" \
        "${leader_before}" \
        "${leader_after}" \
        "ok" \
        "${inject_delay}" \
        "0s" \
        "${leader_commit}" \
        "0" \
        "0" \
        "0" \
        "${scenario_dir}/result.json"
}

fct_run_follower_restart() {
    local profile="$1"
    local root_dir="$2"
    local duration="$3"
    local warmup="$4"
    local inject_delay="$5"
    local outage="$6"
    local concurrency="$7"
    local keyspace="$8"
    local value_size="$9"
    local pprof_seconds="${10}"
    local scenario_dir="${root_dir}/follower-restart"

    fct_prepare_cluster "${profile}"
    local leader_before
    leader_before=$(fct_wait_for_leader "${profile}" 15)
    local target_follower
    target_follower=$(fct_pick_follower "${leader_before}")

    fct_run_background_bench "${scenario_dir}" "follower-restart" \
        -mode set \
        -duration "${duration}" \
        -warmup "${warmup}" \
        -concurrency "${concurrency}" \
        -keyspace "${keyspace}" \
        -value-size "${value_size}" \
        -preload-keys 0

    sleep "${inject_delay}"
    fct_stop_node "${target_follower}"
    sleep "${outage}"
    fct_start_node "${profile}" "${target_follower}"
    fct_wait_for_port "$(fct_node_port "${target_follower}")"

    fct_wait_bench "${BENCH_LAST_PID}" "${BENCH_LAST_STDOUT}"

    local current_leader
    current_leader=$(fct_wait_for_leader "${profile}" 15)
    local leader_commit
    leader_commit=$(fct_node_commit_index "${profile}" "${current_leader}")
    local follower_applied
    follower_applied=$(fct_wait_for_applied_at_least "${profile}" "${target_follower}" "${leader_commit}" 30)

    fct_capture_and_write_summary "${profile}" "${scenario_dir}" "${pprof_seconds}"
    fct_write_fault_json \
        "${scenario_dir}/fault.json" \
        "follower-restart" \
        "${profile}" \
        "${target_follower}" \
        "${leader_before}" \
        "${current_leader}" \
        "ok" \
        "${inject_delay}" \
        "${outage}" \
        "${leader_commit}" \
        "${follower_applied}" \
        "0" \
        "0" \
        "${scenario_dir}/result.json"
}

fct_run_snapshot_catchup() {
    local profile="$1"
    local root_dir="$2"
    local inject_delay="$3"
    local outage="$4"
    local pprof_seconds="$5"
    local scenario_dir="${root_dir}/snapshot-catchup"

    fct_prepare_cluster "${profile}"
    local leader_before
    leader_before=$(fct_wait_for_leader "${profile}" 15)
    local target_follower
    target_follower=$(fct_pick_follower "${leader_before}")

    fct_run_background_bench "${scenario_dir}" "snapshot-catchup" \
        -mode set \
        -duration 20s \
        -warmup 1s \
        -concurrency 32 \
        -keyspace 4096 \
        -value-size 256 \
        -preload-keys 0

    sleep "${inject_delay}"
    fct_stop_node "${target_follower}"
    sleep "${outage}"
    fct_start_node "${profile}" "${target_follower}"
    fct_wait_for_port "$(fct_node_port "${target_follower}")"

    fct_wait_bench "${BENCH_LAST_PID}" "${BENCH_LAST_STDOUT}"

    local current_leader
    current_leader=$(fct_wait_for_leader "${profile}" 15)
    local leader_commit
    leader_commit=$(fct_node_commit_index "${profile}" "${current_leader}")
    local follower_applied
    follower_applied=$(fct_wait_for_applied_at_least "${profile}" "${target_follower}" "${leader_commit}" 45)

    fct_capture_and_write_summary "${profile}" "${scenario_dir}" "${pprof_seconds}"

    local metrics_path="${scenario_dir}/observability/${current_leader}/metrics.prom"
    local snapshot_create_count
    local install_snapshot_count
    snapshot_create_count=$(fct_metric_count_from_file "${metrics_path}" "snapshot_create")
    install_snapshot_count=$(fct_metric_count_from_file "${metrics_path}" "install_snapshot_rpc")
    snapshot_create_count="${snapshot_create_count:-0}"
    install_snapshot_count="${install_snapshot_count:-0}"

    local status="ok"
    local note=""
    if (( snapshot_create_count == 0 || install_snapshot_count == 0 )); then
        status="warn"
        note="snapshot or install_snapshot metric did not increment"
    fi

    fct_write_fault_json \
        "${scenario_dir}/fault.json" \
        "snapshot-catchup" \
        "${profile}" \
        "${target_follower}" \
        "${leader_before}" \
        "${current_leader}" \
        "${status}" \
        "${inject_delay}" \
        "${outage}" \
        "${leader_commit}" \
        "${follower_applied}" \
        "${snapshot_create_count}" \
        "${install_snapshot_count}" \
        "${scenario_dir}/result.json" \
        "${note}"
}

fct_execute_this() {
    local scenario="all"
    local profile="steady"
    local snapshot_profile="snapshot-stress"
    local duration="20s"
    local warmup="2s"
    local inject_delay="4s"
    local outage="6s"
    local concurrency="32"
    local keyspace="1024"
    local value_size="256"
    local pprof_seconds="0"

    if (($# > 0)) && [[ "${1}" != -* ]]; then
        scenario="$1"
        shift
    fi

    while (($# > 0)); do
        case "${1}" in
            --profile)
                profile="$2"
                shift 2
                ;;
            --snapshot-profile)
                snapshot_profile="$2"
                shift 2
                ;;
            --duration)
                duration="$2"
                shift 2
                ;;
            --warmup)
                warmup="$2"
                shift 2
                ;;
            --inject-delay)
                inject_delay="$2"
                shift 2
                ;;
            --outage)
                outage="$2"
                shift 2
                ;;
            --concurrency)
                concurrency="$2"
                shift 2
                ;;
            --keyspace)
                keyspace="$2"
                shift 2
                ;;
            --value-size)
                value_size="$2"
                shift 2
                ;;
            --pprof-seconds)
                pprof_seconds="$2"
                shift 2
                ;;
            *)
                printf 'unknown argument: %s\n' "${1}" >&2
                return 1
                ;;
        esac
    done

    fct_require_profile "${profile}"
    fct_require_profile "${snapshot_profile}"
    fct_build_bench_binaries

    local timestamp
    timestamp=$(date +%Y%m%d-%H%M%S)
    local root_dir="${BENCH_RESULT_DIR}/faults/${timestamp}"
    mkdir -p "${root_dir}"

    trap '"${SCRIPT_DIR}/stop.sh"' EXIT

    case "${scenario}" in
        leader-kill)
            fct_run_leader_kill "${profile}" "${root_dir}" "${duration}" "${warmup}" "${inject_delay}" "${concurrency}" "${keyspace}" "${value_size}" "${pprof_seconds}"
            ;;
        follower-restart)
            fct_run_follower_restart "${profile}" "${root_dir}" "${duration}" "${warmup}" "${inject_delay}" "${outage}" "${concurrency}" "${keyspace}" "${value_size}" "${pprof_seconds}"
            ;;
        snapshot-catchup)
            fct_run_snapshot_catchup "${snapshot_profile}" "${root_dir}" "${inject_delay}" "${outage}" "${pprof_seconds}"
            ;;
        all)
            fct_run_leader_kill "${profile}" "${root_dir}" "${duration}" "${warmup}" "${inject_delay}" "${concurrency}" "${keyspace}" "${value_size}" "${pprof_seconds}"
            fct_run_follower_restart "${profile}" "${root_dir}" "${duration}" "${warmup}" "${inject_delay}" "${outage}" "${concurrency}" "${keyspace}" "${value_size}" "${pprof_seconds}"
            fct_run_snapshot_catchup "${snapshot_profile}" "${root_dir}" "${inject_delay}" "${outage}" "${pprof_seconds}"
            ;;
        *)
            printf 'unsupported scenario: %s\n' "${scenario}" >&2
            return 1
            ;;
    esac

    fct_write_summary "${root_dir}"
    printf 'fault results written under %s\n' "${root_dir}"
}

fct_execute_this "${@}"
