#!/usr/bin/env bash
set -Eeuo pipefail

# shellcheck source=./common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

fct_run_bench_case() {
    local report_dir="$1"
    local label="$2"
    local mode="$3"
    local routing="$4"
    local concurrency="$5"
    local duration="$6"
    local warmup="$7"
    local keyspace="$8"
    local value_size="$9"
    local preload_keys="${10}"
    local read_percent="${11}"
    local write_percent="${12}"
    local delete_percent="${13}"
    local seed="${14}"
    shift 14

    local report_path="${report_dir}/result.json"
    mkdir -p "${report_dir}"

    local cmd=(
        "${BENCH_BIN_DIR}/mini-kv-bench"
        -label "${label}"
        -endpoints "${BENCH_ENDPOINTS}"
        -routing "${routing}"
        -mode "${mode}"
        -duration "${duration}"
        -warmup "${warmup}"
        -concurrency "${concurrency}"
        -keyspace "${keyspace}"
        -value-size "${value_size}"
        -report "${report_path}"
        -seed "${seed}"
    )
    if [[ -n "${preload_keys}" ]]; then
        cmd+=(-preload-keys "${preload_keys}")
    fi
    if [[ "${mode}" == "mixed" ]]; then
        cmd+=(
            -read-percent "${read_percent}"
            -write-percent "${write_percent}"
            -delete-percent "${delete_percent}"
        )
    fi
    if (($# > 0)); then
        cmd+=("$@")
    fi

    printf '\n=== Running case: %s ===\n' "${label}"
    (
        cd "${ROOT_DIR}"
        "${cmd[@]}"
    )
}

fct_run_matrix() {
    local matrix_file="$1"
    local result_dir="$2"
    shift 2

    if [[ ! -f "${matrix_file}" ]]; then
        printf 'matrix file not found: %s\n' "${matrix_file}" >&2
        return 1
    fi

    local label
    local mode
    local routing
    local concurrency
    local duration
    local warmup
    local keyspace
    local value_size
    local preload_keys
    local read_percent
    local write_percent
    local delete_percent
    local seed

    while IFS=',' read -r \
        label mode routing concurrency duration warmup keyspace value_size preload_keys \
        read_percent write_percent delete_percent seed; do
        if [[ -z "${label}" || "${label}" == "label" || "${label}" == \#* ]]; then
            continue
        fi
        fct_run_bench_case \
            "${result_dir}/cases/${label}" \
            "${label}" \
            "${mode}" \
            "${routing}" \
            "${concurrency}" \
            "${duration}" \
            "${warmup}" \
            "${keyspace}" \
            "${value_size}" \
            "${preload_keys}" \
            "${read_percent}" \
            "${write_percent}" \
            "${delete_percent}" \
            "${seed}" \
            "$@"
    done < "${matrix_file}"
}

fct_resolve_matrix_file() {
    local matrix_name="$1"
    if [[ -f "${matrix_name}" ]]; then
        printf '%s\n' "${matrix_name}"
        return 0
    fi
    printf '%s/%s.csv\n' "${BENCH_MATRIX_DIR}" "${matrix_name}"
}

fct_write_summary() {
    local result_dir="$1"
    "${BENCH_BIN_DIR}/mini-kv-bench-report" \
        -input-dir "${result_dir}" \
        -markdown "${result_dir}/summary.md" \
        -json "${result_dir}/summary.json"
}

fct_execute_this() {
    local profile="steady"
    local pprof_seconds=0
    local matrix_name=""
    local matrix_file=""

    if (($# > 0)) && [[ "${1}" != -* ]]; then
        profile="$1"
        shift
    fi
    fct_require_profile "${profile}"

    while (($# > 0)); do
        case "${1}" in
            --pprof-seconds)
                pprof_seconds="$2"
                shift 2
                ;;
            --matrix)
                matrix_name="$2"
                shift 2
                ;;
            --matrix-file)
                matrix_file="$2"
                shift 2
                ;;
            *)
                break
                ;;
        esac
    done

    if [[ -n "${matrix_name}" && -n "${matrix_file}" ]]; then
        printf 'use either --matrix or --matrix-file, not both\n' >&2
        return 1
    fi
    if [[ -n "${matrix_name}" ]]; then
        matrix_file=$(fct_resolve_matrix_file "${matrix_name}")
    fi

    local timestamp
    local result_dir
    timestamp=$(date +%Y%m%d-%H%M%S)
    result_dir="${BENCH_RESULT_DIR}/${profile}/${timestamp}"

    trap '"${SCRIPT_DIR}/stop.sh"' EXIT

    "${SCRIPT_DIR}/clean.sh"
    "${SCRIPT_DIR}/start.sh" "${profile}"
    mkdir -p "${result_dir}"

    if [[ -n "${matrix_file}" ]]; then
        fct_run_matrix "${matrix_file}" "${result_dir}" "$@"
    else
        fct_run_bench_case \
            "${result_dir}" \
            "single" \
            "mixed" \
            "leader" \
            "32" \
            "30s" \
            "5s" \
            "1024" \
            "256" \
            "-1" \
            "70" \
            "30" \
            "0" \
            "1" \
            "$@"
    fi

    "${SCRIPT_DIR}/capture.sh" "${profile}" "${result_dir}/observability" "${pprof_seconds}"
    fct_write_summary "${result_dir}"

    printf 'bench result written under %s\n' "${result_dir}"
}

fct_execute_this "${@}"
