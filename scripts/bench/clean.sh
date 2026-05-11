#!/usr/bin/env bash
set -Eeuo pipefail

# shellcheck source=./common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

fct_execute_this() {
    "${SCRIPT_DIR}/stop.sh"
    rm -rf \
        "${ROOT_DIR}/data/bench" \
        "${BENCH_BIN_DIR}" \
        "${BENCH_PID_DIR}" \
        "${BENCH_LOG_DIR}"
    printf 'bench data cleaned\n'
}

fct_execute_this "${@}"
