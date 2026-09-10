#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -gt 1 || ("$#" -eq 1 && "$1" != "--check") ]]; then
    echo "usage: $0 [--check]" >&2
    exit 2
fi
readonly CHECK_ONLY="${1:-}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly ROOT
# shellcheck source=scripts/sbe-codegen-common.sh
source "${ROOT}/scripts/sbe-codegen-common.sh"

readonly SCHEMA="${ROOT}/trace/kr-runtime-trace-wire/schema/dst-trace.xml"
readonly OUTPUT="${ROOT}/trace/kr-runtime-trace-wire/src"
readonly IR_MODULE="${ROOT}/tools/trace-tool/dst-trace-sbe-ir.js"

sbe_regenerate_rust_and_ir \
    "${CHECK_ONLY}" \
    "${SCHEMA}" \
    dst_trace_sbe \
    DstTraceSbeIr \
    "${OUTPUT}" \
    "${IR_MODULE}" \
    "runtime trace"
