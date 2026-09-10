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

readonly CODEC_ROOT="${ROOT}/trace/kr-runtime-ring-trace-wire"
readonly SCHEMA="${CODEC_ROOT}/schema/storage-trace.xml"
readonly OUTPUT="${CODEC_ROOT}/src"
readonly IR_MODULE="${ROOT}/tools/trace-tool/storage-trace-sbe-ir.js"

sbe_regenerate_rust_and_ir \
    "${CHECK_ONLY}" \
    "${SCHEMA}" \
    storage_trace_sbe \
    StorageTraceSbeIr \
    "${OUTPUT}" \
    "${IR_MODULE}" \
    "storage trace"
