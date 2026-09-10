#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=scripts/check-common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/check-common.sh"

require_deno "storage trace viewer"

./scripts/regenerate-storage-trace-sbe-codecs.sh --check
storage_test="storage::test_support::tests::focused_storage_durability_trace_replays_and_covers_contract_edges"
generator_test="tests::generated_storage_trace_is_deterministic_and_covers_milestones"
require_registered_test "${storage_test}" "storage scenario test" \
    -p kr-runtime-io --lib --features test-support
require_registered_test "${generator_test}" "storage artifact test" \
    -p kr-runtime-trace-tool --example generate_storage_trace

cargo test -p kr-runtime-io --lib --features test-support \
    --locked "${storage_test}" \
    -- --exact
cargo test -p kr-runtime-ring-trace-wire --locked
cargo test -p kr-runtime-trace-tool --example generate_storage_trace \
    --locked "${generator_test}" \
    -- --exact

viewer_js=(
    "${TRACE_VIEWER_SHARED_JS[@]}"
    tools/trace-tool/sbe-ir.js
    tools/trace-tool/storage-trace-model.js
    tools/trace-tool/storage-trace-model.test.js
    tools/trace-tool/storage-trace-viewer.js
)
deno fmt --check \
    "${viewer_js[@]}" \
    tools/trace-tool/storage-trace.html \
    tools/trace-tool/storage-trace.css
deno lint --no-config "${viewer_js[@]}"
deno check --no-config \
    "${viewer_js[@]}" \
    tools/trace-tool/storage-trace-sbe-ir.js \
    tools/trace-tool/storage-trace-data.js
deno test --no-config \
    --allow-read=tools/trace-tool/storage-trace-data.js,tools/trace-tool/storage-trace-model.js,tools/trace-tool/storage-trace.html,tools/trace-tool/storage-trace-viewer.js,tools/trace-tool/trace-viewer-ui.js,tools/trace-tool/trace-viewer.css,tools/trace-tool/storage-trace.css \
    tools/trace-tool/trace-viewer-core.test.js \
    tools/trace-tool/trace-viewer-ui.test.js \
    tools/trace-tool/storage-trace-model.test.js
