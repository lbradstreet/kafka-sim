#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=scripts/check-common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/check-common.sh"

require_deno "ring trace viewer"

ring_test="file::tests::trim_checkpoint_releases_space_and_recovery_crosses_implicit_wrap"
generator_test="tests::generated_ring_trace_is_deterministic_and_contains_the_wrap"
require_registered_test "${ring_test}" "ring scenario test" \
    -p kr-runtime-ring --lib
require_registered_test "${generator_test}" "ring artifact test" \
    -p kr-runtime-trace-tool --example generate_ring_trace

cargo test -p kr-runtime-ring --lib \
    --locked "${ring_test}" \
    -- --exact
cargo test -p kr-runtime-trace-tool --example generate_ring_trace \
    --locked "${generator_test}" \
    -- --exact

viewer_js=(
    "${TRACE_VIEWER_SHARED_JS[@]}"
    tools/trace-tool/ring-trace-model.js
    tools/trace-tool/ring-trace-model.test.js
    tools/trace-tool/ring-trace-viewer.js
)
deno fmt --check "${viewer_js[@]}"
deno lint --no-config "${viewer_js[@]}"
deno check --no-config "${viewer_js[@]}"
deno test --no-config \
    --allow-read=tools/trace-tool/ring-trace-data.js,tools/trace-tool/ring-trace.html,tools/trace-tool/trace-viewer-ui.js,tools/trace-tool/ring-trace-viewer.js,tools/trace-tool/trace-viewer.css \
    tools/trace-tool/trace-viewer-core.test.js \
    tools/trace-tool/trace-viewer-ui.test.js \
    tools/trace-tool/ring-trace-model.test.js
