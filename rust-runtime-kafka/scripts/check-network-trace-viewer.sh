#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=scripts/check-common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/check-common.sh"

require_deno "network trace viewer"

network_test="network::test_support::tests::focused_directional_flow_trace_replays_and_covers_contract_edges"
generator_test="tests::generated_network_trace_is_deterministic_and_covers_contract_edges"
require_registered_test "${network_test}" "network scenario test" \
    -p kr-runtime-io --lib --features test-support
require_registered_test "${generator_test}" "network artifact test" \
    -p kr-runtime-trace-tool --example generate_network_trace

cargo test -p kr-runtime-io --lib --features test-support \
    --locked "${network_test}" \
    -- --exact
cargo test -p kr-runtime-trace-tool --example generate_network_trace \
    --locked "${generator_test}" \
    -- --exact

viewer_js=(
    "${TRACE_VIEWER_SHARED_JS[@]}"
    tools/trace-tool/network-trace-model.js
    tools/trace-tool/network-trace-model.test.js
    tools/trace-tool/network-trace-viewer.js
)
deno fmt --check \
    "${viewer_js[@]}" \
    tools/trace-tool/network-trace.html \
    tools/trace-tool/network-trace.css
deno lint --no-config "${viewer_js[@]}"
deno check --no-config "${viewer_js[@]}"
deno test --no-config \
    --allow-read=tools/trace-tool/network-trace-data.js,tools/trace-tool/network-trace.html,tools/trace-tool/network-trace-viewer.js,tools/trace-tool/trace-viewer-ui.js,tools/trace-tool/trace-viewer.css \
    tools/trace-tool/trace-viewer-core.test.js \
    tools/trace-tool/trace-viewer-ui.test.js \
    tools/trace-tool/network-trace-model.test.js
