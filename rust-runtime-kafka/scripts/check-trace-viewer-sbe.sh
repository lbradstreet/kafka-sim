#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=scripts/check-common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/check-common.sh"

require_deno "browser SBE decoder"

./scripts/regenerate-trace-sbe-codecs.sh --check
cargo test -p kr-runtime-trace-tool --locked
cargo test -p kr-runtime-trace-tool --example generate_browser_sbe_goldens --locked

viewer_js=(
    "${TRACE_VIEWER_SHARED_JS[@]}"
    tools/trace-tool/runtime-trace-time.js
    tools/trace-tool/runtime-trace-time.test.js
)
deno fmt --check "${viewer_js[@]}"
deno lint --no-config "${viewer_js[@]}"
deno check --no-config \
    "${viewer_js[@]}" \
    tools/trace-tool/sbe-ir.js \
    tools/trace-tool/dst-trace-sbe-ir.js \
    tools/trace-tool/dst-trace-sample.js \
    tools/trace-tool/testdata/generic-sbe-ir.js \
    tools/trace-tool/generate-sbe-ir-module.js \
    tools/trace-tool/sbe-ir.test.js \
    tools/trace-tool/sbe-decoder.js \
    tools/trace-tool/sbe-decoder.test.js
deno test --no-config \
    --allow-read=tools/trace-tool/testdata,tools/trace-tool/index.html,tools/trace-tool/trace-viewer-ui.js,tools/trace-tool/trace-viewer.css \
    tools/trace-tool/trace-viewer-core.test.js \
    tools/trace-tool/trace-viewer-ui.test.js \
    tools/trace-tool/sbe-ir.test.js \
    tools/trace-tool/sbe-decoder.test.js \
    tools/trace-tool/runtime-trace-time.test.js
