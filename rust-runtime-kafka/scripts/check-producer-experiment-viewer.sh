#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/check-common.sh"
require_deno "producer experiment viewer"
python3 scripts/pin-producer-experiment-assets.py --check
python3 -B -m unittest discover -s kafka/kr-kafka-experiments/analysis -p 'test_classic*.py'
generator_test="tests::generated_producer_experiment_is_deterministic_bounded_and_covers_recovery"
require_registered_test "${generator_test}" "producer experiment sample test" \
  -p kr-kafka-experiments --example generate_producer_experiment
cargo test -p kr-kafka-experiments --example generate_producer_experiment --locked "${generator_test}" -- --exact
export_test="export::tests::html_export_preserves_script_terminators_as_inert_text"
require_registered_test "${export_test}" "HTML-safe export test" -p kr-kafka-experiments --lib
cargo test -p kr-kafka-experiments --lib --locked export::tests
viewer_js=("${TRACE_VIEWER_SHARED_JS[@]}" tools/trace-tool/producer-experiment-model.js tools/trace-tool/producer-experiment-model.test.js tools/trace-tool/producer-experiment-viewer.js tools/trace-tool/producer-experiment-viewer.test.js tools/trace-tool/producer-comparison-model.js tools/trace-tool/producer-comparison-model.test.js tools/trace-tool/producer-comparison-viewer.js)
deno fmt --check "${viewer_js[@]}" tools/trace-tool/producer-experiment.html tools/trace-tool/producer-experiment.css
deno lint --no-config "${viewer_js[@]}"
deno check --no-config "${viewer_js[@]}"
deno test --no-config --allow-read=tools/trace-tool/producer-experiment-data.js,tools/trace-tool/producer-comparison-data.js,tools/trace-tool/producer-experiment.html,tools/trace-tool/producer-experiment-viewer.js,tools/trace-tool/trace-viewer-ui.js,tools/trace-tool/trace-viewer.css \
  tools/trace-tool/trace-viewer-core.test.js tools/trace-tool/trace-viewer-ui.test.js tools/trace-tool/producer-experiment-model.test.js tools/trace-tool/producer-experiment-viewer.test.js tools/trace-tool/producer-comparison-model.test.js
