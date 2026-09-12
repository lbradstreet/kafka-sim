# Generated simulation reports

Open [index.html](index.html) in a browser from a local checkout. These HTML
exports include their scripts, styles and data, and work without a server.
GitHub displays HTML source; download or check out the files to use the viewers.

The snapshot was regenerated from source revision `b65bafb3fc`, using the Java
producer on this branch. It contains the native Full catalogue (146 runs),
classic Java/native Test and Full catalogues (292 matched pairs each), additional-seed classic
followups, the 324-pair request-grouping dashboard, and seed-0 admission pages.
[Complete campaign results](CAMPAIGN_RESULTS.md) summarize all 2,440 comparison
executions and the 204-run admission trial. Individual runs replayed successfully;
two admission policy gates failed and remain reported as failures.

Raw histories, replay sidecars, machine provenance and ongoing campaign outputs
remain under ignored `target` directories. They are needed to repeat the
analyses, but are not dependencies of these HTML pages.

## Regeneration

Run from the `rust-runtime-kafka` directory. The [experiment guide](../kafka/kr-kafka-experiments/README.md)
and [classic comparison guide](../kafka/kr-kafka-experiments/CLASSIC_COMPARISON.md)
explain how to generate the source runs and prerequisites.

Export the completed native Full runs:

```sh
RUSTC_WRAPPER= cargo run --release -p kr-kafka-experiments --bin kafka-experiments -- \
  --export-html target/experiments/full-html --out target/experiments/admission-trial/regression-full
```

Export completed classic Test runs:

```sh
python3 -B kafka/kr-kafka-experiments/analysis/classic_visualization.py \
  target/classic-scenarios/test-suite --out target/classic-scenarios/test-suite/viewer
```

The admission runner, `python3 -B scripts/rerun-admission-trial.py --stage trial
--seeds 16`, generates seed-0 HTML alongside the complete multi-seed evidence.
Copy only the completed HTML exports into the matching report directories after
validating their data, links and metadata.

| Published directory | Generated source directory |
|---|---|
| `native-full` | `target/experiments/full-html` |
| `classic-test` | `target/classic-scenarios/test-suite/viewer` |
| `admission-independent` | `target/experiments/admission-trial/independent/seed-0/html` |
| `admission-shared` | `target/experiments/admission-trial/original/shared/seed-0/html` |
| `admission-pressure` | `target/experiments/admission-trial/original/partition-pressure/seed-0/html` |
| `admission-skew` | `target/experiments/admission-trial/skew/html` |

The completed Full comparison, additional-seed galleries and request-policy
dashboard are copied from `target/classic-scenarios/full-review/analysis/viewer`,
`target/classic-scenarios/followups/seed-{1,7}/viewer`, and
`target/classic-scenarios/request-policy-viewer`. Compact measured summaries are
in `summaries`; raw histories and machine provenance remain outside this snapshot.
