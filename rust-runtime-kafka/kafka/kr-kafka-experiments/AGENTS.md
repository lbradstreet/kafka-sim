# Experiment presentation

Generated experiment pages should explain the experiment's purpose in readable
prose before the charts: what question it asks, how traffic is supplied, which
parameters are compared, which faults or topology changes occur, and what to
inspect in the result. A generic instruction to compare latency and throughput
is insufficient.

Describe actual primary-run quantities, including Test-size adjustments and
overrides. Explain closed-loop source feedback, refusal-versus-delivery
populations, scheduled idle intervals, and terminal outcomes when relevant.
Keep the catalogue intent in `tools/trace-tool/producer-experiment-model.js`
current when adding scenarios; derive quantities from the loaded artifact.
The topology heading is **Partition topology at cursor time**.
