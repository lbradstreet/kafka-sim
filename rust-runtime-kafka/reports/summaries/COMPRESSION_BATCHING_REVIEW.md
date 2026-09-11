# Classic Java and native producer comparison

584 Full executions, 292 matched pairs, 37 families and 146 variants at seed 0. Each execution includes an exact replay. The additional slow-broker and delay-ramp selections contain eight audited executions at seeds 1 and 7.

Both clients use the same workload generator, broker model and simulated network. Original preserves the native settings; common records adjustments to lanes, admission, encoding cost and selected fault rules. Closed-loop offered populations depend on delivery progress. ACK latency excludes refused and failed records; elapsed host time is not a producer speed measurement.

0 matched Full pairs have incomplete comparable fault exposure. These remain flagged in the measurements and viewer.

[Every Full variant](COMPRESSION_BATCHING_VARIANTS.md) · [CSV](COMPRESSION_BATCHING_RESULTS.csv) · [Paired viewer](../classic-full/index.html) · [Admission comparison](../classic-full/admission-isolation.html)
