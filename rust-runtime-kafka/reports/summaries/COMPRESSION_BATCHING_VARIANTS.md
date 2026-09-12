# Full producer comparison: every variant

Generated from complete, replay-verified evidence by `analysis/classic_full_report.py`.

37 families, 146 variants, 292 pairs, 584 executions, seed 0. Each execution ran twice.

Read [the analysis](COMPRESSION_BATCHING_REVIEW.md) before interpreting these numbers. Java is the classic KafkaProducer; native is the Panama-driven native simulation actor. A/R/F means acknowledged / refused / failed. p99 measures accepted-to-consumed latency for successful records only, in milliseconds. Closed-loop offered populations depend on progress.

**Common forces native Shared admission, including pressure-labeled variants.** It also changes lanes, linger bypass, simulated encoding cost and selected fault semantics. Use original for the admission-policy comparison. Both profiles retain unequal memory accounting.

Links open the existing repository visualizer. CSV retains nanosecond precision, maximum waits, retry counts, wire bytes, pending-demand gaps and investigation flags. Coverage JSON pins each source report and audited result by SHA-256.

## baseline.asymmetric-wan-broker

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [i1-window1024k · original](../classic-full/baseline.asymmetric-wan-broker--full--1.html#pair=4) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 144.398 / 82.000 | 1,012 / 1,419 |
| [i1-window1024k · common](../classic-full/baseline.asymmetric-wan-broker--full--1.html#pair=0) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 144.398 / 82.000 | 1,012 / 884 |
| [i1-window64k · original](../classic-full/baseline.asymmetric-wan-broker--full--1.html#pair=5) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 144.398 / 82.000 | 1,012 / 1,419 |
| [i1-window64k · common](../classic-full/baseline.asymmetric-wan-broker--full--1.html#pair=1) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 144.398 / 82.000 | 1,012 / 884 |
| [i5-window1024k · original](../classic-full/baseline.asymmetric-wan-broker--full--1.html#pair=6) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 51.389 / 54.000 | 868 / 1,649 |
| [i5-window1024k · common](../classic-full/baseline.asymmetric-wan-broker--full--1.html#pair=2) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 51.389 / 57.000 | 868 / 986 |
| [i5-window64k · original](../classic-full/baseline.asymmetric-wan-broker--full--1.html#pair=7) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 51.389 / 54.000 | 868 / 1,649 |
| [i5-window64k · common](../classic-full/baseline.asymmetric-wan-broker--full--1.html#pair=3) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 51.389 / 57.000 | 868 / 986 |

## baseline.bursty-onoff

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [burst200 · original](../classic-full/baseline.bursty-onoff--full--1.html#pair=3) | 2,000 / 0 / 0 | 2,000 / 0 / 0 | 9.226 / 8.000 | 180 / 240 |
| [burst200 · common](../classic-full/baseline.bursty-onoff--full--1.html#pair=0) | 2,000 / 0 / 0 | 2,000 / 0 / 0 | 9.226 / 9.000 | 180 / 194 |
| [burst50 · original](../classic-full/baseline.bursty-onoff--full--1.html#pair=4) | 500 / 0 / 0 | 500 / 0 / 0 | 6.811 / 8.000 | 60 / 78 |
| [burst50 · common](../classic-full/baseline.bursty-onoff--full--1.html#pair=1) | 500 / 0 / 0 | 500 / 0 / 0 | 6.811 / 9.000 | 60 / 77 |
| [burst800 · original](../classic-full/baseline.bursty-onoff--full--1.html#pair=5) | 8,000 / 0 / 0 | 8,000 / 0 / 0 | 26.077 / 7.000 | 680 / 759 |
| [burst800 · common](../classic-full/baseline.bursty-onoff--full--1.html#pair=2) | 8,000 / 0 / 0 | 8,000 / 0 / 0 | 26.077 / 8.000 | 680 / 454 |

## baseline.closed-loop-inflight

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [k1-i1 · original](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=10) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.808 / 7.000 | 4,096 / 4,096 |
| [k1-i1 · common](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=0) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.808 / 7.000 | 4,096 / 4,096 |
| [k1-i5 · original](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=11) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.808 / 7.000 | 4,096 / 4,096 |
| [k1-i5 · common](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=1) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.808 / 7.000 | 4,096 / 4,096 |
| [k16-i1 · original](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=12) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.809 / 7.000 | 768 / 1,408 |
| [k16-i1 · common](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=2) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.809 / 7.000 | 768 / 769 |
| [k16-i5 · original](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=13) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.808 / 7.000 | 768 / 1,408 |
| [k16-i5 · common](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=3) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.808 / 7.000 | 768 / 769 |
| [k256-i1 · original](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=14) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 23.869 / 8.000 | 340 / 255 |
| [k256-i1 · common](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=4) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 23.869 / 8.000 | 340 / 145 |
| [k256-i5 · original](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=15) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 16.650 / 8.000 | 340 / 406 |
| [k256-i5 · common](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=5) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 16.650 / 8.000 | 340 / 299 |
| [k4-i1 · original](../classic-full/baseline.closed-loop-inflight--full--2.html#pair=0) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.808 / 7.000 | 2,353 / 3,264 |
| [k4-i1 · common](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=6) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.808 / 7.000 | 2,353 / 2,447 |
| [k4-i5 · original](../classic-full/baseline.closed-loop-inflight--full--2.html#pair=1) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.808 / 7.000 | 2,369 / 3,264 |
| [k4-i5 · common](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=7) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.808 / 7.000 | 2,369 / 2,447 |
| [k64-i1 · original](../classic-full/baseline.closed-loop-inflight--full--2.html#pair=2) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 5.616 / 8.000 | 340 / 503 |
| [k64-i1 · common](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=8) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 5.616 / 8.000 | 340 / 414 |
| [k64-i5 · original](../classic-full/baseline.closed-loop-inflight--full--2.html#pair=3) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 5.215 / 8.000 | 344 / 516 |
| [k64-i5 · common](../classic-full/baseline.closed-loop-inflight--full--1.html#pair=9) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 5.215 / 8.000 | 344 / 508 |

## baseline.compression

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [random0-zstd0 · original](../classic-full/baseline.compression--full--1.html#pair=6) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 15.647 / 6.000 | 2,368 / 1,881 |
| [random0-zstd0 · common](../classic-full/baseline.compression--full--1.html#pair=0) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 15.647 / 7.000 | 2,368 / 1,478 |
| [random0-zstd1 · original](../classic-full/baseline.compression--full--1.html#pair=7) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 7.021 / 7.000 | 447 / 763 |
| [random0-zstd1 · common](../classic-full/baseline.compression--full--1.html#pair=1) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 7.021 / 7.000 | 447 / 882 |
| [random0-zstd3 · original](../classic-full/baseline.compression--full--1.html#pair=8) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 7.021 / 7.000 | 447 / 763 |
| [random0-zstd3 · common](../classic-full/baseline.compression--full--1.html#pair=2) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 7.021 / 7.000 | 447 / 882 |
| [random1-zstd0 · original](../classic-full/baseline.compression--full--1.html#pair=9) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 15.647 / 6.000 | 2,368 / 1,881 |
| [random1-zstd0 · common](../classic-full/baseline.compression--full--1.html#pair=3) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 15.647 / 7.000 | 2,368 / 1,478 |
| [random1-zstd1 · original](../classic-full/baseline.compression--full--1.html#pair=10) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 15.647 / 6.000 | 2,368 / 2,047 |
| [random1-zstd1 · common](../classic-full/baseline.compression--full--1.html#pair=4) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 15.647 / 6.000 | 2,368 / 1,632 |
| [random1-zstd3 · original](../classic-full/baseline.compression--full--1.html#pair=11) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 15.647 / 6.000 | 2,368 / 2,047 |
| [random1-zstd3 · common](../classic-full/baseline.compression--full--1.html#pair=5) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 15.647 / 6.000 | 2,368 / 1,632 |

## baseline.linger-sweep

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [linger0-skip0 · original](../classic-full/baseline.linger-sweep--full--1.html#pair=8) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 4.012 / 8.000 | 1,209 / 2,741 |
| [linger0-skip0 · common](../classic-full/baseline.linger-sweep--full--1.html#pair=0) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 4.012 / 8.000 | 1,209 / 1,599 |
| [linger0-skip1 · original](../classic-full/baseline.linger-sweep--full--1.html#pair=9) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 4.012 / 8.000 | 1,209 / 2,741 |
| [linger0-skip1 · common](../classic-full/baseline.linger-sweep--full--1.html#pair=1) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 4.012 / 8.000 | 1,209 / 1,599 |
| [linger1-skip0 · original](../classic-full/baseline.linger-sweep--full--1.html#pair=10) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 3.611 / 3.000 | 754 / 1,459 |
| [linger1-skip0 · common](../classic-full/baseline.linger-sweep--full--1.html#pair=2) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 3.611 / 5.000 | 754 / 1,222 |
| [linger1-skip1 · original](../classic-full/baseline.linger-sweep--full--1.html#pair=11) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 3.611 / 8.000 | 754 / 2,793 |
| [linger1-skip1 · common](../classic-full/baseline.linger-sweep--full--1.html#pair=3) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 3.611 / 5.000 | 754 / 1,222 |
| [linger20-skip0 · original](../classic-full/baseline.linger-sweep--full--1.html#pair=12) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 21.212 / 22.000 | 347 / 556 |
| [linger20-skip0 · common](../classic-full/baseline.linger-sweep--full--1.html#pair=4) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 21.212 / 22.000 | 347 / 548 |
| [linger20-skip1 · original](../classic-full/baseline.linger-sweep--full--1.html#pair=13) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 21.212 / 8.000 | 347 / 2,748 |
| [linger20-skip1 · common](../classic-full/baseline.linger-sweep--full--1.html#pair=5) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 21.212 / 22.000 | 347 / 548 |
| [linger5-skip0 · original](../classic-full/baseline.linger-sweep--full--1.html#pair=14) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.618 / 7.000 | 352 / 681 |
| [linger5-skip0 · common](../classic-full/baseline.linger-sweep--full--1.html#pair=6) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.618 / 8.000 | 352 / 640 |
| [linger5-skip1 · original](../classic-full/baseline.linger-sweep--full--1.html#pair=15) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.618 / 8.000 | 352 / 2,752 |
| [linger5-skip1 · common](../classic-full/baseline.linger-sweep--full--1.html#pair=7) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.618 / 8.000 | 352 / 640 |

## baseline.open-loop-rate

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [rate2000 · original](../classic-full/baseline.open-loop-rate--full--1.html#pair=4) | 39,980 / 20 / 0 | 40,000 / 0 / 0 | 6.404 / 6.500 | 9,995 / 16,874 |
| [rate2000 · common](../classic-full/baseline.open-loop-rate--full--1.html#pair=0) | 39,980 / 20 / 0 | 40,000 / 0 / 0 | 6.404 / 7.000 | 9,995 / 16,874 |
| [rate32000 · original](../classic-full/baseline.open-loop-rate--full--1.html#pair=5) | 623,082 / 16,918 / 0 | 509,644 / 130,356 / 0 | 1,046.625 / 4.969 | 51,743 / 63,736 |
| [rate32000 · common](../classic-full/baseline.open-loop-rate--full--1.html#pair=1) | 623,082 / 16,918 / 0 | 360,513 / 279,487 / 0 | 1,046.625 / 6.250 | 51,743 / 43,913 |
| [rate500 · original](../classic-full/baseline.open-loop-rate--full--1.html#pair=6) | 9,995 / 5 / 0 | 10,000 / 0 / 0 | 7.212 / 7.000 | 5,777 / 7,970 |
| [rate500 · common](../classic-full/baseline.open-loop-rate--full--1.html#pair=2) | 9,995 / 5 / 0 | 10,000 / 0 / 0 | 7.212 / 7.000 | 5,777 / 7,970 |
| [rate8000 · original](../classic-full/baseline.open-loop-rate--full--1.html#pair=7) | 159,917 / 83 / 0 | 160,000 / 0 / 0 | 6.279 / 6.500 | 15,242 / 23,437 |
| [rate8000 · common](../classic-full/baseline.open-loop-rate--full--1.html#pair=3) | 159,917 / 83 / 0 | 160,000 / 0 / 0 | 6.279 / 6.875 | 15,242 / 23,437 |

## baseline.partition-admission-skew

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [hot-rate32000-pressure · original](../classic-full/baseline.partition-admission-skew--full--1.html#pair=12) | 84,127 / 235,881 / 0 | 119,680 / 200,328 / 0 | 2,054.141 / 4.031 | 12,019 / 9,974 |
| [hot-rate32000-pressure · common](../classic-full/baseline.partition-admission-skew--full--1.html#pair=0) | 84,127 / 235,881 / 0 | 159,563 / 160,445 / 0 | 2,054.141 / 4.031 | 12,019 / 9,974 |
| [hot-rate32000-shared · original](../classic-full/baseline.partition-admission-skew--full--1.html#pair=13) | 84,127 / 235,881 / 0 | 159,563 / 160,445 / 0 | 2,054.141 / 4.031 | 12,019 / 9,974 |
| [hot-rate32000-shared · common](../classic-full/baseline.partition-admission-skew--full--1.html#pair=1) | 84,127 / 235,881 / 0 | 159,563 / 160,445 / 0 | 2,054.141 / 4.031 | 12,019 / 9,974 |
| [hot-rate8000-pressure · original](../classic-full/baseline.partition-admission-skew--full--1.html#pair=14) | 80,008 / 0 / 0 | 80,008 / 0 / 0 | 1,450.090 / 4.125 | 11,431 / 9,973 |
| [hot-rate8000-pressure · common](../classic-full/baseline.partition-admission-skew--full--1.html#pair=2) | 80,008 / 0 / 0 | 80,008 / 0 / 0 | 1,450.090 / 4.125 | 11,431 / 9,973 |
| [hot-rate8000-shared · original](../classic-full/baseline.partition-admission-skew--full--1.html#pair=15) | 80,008 / 0 / 0 | 80,008 / 0 / 0 | 1,450.090 / 4.125 | 11,431 / 9,973 |
| [hot-rate8000-shared · common](../classic-full/baseline.partition-admission-skew--full--1.html#pair=3) | 80,008 / 0 / 0 | 80,008 / 0 / 0 | 1,450.090 / 4.125 | 11,431 / 9,973 |
| [skew90-rate32000-pressure · original](../classic-full/baseline.partition-admission-skew--full--2.html#pair=0) | 86,234 / 233,774 / 0 | 122,992 / 197,016 / 0 | 2,054.141 / 8.264 | 12,257 / 13,174 |
| [skew90-rate32000-pressure · common](../classic-full/baseline.partition-admission-skew--full--1.html#pair=4) | 86,234 / 233,774 / 0 | 156,560 / 163,448 / 0 | 2,054.141 / 6.979 | 12,257 / 13,276 |
| [skew90-rate32000-shared · original](../classic-full/baseline.partition-admission-skew--full--2.html#pair=1) | 86,234 / 233,774 / 0 | 156,557 / 163,451 / 0 | 2,054.141 / 6.979 | 12,257 / 13,279 |
| [skew90-rate32000-shared · common](../classic-full/baseline.partition-admission-skew--full--1.html#pair=5) | 86,234 / 233,774 / 0 | 156,560 / 163,448 / 0 | 2,054.141 / 6.979 | 12,257 / 13,276 |
| [skew90-rate8000-pressure · original](../classic-full/baseline.partition-admission-skew--full--2.html#pair=2) | 80,008 / 0 / 0 | 80,008 / 0 / 0 | 315.000 / 8.056 | 13,486 / 13,228 |
| [skew90-rate8000-pressure · common](../classic-full/baseline.partition-admission-skew--full--1.html#pair=6) | 80,008 / 0 / 0 | 80,008 / 0 / 0 | 315.000 / 8.056 | 13,486 / 13,223 |
| [skew90-rate8000-shared · original](../classic-full/baseline.partition-admission-skew--full--2.html#pair=3) | 80,008 / 0 / 0 | 80,008 / 0 / 0 | 315.000 / 8.056 | 13,486 / 13,228 |
| [skew90-rate8000-shared · common](../classic-full/baseline.partition-admission-skew--full--1.html#pair=7) | 80,008 / 0 / 0 | 80,008 / 0 / 0 | 315.000 / 8.056 | 13,486 / 13,223 |
| [sparse1024-rate32000-pressure · original](../classic-full/baseline.partition-admission-skew--full--2.html#pair=4) | 86,233 / 233,775 / 0 | 116,808 / 203,200 / 0 | 2,054.141 / 8.750 | 12,256 / 13,174 |
| [sparse1024-rate32000-pressure · common](../classic-full/baseline.partition-admission-skew--full--1.html#pair=8) | 86,233 / 233,775 / 0 | 144,896 / 175,112 / 0 | 2,054.141 / 8.750 | 12,256 / 12,648 |
| [sparse1024-rate32000-shared · original](../classic-full/baseline.partition-admission-skew--full--2.html#pair=5) | 86,233 / 233,775 / 0 | 144,776 / 175,232 / 0 | 2,054.141 / 8.750 | 12,256 / 12,639 |
| [sparse1024-rate32000-shared · common](../classic-full/baseline.partition-admission-skew--full--1.html#pair=9) | 86,233 / 233,775 / 0 | 144,896 / 175,112 / 0 | 2,054.141 / 8.750 | 12,256 / 12,648 |
| [sparse1024-rate8000-pressure · original](../classic-full/baseline.partition-admission-skew--full--2.html#pair=6) | 80,008 / 0 / 0 | 80,008 / 0 / 0 | 315.000 / 8.333 | 13,486 / 13,186 |
| [sparse1024-rate8000-pressure · common](../classic-full/baseline.partition-admission-skew--full--1.html#pair=10) | 80,008 / 0 / 0 | 80,008 / 0 / 0 | 315.000 / 8.333 | 13,486 / 13,187 |
| [sparse1024-rate8000-shared · original](../classic-full/baseline.partition-admission-skew--full--2.html#pair=7) | 80,008 / 0 / 0 | 80,008 / 0 / 0 | 315.000 / 8.333 | 13,486 / 13,186 |
| [sparse1024-rate8000-shared · common](../classic-full/baseline.partition-admission-skew--full--1.html#pair=11) | 80,008 / 0 / 0 | 80,008 / 0 / 0 | 315.000 / 8.333 | 13,486 / 13,187 |

## baseline.partition-fanout-skew

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [p1-skew0 · original](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=9) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 5.215 / 5.000 | 586 / 509 |
| [p1-skew0 · common](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=0) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 5.215 / 5.000 | 586 / 509 |
| [p1-skew50 · original](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=10) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 5.215 / 5.000 | 586 / 509 |
| [p1-skew50 · common](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=1) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 5.215 / 5.000 | 586 / 509 |
| [p1-skew90 · original](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=11) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 5.215 / 5.000 | 586 / 509 |
| [p1-skew90 · common](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=2) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 5.215 / 5.000 | 586 / 509 |
| [p16-skew0 · original](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=12) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 7.407 / 8.000 | 384 / 989 |
| [p16-skew0 · common](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=3) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 7.407 / 9.000 | 384 / 588 |
| [p16-skew50 · original](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=13) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 7.216 / 8.000 | 462 / 1,181 |
| [p16-skew50 · common](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=4) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 7.216 / 8.000 | 462 / 1,064 |
| [p16-skew90 · original](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=14) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.417 / 8.000 | 667 / 756 |
| [p16-skew90 · common](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=5) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.417 / 8.000 | 667 / 736 |
| [p6-skew0 · original](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=15) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.618 / 7.000 | 352 / 681 |
| [p6-skew0 · common](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=6) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.618 / 8.000 | 352 / 640 |
| [p6-skew50 · original](../classic-full/baseline.partition-fanout-skew--full--2.html#pair=0) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.818 / 7.000 | 495 / 721 |
| [p6-skew50 · common](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=7) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.818 / 7.000 | 495 / 684 |
| [p6-skew90 · original](../classic-full/baseline.partition-fanout-skew--full--2.html#pair=1) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.417 / 7.000 | 678 / 694 |
| [p6-skew90 · common](../classic-full/baseline.partition-fanout-skew--full--1.html#pair=8) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 6.417 / 8.000 | 678 / 674 |

## hard.bootstrap-down-at-start

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [b1-first · original](../classic-full/hard.bootstrap-down-at-start--full--1.html#pair=3) | 176 / 24 / 0 | 200 / 0 / 0 | 5,004.080 / 5,001.000 | 62 / 109 |
| [b1-first · common](../classic-full/hard.bootstrap-down-at-start--full--1.html#pair=0) | 176 / 24 / 0 | 200 / 0 / 0 | 5,004.080 / 5,001.000 | 62 / 106 |
| [b2-first · original](../classic-full/hard.bootstrap-down-at-start--full--1.html#pair=4) | 190 / 10 / 0 | 200 / 0 / 0 | 5,059.080 / 5,012.000 | 67 / 112 |
| [b2-first · common](../classic-full/hard.bootstrap-down-at-start--full--1.html#pair=1) | 190 / 10 / 0 | 200 / 0 / 0 | 5,059.080 / 5,012.000 | 67 / 111 |
| [single · original](../classic-full/hard.bootstrap-down-at-start--full--1.html#pair=5) | 100 / 100 / 0 | 0 / 100 / 100 | 9.212 / — | 39 / 0 |
| [single · common](../classic-full/hard.bootstrap-down-at-start--full--1.html#pair=2) | 100 / 100 / 0 | 0 / 100 / 100 | 9.212 / — | 39 / 0 |

## hard.close-during-outage

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [deadline15s · original](../classic-full/hard.close-during-outage--full--1.html#pair=2) | 26,053 / 0 / 0 | 22,069 / 0 / 0 | 16.012 / 28.000 | 7,857 / 14,559 |
| [deadline15s · common](../classic-full/hard.close-during-outage--full--1.html#pair=0) | 26,053 / 0 / 0 | 21,084 / 0 / 0 | 16.012 / 20.000 | 7,857 / 8,017 |
| [deadline2s · original](../classic-full/hard.close-during-outage--full--1.html#pair=3) | 26,021 / 0 / 32 | 22,037 / 0 / 32 | 16.011 / 28.000 | 7,852 / 14,552 |
| [deadline2s · common](../classic-full/hard.close-during-outage--full--1.html#pair=1) | 26,021 / 0 / 32 | 21,052 / 0 / 32 | 16.011 / 20.000 | 7,852 / 8,013 |

## hard.crash-restart-closed

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [k16-slow0 · original](../classic-full/hard.crash-restart-closed--full--1.html#pair=4) | 21,923 / 0 / 0 | 20,800 / 0 / 0 | 12.407 / 10.000 | 4,125 / 6,906 |
| [k16-slow0 · common](../classic-full/hard.crash-restart-closed--full--1.html#pair=0) | 21,923 / 0 / 0 | 20,762 / 0 / 0 | 12.407 / 10.000 | 4,125 / 3,973 |
| [k16-slow1 · original](../classic-full/hard.crash-restart-closed--full--1.html#pair=5) | 21,141 / 0 / 0 | 20,736 / 0 / 0 | 12.407 / 10.000 | 4,022 / 6,888 |
| [k16-slow1 · common](../classic-full/hard.crash-restart-closed--full--1.html#pair=1) | 21,141 / 0 / 0 | 20,761 / 0 / 0 | 12.407 / 10.000 | 4,022 / 3,914 |
| [k64-slow0 · original](../classic-full/hard.crash-restart-closed--full--1.html#pair=6) | 81,110 / 0 / 0 | 96,622 / 0 / 0 | 16.012 / 11.000 | 7,745 / 14,995 |
| [k64-slow0 · common](../classic-full/hard.crash-restart-closed--full--1.html#pair=2) | 81,110 / 0 / 0 | 50,572 / 0 / 0 | 16.012 / 20.000 | 7,745 / 9,274 |
| [k64-slow1 · original](../classic-full/hard.crash-restart-closed--full--1.html#pair=7) | 79,571 / 0 / 0 | 96,490 / 0 / 0 | 16.012 / 11.000 | 7,608 / 14,985 |
| [k64-slow1 · common](../classic-full/hard.crash-restart-closed--full--1.html#pair=3) | 79,571 / 0 / 0 | 50,392 / 0 / 0 | 16.012 / 20.000 | 7,608 / 9,162 |

## hard.crash-restart-open

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [rate1000 · original](../classic-full/hard.crash-restart-open--full--1.html#pair=3) | 29,990 / 10 / 0 | 28,905 / 1,095 / 0 | 1,961.248 / 1,934.000 | 11,875 / 18,198 |
| [rate1000 · common](../classic-full/hard.crash-restart-open--full--1.html#pair=0) | 29,990 / 10 / 0 | 28,899 / 1,101 / 0 | 1,961.248 / 1,947.000 | 11,875 / 18,194 |
| [rate16000 · original](../classic-full/hard.crash-restart-open--full--1.html#pair=4) | 479,848 / 152 / 0 | 433,764 / 46,236 / 0 | 2,321.995 / 6.000 | 42,309 / 55,024 |
| [rate16000 · common](../classic-full/hard.crash-restart-open--full--1.html#pair=1) | 479,848 / 152 / 0 | 433,675 / 46,325 / 0 | 2,321.995 / 6.250 | 42,309 / 55,012 |
| [rate4000 · original](../classic-full/hard.crash-restart-open--full--1.html#pair=5) | 119,961 / 39 / 0 | 109,855 / 10,145 / 0 | 2,022.581 / 6.500 | 17,220 / 26,474 |
| [rate4000 · common](../classic-full/hard.crash-restart-open--full--1.html#pair=2) | 119,961 / 39 / 0 | 109,883 / 10,117 / 0 | 2,022.581 / 7.250 | 17,220 / 26,480 |

## hard.flapping-broker

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [duty10-slow0 · original](../classic-full/hard.flapping-broker--full--1.html#pair=4) | 61,980 / 20 / 0 | 62,000 / 0 / 0 | 97.212 / 59.500 | 15,176 / 25,623 |
| [duty10-slow0 · common](../classic-full/hard.flapping-broker--full--1.html#pair=0) | 61,980 / 20 / 0 | 62,000 / 0 / 0 | 97.212 / 58.000 | 15,176 / 25,609 |
| [duty10-slow1 · original](../classic-full/hard.flapping-broker--full--1.html#pair=5) | 61,829 / 171 / 0 | 62,000 / 0 / 0 | 182.730 / 91.500 | 15,020 / 25,462 |
| [duty10-slow1 · common](../classic-full/hard.flapping-broker--full--1.html#pair=1) | 61,829 / 171 / 0 | 62,000 / 0 / 0 | 182.730 / 97.500 | 15,020 / 25,417 |
| [duty50-slow0 · original](../classic-full/hard.flapping-broker--full--1.html#pair=6) | 61,980 / 20 / 0 | 62,000 / 0 / 0 | 486.221 / 456.500 | 14,328 / 23,653 |
| [duty50-slow0 · common](../classic-full/hard.flapping-broker--full--1.html#pair=2) | 61,980 / 20 / 0 | 62,000 / 0 / 0 | 486.221 / 457.500 | 14,328 / 23,650 |
| [duty50-slow1 · original](../classic-full/hard.flapping-broker--full--1.html#pair=7) | 61,829 / 171 / 0 | 62,000 / 0 / 0 | 650.712 / 527.000 | 13,942 / 23,311 |
| [duty50-slow1 · common](../classic-full/hard.flapping-broker--full--1.html#pair=3) | 61,829 / 171 / 0 | 62,000 / 0 / 0 | 650.712 / 535.000 | 13,942 / 23,280 |

## hard.leader-failover-during-outage

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [move10s-metadata1000ms · original](../classic-full/hard.leader-failover-during-outage--full--1.html#pair=4) | 92,091 / 0 / 0 | 58,178 / 0 / 0 | 10.607 / 19.000 | 8,916 / 17,073 |
| [move10s-metadata1000ms · common](../classic-full/hard.leader-failover-during-outage--full--1.html#pair=0) | 92,091 / 0 / 0 | 47,621 / 0 / 0 | 10.607 / 24.000 | 8,916 / 10,937 |
| [move10s-metadata20ms · original](../classic-full/hard.leader-failover-during-outage--full--1.html#pair=5) | 83,480 / 0 / 0 | 59,134 / 0 / 0 | 12.409 / 18.000 | 8,766 / 17,147 |
| [move10s-metadata20ms · common](../classic-full/hard.leader-failover-during-outage--full--1.html#pair=1) | 83,480 / 0 / 0 | 47,418 / 0 / 0 | 12.409 / 24.000 | 8,766 / 11,018 |
| [move1s-metadata1000ms · original](../classic-full/hard.leader-failover-during-outage--full--1.html#pair=6) | 145,799 / 0 / 0 | 87,326 / 0 / 0 | 9.810 / 18.000 | 11,796 / 23,407 |
| [move1s-metadata1000ms · common](../classic-full/hard.leader-failover-during-outage--full--1.html#pair=2) | 145,799 / 0 / 0 | 64,353 / 0 / 0 | 9.810 / 25.000 | 11,796 / 14,172 |
| [move1s-metadata20ms · original](../classic-full/hard.leader-failover-during-outage--full--1.html#pair=7) | 126,782 / 0 / 0 | 88,779 / 0 / 0 | 12.409 / 16.000 | 11,361 / 23,386 |
| [move1s-metadata20ms · common](../classic-full/hard.leader-failover-during-outage--full--1.html#pair=3) | 126,782 / 0 / 0 | 64,636 / 0 / 0 | 12.409 / 25.000 | 11,361 / 14,110 |

## hard.partition-admission-isolation

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [independent-rate1000-pressure · original](../classic-full/hard.partition-admission-isolation--full--1.html#pair=6) | 29,988 / 12 / 0 | 29,370 / 630 / 0 | 2,170.171 / 2,150.809 | 14,577 / 28,998 |
| [independent-rate1000-pressure · common](../classic-full/hard.partition-admission-isolation--full--1.html#pair=0) | 29,988 / 12 / 0 | 28,512 / 1,488 / 0 | 2,170.171 / 2,168.737 | 14,577 / 23,448 |
| [independent-rate1000-shared · original](../classic-full/hard.partition-admission-isolation--full--1.html#pair=7) | 29,988 / 12 / 0 | 28,506 / 1,494 / 0 | 2,170.171 / 2,170.737 | 14,577 / 28,004 |
| [independent-rate1000-shared · common](../classic-full/hard.partition-admission-isolation--full--1.html#pair=1) | 29,988 / 12 / 0 | 28,512 / 1,488 / 0 | 2,170.171 / 2,168.737 | 14,577 / 23,448 |
| [independent-rate16000-pressure · original](../classic-full/hard.partition-admission-isolation--full--1.html#pair=8) | 474,633 / 5,367 / 0 | 464,242 / 15,758 / 0 | 2,458.215 / 4.126 | 33,913 / 57,986 |
| [independent-rate16000-pressure · common](../classic-full/hard.partition-admission-isolation--full--1.html#pair=2) | 474,633 / 5,367 / 0 | 433,396 / 46,604 / 0 | 2,458.215 / 5.249 | 33,913 / 54,127 |
| [independent-rate16000-shared · original](../classic-full/hard.partition-admission-isolation--full--1.html#pair=9) | 474,633 / 5,367 / 0 | 433,222 / 46,778 / 0 | 2,458.215 / 4.126 | 33,913 / 54,106 |
| [independent-rate16000-shared · common](../classic-full/hard.partition-admission-isolation--full--1.html#pair=3) | 474,633 / 5,367 / 0 | 433,396 / 46,604 / 0 | 2,458.215 / 5.249 | 33,913 / 54,127 |
| [independent-rate4000-pressure · original](../classic-full/hard.partition-admission-isolation--full--1.html#pair=10) | 119,952 / 48 / 0 | 116,362 / 3,638 / 0 | 2,237.458 / 7.366 | 14,788 / 29,002 |
| [independent-rate4000-pressure · common](../classic-full/hard.partition-admission-isolation--full--1.html#pair=4) | 119,952 / 48 / 0 | 109,456 / 10,544 / 0 | 2,237.458 / 7.390 | 14,788 / 22,749 |
| [independent-rate4000-shared · original](../classic-full/hard.partition-admission-isolation--full--1.html#pair=11) | 119,952 / 48 / 0 | 109,504 / 10,496 / 0 | 2,237.458 / 7.373 | 14,788 / 27,264 |
| [independent-rate4000-shared · common](../classic-full/hard.partition-admission-isolation--full--1.html#pair=5) | 119,952 / 48 / 0 | 109,456 / 10,544 / 0 | 2,237.458 / 7.390 | 14,788 / 22,749 |

## hard.rolling-restart

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [overlap0s · original](../classic-full/hard.rolling-restart--full--1.html#pair=2) | 96,565 / 0 / 0 | 73,071 / 0 / 0 | 10.985 / 10.000 | 17,974 / 23,653 |
| [overlap0s · common](../classic-full/hard.rolling-restart--full--1.html#pair=0) | 96,565 / 0 / 0 | 71,721 / 0 / 0 | 10.985 / 20.000 | 17,974 / 19,121 |
| [overlap1s · original](../classic-full/hard.rolling-restart--full--1.html#pair=3) | 61,995 / 0 / 0 | 47,589 / 0 / 0 | 11.407 / 10.000 | 11,924 / 14,915 |
| [overlap1s · common](../classic-full/hard.rolling-restart--full--1.html#pair=1) | 61,995 / 0 / 0 | 47,226 / 0 / 0 | 11.407 / 18.000 | 11,924 / 12,399 |

## hard.short-vs-long-outage

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [outage2s-request2000ms · original](../classic-full/hard.short-vs-long-outage--full--1.html#pair=4) | 18,936 / 0 / 0 | 15,996 / 0 / 0 | 16.012 / 28.000 | 5,589 / 10,373 |
| [outage2s-request2000ms · common](../classic-full/hard.short-vs-long-outage--full--1.html#pair=0) | 18,936 / 0 / 0 | 15,357 / 0 / 0 | 16.012 / 20.000 | 5,589 / 5,763 |
| [outage2s-request200ms · original](../classic-full/hard.short-vs-long-outage--full--1.html#pair=5) | 18,936 / 0 / 0 | 15,996 / 0 / 0 | 16.012 / 28.000 | 5,589 / 10,373 |
| [outage2s-request200ms · common](../classic-full/hard.short-vs-long-outage--full--1.html#pair=1) | 18,936 / 0 / 0 | 15,357 / 0 / 0 | 16.012 / 20.000 | 5,589 / 5,763 |
| [outage8s-request2000ms · original](../classic-full/hard.short-vs-long-outage--full--1.html#pair=6) | 13,018 / 0 / 48 | 11,058 / 0 / 48 | 16.012 / 28.000 | 3,926 / 7,303 |
| [outage8s-request2000ms · common](../classic-full/hard.short-vs-long-outage--full--1.html#pair=2) | 13,018 / 0 / 48 | 10,617 / 0 / 48 | 16.012 / 20.000 | 3,926 / 4,032 |
| [outage8s-request200ms · original](../classic-full/hard.short-vs-long-outage--full--1.html#pair=7) | 13,018 / 0 / 48 | 11,058 / 0 / 48 | 16.012 / 28.000 | 3,926 / 7,303 |
| [outage8s-request200ms · common](../classic-full/hard.short-vs-long-outage--full--1.html#pair=3) | 13,018 / 0 / 48 | 10,617 / 0 / 48 | 16.012 / 20.000 | 3,926 / 4,032 |

## resources.delivery-timeout-tuning

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [deadline2s · original](../classic-full/resources.delivery-timeout-tuning--full--1.html#pair=3) | 25,129 / 0 / 48 | 21,803 / 0 / 48 | 16.012 / 28.000 | 7,628 / 14,302 |
| [deadline2s · common](../classic-full/resources.delivery-timeout-tuning--full--1.html#pair=0) | 25,129 / 0 / 48 | 20,818 / 0 / 48 | 16.012 / 20.000 | 7,628 / 7,764 |
| [deadline30s · original](../classic-full/resources.delivery-timeout-tuning--full--1.html#pair=4) | 32,957 / 0 / 0 | 27,321 / 0 / 0 | 16.012 / 28.000 | 9,765 / 17,890 |
| [deadline30s · common](../classic-full/resources.delivery-timeout-tuning--full--1.html#pair=1) | 32,957 / 0 / 0 | 26,095 / 0 / 0 | 16.012 / 20.000 | 9,765 / 10,006 |
| [deadline6s · original](../classic-full/resources.delivery-timeout-tuning--full--1.html#pair=5) | 32,957 / 0 / 0 | 27,321 / 0 / 0 | 16.012 / 28.000 | 9,765 / 17,890 |
| [deadline6s · common](../classic-full/resources.delivery-timeout-tuning--full--1.html#pair=2) | 32,957 / 0 / 0 | 26,095 / 0 / 0 | 16.012 / 20.000 | 9,765 / 10,006 |

## resources.memory-bounded-overload

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [value2048 · original](../classic-full/resources.memory-bounded-overload--full--1.html#pair=2) | 9,883 / 310,141 / 0 | 5,095 / 314,929 / 0 | 200.128 / 363.406 | 6,650 / 3,849 |
| [value2048 · common](../classic-full/resources.memory-bounded-overload--full--1.html#pair=0) | 9,883 / 310,141 / 0 | 13,046 / 306,978 / 0 | 200.128 / 900.656 | 6,650 / 5,977 |
| [value512 · original](../classic-full/resources.memory-bounded-overload--full--1.html#pair=3) | 66,753 / 253,271 / 0 | 6,257 / 313,767 / 0 | 229.385 / 1,128.938 | 6,685 / 4,105 |
| [value512 · common](../classic-full/resources.memory-bounded-overload--full--1.html#pair=1) | 66,753 / 253,271 / 0 | 43,251 / 276,773 / 0 | 229.385 / 256.156 | 6,685 / 7,236 |

## resources.stop-polling-backpressure

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [events1024 · original](../classic-full/resources.stop-polling-backpressure--full--1.html#pair=2) | 29,980 / 20 / 0 | 27,017 / 2,983 / 0 | 1,853.000 / 1,868.500 | 7,494 / 11,397 |
| [events1024 · common](../classic-full/resources.stop-polling-backpressure--full--1.html#pair=0) | 29,980 / 20 / 0 | 27,017 / 2,983 / 0 | 1,853.000 / 1,868.500 | 7,494 / 11,397 |
| [events64 · original](../classic-full/resources.stop-polling-backpressure--full--1.html#pair=3) | 29,980 / 20 / 0 | 26,057 / 3,943 / 0 | 1,853.000 / 6.500 | 7,494 / 10,992 |
| [events64 · common](../classic-full/resources.stop-polling-backpressure--full--1.html#pair=1) | 29,980 / 20 / 0 | 26,057 / 3,943 / 0 | 1,853.000 / 7.000 | 7,494 / 10,992 |

## resources.wire-window-vs-latency

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [window1024k · original](../classic-full/resources.wire-window-vs-latency--full--1.html#pair=3) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 165.020 / 206.000 | 379 / 497 |
| [window1024k · common](../classic-full/resources.wire-window-vs-latency--full--1.html#pair=0) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 165.020 / 206.000 | 379 / 292 |
| [window256k · original](../classic-full/resources.wire-window-vs-latency--full--1.html#pair=4) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 165.020 / 206.000 | 379 / 497 |
| [window256k · common](../classic-full/resources.wire-window-vs-latency--full--1.html#pair=1) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 165.020 / 206.000 | 379 / 292 |
| [window64k · original](../classic-full/resources.wire-window-vs-latency--full--1.html#pair=5) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 165.020 / 206.000 | 379 / 497 |
| [window64k · common](../classic-full/resources.wire-window-vs-latency--full--1.html#pair=2) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 165.020 / 206.000 | 379 / 292 |

## soft.blackhole-vs-failfast

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [blackhole-request1000ms · original](../classic-full/soft.blackhole-vs-failfast--full--1.html#pair=4) | 55,690 / 0 / 0 | 40,103 / 0 / 0 | 11.408 / 10.000 | 6,382 / 12,997 |
| [blackhole-request1000ms · common](../classic-full/soft.blackhole-vs-failfast--full--1.html#pair=0) | 55,690 / 0 / 0 | 34,440 / 0 / 0 | 11.408 / 20.000 | 6,382 / 8,617 |
| [blackhole-request200ms · original](../classic-full/soft.blackhole-vs-failfast--full--1.html#pair=5) | 55,420 / 0 / 0 | 40,071 / 0 / 0 | 11.409 / 10.000 | 6,360 / 12,987 |
| [blackhole-request200ms · common](../classic-full/soft.blackhole-vs-failfast--full--1.html#pair=1) | 55,420 / 0 / 0 | 34,553 / 0 / 0 | 11.409 / 20.000 | 6,360 / 8,591 |
| [failfast-request1000ms · original](../classic-full/soft.blackhole-vs-failfast--full--1.html#pair=6) | 55,596 / 0 / 0 | 40,044 / 0 / 0 | 11.408 / 10.000 | 6,359 / 12,945 |
| [failfast-request1000ms · common](../classic-full/soft.blackhole-vs-failfast--full--1.html#pair=2) | 55,596 / 0 / 0 | 35,474 / 0 / 0 | 11.408 / 20.000 | 6,359 / 8,363 |
| [failfast-request200ms · original](../classic-full/soft.blackhole-vs-failfast--full--1.html#pair=7) | 55,596 / 0 / 0 | 40,044 / 0 / 0 | 11.408 / 10.000 | 6,359 / 12,945 |
| [failfast-request200ms · common](../classic-full/soft.blackhole-vs-failfast--full--1.html#pair=3) | 55,596 / 0 / 0 | 35,474 / 0 / 0 | 11.408 / 20.000 | 6,359 / 8,363 |

## soft.degrading-broker-ramp

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [request1000ms · original](../classic-full/soft.degrading-broker-ramp--full--1.html#pair=2) | 62,387 / 0 / 0 | 49,938 / 0 / 0 | 678.324 / 764.000 | 9,812 / 16,741 |
| [request1000ms · common](../classic-full/soft.degrading-broker-ramp--full--1.html#pair=0) | 62,387 / 0 / 0 | 47,478 / 0 / 0 | 678.324 / 812.000 | 9,812 / 11,588 |
| [request200ms · original](../classic-full/soft.degrading-broker-ramp--full--1.html#pair=3) | 58,424 / 0 / 0 | 46,031 / 0 / 0 | 304.254 / 231.000 | 9,489 / 14,807 |
| [request200ms · common](../classic-full/soft.degrading-broker-ramp--full--1.html#pair=1) | 58,424 / 0 / 0 | 41,858 / 0 / 0 | 304.254 / 175.000 | 9,489 / 10,562 |

## soft.disconnect-storm

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [i1 · original](../classic-full/soft.disconnect-storm--full--1.html#pair=2) | 88,441 / 0 / 0 | 49,483 / 0 / 0 | 33.428 / 65.000 | 10,724 / 20,608 |
| [i1 · common](../classic-full/soft.disconnect-storm--full--1.html#pair=0) | 88,441 / 0 / 0 | 51,641 / 0 / 0 | 33.428 / 79.000 | 10,724 / 10,900 |
| [i5 · original](../classic-full/soft.disconnect-storm--full--1.html#pair=3) | 25,901 / 0 / 0 | 25,184 / 0 / 0 | 92.474 / 129.000 | 11,870 / 20,020 |
| [i5 · common](../classic-full/soft.disconnect-storm--full--1.html#pair=1) | 25,901 / 0 / 0 | 20,991 / 0 / 0 | 92.474 / 149.000 | 11,870 / 11,556 |

## soft.high-jitter

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [jitter0ms-i1 · original](../classic-full/soft.high-jitter--full--1.html#pair=4) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 20.999 / 15.000 | 499 / 1,192 |
| [jitter0ms-i1 · common](../classic-full/soft.high-jitter--full--1.html#pair=0) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 20.999 / 16.000 | 499 / 691 |
| [jitter0ms-i5 · original](../classic-full/soft.high-jitter--full--1.html#pair=5) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 14.001 / 13.000 | 763 / 1,334 |
| [jitter0ms-i5 · common](../classic-full/soft.high-jitter--full--1.html#pair=1) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 14.001 / 14.000 | 763 / 769 |
| [jitter5ms-i1 · original](../classic-full/soft.high-jitter--full--1.html#pair=6) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 55.859 / 43.000 | 507 / 1,180 |
| [jitter5ms-i1 · common](../classic-full/soft.high-jitter--full--1.html#pair=2) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 55.859 / 44.000 | 507 / 652 |
| [jitter5ms-i5 · original](../classic-full/soft.high-jitter--full--1.html#pair=7) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 74.012 / 79.000 | 1,588 / 2,490 |
| [jitter5ms-i5 · common](../classic-full/soft.high-jitter--full--1.html#pair=3) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 74.012 / 84.000 | 1,588 / 1,966 |

## soft.metadata-loss-during-move

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [metadata1000ms · original](../classic-full/soft.metadata-loss-during-move--full--1.html#pair=2) | 68,701 / 0 / 0 | 44,865 / 0 / 0 | 10.608 / 18.000 | 6,567 / 13,719 |
| [metadata1000ms · common](../classic-full/soft.metadata-loss-during-move--full--1.html#pair=0) | 65,644 / 0 / 0 | 36,541 / 0 / 0 | 14.812 / 24.000 | 6,404 / 8,632 |
| [metadata20ms · original](../classic-full/soft.metadata-loss-during-move--full--1.html#pair=3) | 63,074 / 0 / 0 | 44,572 / 0 / 0 | 12.408 / 20.000 | 6,560 / 13,602 |
| [metadata20ms · common](../classic-full/soft.metadata-loss-during-move--full--1.html#pair=1) | 61,586 / 0 / 0 | 36,453 / 0 / 0 | 20.216 / 24.000 | 6,433 / 8,621 |

## soft.one-way-loss-responses

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [outage3000ms-i1 · original](../classic-full/soft.one-way-loss-responses--full--1.html#pair=4) | 72,095 / 0 / 0 | 41,169 / 0 / 0 | 13.211 / 22.000 | 8,709 / 16,589 |
| [outage3000ms-i1 · common](../classic-full/soft.one-way-loss-responses--full--1.html#pair=0) | 72,095 / 0 / 0 | 45,010 / 0 / 0 | 13.211 / 18.000 | 8,709 / 8,861 |
| [outage3000ms-i5 · original](../classic-full/soft.one-way-loss-responses--full--1.html#pair=5) | 21,597 / 0 / 0 | 21,092 / 0 / 0 | 24.017 / 28.000 | 9,585 / 16,652 |
| [outage3000ms-i5 · common](../classic-full/soft.one-way-loss-responses--full--1.html#pair=1) | 21,597 / 0 / 0 | 18,067 / 0 / 0 | 24.017 / 28.000 | 9,585 / 9,785 |
| [outage300ms-i1 · original](../classic-full/soft.one-way-loss-responses--full--1.html#pair=6) | 69,762 / 0 / 0 | 40,699 / 0 / 0 | 13.211 / 22.000 | 8,106 / 15,991 |
| [outage300ms-i1 · common](../classic-full/soft.one-way-loss-responses--full--1.html#pair=2) | 69,762 / 0 / 0 | 44,002 / 0 / 0 | 13.211 / 18.000 | 8,106 / 8,250 |
| [outage300ms-i5 · original](../classic-full/soft.one-way-loss-responses--full--1.html#pair=7) | 19,265 / 0 / 0 | 20,412 / 0 / 0 | 24.017 / 28.000 | 8,980 / 15,976 |
| [outage300ms-i5 · common](../classic-full/soft.one-way-loss-responses--full--1.html#pair=3) | 19,265 / 0 / 0 | 17,385 / 0 / 0 | 24.017 / 28.000 | 8,980 / 9,111 |

## soft.retriable-error-storm

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [error19-slow0 · original](../classic-full/soft.retriable-error-storm--full--1.html#pair=6) | 54,522 / 0 / 0 | 39,632 / 0 / 0 | 11.409 / 10.000 | 6,275 / 12,856 |
| [error19-slow0 · common](../classic-full/soft.retriable-error-storm--full--1.html#pair=0) | 54,522 / 0 / 0 | 33,854 / 0 / 0 | 11.409 / 20.000 | 6,275 / 8,467 |
| [error19-slow1 · original](../classic-full/soft.retriable-error-storm--full--1.html#pair=7) | 54,232 / 0 / 0 | 35,463 / 0 / 0 | 11.408 / 10.000 | 6,175 / 11,673 |
| [error19-slow1 · common](../classic-full/soft.retriable-error-storm--full--1.html#pair=1) | 54,232 / 0 / 0 | 33,727 / 0 / 0 | 11.408 / 20.000 | 6,175 / 8,272 |
| [error6-slow0 · original](../classic-full/soft.retriable-error-storm--full--1.html#pair=8) | 69,026 / 0 / 0 | 44,895 / 0 / 0 | 10.608 / 19.000 | 6,610 / 13,489 |
| [error6-slow0 · common](../classic-full/soft.retriable-error-storm--full--1.html#pair=2) | 69,026 / 0 / 0 | 36,569 / 0 / 0 | 10.608 / 24.000 | 6,610 / 8,630 |
| [error6-slow1 · original](../classic-full/soft.retriable-error-storm--full--1.html#pair=9) | 68,020 / 0 / 0 | 45,095 / 0 / 0 | 10.809 / 16.000 | 6,565 / 13,581 |
| [error6-slow1 · common](../classic-full/soft.retriable-error-storm--full--1.html#pair=3) | 68,020 / 0 / 0 | 36,434 / 0 / 0 | 10.809 / 24.000 | 6,565 / 8,593 |
| [error7-slow0 · original](../classic-full/soft.retriable-error-storm--full--1.html#pair=10) | 54,522 / 0 / 0 | 39,632 / 0 / 0 | 11.409 / 10.000 | 6,275 / 12,856 |
| [error7-slow0 · common](../classic-full/soft.retriable-error-storm--full--1.html#pair=4) | 54,522 / 0 / 0 | 33,854 / 0 / 0 | 11.409 / 20.000 | 6,275 / 8,467 |
| [error7-slow1 · original](../classic-full/soft.retriable-error-storm--full--1.html#pair=11) | 54,232 / 0 / 0 | 35,463 / 0 / 0 | 11.408 / 10.000 | 6,175 / 11,673 |
| [error7-slow1 · common](../classic-full/soft.retriable-error-storm--full--1.html#pair=5) | 54,232 / 0 / 0 | 33,727 / 0 / 0 | 11.408 / 20.000 | 6,175 / 8,272 |

## soft.slow-broker-window

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [lanes1-i1 · original](../classic-full/soft.slow-broker-window--full--1.html#pair=4) | 63,181 / 0 / 0 | 48,894 / 0 / 0 | 154.404 / 293.000 | 7,633 / 9,537 |
| [lanes1-i1 · common](../classic-full/soft.slow-broker-window--full--1.html#pair=0) | 63,181 / 0 / 0 | 48,898 / 0 / 0 | 154.404 / 293.000 | 7,633 / 9,526 |
| [lanes1-i5 · original](../classic-full/soft.slow-broker-window--full--1.html#pair=5) | 59,306 / 0 / 0 | 35,781 / 0 / 0 | 12.409 / 27.000 | 7,436 / 9,180 |
| [lanes1-i5 · common](../classic-full/soft.slow-broker-window--full--1.html#pair=1) | 59,306 / 0 / 0 | 35,063 / 0 / 0 | 12.409 / 27.000 | 7,436 / 9,522 |
| [lanes4-i1 · original](../classic-full/soft.slow-broker-window--full--1.html#pair=6) | 63,181 / 0 / 0 | 46,881 / 0 / 0 | 154.404 / 294.000 | 7,633 / 15,061 |
| [lanes4-i1 · common](../classic-full/soft.slow-broker-window--full--1.html#pair=2) | 63,181 / 0 / 0 | 48,898 / 0 / 0 | 154.404 / 293.000 | 7,633 / 9,526 |
| [lanes4-i5 · original](../classic-full/soft.slow-broker-window--full--1.html#pair=7) | 59,306 / 0 / 0 | 43,163 / 0 / 0 | 12.409 / 10.000 | 7,436 / 13,957 |
| [lanes4-i5 · common](../classic-full/soft.slow-broker-window--full--1.html#pair=3) | 59,306 / 0 / 0 | 35,063 / 0 / 0 | 12.409 / 27.000 | 7,436 / 9,522 |

## soft.slow-setup

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [request2000ms · original](../classic-full/soft.slow-setup--full--1.html#pair=2) | 71,889 / 0 / 0 | 51,020 / 0 / 0 | 11.409 / 10.000 | 8,071 / 16,325 |
| [request2000ms · common](../classic-full/soft.slow-setup--full--1.html#pair=0) | 71,889 / 0 / 0 | 46,450 / 0 / 0 | 11.409 / 20.000 | 8,071 / 9,994 |
| [request200ms · original](../classic-full/soft.slow-setup--full--1.html#pair=3) | 55,671 / 0 / 0 | 40,427 / 0 / 0 | 11.409 / 10.000 | 6,512 / 13,024 |
| [request200ms · common](../classic-full/soft.slow-setup--full--1.html#pair=1) | 55,671 / 0 / 0 | 35,787 / 0 / 0 | 11.409 / 20.000 | 6,512 / 8,493 |

## soft.sustained-random-loss

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [loss1-slow0 · original](../classic-full/soft.sustained-random-loss--full--1.html#pair=4) | 6,859 / 0 / 15 | 58,371 / 0 / 0 | 20.620 / 34.000 | 1,687 / 18,109 |
| [loss1-slow0 · common](../classic-full/soft.sustained-random-loss--full--1.html#pair=0) | 85,121 / 0 / 0 | 50,324 / 0 / 0 | 25.220 / 29.000 | 9,230 / 11,415 |
| [loss1-slow1 · original](../classic-full/soft.sustained-random-loss--full--1.html#pair=5) | 6,828 / 0 / 23 | 41,951 / 0 / 0 | 13.212 / 161.000 | 1,686 / 13,084 |
| [loss1-slow1 · common](../classic-full/soft.sustained-random-loss--full--1.html#pair=1) | 65,985 / 0 / 0 | 36,355 / 0 / 0 | 113.670 / 174.000 | 7,573 / 8,503 |
| [loss5-slow0 · original](../classic-full/soft.sustained-random-loss--full--1.html#pair=6) | 6,859 / 0 / 15 | 42,960 / 0 / 0 | 20.620 / 186.000 | 1,687 / 14,107 |
| [loss5-slow0 · common](../classic-full/soft.sustained-random-loss--full--1.html#pair=2) | 71,767 / 0 / 0 | 44,356 / 0 / 0 | 41.630 / 61.000 | 8,279 / 10,478 |
| [loss5-slow1 · original](../classic-full/soft.sustained-random-loss--full--1.html#pair=7) | 6,828 / 0 / 23 | 25,391 / 0 / 0 | 13.212 / 389.000 | 1,686 / 8,621 |
| [loss5-slow1 · common](../classic-full/soft.sustained-random-loss--full--1.html#pair=3) | 36,527 / 0 / 0 | 21,108 / 0 / 0 | 254.127 / 417.000 | 5,138 / 5,006 |

## soft.throttle-window

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [i1 · original](../classic-full/soft.throttle-window--full--1.html#pair=2) | 58,948 / 0 / 0 | 41,839 / 0 / 0 | 11.809 / 10.000 | 6,628 / 13,153 |
| [i1 · common](../classic-full/soft.throttle-window--full--1.html#pair=0) | 58,948 / 0 / 0 | 44,163 / 0 / 0 | 11.809 / 14.000 | 6,628 / 8,361 |
| [i5 · original](../classic-full/soft.throttle-window--full--1.html#pair=3) | 56,565 / 0 / 0 | 41,839 / 0 / 0 | 12.409 / 10.000 | 6,679 / 13,652 |
| [i5 · common](../classic-full/soft.throttle-window--full--1.html#pair=1) | 56,565 / 0 / 0 | 35,239 / 0 / 0 | 12.409 / 28.000 | 6,679 / 8,813 |

## soft.tiny-chunk-transport

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [window1024k-i1 · original](../classic-full/soft.tiny-chunk-transport--full--1.html#pair=4) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 10.077 / 9.000 | 431 / 767 |
| [window1024k-i1 · common](../classic-full/soft.tiny-chunk-transport--full--1.html#pair=0) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 10.077 / 12.000 | 431 / 766 |
| [window1024k-i5 · original](../classic-full/soft.tiny-chunk-transport--full--1.html#pair=5) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 10.036 / 9.000 | 428 / 767 |
| [window1024k-i5 · common](../classic-full/soft.tiny-chunk-transport--full--1.html#pair=1) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 10.036 / 10.000 | 428 / 638 |
| [window64k-i1 · original](../classic-full/soft.tiny-chunk-transport--full--1.html#pair=6) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 10.077 / 9.000 | 431 / 767 |
| [window64k-i1 · common](../classic-full/soft.tiny-chunk-transport--full--1.html#pair=2) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 10.077 / 12.000 | 431 / 766 |
| [window64k-i5 · original](../classic-full/soft.tiny-chunk-transport--full--1.html#pair=7) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 10.036 / 9.000 | 428 / 767 |
| [window64k-i5 · common](../classic-full/soft.tiny-chunk-transport--full--1.html#pair=3) | 4,096 / 0 / 0 | 4,096 / 0 / 0 | 10.036 / 10.000 | 428 / 638 |

## topology.delete-recreate

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [reopen0 · original](../classic-full/topology.delete-recreate--full--1.html#pair=2) | 4,512 / 0 / 0 | 4,112 / 391 / 9 | 127.381 / 8.000 | 407 / 335 |
| [reopen0 · common](../classic-full/topology.delete-recreate--full--1.html#pair=0) | 4,512 / 0 / 0 | 4,112 / 391 / 9 | 127.381 / 19.000 | 407 / 191 |
| [reopen1 · original](../classic-full/topology.delete-recreate--full--1.html#pair=3) | 4,512 / 0 / 0 | 4,512 / 0 / 0 | 127.381 / 8.000 | 402 / 433 |
| [reopen1 · common](../classic-full/topology.delete-recreate--full--1.html#pair=1) | 4,512 / 0 / 0 | 4,512 / 0 / 0 | 127.381 / 19.000 | 402 / 289 |

## topology.leader-rebalance-churn

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [i1 · original](../classic-full/topology.leader-rebalance-churn--full--1.html#pair=2) | 274,664 / 0 / 0 | 198,139 / 0 / 0 | 10.809 / 10.000 | 49,539 / 65,244 |
| [i1 · common](../classic-full/topology.leader-rebalance-churn--full--1.html#pair=0) | 274,664 / 0 / 0 | 205,914 / 0 / 0 | 10.809 / 14.000 | 49,539 / 53,878 |
| [i5 · original](../classic-full/topology.leader-rebalance-churn--full--1.html#pair=3) | 273,963 / 0 / 0 | 198,257 / 0 / 0 | 10.407 / 10.000 | 50,564 / 66,468 |
| [i5 · common](../classic-full/topology.leader-rebalance-churn--full--1.html#pair=1) | 273,963 / 0 / 0 | 187,613 / 0 / 0 | 10.407 / 24.000 | 50,564 / 53,327 |

## topology.multi-topic-isolation

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [lanes1 · original](../classic-full/topology.multi-topic-isolation--full--1.html#pair=2) | 48,641 / 0 / 0 | 57,912 / 0 / 0 | 16.012 / 9.000 | 7,073 / 7,237 |
| [lanes1 · common](../classic-full/topology.multi-topic-isolation--full--1.html#pair=0) | 48,641 / 0 / 0 | 57,912 / 0 / 0 | 16.012 / 9.000 | 7,073 / 7,237 |
| [lanes4 · original](../classic-full/topology.multi-topic-isolation--full--1.html#pair=3) | 48,641 / 0 / 0 | 57,912 / 0 / 0 | 16.012 / 9.000 | 7,073 / 7,237 |
| [lanes4 · common](../classic-full/topology.multi-topic-isolation--full--1.html#pair=1) | 48,641 / 0 / 0 | 57,912 / 0 / 0 | 16.012 / 9.000 | 7,073 / 7,237 |

## topology.partition-expansion

| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |
| --- | ---: | ---: | ---: | ---: |
| [metadata1000ms · original](../classic-full/topology.partition-expansion--full--1.html#pair=2) | 9,112 / 0 / 0 | 9,112 / 0 / 0 | 124.374 / 7.000 | 2,844 / 5,335 |
| [metadata1000ms · common](../classic-full/topology.partition-expansion--full--1.html#pair=0) | 9,112 / 0 / 0 | 9,112 / 0 / 0 | 124.374 / 17.000 | 2,844 / 5,191 |
| [metadata20ms · original](../classic-full/topology.partition-expansion--full--1.html#pair=3) | 9,112 / 0 / 0 | 9,112 / 0 / 0 | 125.377 / 7.000 | 2,844 / 5,335 |
| [metadata20ms · common](../classic-full/topology.partition-expansion--full--1.html#pair=1) | 9,112 / 0 / 0 | 9,112 / 0 / 0 | 125.377 / 17.000 | 2,844 / 5,191 |

† At least one fault rule had no matching opportunity; inspect the coverage gap before comparing fault effects.
