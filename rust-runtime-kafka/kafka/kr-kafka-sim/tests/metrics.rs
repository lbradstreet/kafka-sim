use kr_kafka_sim::{CampaignLimits, ReplayManifest, run, verify_trace_transparency};

#[test]
fn hdr_recording_changes_neither_delivery_history_nor_runtime_checkpoint() {
    for seed in [0, 3, 9, 36] {
        let mut enabled = ReplayManifest::from_seed(seed, CampaignLimits::default()).unwrap();
        enabled.producer.metrics.max_broker_scopes = 2;
        enabled.producer.metrics.max_partition_scopes = 8;
        enabled.producer.metrics.significant_digits = 2;
        let recorded = run(&enabled).unwrap_or_else(|failure| panic!("{failure}"));
        let mut disabled = enabled.clone();
        disabled.producer.metrics.enabled = false;
        let plain = run(&disabled).unwrap_or_else(|failure| panic!("{failure}"));
        assert!(
            recorded.metrics_counts.iter().any(|count| *count != 0),
            "seed={seed}"
        );
        assert!(
            plain.metrics_counts.iter().all(|count| *count == 0),
            "seed={seed}"
        );
        assert_eq!(recorded.history, plain.history, "seed={seed}");
        assert_eq!(recorded.coverage, plain.coverage, "seed={seed}");
        assert_eq!(recorded.checkpoint, plain.checkpoint, "seed={seed}");
        assert_eq!(recorded.pool_peaks, plain.pool_peaks, "seed={seed}");
        if seed == 9 {
            verify_trace_transparency(&enabled).unwrap();
        }
    }
}
