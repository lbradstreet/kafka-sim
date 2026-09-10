//! Controlled Raw/default comparison on the current native simulation driver.
//! Every run is replayed and payload-checked; complete-history hashes and the
//! initial manifest make the bounded measurements reproducible.
use kr_kafka_experiments::{Size, catalogue, derive_checked};
use kr_kafka_producer::config::BatchTargetMode;
use sha2::{Digest, Sha256};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scenario = catalogue()
        .into_iter()
        .find(|scenario| scenario.id == "baseline.compression")
        .expect("compression catalogue");
    for seed in [0, 1, 7] {
        for variant in &scenario.variants {
            for mode in [BatchTargetMode::Raw, BatchTargetMode::EstimatedWire] {
                let mut manifest = scenario.build(variant, seed, Size::Full)?;
                manifest.producer.batch_target_mode = mode;
                let run = kr_kafka_sim::run_replayed(&manifest)?;
                assert_eq!(run.coverage.acked, 4096);
                assert_eq!(run.coverage.not_written + run.coverage.unknown, 0);
                let report = derive_checked(&scenario, variant, Size::Full, true, &run)?;
                let history_sha256: String = Sha256::digest(serde_json::to_vec(&run.history)?)
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect();
                println!(
                    "{}",
                    serde_json::json!({
                        "schema": "kr-native-batching-control/v1",
                        "scenario": scenario.id, "variant": variant.name, "seed": seed,
                        "mode": format!("{mode:?}"), "size": "full", "replay_verified": true,
                        "initial_manifest": manifest, "checkpoint": run.checkpoint,
                        "history_sha256": history_sha256, "coverage": run.coverage,
                        "pool_peaks": run.pool_peaks, "summary": report.summary,
                    })
                );
            }
        }
    }
    Ok(())
}
