//! Capture full evidence plus the compact committed compatibility fingerprint.
//! Run only before a deliberate behavior/schema change; do not refresh goldens
//! merely because a regression test fails.
#[path = "../tests/support/legacy.rs"]
mod legacy;
use kr_kafka_sim::{PINNED_CASES, campaign_manifest, run};
use serde_json::json;
use std::{fs, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("expected output directory")?,
    );
    fs::create_dir_all(&directory)?;
    let mut cases = Vec::new();
    for &(seed, variant) in PINNED_CASES {
        let report = run(&campaign_manifest(seed, variant)?)?;
        fs::write(
            directory.join(format!("{}-{seed}.json", variant.as_str())),
            serde_json::to_vec(&report)?,
        )?;
        let projected = legacy::project(&report);
        cases.push(json!({
            "seed": seed, "variant": variant,
            "checkpoint": report.checkpoint,
            "legacy_sha256": legacy::digest(&projected),
        }));
    }
    fs::write(
        directory.join("pinned.json"),
        serde_json::to_vec_pretty(&cases)?,
    )?;
    Ok(())
}
