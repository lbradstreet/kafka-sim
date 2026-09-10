//! Resource pilot: use /usr/bin/time -l for peak resident bytes. Deliberately
//! retain the exact realized manifest and checkpoint beside the chart report.
use kr_kafka_experiments::{Size, catalogue, derive_checked};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let id = args
        .get(1)
        .map(String::as_str)
        .unwrap_or("baseline.open-loop-rate");
    let variant = args.get(2).map(String::as_str).unwrap_or("rate32000");
    let size = if args.get(3).is_some_and(|s| s == "test") {
        Size::Test
    } else {
        Size::Full
    };
    let replay = args.iter().any(|s| s == "--replay");
    let s = catalogue()
        .into_iter()
        .find(|s| s.id == id)
        .ok_or("scenario")?;
    let v = s
        .variants
        .iter()
        .find(|v| v.name == variant)
        .ok_or("variant")?;
    let m = s.build(v, 0, size)?;
    let start = std::time::Instant::now();
    let run = if replay {
        kr_kafka_sim::run_replayed(&m)
    } else {
        kr_kafka_sim::run(&m)
    };
    let out = std::path::PathBuf::from(format!("target/experiments/pilot-{id}-{variant}"));
    std::fs::create_dir_all(&out)?;
    let run = match run {
        Ok(run) => run,
        Err(failure) => {
            serde_json::to_writer(std::fs::File::create(out.join("failure.json"))?, &failure)?;
            return Err(failure.to_string().into());
        }
    };
    let execution_ms = start.elapsed().as_millis();
    {
        use std::io::Write;
        let mut writer = std::io::BufWriter::with_capacity(
            1024 * 1024,
            std::fs::File::create(out.join("replay.json"))?,
        );
        serde_json::to_writer(&mut writer, &run.manifest)?;
        writer.flush()?;
    }
    serde_json::to_writer(
        std::fs::File::create(out.join("checkpoint.json"))?,
        &run.checkpoint,
    )?;
    let report = derive_checked(&s, v, size, replay, &run)?;
    let bytes = report.to_json()?;
    std::fs::write(out.join("report.json"), &bytes)?;
    let measurements = serde_json::json!({"scenario":id,"variant":variant,"size":size,"replay":replay,"execution_ms":execution_ms,"total_ms":start.elapsed().as_millis(),"offered":run.coverage.offered,"accepted":run.coverage.accepted,"refused":run.coverage.refused,"history_events":run.history.entries.len(),"decisions":run.fault_stats.decisions,"steps":run.checkpoint.total_steps,"duration_ns":report.meta["duration"],"report_bytes":bytes.len(),"sample_rows":report.records["count"],"summary":report.summary});
    serde_json::to_writer_pretty(
        std::fs::File::create(out.join("measurements.json"))?,
        &measurements,
    )?;
    println!("{measurements}");
    Ok(())
}
