#![forbid(unsafe_code)]
fn main() {
    if let Err(error) = run() {
        eprintln!("benchmark failed: {error}");
        std::process::exit(1);
    }
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: kr-kafka-bench PROFILE.json RESULT.json".into());
    }
    let profile = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let report = kr_kafka_bench::host::run(profile)?;
    std::fs::write(&args[2], serde_json::to_vec_pretty(&report)?)?;
    if report["complete"] != true {
        return Err("run incomplete; diagnostics preserved in result".into());
    }
    Ok(())
}
