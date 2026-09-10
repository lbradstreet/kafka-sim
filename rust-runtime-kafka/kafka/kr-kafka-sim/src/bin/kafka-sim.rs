use kr_kafka_sim::{
    CampaignLimits, CampaignVariant, Coverage, PINNED_CASES, ReplayManifest, Workload,
    campaign_cases, campaign_manifest, run_and_retain_failure,
};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

const SEED_WATCHDOG_MS: u64 = 120_000;
const CAMPAIGN_WATCHDOG_MS: u64 = 900_000;
const HELP: &str = "kafka-sim [--seed N [--variant legacy|clean|finite-fault|isolation] | \
--campaign [--seed-start N] [--seed-count N] | --replay PATH] \
[--failure-dir PATH] [--watchdog-ms N]\n\
Default: seed 0, legacy variant. --variant may also select a variant of seed 0.\n\
Campaign seed-count is the expansion range length (default 128), not the final case count; \
pinned cases are retained and duplicates removed. The actual count is printed.\n\
Default watchdog: 120000 ms for one seed/replay, 900000 ms for a campaign.";

#[derive(Debug, PartialEq, Eq)]
enum Mode {
    Seed { seed: u64, variant: CampaignVariant },
    Campaign { start: u64, count: u64 },
    Replay(PathBuf),
}

#[derive(Debug, PartialEq, Eq)]
struct Options {
    mode: Mode,
    directory: PathBuf,
    watchdog_ms: u64,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Option<Options>, String> {
    let mut args = args.into_iter().peekable();
    if args.peek().is_some_and(|arg| arg == "--help") {
        args.next();
        return if args.next().is_none() {
            Ok(None)
        } else {
            Err("--help cannot be combined with other arguments".into())
        };
    }
    let mut seed = None;
    let mut variant = None;
    let mut start = None;
    let mut count = None;
    let mut replay = None;
    let mut campaign = false;
    let mut directory = None;
    let mut watchdog = None;
    while let Some(arg) = args.next() {
        if arg == "--campaign" {
            if campaign {
                return Err("duplicate --campaign".into());
            }
            campaign = true;
            continue;
        }
        if !matches!(
            arg.as_str(),
            "--seed"
                | "--variant"
                | "--seed-start"
                | "--seed-count"
                | "--replay"
                | "--failure-dir"
                | "--watchdog-ms"
        ) {
            return Err(format!("unknown argument: {arg}"));
        }
        let value = args.next().ok_or_else(|| format!("missing {arg} value"))?;
        if value.starts_with("--") {
            return Err(format!("missing {arg} value before {value}"));
        }
        match arg.as_str() {
            "--seed" => set_once(&mut seed, parse_number(&arg, &value)?, &arg)?,
            "--variant" => set_once(&mut variant, value.parse()?, &arg)?,
            "--seed-start" => set_once(&mut start, parse_number(&arg, &value)?, &arg)?,
            "--seed-count" => set_once(&mut count, parse_number(&arg, &value)?, &arg)?,
            "--replay" => set_once(&mut replay, PathBuf::from(value), &arg)?,
            "--failure-dir" => set_once(&mut directory, PathBuf::from(value), &arg)?,
            "--watchdog-ms" => set_once(&mut watchdog, parse_number(&arg, &value)?, &arg)?,
            _ => unreachable!("validated option"),
        }
    }
    if usize::from(seed.is_some()) + usize::from(replay.is_some()) + usize::from(campaign) > 1 {
        return Err("--seed, --replay and --campaign are mutually exclusive".into());
    }
    if variant.is_some() && (campaign || replay.is_some()) {
        return Err("--variant is only valid for a single seed".into());
    }
    if !campaign && (start.is_some() || count.is_some()) {
        return Err("--seed-start and --seed-count require --campaign".into());
    }
    let mode = if campaign {
        Mode::Campaign {
            start: start.unwrap_or(0),
            count: count.unwrap_or(128),
        }
    } else if let Some(path) = replay {
        Mode::Replay(path)
    } else {
        Mode::Seed {
            seed: seed.unwrap_or(0),
            variant: variant.unwrap_or(CampaignVariant::Legacy),
        }
    };
    let watchdog_ms = watchdog.unwrap_or(if campaign {
        CAMPAIGN_WATCHDOG_MS
    } else {
        SEED_WATCHDOG_MS
    });
    if !(1..=3_600_000).contains(&watchdog_ms) {
        return Err("watchdog must be in 1..=3600000ms".into());
    }
    Ok(Some(Options {
        mode,
        directory: directory.unwrap_or_else(|| PathBuf::from("kafka-sim-failure")),
        watchdog_ms,
    }))
}

fn set_once<T>(slot: &mut Option<T>, value: T, flag: &str) -> Result<(), String> {
    if slot.is_some() {
        return Err(format!("duplicate {flag}"));
    }
    *slot = Some(value);
    Ok(())
}

fn parse_number(flag: &str, value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} requires an unsigned 64-bit integer"))
}

fn main() {
    if let Err(error) = execute() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn execute() -> Result<(), String> {
    let Some(options) = parse_args(std::env::args().skip(1))? else {
        println!("{HELP}");
        return Ok(());
    };
    let watchdog_ms = options.watchdog_ms;
    let (done, receiver) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        if receiver.recv_timeout(Duration::from_millis(watchdog_ms))
            == Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        {
            eprintln!("Kafka simulation watchdog exceeded {watchdog_ms}ms");
            std::process::exit(124);
        }
    });
    let result = run(options);
    let _ = done.send(());
    result
}

fn run(options: Options) -> Result<(), String> {
    let mut total = Coverage::default();
    let mut fault_totals = [0u64; 4];
    let mut run_case = |manifest: &ReplayManifest, label: &str| -> Result<(), String> {
        // Export before entering the runtime, so even the independent watchdog
        // can leave a replayable input. Ordinary failures additionally retain
        // the completed diagnostic rerun's history and trace.
        retain_active_manifest(manifest, &options.directory)?;
        println!("running seed={} variant={label}", manifest.seed);
        let report = run_and_retain_failure(manifest, &options.directory)
            .map_err(|failure| failure.to_string())?;
        total.merge(&report.coverage)?;
        for (total, value) in fault_totals.iter_mut().zip([
            report.fault_stats.script_effects,
            report.fault_stats.random_effects,
            report.fault_stats.isolation_closed,
            report.fault_stats.committed_response_losses,
        ]) {
            *total = total
                .checked_add(value)
                .ok_or("campaign fault counter overflow")?;
        }
        println!(
            "seed={} variant={} accepted={} acked={} unknown={} fetched={} steps={}",
            manifest.seed,
            label,
            report.coverage.accepted,
            report.coverage.acked,
            report.coverage.unknown,
            report.fetched_records,
            report.checkpoint.total_steps
        );
        Ok(())
    };
    match options.mode {
        Mode::Seed { seed, variant } => {
            run_case(&campaign_manifest(seed, variant)?, variant.as_str())?;
        }
        Mode::Replay(path) => {
            let metadata = std::fs::metadata(&path).map_err(|e| e.to_string())?;
            if metadata.len() > 16 * 1024 * 1024 {
                return Err("manifest byte bound".into());
            }
            let manifest =
                ReplayManifest::from_json(&std::fs::read(path).map_err(|e| e.to_string())?)?;
            run_case(&manifest, "replay")?;
        }
        Mode::Campaign { start, count } => {
            let cases = campaign_cases(start, count)?;
            println!(
                "campaign seed_start={} seed_count={} pinned_cases={} selected_cases={} capability_checks=1 total_runs={}",
                start,
                count,
                PINNED_CASES.len(),
                cases.len(),
                cases.len() + 1
            );
            for (seed, variant) in cases {
                run_case(&campaign_manifest(seed, variant)?, variant.as_str())?;
            }
            let mut old = ReplayManifest::from_seed(0, CampaignLimits::default())?;
            old.produce_max_version = 9;
            old.workload.truncate(2);
            old.workload.push(Workload::Close {
                deadline_ns: 2_000_000_000,
            });
            run_case(&old, "produce9-rejection")?;
            total.require_campaign_gates()?;
            if fault_totals.contains(&0) {
                return Err(format!(
                    "campaign script/random/isolation/committed-response coverage: {fault_totals:?}"
                ));
            }
            println!(
                "campaign fault coverage: script_actions={} random_actions={} isolated_sockets={} committed_response_losses={}",
                fault_totals[0], fault_totals[1], fault_totals[2], fault_totals[3]
            );
        }
    }
    Ok(())
}

fn retain_active_manifest(manifest: &ReplayManifest, directory: &Path) -> Result<(), String> {
    let bytes = manifest.to_json()?;
    std::fs::create_dir_all(directory).map_err(|error| error.to_string())?;
    let temporary = directory.join("active-replay.json.tmp");
    std::fs::write(&temporary, bytes).map_err(|error| error.to_string())?;
    std::fs::rename(temporary, directory.join("active-replay.json"))
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Option<Options>, String> {
        parse_args(args.iter().map(|arg| (*arg).to_owned()))
    }

    #[test]
    fn defaults_preserve_legacy_seed_and_campaign_has_a_separate_watchdog() {
        let seed = parse(&[]).unwrap().unwrap();
        assert_eq!(
            seed.mode,
            Mode::Seed {
                seed: 0,
                variant: CampaignVariant::Legacy
            }
        );
        assert_eq!(seed.watchdog_ms, 120_000);
        let campaign = parse(&["--campaign"]).unwrap().unwrap();
        assert_eq!(
            campaign.mode,
            Mode::Campaign {
                start: 0,
                count: 128
            }
        );
        assert_eq!(campaign.watchdog_ms, 900_000);
        assert!(parse(&["--help"]).unwrap().is_none());
    }

    #[test]
    fn explicit_selection_preserves_full_width_seed_and_resource_options() {
        let options = parse(&[
            "--seed",
            "18446744073709551615",
            "--variant",
            "clean",
            "--failure-dir",
            "owned failures",
            "--watchdog-ms",
            "250000",
        ])
        .unwrap()
        .unwrap();
        assert_eq!(
            options.mode,
            Mode::Seed {
                seed: u64::MAX,
                variant: CampaignVariant::Clean
            }
        );
        assert_eq!(options.directory, PathBuf::from("owned failures"));
        assert_eq!(options.watchdog_ms, 250_000);
        assert_eq!(
            parse(&["--campaign", "--seed-start", "41", "--seed-count", "0"])
                .unwrap()
                .unwrap()
                .mode,
            Mode::Campaign {
                start: 41,
                count: 0
            }
        );
        assert_eq!(
            parse(&["--replay", "failure/replay.json"])
                .unwrap()
                .unwrap()
                .mode,
            Mode::Replay(PathBuf::from("failure/replay.json"))
        );
    }

    #[test]
    fn conflicting_modes_and_ambiguous_options_never_silently_change_the_run() {
        for args in [
            vec!["--campaign", "--seed", "7"],
            vec!["--campaign", "--replay", "record.json"],
            vec!["--seed", "7", "--replay", "record.json"],
            vec!["--campaign", "--variant", "clean"],
            vec!["--replay", "record.json", "--variant", "clean"],
            vec!["--seed-start", "1"],
            vec!["--seed-count", "8"],
            vec!["--seed", "1", "--seed", "2"],
            vec!["--campaign", "--campaign"],
            vec!["--watchdog-ms", "0"],
            vec!["--watchdog-ms", "3600001"],
            vec!["--seed", "18446744073709551616"],
            vec!["--seed", "-1"],
            vec!["--variant", "not-a-variant"],
            vec!["--replay"],
            vec!["--replay", "--campaign"],
            vec!["--help", "--campaign"],
        ] {
            assert!(parse(&args).is_err(), "unexpectedly accepted {args:?}");
        }
    }

    #[test]
    fn active_manifest_replaces_the_previous_case_with_a_valid_replay() {
        struct OwnedDirectory(PathBuf);
        impl Drop for OwnedDirectory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let directory = (0..100)
            .find_map(|suffix| {
                let path = std::env::temp_dir().join(format!(
                    "kafka-sim-active-replay-{}-{suffix}",
                    std::process::id()
                ));
                std::fs::create_dir(&path)
                    .ok()
                    .map(|()| OwnedDirectory(path))
            })
            .expect("unique owned test directory");
        for seed in [0, 36] {
            let manifest = ReplayManifest::from_seed(seed, CampaignLimits::default()).unwrap();
            retain_active_manifest(&manifest, &directory.0).unwrap();
            let bytes = std::fs::read(directory.0.join("active-replay.json")).unwrap();
            let replay = ReplayManifest::from_json(&bytes).unwrap();
            assert_eq!(replay.to_json().unwrap(), manifest.to_json().unwrap());
            assert!(!directory.0.join("active-replay.json.tmp").exists());
        }
    }
}
