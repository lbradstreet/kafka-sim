use crate::{Params, Size};
use std::{collections::BTreeSet, path::PathBuf};
pub const HELP: &str = "kafka-experiments --list\n\
kafka-experiments --scenario ID [--variant V] [--seed N] [--size test|full] [--out DIR]\n\
kafka-experiments --all [--seed N] [--size test|full] [--out DIR]\n\
kafka-experiments --replay-dir DIR [--out DIR]\n\
kafka-experiments --export-html HTML_DIR [--out SAVED_DIR]\n\
--export-html HTML_DIR also exports after a new run or saved replay.\n\
Defaults: seed 0, size test, out target/experiments. Full runs accept --no-replay.\n\
Overrides: --in-flight N --lanes N --linger-ms N --backoff-ms MIN:MAX\n\
--request-timeout-ms N --delivery-timeout-ms N --metadata-max-age-ms N\n\
--compression none|zstd1|zstd3 --outstanding N --rate-per-s N\n\
--batch-target-bytes N --wire-window-bytes N --value-bytes N\n\
--descriptor-admission shared|partition-pressure";
#[derive(Debug, PartialEq, Eq)]
pub enum Mode {
    List,
    Scenario(String),
    All,
    Replay(PathBuf),
    Export,
}
#[derive(Debug)]
pub struct Options {
    pub mode: Mode,
    pub variant: Option<String>,
    pub seed: u64,
    pub size: Size,
    pub out: PathBuf,
    pub replay: bool,
    pub overrides: Params,
    pub export_html: Option<PathBuf>,
}
fn number(s: &str) -> Result<u64, String> {
    let v = s
        .parse::<u64>()
        .map_err(|_| format!("invalid unsigned integer: {s}"))?;
    if v.to_string() != s {
        return Err("integers must use canonical decimal notation".into());
    }
    Ok(v)
}
fn ms(s: &str) -> Result<u64, String> {
    number(s)?
        .checked_mul(1_000_000)
        .ok_or("millisecond value overflow".into())
}
fn narrow<T: TryFrom<u64>>(s: &str) -> Result<T, String> {
    number(s)?
        .try_into()
        .map_err(|_| "numeric value out of range".into())
}
pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Option<Options>, String> {
    let args: Vec<_> = args.into_iter().collect();
    if args == ["--help"] {
        return Ok(None);
    }
    let mut iter = args.into_iter();
    let mut seen = BTreeSet::new();
    let mut mode = None;
    let mut o = Options {
        mode: Mode::All,
        variant: None,
        seed: 0,
        size: Size::Test,
        out: PathBuf::from("target/experiments"),
        replay: true,
        overrides: Params::default(),
        export_html: None,
    };
    let mut overridden = false;
    while let Some(arg) = iter.next() {
        if !seen.insert(arg.clone()) {
            return Err(format!("duplicate {arg}"));
        }
        if arg == "--no-replay" {
            o.replay = false;
            continue;
        }
        let value = if matches!(arg.as_str(), "--list" | "--all") {
            String::new()
        } else {
            let v = iter
                .next()
                .ok_or_else(|| format!("missing value for {arg}"))?;
            if v.starts_with("--") {
                return Err(format!("missing value for {arg}"));
            }
            v
        };
        let selected = match arg.as_str() {
            "--list" => Some(Mode::List),
            "--all" => Some(Mode::All),
            "--scenario" => Some(Mode::Scenario(value.clone())),
            "--replay-dir" => Some(Mode::Replay(PathBuf::from(&value))),
            _ => None,
        };
        if let Some(selected) = selected {
            if mode.replace(selected).is_some() {
                return Err("choose one operation".into());
            }
            continue;
        }
        match arg.as_str() {
            "--variant" => o.variant = Some(value),
            "--seed" => o.seed = number(&value)?,
            "--out" => o.out = PathBuf::from(value),
            "--export-html" => o.export_html = Some(PathBuf::from(value)),
            "--size" => {
                o.size = match value.as_str() {
                    "test" => Size::Test,
                    "full" => Size::Full,
                    _ => return Err("size must be test or full".into()),
                }
            }
            _ => {
                overridden = true;
                match arg.as_str() {
                    "--descriptor-admission" => {
                        o.overrides.partition_pressure = Some(match value.as_str() {
                            "shared" => false,
                            "partition-pressure" => true,
                            _ => {
                                return Err(
                                    "descriptor admission must be shared or partition-pressure"
                                        .into(),
                                );
                            }
                        })
                    }
                    "--in-flight" => o.overrides.in_flight = Some(narrow(&value)?),
                    "--lanes" => o.overrides.lanes = Some(narrow(&value)?),
                    "--linger-ms" => o.overrides.linger_ns = Some(ms(&value)?),
                    "--request-timeout-ms" => o.overrides.request_timeout_ns = Some(ms(&value)?),
                    "--delivery-timeout-ms" => o.overrides.delivery_timeout_ns = Some(ms(&value)?),
                    "--metadata-max-age-ms" => o.overrides.metadata_max_age_ns = Some(ms(&value)?),
                    "--backoff-ms" => {
                        let (min, max) = value.split_once(':').ok_or("backoff must be MIN:MAX")?;
                        o.overrides.backoff_ns = Some((ms(min)?, ms(max)?));
                    }
                    "--compression" => {
                        o.overrides.compression = Some(match value.as_str() {
                            "none" => 0,
                            "zstd1" => 1,
                            "zstd3" => 3,
                            _ => return Err("compression must be none, zstd1 or zstd3".into()),
                        })
                    }
                    "--outstanding" => o.overrides.outstanding = Some(narrow(&value)?),
                    "--rate-per-s" => o.overrides.rate_per_s = Some(number(&value)?),
                    "--batch-target-bytes" => {
                        o.overrides.batch_target_bytes = Some(narrow(&value)?)
                    }
                    "--wire-window-bytes" => o.overrides.wire_window_bytes = Some(narrow(&value)?),
                    "--value-bytes" => o.overrides.value_bytes = Some(narrow(&value)?),
                    _ => return Err(format!("unknown argument: {arg}")),
                }
            }
        }
    }
    o.mode = mode
        .or_else(|| o.export_html.as_ref().map(|_| Mode::Export))
        .ok_or("choose --list, --scenario, --all, --replay-dir or --export-html")?;
    if o.variant.is_some() && !matches!(o.mode, Mode::Scenario(_)) {
        return Err("--variant requires --scenario".into());
    }
    if !o.replay
        && (o.size != Size::Full || matches!(o.mode, Mode::List | Mode::Replay(_) | Mode::Export))
    {
        return Err("--no-replay requires a new Full run".into());
    }
    if matches!(o.mode, Mode::List) && seen.len() != 1 {
        return Err("--list accepts no run options".into());
    }
    if matches!(o.mode, Mode::Replay(_) | Mode::Export)
        && (overridden || seen.contains("--seed") || seen.contains("--size"))
    {
        return Err("replay uses saved manifests; generation overrides are not accepted".into());
    }
    if let Mode::Replay(ref source) = o.mode
        && !seen.contains("--out")
    {
        o.out = source.join("replayed");
    }
    Ok(Some(o))
}
pub fn apply(to: &mut Params, from: &Params) {
    macro_rules! fields {($($field:ident),*)=>{$(if from.$field.is_some(){to.$field=from.$field;})*};}
    fields!(
        in_flight,
        lanes,
        linger_ns,
        backoff_ns,
        request_timeout_ns,
        delivery_timeout_ns,
        compression,
        rate_per_s,
        outstanding,
        batch_target_bytes,
        wire_window_bytes,
        value_bytes,
        metadata_max_age_ns,
        partition_pressure
    );
}
#[cfg(test)]
mod tests {
    use super::*;
    fn p(s: &str) -> Result<Option<Options>, String> {
        parse(s.split_whitespace().map(str::to_owned))
    }
    #[test]
    fn parse_modes_defaults_full_width_seeds_and_sweep_overrides() {
        let o=p("--scenario baseline.compression --seed 18446744073709551615 --size full --no-replay --lanes 4 --backoff-ms 10:100 --compression zstd3").unwrap().unwrap();
        assert_eq!(o.seed, u64::MAX);
        assert!(!o.replay);
        assert_eq!(o.overrides.backoff_ns, Some((10_000_000, 100_000_000)));
        assert_eq!(o.overrides.lanes, Some(4));
        assert_eq!(p("--all").unwrap().unwrap().size, Size::Test);
        assert_eq!(
            p("--replay-dir saved").unwrap().unwrap().out,
            PathBuf::from("saved/replayed")
        );
    }
    #[test]
    fn reject_ambiguous_modes_duplicate_flags_and_invalid_overrides() {
        for args in [
            "",
            "--all --list",
            "--list --seed 0",
            "--all --variant x",
            "--scenario x --no-replay",
            "--all --seed 01",
            "--all --seed 18446744073709551616",
            "--all --lanes 256",
            "--all --seed 0 --seed 1",
            "--replay-dir x --seed 0",
            "--all --backoff-ms 10",
            "--all --unknown 1",
            "--all --out",
            "--help --all",
        ] {
            assert!(p(args).is_err(), "{args}");
        }
    }
}
