//! Experiment-only peer-visibility profiles over SimNetwork's bounded pipes.
use crate::{ReplayManifest, faults::FaultConfig};
use kr_runtime::{RuntimeDuration, RuntimeInstant};
use kr_runtime_io::network::{PropagationProfile, PropagationWindow};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrokerLink {
    pub broker: i32,
    pub to_broker_latency_ns: u64,
    pub from_broker_latency_ns: u64,
    pub chunk_bytes: usize,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum LinkDirection {
    ToBroker,
    FromBroker,
    Both,
}
impl LinkDirection {
    pub(crate) fn overlaps(self, other: Self) -> bool {
        self == Self::Both || other == Self::Both || self == other
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum OutageMode {
    BlackHole,
    FailFast,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LinkOutage {
    pub broker: i32,
    pub direction: LinkDirection,
    pub mode: OutageMode,
    pub start_ns: u64,
    pub end_ns: u64,
}
pub(crate) fn validate(config: &FaultConfig) -> Result<(), String> {
    if config.links.len() > 256 || config.link_outages.len() > 256 {
        return Err("experiment link capacity".into());
    }
    for (i, link) in config.links.iter().enumerate() {
        if link.broker < 0
            || link.chunk_bytes == 0
            || link.chunk_bytes > 1024 * 1024
            || link.to_broker_latency_ns > 300_000_000_000
            || link.from_broker_latency_ns > 300_000_000_000
            || config.links[..i].iter().any(|l| l.broker == link.broker)
        {
            return Err("invalid/repeated broker link".into());
        }
    }
    for (i, window) in config.link_outages.iter().enumerate() {
        if window.broker < 0
            || window.start_ns >= window.end_ns
            || window.end_ns > 300_000_000_000
            || config.link_outages[..i].iter().any(|w| {
                w.broker == window.broker
                    && w.direction.overlaps(window.direction)
                    && w.start_ns < window.end_ns
                    && window.start_ns < w.end_ns
            })
        {
            return Err("invalid/overlapping link outage".into());
        }
    }
    Ok(())
}
pub(crate) fn enabled(config: &FaultConfig) -> bool {
    !config.links.is_empty() || !config.link_outages.is_empty()
}
pub(crate) fn setup_failed(config: &FaultConfig, broker: i32, now_ns: u64) -> bool {
    config.link_outages.iter().any(|w| {
        w.broker == broker
            && w.mode == OutageMode::FailFast
            && w.start_ns <= now_ns
            && now_ns < w.end_ns
    })
}
pub(crate) fn profiles(
    manifest: &ReplayManifest,
    broker: i32,
) -> (PropagationProfile, PropagationProfile) {
    let link = manifest
        .faults
        .links
        .iter()
        .find(|link| link.broker == broker);
    let make = |direction, delay| {
        let mut profile = PropagationProfile {
            latency: RuntimeDuration::from_nanos(delay),
            ..PropagationProfile::default()
        };
        for window in manifest
            .faults
            .link_outages
            .iter()
            .filter(|w| w.broker == broker && w.direction.overlaps(direction))
        {
            let interval = PropagationWindow {
                start: RuntimeInstant::from_nanos(manifest.start_ns + window.start_ns),
                end: RuntimeInstant::from_nanos(manifest.start_ns + window.end_ns),
            };
            match window.mode {
                OutageMode::BlackHole => profile.black_holes.push(interval),
                OutageMode::FailFast => profile.fail_fast.push(interval),
            }
        }
        profile
    };
    (
        make(
            LinkDirection::ToBroker,
            link.map_or(0, |l| l.to_broker_latency_ns),
        ),
        make(
            LinkDirection::FromBroker,
            link.map_or(0, |l| l.from_broker_latency_ns),
        ),
    )
}
