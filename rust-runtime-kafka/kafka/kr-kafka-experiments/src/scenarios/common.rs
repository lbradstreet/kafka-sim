use super::*;
use crate::build::{MS, SECOND};
use kr_kafka_sim::{LoadShape, LoadSpec, Partitioning, ScheduledAction, TimedControl};
pub(super) fn variant(name: String, params: Params) -> Variant {
    Variant {
        summary: name.replace('-', " "),
        name,
        params,
    }
}
pub(super) fn next_id(m: &ReplayManifest) -> u64 {
    1 + m
        .experiment
        .as_ref()
        .unwrap()
        .loads
        .iter()
        .map(|l| u64::from(l.shape.offer_budget().unwrap()))
        .sum::<u64>()
}
pub(super) fn finite(
    m: &mut ReplayManifest,
    p: &Params,
    at: u64,
    count: u32,
    partition: Option<i32>,
) {
    let mut template = build::template(next_id(m), p);
    if let Some(partition) = partition {
        template.partitioning = Partitioning::Fixed { partition };
    }
    m.experiment.as_mut().unwrap().loads.push(LoadSpec {
        template,
        shape: LoadShape::ClosedLoop {
            start_ns: at,
            count,
            outstanding: count,
        },
    });
}
pub(super) fn open(
    m: &mut ReplayManifest,
    p: &Params,
    start: u64,
    end: u64,
    rate: u64,
    partition: Option<i32>,
) {
    let mut template = build::template(next_id(m), p);
    if let Some(partition) = partition {
        template.partitioning = Partitioning::Fixed { partition };
    }
    m.experiment.as_mut().unwrap().loads.push(LoadSpec {
        template,
        shape: LoadShape::OpenLoop {
            start_ns: start,
            end_ns: end,
            rate_per_s: rate,
        },
    });
}
pub(super) fn sustained(m: &mut ReplayManifest, p: &Params, end: u64) {
    // A declared 4 ms serial broker service bounds the fastest K=64 source
    // below the one-million-offer envelope over these <= 62 s fixtures.
    m.driver.service_delay_ns = 4 * MS;
    let template = build::template(next_id(m), p);
    m.experiment.as_mut().unwrap().loads.push(LoadSpec {
        template,
        shape: LoadShape::ClosedLoopUntil {
            start_ns: 0,
            end_ns: end,
            max_offers: 900_000,
            outstanding: p.outstanding.unwrap_or(32),
        },
    });
}
pub(super) fn control(m: &mut ReplayManifest, at: u64, action: TimedControl) {
    m.experiment
        .as_mut()
        .unwrap()
        .scheduled_actions
        .push(ScheduledAction { at_ns: at, action });
}
pub(super) fn finish(m: &mut ReplayManifest) -> Result<(), String> {
    m.experiment
        .as_mut()
        .unwrap()
        .scheduled_actions
        .sort_by_key(|c| c.at_ns);
    m.validate()
}
pub(super) fn affected_partition(m: &ReplayManifest, broker: i32) -> i32 {
    m.topics[0]
        .leaders
        .iter()
        .position(|b| *b == broker)
        .unwrap() as i32
}
pub(super) fn phase_fixture(
    m: &mut ReplayManifest,
    p: &Params,
    broker: i32,
    start: u64,
    end: u64,
    records: u32,
) {
    let partition = affected_partition(m, broker);
    let healthy = (broker as usize + 1) % m.brokers.len();
    let healthy = affected_partition(m, m.brokers[healthy].id);
    if start >= SECOND {
        finite(m, p, start - 100 * MS, records, Some(partition));
        finite(m, p, start - 500_000, records, Some(partition));
    }
    finite(m, p, start + 10 * MS, records, Some(partition));
    finite(m, p, start + 20 * MS, records, Some(healthy));
    finite(m, p, end + 10 * MS, records, Some(partition));
    finite(m, p, end + 20 * MS, records, Some(healthy));
}

pub(super) fn healthy_during(
    m: &mut ReplayManifest,
    p: &Params,
    broker: i32,
    start: u64,
    end: u64,
) {
    let other = (broker as usize + 1) % m.brokers.len();
    let partition = affected_partition(m, m.brokers[other].id);
    let mut template = build::template(next_id(m), p);
    template.partitioning = Partitioning::Fixed { partition };
    let maximum = ((end - start).div_ceil(4 * MS) * 4 + 4) as u32;
    m.experiment.as_mut().unwrap().loads.push(LoadSpec {
        template,
        shape: LoadShape::ClosedLoopUntil {
            start_ns: start,
            end_ns: end,
            max_offers: maximum,
            outstanding: 4,
        },
    });
}
