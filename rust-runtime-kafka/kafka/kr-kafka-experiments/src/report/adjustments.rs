use crate::{Scenario, Size, Variant};
use kr_kafka_sim::ReplayManifest;
/// Keep the precise materialized phases and configured capacities next to an
/// explanation of why Test differs from Full. These are scenario contracts,
/// not seed-dependent reductions made after a run starts.
pub(super) fn describe(s: &Scenario, v: &Variant, size: Size, m: &ReplayManifest) -> String {
    if size == Size::Full {
        return "Full catalogue phases and capacities; unused ClosedLoopUntil budget is cancelled explicitly".into();
    }
    let recipe = match s.id {
        "baseline.closed-loop-inflight" => {
            "128–512 finite records depending on K, versus 4,096 in Full"
        }
        "baseline.open-loop-rate" => {
            "512 offers at the Full rate, shortening the source from Full's twenty seconds; both sizes retain 64 descriptors"
        }
        "baseline.bursty-onoff" => {
            "Three bursts at 0/2/4 seconds versus ten in Full; the 800 variant uses 128-record bursts and 16 descriptors"
        }
        "hard.crash-restart-open" => {
            "Six four-record boundary cohorts plus a 256-offer during-band source and 32 descriptors, versus thirty seconds of Full open-loop offers"
        }
        "hard.partition-admission-isolation" => {
            "Six independent sources run from 9.8 to 13.2 seconds at an aggregate rate capped at 1,000/s with 32 descriptors; Full runs for thirty seconds at the selected rate with 512 descriptors. The 10–13 second outage is unchanged"
        }
        "baseline.partition-admission-skew" => {
            "Both sizes warm up eight records and use 64 descriptors. Test open traffic lasts 200 ms from 200 ms; Full lasts ten seconds. Offered rates, skew and configured partition counts are unchanged"
        }
        "hard.bootstrap-down-at-start" => {
            "Both sizes use two 100-offer resolving/recovery cohorts with unchanged fault and resolve deadlines"
        }
        "hard.rolling-restart" => {
            "Six eight-record phases per isolation, versus Full sustained primary and healthy sources"
        }
        "hard.flapping-broker" => {
            "Three four-record phases per window plus warmup and final cohorts, versus Full 2,000/s open-loop load"
        }
        "hard.close-during-outage" => {
            "Bounded warmup, crossing, queued, healthy and cancelled future cohorts; the Full source remains active until scheduled Close"
        }
        "soft.sustained-random-loss" => {
            "32 offers/s for ten seconds and a 16-record post-band cohort, versus Full sustained sources"
        }
        "soft.disconnect-storm" => {
            "64 offers/s during the five-second band plus warmup/healthy/recovery cohorts, versus Full sustained sources"
        }
        "soft.degrading-broker-ramp" => {
            "Six 16-record boundary cohorts plus 16-record probes at 15/25/35/44 seconds; the 5–45 second ramp is unchanged"
        }
        "soft.high-jitter" | "soft.tiny-chunk-transport" => {
            "128 finite records versus 4,096 in Full; transport settings are unchanged"
        }
        "topology.leader-rebalance-churn" => {
            "Four records before and after each of thirty unchanged moves plus warmup/final cohorts, versus a Full source through 62 seconds"
        }
        "topology.partition-expansion" => {
            "64 initial records, 16 pre-control records and 64/s for two seconds after expansion, versus 4,096 initial records and 1,000/s for five seconds"
        }
        "topology.delete-recreate" => {
            "64 initial records and 1,000/s for 100 ms after recreation, versus 4,096 and 4,000/s; both retain a 16-record pre-control cohort"
        }
        "topology.multi-topic-isolation" => {
            "Four 32-record phases per topic versus two independent Full sustained sources; topic lane assignments and fault interval are unchanged"
        }
        "resources.memory-bounded-overload" => {
            "Eight warmup records, a 15 ms overload at 32,000/s from 200 ms, and 16 recovery records; Full overload lasts ten seconds. Both sizes use 2 KiB/ms simulated encoding and defer metadata refresh beyond the load"
        }
        "resources.wire-window-vs-latency" => {
            "128 finite records versus 4,096 in Full, retaining the configured outstanding limit and transport settings"
        }
        "resources.stop-polling-backpressure" => {
            if v.params.extra["events"] == 64 {
                "Delivery-event capacity 16 versus 64 in Full; 448 during-pause offers and explicit warmup/recovery cohorts replace fifteen seconds at 2,000/s"
            } else {
                "Delivery-event capacity 128 versus 1,024 in Full; 448 during-pause offers and explicit warmup/recovery cohorts replace fifteen seconds at 2,000/s"
            }
        }
        _ => match s.category {
            crate::Category::Baseline => {
                "128 finite records versus 4,096 in Full; sweep settings are unchanged"
            }
            crate::Category::Hard | crate::Category::Soft | crate::Category::Resources => {
                "Bounded before/during/recovery cohorts replace Full sustained sources; all fault/control durations and deadlines are unchanged"
            }
            _ => "Explicit source phases are listed below",
        },
    };
    format!(
        "{recipe}. Test capacities: {} offer IDs, {} descriptors, {} input bytes, {} delivery events; {} explicit sources. Full-only trend comparisons are not applicable.",
        m.limits.records,
        m.producer.record_descriptors,
        m.producer.input_bytes,
        m.producer.delivery_event_capacity,
        m.experiment.as_ref().unwrap().loads.len()
    )
}
