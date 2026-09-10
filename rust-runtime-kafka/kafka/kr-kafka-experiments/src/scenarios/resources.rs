use super::common::*;
use super::*;
use crate::build::{MS, SECOND};
use kr_kafka_sim::{PollingPause, faults::IsolationWindow};
fn scenario(
    id: &'static str,
    title: &'static str,
    description: &'static str,
    variants: Vec<Variant>,
) -> Scenario {
    Scenario {
        id,
        title,
        description,
        category: Category::Resources,
        what_to_look_for: "Inspect the limiting credit pool and exact refusal evidence. Delivery outcomes count accepted records only; watch drain after the load or pause ends.",
        variants,
    }
}
pub(super) fn catalogue() -> Vec<Scenario> {
    vec![
        scenario(
            "resources.memory-bounded-overload",
            "Bounded admission overload",
            "A 32,000/s open source exceeds capacity with 256 descriptors and a 256 KiB input pool; payload size selects the limiting resource.",
            [512, 2048]
                .map(|bytes| {
                    variant(
                        format!("value{bytes}"),
                        Params {
                            value_bytes: Some(bytes),
                            rate_per_s: Some(32000),
                            ..Default::default()
                        },
                    )
                })
                .to_vec(),
        ),
        scenario(
            "resources.wire-window-vs-latency",
            "Wire window and latency",
            "Twenty milliseconds of propagation each way with five in-flight requests per connection and three wire-window sizes.",
            [64, 256, 1024]
                .map(|window| {
                    variant(
                        format!("window{window}k"),
                        Params {
                            in_flight: Some(5),
                            wire_window_bytes: Some(window * 1024),
                            outstanding: Some(256),
                            ..Default::default()
                        },
                    )
                })
                .to_vec(),
        ),
        scenario(
            "resources.delivery-timeout-tuning",
            "Delivery timeout tuning",
            "Broker 1 is isolated from 10 to 14 seconds while delivery deadlines vary from two to thirty seconds.",
            [2, 6, 30]
                .map(|seconds| {
                    variant(
                        format!("deadline{seconds}s"),
                        Params {
                            delivery_timeout_ns: Some(seconds * SECOND),
                            linger_ns: Some(0),
                            ..Default::default()
                        },
                    )
                })
                .to_vec(),
        ),
        scenario(
            "resources.stop-polling-backpressure",
            "Paused event consumption",
            "Open-loop offers continue while client event consumption pauses from 10 to 12 seconds.",
            [64, 1024]
                .map(|capacity| {
                    variant(
                        format!("events{capacity}"),
                        Params {
                            extra: BTreeMap::from([("events".into(), capacity)]),
                            ..Default::default()
                        },
                    )
                })
                .to_vec(),
        ),
    ]
}
pub(super) fn build(
    s: &Scenario,
    v: &Variant,
    seed: u64,
    size: Size,
) -> Result<ReplayManifest, String> {
    let p = &v.params;
    let mut m = build::base(seed, size, 3)?;
    build::apply(&mut m, p);
    match s.id {
        "resources.memory-bounded-overload" => {
            m.producer.input_bytes = 256 * 1024;
            m.producer.record_descriptors = 256;
            m.producer.pending_records_per_topic = 256;
            m.driver.service_delay_ns = 4 * MS;
            // Measure steady admission after resolution, without competing
            // metadata publication allocations during this fixed topology.
            if p.metadata_max_age_ns.is_none() {
                m.producer.metadata_max_age = build::ns(60 * SECOND);
            }
            m.driver.encode_bytes = 2048;
            m.driver.encode_cost_ns = MS;
            finite(&mut m, p, 0, 8, None);
            open(
                &mut m,
                p,
                200 * MS,
                if size == Size::Test {
                    215 * MS
                } else {
                    10 * SECOND + 200 * MS
                },
                32000,
                None,
            );
            finite(
                &mut m,
                p,
                if size == Size::Test {
                    SECOND
                } else {
                    11 * SECOND
                },
                16,
                None,
            );
        }
        "resources.wire-window-vs-latency" => {
            for link in &mut m.faults.links {
                link.to_broker_latency_ns = 20 * MS;
                link.from_broker_latency_ns = 20 * MS;
            }
            build::closed(&mut m, p, size);
        }
        "resources.delivery-timeout-tuning" => {
            m.faults.crash_on_isolation = true;
            m.faults.isolations.push(IsolationWindow {
                broker: 1,
                start_ns: 10 * SECOND,
                end_ns: 14 * SECOND,
            });
            if size == Size::Test {
                phase_fixture(&mut m, p, 1, 10 * SECOND, 14 * SECOND, 16);
            } else {
                let end = if p.delivery_timeout_ns == Some(2 * SECOND) {
                    11 * SECOND
                } else {
                    16 * SECOND
                };
                sustained(&mut m, p, end);
                healthy_during(&mut m, p, 1, 10 * SECOND, end.min(15 * SECOND));
                finite(&mut m, p, 14 * SECOND + 10 * MS, 16, Some(0));
            }
            finite(&mut m, p, 10 * SECOND - 100_000, 8, Some(0));
            finite(&mut m, p, 10 * SECOND + 100_000, 8, Some(0));
        }
        "resources.stop-polling-backpressure" => {
            m.producer.delivery_event_capacity = if size == Size::Test {
                if p.extra["events"] == 64 { 16 } else { 128 }
            } else {
                p.extra["events"] as u32
            };
            m.producer.record_descriptors = m.producer.delivery_event_capacity.min(512);
            m.producer.pending_records_per_topic = m.producer.record_descriptors;
            m.experiment
                .as_mut()
                .unwrap()
                .polling_pauses
                .push(PollingPause {
                    start_ns: 10 * SECOND,
                    end_ns: 12 * SECOND,
                });
            if size == Size::Full {
                open(&mut m, p, 0, 15 * SECOND, 2000, None);
            } else {
                finite(&mut m, p, 9 * SECOND + 900 * MS, 16, None);
                open(&mut m, p, 10 * SECOND, 11 * SECOND + 750 * MS, 256, None);
                finite(&mut m, p, 12 * SECOND + 10 * MS, 32, None);
            }
        }
        _ => return Err("unknown resource fixture".into()),
    }
    finish(&mut m)?;
    Ok(m)
}
pub(super) fn invariants(s: &Scenario, r: &RunReport) -> Result<(), String> {
    let expiry = s.id == "resources.delivery-timeout-tuning"
        && r.manifest.producer.delivery_timeout.as_nanos() == 2 * SECOND;
    if !expiry && r.coverage.accepted != r.coverage.acked {
        return Err("resource fixture did not acknowledge all accepted records".into());
    }
    if expiry && r.coverage.not_written + r.coverage.unknown == 0 {
        return Err("short delivery deadline did not expire".into());
    }
    Ok(())
}
