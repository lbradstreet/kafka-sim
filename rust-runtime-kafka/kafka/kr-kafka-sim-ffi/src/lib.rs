//! Test-only, owner-thread JSON/Panama bridge. Deliberately separate from the
//! production ABI: no host owner, background thread or real network is started.
use kr_kafka_sim::external::ExternalSession;
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    ffi::{CStr, CString, c_char},
};

#[derive(Default)]
struct Bridge {
    session: Option<ExternalSession>,
    reply: CString,
}
thread_local! { static BRIDGE: RefCell<Bridge> = RefCell::new(Bridge::default()); }
fn number(v: &Value, key: &str) -> Result<u64, String> {
    v.get(key)
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
        .ok_or_else(|| format!("missing unsigned {key}"))
}

fn string<'a>(v: &'a Value, key: &str) -> Result<&'a str, String> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing string {key}"))
}

// Read harness configuration only at the outer init boundary, then make it an
// explicit command/manifest input. The session never consults the environment.
fn configure_init(mut request: Value, mode: Option<&str>) -> Result<Value, String> {
    if request["op"] == "init"
        && let Some(mode) = mode
    {
        if !matches!(mode, "Raw" | "EstimatedWire") {
            return Err("KR_SIM_BATCH_TARGET_MODE must be Raw or EstimatedWire".into());
        }
        if let Some(explicit) = request.get("batch_target_mode")
            && explicit != mode
        {
            return Err("conflicting batch target mode inputs".into());
        }
        request["batch_target_mode"] = json!(mode);
    }
    Ok(request)
}
fn configure_request_policy(mut request: Value, policy: Option<&str>) -> Result<Value, String> {
    if request["op"] == "init"
        && let Some(policy) = policy
    {
        if !matches!(policy, "SinglePartition" | "Sealed" | "BrokerReady") {
            return Err(
                "KR_SIM_REQUEST_BATCHING_POLICY must be SinglePartition, Sealed or BrokerReady"
                    .into(),
            );
        }
        if let Some(explicit) = request.get("request_batching_policy")
            && explicit != policy
        {
            return Err("conflicting request batching policy inputs".into());
        }
        request["request_batching_policy"] = json!(policy);
    }
    Ok(request)
}
impl Bridge {
    fn call(&mut self, v: Value) -> Result<Value, String> {
        let op = string(&v, "op")?;
        if op == "catalogue" {
            return Ok(json!(
                kr_kafka_experiments::catalogue()
                    .iter()
                    .flat_map(|s| s
                        .variants
                        .iter()
                        .map(move |v| json!({"scenario":s.id,"variant":v.name,"title":s.title})))
                    .collect::<Vec<_>>()
            ));
        }
        if op == "init" {
            if self.session.is_some() {
                return Err("destroy the current session before init".into());
            }
            let catalogue = kr_kafka_experiments::catalogue();
            let scenario = catalogue
                .iter()
                .find(|s| Some(s.id) == v["scenario"].as_str())
                .ok_or("unknown scenario")?;
            let variant = scenario
                .variants
                .iter()
                .find(|x| Some(x.name.as_str()) == v["variant"].as_str())
                .ok_or("unknown variant")?;
            let size = match string(&v, "size")? {
                "test" => kr_kafka_experiments::Size::Test,
                "full" => kr_kafka_experiments::Size::Full,
                _ => return Err("size must be test or full".into()),
            };
            let mut manifest = scenario.build(variant, number(&v, "seed")?, size)?;
            let original = serde_json::to_value(&manifest).map_err(|e| e.to_string())?;
            let profile = v["profile"].as_str().unwrap_or("original");
            let mut adjustments = Vec::new();
            match profile {
                "original" => {}
                "common" => {
                    manifest.producer.lanes = 1;
                    manifest.producer.linger_skip_below_rate = None;
                    manifest.driver.encode_cost_ns = 1;
                    manifest.producer.descriptor_admission_policy = Default::default();
                    for load in &mut manifest.experiment.as_mut().unwrap().loads {
                        if let kr_kafka_sim::LanePolicy::Fixed(_) = load.template.lane {
                            load.template.lane = kr_kafka_sim::LanePolicy::Fixed(0);
                        }
                    }
                    adjustments.extend(
                        [
                            "one lane",
                            "sparse linger bypass disabled",
                            "native encode quantum reduced to its required 1 ns minimum",
                            "shared descriptor admission",
                        ]
                        .map(str::to_string),
                    );
                    for rule in &mut manifest.faults.environment {
                        if scenario.id == "soft.metadata-loss-during-move" {
                            rule.broker = None;
                            adjustments.push("metadata outage covers every broker; metadata broker selection differs".into());
                        }
                        if rule.effects.outcome == kr_kafka_sim::faults::Outcome::Drop {
                            rule.effects.outcome = kr_kafka_sim::faults::Outcome::Disconnect;
                            adjustments.push(format!(
                                "broker-hook Drop becomes Disconnect at {:?}",
                                rule.phase
                            ));
                        }
                    }
                }
                _ => return Err("profile must be original or common".into()),
            }
            if let Some(mode) = v.get("batch_target_mode") {
                manifest.producer.batch_target_mode = match mode.as_str() {
                    Some("Raw") => kr_kafka_producer::config::BatchTargetMode::Raw,
                    Some("EstimatedWire") => {
                        kr_kafka_producer::config::BatchTargetMode::EstimatedWire
                    }
                    _ => return Err("batch_target_mode must be Raw or EstimatedWire".into()),
                };
                adjustments.push(format!(
                    "batch target mode override: {:?}",
                    manifest.producer.batch_target_mode
                ));
            }
            if let Some(policy) = v.get("request_batching_policy") {
                use kr_kafka_producer::config::RequestBatchingPolicy;
                manifest.producer.request_batching_policy = match policy.as_str() {
                    Some("SinglePartition") => RequestBatchingPolicy::SinglePartition,
                    Some("Sealed") => RequestBatchingPolicy::Sealed,
                    Some("BrokerReady") => RequestBatchingPolicy::BrokerReady,
                    _ => return Err(
                        "request_batching_policy must be SinglePartition, Sealed or BrokerReady"
                            .into(),
                    ),
                };
                adjustments.push(format!(
                    "request batching policy override: {:?}",
                    manifest.producer.request_batching_policy
                ));
            }
            let native = match string(&v, "adapter")? {
                "native" => true,
                "classic" => false,
                _ => return Err("adapter must be native or classic".into()),
            };
            self.session = Some(ExternalSession::new(manifest, native)?);
            return Ok(json!({"schema":"kr-classic-sim/v1", "profile":profile,
                "original_manifest":original, "adjustments":adjustments,
                "manifest":self.session.as_ref().unwrap().manifest()}));
        }
        if op == "destroy" {
            if let Some(mut session) = self.session.take() {
                session.shutdown()?;
            }
            return Ok(json!({}));
        }
        let s = self.session.as_mut().ok_or("no simulation session")?;
        match op {
            "advance" => s.advance(number(&v, "until_ns")?),
            "connect" => {
                s.connect(
                    number(&v, "id")?,
                    i32::try_from(number(&v, "broker")?).map_err(|_| "broker overflow")?,
                    number(&v, "timeout_ns")?,
                )?;
                Ok(json!({}))
            }
            "write" => {
                s.write(
                    number(&v, "id")?,
                    serde_json::from_value(v["bytes"].clone()).map_err(|e| e.to_string())?,
                )?;
                Ok(json!({}))
            }
            "disconnect" => {
                s.disconnect(number(&v, "id")?)?;
                Ok(json!({}))
            }
            "record" => s.materialize(
                usize::try_from(number(&v, "load")?).map_err(|_| "load overflow")?,
                u32::try_from(number(&v, "index")?).map_err(|_| "index overflow")?,
                usize::try_from(number(&v, "partitions")?).map_err(|_| "partitions overflow")?,
            ),
            "accept" => s.accept(number(&v, "id")?),
            "poll" => s.poll_native(),
            "metadata" => s.metadata(number(&v, "topic")? as usize),
            "close" => {
                s.close_native(number(&v, "timeout_ns")?)?;
                Ok(json!({}))
            }
            "evidence" => s.evidence(),
            "shutdown" => s.shutdown(),
            "export_history" => {
                s.export_history(std::path::Path::new(string(&v, "path")?))?;
                Ok(json!({}))
            }
            "export" => {
                s.export_evidence(std::path::Path::new(string(&v, "path")?))?;
                Ok(json!({}))
            }
            "history" => Ok(json!(s.history())),
            _ => Err(format!("unknown operation {op}")),
        }
    }
}

/// Execute a command on this thread's session. The returned string remains
/// valid until the next call on this thread. No pointer is retained from input.
/// Call `destroy` on the creating thread before that thread exits or the library
/// is unloaded. Runtime teardown uses owner-thread state and cannot be deferred
/// to the platform's unspecified TLS destruction order. `destroy` is idempotent.
///
/// # Safety
/// `request` must point to a readable NUL-terminated UTF-8 string, at most 16 MiB.
/// The caller must not retain or mutate the returned pointer across calls.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_sim_call(request: *const c_char) -> *const c_char {
    BRIDGE.with(|cell| {
        let mut bridge = cell.borrow_mut();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if request.is_null() {
                return Err("null request".to_string());
            }
            // SAFETY: the caller guarantees a readable NUL-terminated input.
            let bytes = unsafe { CStr::from_ptr(request) }.to_bytes();
            if bytes.len() > 16 * 1024 * 1024 {
                return Err("command exceeds 16 MiB".into());
            }
            let request: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
            let mode = if request["op"] == "init" {
                match std::env::var("KR_SIM_BATCH_TARGET_MODE") {
                    Ok(value) => Some(value),
                    Err(std::env::VarError::NotPresent) => None,
                    Err(error) => return Err(error.to_string()),
                }
            } else {
                None
            };
            let request = configure_init(request, mode.as_deref())?;
            let policy = if request["op"] == "init" {
                match std::env::var("KR_SIM_REQUEST_BATCHING_POLICY") {
                    Ok(policy) => Some(policy),
                    Err(std::env::VarError::NotPresent) => None,
                    Err(error) => return Err(error.to_string()),
                }
            } else {
                None
            };
            bridge.call(configure_request_policy(request, policy.as_deref())?)
        }));
        let reply = match result {
            Ok(Ok(value)) => json!({"ok":value}),
            Ok(Err(error)) => json!({"error":error}),
            Err(_) => {
                bridge.session = None;
                json!({"error":"simulation panic; session destroyed"})
            }
        };
        bridge.reply = CString::new(reply.to_string()).expect("JSON escapes NUL");
        bridge.reply.as_ptr()
    })
}
/// Byte length including NUL of this thread's most recent reply.
#[unsafe(no_mangle)]
pub extern "C" fn kr_sim_reply_len() -> usize {
    BRIDGE.with(|b| b.borrow().reply.as_bytes_with_nul().len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_override_changes_only_the_recorded_policy() {
        for profile in ["original", "common"] {
            let command = json!({"op":"init", "scenario":"baseline.compression",
                "variant":"random0-zstd1", "size":"test", "seed":0,
                "adapter":"native", "profile":profile});
            let mut expected = None;
            for mode in ["Raw", "EstimatedWire"] {
                let mut bridge = Bridge::default();
                let mut result = bridge
                    .call(configure_init(command.clone(), Some(mode)).unwrap())
                    .unwrap();
                assert_eq!(result["manifest"]["producer"]["batch_target_mode"], mode);
                assert_eq!(
                    result["original_manifest"]["producer"]["batch_target_mode"],
                    "EstimatedWire"
                );
                result["manifest"]["producer"]
                    .as_object_mut()
                    .unwrap()
                    .remove("batch_target_mode");
                assert_eq!(
                    result["adjustments"].as_array_mut().unwrap().pop().unwrap(),
                    format!("batch target mode override: {mode}")
                );
                if let Some(expected) = &expected {
                    assert_eq!(&result, expected);
                } else {
                    expected = Some(result);
                }
                bridge.call(json!({"op":"destroy"})).unwrap();
            }
            let mut bad = command;
            bad["batch_target_mode"] = json!("unknown");
            let mut bridge = Bridge::default();
            assert!(bridge.call(bad).is_err());
            assert!(bridge.session.is_none());
        }
        let init = json!({"op":"init"});
        assert_eq!(configure_init(init.clone(), None).unwrap(), init);
        assert!(configure_init(init, Some("unknown")).is_err());
        assert!(
            configure_init(
                json!({"op":"init", "batch_target_mode":"Raw"}),
                Some("EstimatedWire")
            )
            .is_err()
        );
        let poll = json!({"op":"poll"});
        assert_eq!(configure_init(poll.clone(), Some("unknown")).unwrap(), poll);
    }

    #[test]
    fn request_policy_override_changes_only_the_recorded_policy() {
        for profile in ["original", "common"] {
            let command = json!({"op":"init", "scenario":"baseline.compression",
                "variant":"random0-zstd1", "size":"test", "seed":0,
                "adapter":"native", "profile":profile});
            let mut expected = None;
            for mode in ["SinglePartition", "Sealed", "BrokerReady"] {
                let mut bridge = Bridge::default();
                let mut result = bridge
                    .call(configure_request_policy(command.clone(), Some(mode)).unwrap())
                    .unwrap();
                assert_eq!(
                    result["manifest"]["producer"]["request_batching_policy"],
                    mode
                );
                assert_eq!(
                    result["original_manifest"]["producer"]["request_batching_policy"],
                    "Sealed"
                );
                result["manifest"]["producer"]
                    .as_object_mut()
                    .unwrap()
                    .remove("request_batching_policy");
                assert_eq!(
                    result["adjustments"].as_array_mut().unwrap().pop().unwrap(),
                    format!("request batching policy override: {mode}")
                );
                if let Some(expected) = &expected {
                    assert_eq!(&result, expected);
                } else {
                    expected = Some(result);
                }
                bridge.call(json!({"op":"destroy"})).unwrap();
            }
            let mut bad = command;
            bad["request_batching_policy"] = json!("unknown");
            let mut bridge = Bridge::default();
            assert!(bridge.call(bad).is_err());
            assert!(bridge.session.is_none());
        }
        let init = json!({"op":"init"});
        assert_eq!(configure_request_policy(init.clone(), None).unwrap(), init);
        assert!(configure_request_policy(init, Some("unknown")).is_err());
        assert!(
            configure_request_policy(
                json!({"op":"init", "request_batching_policy":"SinglePartition"}),
                Some("Sealed")
            )
            .is_err()
        );
        let poll = json!({"op":"poll"});
        assert_eq!(
            configure_request_policy(poll.clone(), Some("unknown")).unwrap(),
            poll
        );
    }

    fn native_run(seed: u64) -> Value {
        let mut bridge = Bridge::default();
        bridge
            .call(
                json!({"op":"init", "scenario":"baseline.closed-loop-inflight",
            "variant":"k64-i5", "size":"test", "seed":seed, "adapter":"native"}),
            )
            .unwrap();
        for index in 0..32 {
            let record = bridge
                .call(json!({"op":"record", "load":0,"index":index,"partitions":6}))
                .unwrap();
            assert_eq!(record["id"], index + 1);
            assert!(
                bridge.call(json!({"op":"accept","id":index + 1})).unwrap()["accepted"]
                    .as_bool()
                    .unwrap()
            );
        }
        let mut delivered = std::collections::BTreeSet::new();
        for tick in 1..=1000 {
            bridge
                .call(json!({"op":"advance","until_ns":tick * 1_000_000}))
                .unwrap();
            for event in bridge
                .call(json!({"op":"poll"}))
                .unwrap()
                .as_array()
                .unwrap()
            {
                if event["kind"] == "delivery" {
                    assert_eq!(event["outcome"], 0);
                    assert!(delivered.insert(event["id"].as_u64().unwrap()));
                }
            }
            if delivered.len() == 32 {
                break;
            }
        }
        assert_eq!(
            delivered.len(),
            32,
            "native Panama command path must terminate"
        );
        let evidence = bridge.call(json!({"op":"evidence"})).unwrap();
        assert_eq!(evidence["log"].as_array().unwrap().len(), 32);
        let path =
            std::env::temp_dir().join(format!("kr-sim-export-{}-{seed}.json", std::process::id()));
        bridge.call(json!({"op":"export", "path":path})).unwrap();
        let streamed: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(
            streamed, evidence,
            "Full export must preserve complete evidence"
        );
        assert_eq!(
            bridge.call(json!({"op":"shutdown"})).unwrap(),
            json!({"connections":0,"operations":0,"read_bytes":0,"write_bytes":0})
        );
        bridge.call(json!({"op":"destroy"})).unwrap();
        evidence
    }

    #[test]
    fn native_command_path_preserves_records_and_replays() {
        for seed in [0, 7, u64::MAX] {
            assert_eq!(native_run(seed), native_run(seed), "seed {seed}");
        }
    }

    #[test]
    fn catalogue_and_common_profile_keep_the_original_scenario_available() {
        let mut bridge = Bridge::default();
        assert_eq!(
            bridge
                .call(json!({"op":"catalogue"}))
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            146
        );
        let profile = bridge.call(json!({"op":"init", "scenario":"soft.sustained-random-loss",
            "variant":"loss1-slow0", "size":"test", "seed":0, "adapter":"classic", "profile":"common"})).unwrap();
        assert_eq!(
            profile["original_manifest"]["faults"]["environment"][0]["effects"]["outcome"],
            "Drop"
        );
        assert_eq!(
            profile["manifest"]["faults"]["environment"][0]["effects"]["outcome"],
            "Disconnect"
        );
        assert_eq!(
            profile["manifest"]["experiment"],
            profile["original_manifest"]["experiment"]
        );
        assert!(
            bridge
                .call(json!({"op":"record", "load":999,"index":0,"partitions":6}))
                .is_err()
        );
        assert!(
            bridge
                .call(json!({"op":"advance", "until_ns":u64::MAX}))
                .is_err()
        );
        bridge.call(json!({"op":"destroy"})).unwrap();
        assert!(bridge.call(json!({"op":"poll"})).is_err());
    }
}
