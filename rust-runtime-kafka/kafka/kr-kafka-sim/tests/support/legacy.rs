//! Deliberately narrow compatibility projection. Keep every legacy event field;
//! only explicitly listed additive diagnostics may disappear from this view.
use kr_kafka_sim::RunReport;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub(crate) fn project(report: &RunReport) -> Value {
    project_json(serde_json::to_value(report).unwrap())
}

pub(crate) fn project_json(mut value: Value) -> Value {
    let mut entries = value["history"]["entries"].as_array().unwrap().clone();
    entries.retain(|entry| {
        let event = entry["event"].as_object().unwrap();
        !event
            .keys()
            .any(|name| matches!(name.as_str(), "ConnectionOpened" | "ConnectionClosed"))
    });
    // Legacy serialized events have no references to history ordinals. Hook,
    // connection and operation IDs are semantic identities and must stay exact.
    for (index, entry) in entries.iter_mut().enumerate() {
        entry["ordinal"] = json!(index + 1);
    }
    value["history"] = json!({"entries": entries});
    value["fault_decisions"] = value["manifest"]["fault_decisions"].clone();
    value["realized_faults"] = value["manifest"]["realized_faults"].clone();
    value.as_object_mut().unwrap().remove("manifest");
    value
}

pub(crate) fn digest(value: &Value) -> String {
    let hash = Sha256::digest(serde_json::to_vec(value).unwrap());
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}
