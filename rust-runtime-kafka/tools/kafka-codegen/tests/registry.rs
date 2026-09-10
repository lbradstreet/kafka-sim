use kr_kafka_codegen::{registry, schema};

fn message(name: &str, kind: &str, key: i16, versions: &[i16]) -> schema::CompiledMessage {
    let source = serde_json::json!({"name":name,"type":kind,"apiKey":key,"validVersions":"0-3","flexibleVersions":"3+","fields":[]});
    schema::compile(&schema::parse(&source.to_string()).unwrap(), versions).unwrap()
}

#[test]
fn registry_rejects_ambiguous_incomplete_and_invalid_inventories() {
    let req = message("ExampleRequest", "request", 1, &[0, 3]);
    let resp = message("ExampleResponse", "response", 1, &[0, 3]);
    assert!(registry::emit(&[req.clone(), resp.clone()]).is_ok());
    for messages in [
        vec![],
        vec![req.clone()],
        vec![resp.clone()],
        vec![
            req.clone(),
            resp.clone(),
            message("OtherRequest", "request", 1, &[0, 3]),
        ],
        vec![
            req.clone(),
            resp.clone(),
            message("OtherResponse", "response", 1, &[0, 3]),
        ],
        vec![req.clone(), message("ExampleResponse", "response", 1, &[0])],
        vec![
            req.clone(),
            message("ExampleResponse", "response", 2, &[0, 3]),
        ],
    ] {
        assert!(registry::emit(&messages).is_err());
    }
    let mut invalid = req;
    invalid.api_key = None;
    assert!(registry::emit(&[invalid, resp]).is_err());
    assert!(
        registry::emit(&[
            message("ControlRequest", "request", 7, &[0]),
            message("ControlResponse", "response", 7, &[0])
        ])
        .is_err()
    );
}
