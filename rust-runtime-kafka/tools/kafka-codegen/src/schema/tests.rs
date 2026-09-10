use super::*;
use serde_json::{Value, json};

fn base(fields: Value) -> Value {
    json!({"name":"TestRequest","apiKey":1,"type":"request","validVersions":"0-3","flexibleVersions":"2+","fields":fields})
}
fn read(value: Value) -> Result<Schema, Error> {
    parse(&value.to_string())
}
fn field(ty: &str) -> Value {
    json!({"name":"Value","type":ty,"versions":"0+"})
}
fn rejected(value: Value, expected: &str) {
    let error = read(value).unwrap_err().to_string();
    assert!(
        error.contains(expected),
        "expected {expected:?} in {error:?}"
    );
}

#[test]
fn comments_are_not_removed_from_strings_and_error_lines_are_preserved() {
    let source = "// copyright 日本語\n{\"name\":\"TestRequest\",\"type\":\"request\",\"apiKey\":1,\n\"validVersions\":\"0\",\"flexibleVersions\":\"none\",\"fields\":[\n{\"name\":\"Path\",\"type\":\"string\",\"versions\":\"0+\",\"default\":\"https://x/\\\"//日本語\"}// comment\n]}// final";
    let schema = parse(source).unwrap();
    let layout = compile(&schema, &[0]).unwrap();
    assert_eq!(
        layout.layouts[0].root.fields[0].default,
        DefaultValue::String("https://x/\"//日本語".into())
    );
    assert!(
        parse("// a\n{}\n,")
            .unwrap_err()
            .to_string()
            .contains("line 2")
    );
    assert!(parse("/* not Kafka comments */ {}").is_err());
}

#[test]
fn version_grammar_is_exact_and_bounded_to_nonnegative_i16() {
    for (range, min, max) in [
        ("none", None, None),
        ("0", Some(0), Some(0)),
        ("1-3", Some(1), Some(3)),
        ("7+", Some(7), Some(i16::MAX)),
        ("32767", Some(i16::MAX), Some(i16::MAX)),
    ] {
        let parsed = VersionRange::parse(range).unwrap();
        assert_eq!((parsed.min(), parsed.max()), (min, max));
        assert!(!parsed.contains(-1));
        if let Some(min) = min {
            assert!(parsed.contains(min));
        }
        if let Some(max) = max {
            assert!(parsed.contains(max));
        }
    }
    for range in [
        "", "-1", "32768", "3-1", "1--2", "1-2-3", "1++", "+1", "1,2", " 1", "1 ", "None", "0-",
        "0x1", "1-32768",
    ] {
        assert!(VersionRange::parse(range).is_err(), "accepted {range:?}");
    }
}

#[test]
fn unknown_and_duplicate_properties_never_disappear() {
    let mut schema = base(json!([]));
    schema["typo"] = json!(true);
    rejected(schema, "unknown field");
    let mut f = field("int32");
    f["nullableVersion"] = json!("0+");
    rejected(base(json!([f])), "unknown field");
    let source =
        r#"{"name":"A","name":"B","type":"data","validVersions":"0","flexibleVersions":"none"}"#;
    assert!(
        parse(source)
            .unwrap_err()
            .to_string()
            .contains("duplicate field")
    );
    let mut schema = base(json!([]));
    schema["commonStructs"] = json!([{"name":"Unused","versions":"0+","field":[]}]);
    rejected(schema, "unknown field");
}

#[test]
fn all_pinned_schemas_parse_validate_and_classify_every_valid_version() {
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../schemas");
    let mut paths = std::fs::read_dir(directory)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|s| s == "json"))
        .collect::<Vec<_>>();
    paths.sort();
    assert!(paths.len() >= 200, "pinned corpus unexpectedly shrank");
    for path in paths {
        let source = std::fs::read_to_string(&path).unwrap();
        let schema = parse(&source).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        if let (Some(min), Some(max)) = (schema.valid_versions.min(), schema.valid_versions.max()) {
            let versions = (min..=max).collect::<Vec<_>>();
            let compiled =
                compile(&schema, &versions).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let actual = compiled
                .layouts
                .iter()
                .flat_map(|l| l.versions.iter().copied())
                .collect::<BTreeSet<_>>();
            assert_eq!(actual, versions.into_iter().collect(), "{}", path.display());
        }
    }
}

#[test]
fn producer_layout_classes_include_nested_tags_and_topic_id_transition() {
    let schemas = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../schemas");
    for (name, expected) in [
        ("ProduceRequest", vec![vec![9, 10, 11, 12], vec![13]]),
        ("ProduceResponse", vec![vec![9], vec![10, 11, 12], vec![13]]),
    ] {
        let schema =
            parse(&std::fs::read_to_string(schemas.join(format!("{name}.json"))).unwrap()).unwrap();
        let message = compile(&schema, &[13, 12, 11, 10, 9]).unwrap();
        assert_eq!(
            message
                .layouts
                .iter()
                .map(|l| l.versions.clone())
                .collect::<Vec<_>>(),
            expected
        );
        let singleton = compile(&schema, &[9]).unwrap();
        assert_eq!(singleton.layouts[0].root, message.layouts[0].root);
    }
}

#[test]
fn header_field_override_keeps_client_id_classic_in_flexible_header() {
    let source = include_str!("../../../../schemas/RequestHeader.json");
    let schema = parse(source).unwrap();
    let message = compile(&schema, &[1, 2]).unwrap();
    assert!(!message.layouts[0].root.flexible);
    assert!(message.layouts[1].root.flexible);
    let client = message.layouts[1]
        .root
        .fields
        .iter()
        .find(|f| f.name == "ClientId")
        .unwrap();
    assert!(!client.compact);
    assert!(client.nullable);
}

#[test]
fn common_structs_inline_structs_and_defaulted_tag_versions_resolve() {
    let mut value = base(json!([
        {"name":"One","type":"Common","versions":"0+"},
        {"name":"Many","type":"[]Common","versions":"0+"},
        {"name":"Tag","type":"Inner","taggedVersions":"2+","tag":0,"fields":[
            {"name":"N","type":"int32","versions":"0+","default":"-1"}
        ]}
    ]));
    value["commonStructs"] = json!([{"name":"Common","versions":"0+","fields":[
        {"name":"Text","type":"string","versions":"0+"}
    ]}]);
    let schema = read(value).unwrap();
    let compiled = compile(&schema, &[0, 1, 2, 3]).unwrap();
    assert_eq!(compiled.layouts.len(), 2);
    let fields = &compiled.layouts[1].root.fields;
    assert_eq!(fields[2].tag, Some(0));
    let FieldType::Struct(one) = &fields[0].ty else {
        panic!()
    };
    let FieldType::Array(many) = &fields[1].ty else {
        panic!()
    };
    let FieldType::Struct(many) = &**many else {
        panic!()
    };
    assert_eq!(one, many);
    assert!(one.flexible && one.fields[0].compact);
}

#[test]
fn schema_and_reference_validation_includes_unselected_and_unused_definitions() {
    rejected(base(json!([field("garbage")])), "unknown field type");
    rejected(base(json!([field("Missing")])), "undefined struct");
    rejected(base(json!([field("[][]int32")])), "arrays of arrays");
    let mut f = field("int32");
    f["fields"] = json!([]);
    rejected(base(json!([f])), "non-struct field");
    let mut schema = base(json!([]));
    schema["commonStructs"] = json!([{"name":"Unused","versions":"0+","fields":[{"name":"Bad","type":"Missing","versions":"100+"}]}]);
    rejected(schema, "undefined struct");
    let mut schema = base(json!([]));
    schema["commonStructs"] = json!([
        {"name":"A","versions":"0+","fields":[{"name":"B","type":"B","versions":"0+"}]},
        {"name":"B","versions":"0+","fields":[{"name":"A","type":"A","versions":"0+"}]}
    ]);
    rejected(schema, "recursive struct");
    let mut schema = base(json!([field("Common")]));
    schema["commonStructs"] = json!([{"name":"Common","versions":"2+","fields":[]}]);
    rejected(schema, "unavailable");
}

#[test]
fn parent_versions_limit_reachable_common_struct_versions() {
    let mut schema = base(
        json!([{"name":"Wrapper","type":"Wrapper","versions":"2+","fields":[
            {"name":"Common","type":"Common","versions":"0+"}
        ]}]),
    );
    schema["commonStructs"] = json!([{"name":"Common","versions":"2+","fields":[]}]);
    assert!(read(schema).is_ok());
}

#[test]
fn invalid_names_duplicates_and_version_selection_are_errors() {
    for name in ["", "snake_name", "2Bad", "Bad-Name", "É"] {
        let mut f = field("int32");
        f["name"] = json!(name);
        rejected(base(json!([f])), "invalid schema name");
    }
    rejected(
        base(json!([field("int32"), field("int32")])),
        "duplicate field",
    );
    let mut schema = base(json!([]));
    schema["validVersions"] = json!("0+");
    rejected(schema, "finite maximum");
    let mut schema = base(json!([]));
    schema["flexibleVersions"] = json!("1-2");
    rejected(schema, "open-ended");
    let schema = read(base(json!([]))).unwrap();
    for versions in [vec![], vec![1, 1], vec![-1], vec![4]] {
        assert!(compile(&schema, &versions).is_err());
    }
    let mut schema = schema;
    schema.fields.push(Field {
        name: "Bad".into(),
        type_name: "nonsense".into(),
        versions: Some(VersionRange::Open { start: 0 }),
        nullable_versions: VersionRange::None,
        flexible_versions: None,
        tagged_versions: VersionRange::None,
        tag: None,
        default: None,
        ignorable: false,
        map_key: false,
        zero_copy: false,
        entity_type: None,
        about: None,
        fields: None,
    });
    assert!(compile(&schema, &[0]).is_err());
}

#[test]
fn tags_require_unique_contiguous_ids_valid_versions_and_uniform_nullability() {
    let tagged =
        || json!({"name":"Tag","type":"string","versions":"0+","taggedVersions":"2+","tag":0});
    for (key, value, expected) in [
        ("tag", json!(-1), "schema JSON"),
        ("tag", json!(2147483648_u64), "signed int32"),
        ("tag", json!(1), "contiguous"),
        ("taggedVersions", json!("2-3"), "open-ended"),
        ("taggedVersions", json!("none"), "open-ended"),
        ("taggedVersions", json!("1+"), "message flexibleVersions"),
        ("versions", json!("3+"), "subset of field versions"),
        ("nullableVersions", json!("3+"), "all tagged versions"),
        ("mapKey", json!(true), "map keys"),
    ] {
        let mut f = tagged();
        f[key] = value;
        rejected(base(json!([f])), expected);
    }
    let mut f = tagged();
    f.as_object_mut().unwrap().remove("tag");
    rejected(base(json!([f])), "requires a tag");
    let a = tagged();
    let mut b = tagged();
    b["name"] = json!("Other");
    rejected(base(json!([a, b])), "duplicate tag");
    let mut a = tagged();
    a["nullableVersions"] = json!("0-1");
    assert!(read(base(json!([a]))).is_ok());
}

#[test]
fn nullable_defaults_and_generation_metadata_are_checked() {
    let mut f = field("int32");
    f["nullableVersions"] = json!("0+");
    rejected(base(json!([f])), "cannot be nullable");
    let mut f = field("string");
    f["default"] = json!("null");
    f["nullableVersions"] = json!("1+");
    rejected(base(json!([f])), "all field versions");
    let mut f = field("int32");
    f["zeroCopy"] = json!(true);
    rejected(base(json!([f])), "only valid on bytes");
    let mut f = field("int32");
    f["entityType"] = json!("groupId");
    rejected(base(json!([f])), "does not match");
    let mut f = field("string");
    f["entityType"] = json!("unknownFutureEntity");
    rejected(base(json!([f])), "unknown variant");
    let mut f = field("[]string");
    f["flexibleVersions"] = json!("none");
    rejected(base(json!([f])), "only valid on string or bytes");
    let mut f = field("string");
    f["flexibleVersions"] = json!("1+");
    rejected(base(json!([f])), "subset of message");
    let mut f = field("bytes");
    f["zeroCopy"] = json!(true);
    assert!(read(base(json!([f]))).is_ok());
}

#[test]
fn defaults_are_typed_and_checked_for_lossless_numeric_range() {
    for (ty, literal, expected) in [
        ("int8", "-128", DefaultValue::Signed(-128)),
        ("int16", "0x7fff", DefaultValue::Signed(32767)),
        ("int32", "010", DefaultValue::Signed(8)),
        (
            "int64",
            "-9223372036854775808",
            DefaultValue::Signed(i64::MIN),
        ),
        ("uint16", "65535", DefaultValue::Unsigned(65535)),
        ("uint32", "4294967295", DefaultValue::Unsigned(4294967295)),
        ("bool", "TRUE", DefaultValue::Bool(true)),
        ("float64", "-0.0", DefaultValue::Float((-0.0_f64).to_bits())),
    ] {
        let mut f = field(ty);
        f["default"] = json!(literal);
        let compiled = compile(&read(base(json!([f]))).unwrap(), &[0]).unwrap();
        assert_eq!(compiled.layouts[0].root.fields[0].default, expected);
    }
    for (ty, literal) in [
        ("int8", "128"),
        ("int16", "32768"),
        ("int32", "2147483648"),
        ("int64", "9223372036854775808"),
        ("uint16", "-1"),
        ("uint32", "4294967296"),
        ("bool", "yes"),
        ("bytes", "abc"),
        ("[]int32", "[1]"),
        ("float64", "NaN"),
        ("uuid", "custom"),
    ] {
        let mut f = field(ty);
        f["default"] = json!(literal);
        assert!(read(base(json!([f]))).is_err(), "{ty} {literal}");
    }
}

#[test]
fn recursive_nullability_and_tag_changes_prevent_accidental_layout_merging() {
    let schema = read(base(
        json!([{"name":"Nested","type":"Nested","versions":"0+","fields":[
            {"name":"Text","type":"string","versions":"0+","nullableVersions":"1+"},
            {"name":"Tagged","type":"int32","versions":"0+","taggedVersions":"3+","tag":0}
        ]}]),
    ))
    .unwrap();
    let compiled = compile(&schema, &[0, 1, 2, 3]).unwrap();
    assert_eq!(compiled.layouts.len(), 4);
    assert!(compiled.layouts.iter().all(|l| l.versions.len() == 1));
}

#[test]
fn optional_defaults_are_strict_and_known_unsupported_constructs_are_errors() {
    for literal in [Value::Null, json!([]), json!({})] {
        let mut f = field("int32");
        f["default"] = literal;
        rejected(base(json!([f])), "schema JSON");
    }
    for literal in ["--1", "++1", "-+1", "+-1", "0x-1", " 1", "1 "] {
        let mut f = field("int32");
        f["default"] = json!(literal);
        rejected(base(json!([f])), "invalid default");
    }
    let mut f = field("records");
    f["tag"] = json!(0);
    f["taggedVersions"] = json!("2+");
    rejected(base(json!([f])), "records cannot be tagged");
    let mut f = field("string");
    f["tag"] = json!(0);
    f["taggedVersions"] = json!("2+");
    f["flexibleVersions"] = json!("none");
    rejected(base(json!([f])), "classic encoding overrides for tagged");
    let mut schema = base(json!([]));
    schema.as_object_mut().unwrap().remove("flexibleVersions");
    rejected(schema.clone(), "must specify flexibleVersions");
    schema["validVersions"] = json!("none");
    assert!(read(schema).is_ok());
}

#[test]
fn unused_common_structs_still_require_valid_references_and_names() {
    let mut schema = base(json!([]));
    schema["commonStructs"] = json!([
        {"name":"Unused","versions":"0+","fields":[{"name":"Other","type":"Other","versions":"0+"}]},
        {"name":"Other","versions":"2+","fields":[]}
    ]);
    rejected(schema, "unavailable");
    let mut schema = base(json!([]));
    schema["commonStructs"] = json!([{"name":"bad","versions":"0+","fields":[]}]);
    rejected(schema, "uppercase");
}

#[test]
fn common_struct_chains_have_explicit_depth_limits_and_expansion_is_bounded() {
    let mut schema = base(json!([{"name":"Root","type":"Node0","versions":"0+"}]));
    schema["commonStructs"] = Value::Array(
        (0..129)
            .map(|i| {
                let fields = if i == 128 {
                    json!([])
                } else {
                    json!([{"name":"Next","type":format!("Node{}",i+1),"versions":"0+"}])
                };
                json!({"name":format!("Node{i}"),"versions":"0+","fields":fields})
            })
            .collect(),
    );
    rejected(schema, "128-level limit");

    // A small acyclic source can expand exponentially. Exercise the same
    // resolver budget with a reduced bound so this regression remains cheap.
    let mut schema = base(json!([{"name":"Root","type":"Node0","versions":"0+"}]));
    schema["commonStructs"] = Value::Array(
        (0..12)
            .map(|i| {
                let fields = if i == 11 {
                    json!([])
                } else {
                    json!([
                        {"name":"Left","type":format!("Node{}",i+1),"versions":"0+"},
                        {"name":"Right","type":format!("Node{}",i+1),"versions":"0+"}
                    ])
                };
                json!({"name":format!("Node{i}"),"versions":"0+","fields":fields})
            })
            .collect(),
    );
    let schema = read(schema).unwrap();
    let definitions = registry(&schema).unwrap();
    let mut remaining = 64;
    assert!(
        resolve_struct(&schema.name, 0, false, &definitions, &mut remaining)
            .unwrap_err()
            .to_string()
            .contains("compilation limit")
    );
    assert_eq!(remaining, 0);
}

#[test]
fn version_resolution_matches_an_independent_field_presence_oracle() {
    // Exercise intersections across every version, scalar nullability changes,
    // nested field introduction/removal, tag promotion, and an encoding override.
    let schema = read(base(json!([
        {"name":"Text","type":"string","versions":"0+","nullableVersions":"1+"},
        {"name":"Classic","type":"bytes","versions":"1-2","flexibleVersions":"none"},
        {"name":"Nested","type":"Item","versions":"0+","fields":[
            {"name":"Old","type":"int32","versions":"0-1"},
            {"name":"New","type":"int64","versions":"2+"},
            {"name":"Tag","type":"bool","versions":"0+","taggedVersions":"3+","tag":0}
        ]}
    ])))
    .unwrap();
    let compiled = compile(&schema, &[3, 1, 2, 0]).unwrap();
    for v in 0..=3 {
        let root = &compiled
            .layouts
            .iter()
            .find(|l| l.versions.contains(&v))
            .unwrap()
            .root;
        assert_eq!(root.flexible, v >= 2);
        assert_eq!(root.fields[0].nullable, v >= 1);
        assert_eq!(root.fields[0].compact, v >= 2);
        assert_eq!(
            root.fields.iter().any(|f| f.name == "Classic"),
            (1..=2).contains(&v)
        );
        if let Some(classic) = root.fields.iter().find(|f| f.name == "Classic") {
            assert!(!classic.compact);
        }
        let FieldType::Struct(nested) = &root.fields.last().unwrap().ty else {
            panic!()
        };
        assert_eq!(nested.fields[0].name, if v < 2 { "Old" } else { "New" });
        assert_eq!(nested.fields[1].tag, if v == 3 { Some(0) } else { None });
    }
}

#[test]
fn pathological_array_type_strings_are_rejected_before_recursive_parsing() {
    let ty = format!("{}int32", "[]".repeat(100_000));
    rejected(base(json!([field(&ty)])), "arrays of arrays");
}
