//! Adversarial backend checks and execution of freshly generated Rust against
//! the actual wire runtime. These do not depend on checked-in generated output.
use kr_kafka_codegen::{
    emit,
    schema::{self, CompiledMessage, DefaultValue, FieldType},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

const FINGERPRINT: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn message(name: &str, fields: Value) -> CompiledMessage {
    let source = json!({"name": name, "type":"request", "apiKey":1,
        "validVersions":"0-2", "flexibleVersions":"1+", "fields":fields});
    schema::compile(&schema::parse(&source.to_string()).unwrap(), &[0, 1, 2]).unwrap()
}

#[test]
fn names_that_collide_after_rust_conversion_are_rejected() {
    let messages = [message("FooBar", json!([])), message("FooBAR", json!([]))];
    assert!(
        emit::emit(&messages, FINGERPRINT)
            .unwrap_err()
            .contains("module name collision")
    );
    for fields in [
        json!([
            {"name":"FooBar","type":"int32","versions":"0+"},
            {"name":"FooBAR","type":"int32","versions":"0+"}
        ]),
        json!([{"name":"UnknownTags","type":"int32","versions":"0+"}]),
    ] {
        assert!(
            emit::emit(&[message("Example", fields)], FINGERPRINT)
                .unwrap_err()
                .contains("field name collision")
        );
    }
}

#[test]
fn generated_type_names_cannot_shadow_dependencies_keywords_or_primitives() {
    for name in [
        "Self", "Wire", "Reader", "Writer", "Result", "Option", "Default", "Some", "None", "Ok",
        "Err", "type", "mod", "async", "gen", "alloc", "bool", "str", "i32", "f64",
    ] {
        let mut candidate = message("Example", json!([]));
        candidate.name = name.into();
        for layout in &mut candidate.layouts {
            layout.root.name = name.into();
        }
        assert!(
            emit::emit(&[candidate], FINGERPRINT).is_err(),
            "accepted {name}"
        );
    }
}

#[test]
fn public_ir_and_fingerprint_cannot_inject_code_or_panic_the_emitter() {
    let original = message(
        "Example",
        json!([{"name":"Value","type":"int32","versions":"0+"}]),
    );
    for fingerprint in [
        "",
        "test",
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\npub fn injected() {}",
    ] {
        assert!(emit::emit(std::slice::from_ref(&original), fingerprint).is_err());
    }
    for invalid in ["", "mod Evil", "Evil{}", "Evil\n", "Évil"] {
        let mut altered = original.clone();
        altered.name = invalid.into();
        assert!(emit::emit(&[altered], FINGERPRINT).is_err());
        let mut altered = original.clone();
        altered.layouts[0].root.fields[0].name = invalid.into();
        assert!(emit::emit(&[altered], FINGERPRINT).is_err());
    }
    let mut altered = original.clone();
    altered.layouts[0].versions.clear();
    assert!(emit::emit(&[altered], FINGERPRINT).is_err());
    let mut altered = original.clone();
    altered.layouts[0].versions = vec![0, 0];
    assert!(emit::emit(&[altered], FINGERPRINT).is_err());
    let mut altered = original.clone();
    altered.layouts[0].root.fields[0].default =
        DefaultValue::String("\"}; pub fn injected() {}".into());
    assert!(emit::emit(&[altered], FINGERPRINT).is_err());
    let mut altered = original;
    altered.layouts[0].root.fields[0].ty = FieldType::Int8;
    altered.layouts[0].root.fields[0].default = DefaultValue::Signed(128);
    assert!(emit::emit(&[altered], FINGERPRINT).is_err());
}

#[test]
fn unsupported_nonnullable_records_fail_before_emitting_invalid_defaults() {
    let message = message(
        "Example",
        json!([{"name":"Value","type":"records","versions":"0+"}]),
    );
    assert!(
        emit::emit(&[message], FINGERPRINT)
            .unwrap_err()
            .contains("nonnullable records")
    );
}

struct Workspace(PathBuf);
impl Workspace {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "kafka-codegen-review-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn compile_and_execute(message: CompiledMessage, main: &str) {
    let work = Workspace::new();
    let generated = work.0.join("generated.rs");
    fs::write(&generated, emit::emit(&[message], FINGERPRINT).unwrap()).unwrap();
    let runtime =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../kafka/kr-kafka-protocol/src");
    let driver = format!(
        "extern crate alloc;\n#[path={:?}] pub mod plan;\n#[path={:?}] pub mod wire;\n#[path={:?}] pub mod generated;\n{}",
        runtime.join("plan.rs"),
        runtime.join("wire.rs"),
        generated,
        main
    );
    let source = work.0.join("driver.rs");
    fs::write(&source, driver).unwrap();
    let executable = work.0.join("review");
    let shared_bytes = work.0.join("libkr_shared_bytes.rlib");
    let output = Command::new("rustc")
        .args([
            "--edition=2024",
            "--crate-name=kr_shared_bytes",
            "--crate-type=rlib",
        ])
        .arg(runtime.join("../../../bytes/kr-shared-bytes/src/lib.rs"))
        .arg("-o")
        .arg(&shared_bytes)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "shared byte dependency failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = Command::new("rustc")
        .args([
            "--edition=2024",
            "--crate-name=kafka_codegen_review",
            "-A",
            "warnings",
        ])
        .arg("--extern")
        .arg(format!("kr_shared_bytes={}", shared_bytes.display()))
        .arg(&source)
        .arg("-o")
        .arg(&executable)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fresh generated Rust failed to compile:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = Command::new(&executable).output().unwrap();
    assert!(
        output.status.success(),
        "fresh generated codec execution failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn freshly_generated_defaults_primitives_names_and_nullable_structs_execute() {
    let source = json!({"name":"ReviewRequest","apiKey":1,"type":"request","validVersions":"0-3","flexibleVersions":"1+",
        "fields":[
            {"name":"R","type":"int32","versions":"0+"},
            {"name":"Type","type":"bool","versions":"0+","default":true},
            {"name":"Byte","type":"int8","versions":"0+","default":"-128"},
            {"name":"Short","type":"int16","versions":"0+","default":"32767"},
            {"name":"UnsignedShort","type":"uint16","versions":"0+","default":"65535"},
            {"name":"UnsignedInt","type":"uint32","versions":"0+","default":"4294967295"},
            {"name":"Long","type":"int64","versions":"0+","default":"-9223372036854775808"},
            {"name":"Double","type":"float64","versions":"0+","default":"-0.0"},
            {"name":"Uuid","type":"uuid","versions":"0+"},
            {"name":"Payload","type":"bytes","versions":"0+","nullableVersions":"0+","default":"null"},
            {"name":"Text","type":"string","versions":"0+","taggedVersions":"2+","tag":0,"default":"https://example/\"日本語\"\n\u{0000}"},
            {"name":"Child","type":"Child","versions":"0+","nullableVersions":"0+","taggedVersions":"2+","tag":1,"default":"null"},
            {"name":"DefaultChild","type":"Child","versions":"0+","nullableVersions":"0+","taggedVersions":"2+","tag":2},
            {"name":"Children","type":"[]Child","versions":"0+"},
            {"name":"Strings","type":"[]string","versions":"0+"},
            {"name":"Numbers","type":"[]int32","versions":"0+"}
        ],
        "commonStructs":[{"name":"Child","versions":"0+","fields":[
            {"name":"Value","type":"int32","versions":"0+","default":"-1"}
        ]}]
    });
    let compiled =
        schema::compile(&schema::parse(&source.to_string()).unwrap(), &[0, 1, 2, 3]).unwrap();
    assert_eq!(
        compiled
            .layouts
            .iter()
            .map(|v| &v.versions)
            .collect::<Vec<_>>(),
        [&vec![0], &vec![1], &vec![2, 3]]
    );
    compile_and_execute(
        compiled,
        r#"
use wire::{Wire, Reader, Writer, DecodeLimits, EncodeLimits, Sequence, TaggedFields};
fn main() {
    macro_rules! check {
        ($version:expr, $module:ident) => {{
            use generated::review_request::$module::{ReviewRequest, Child};
            let mut value = ReviewRequest::default();
            assert!(value.is_default());
            assert_eq!(value.byte, -128);
            assert_eq!(value.short, 32767);
            assert_eq!(value.unsigned_short, 65535);
            assert_eq!(value.unsigned_int, 4294967295);
            assert_eq!(value.long, i64::MIN);
            assert_eq!(value.double.to_bits(), (-0.0_f64).to_bits());
            assert!(value.child.is_none());
            assert_eq!(value.default_child.as_ref().unwrap().value, -1);
            value.r = 91;
            value.type_ = false;
            value.uuid = [255; 16];
            value.payload = Some(&[0, 1, 255]);
            value.text = "nondefault-π";
            value.child = Some(Child { value: 123, ..Default::default() });
            value.default_child = None;
            let children = [Child { value: 456, ..Default::default() }];
            value.children = Sequence::from_slice(&children);
            value.strings = Sequence::from_slice(&["π", "", "x"]);
            value.numbers = Sequence::from_slice(&[i32::MIN, 0, i32::MAX]);
            let mut writer = Writer::new($version, false, EncodeLimits::default());
            value.write(&mut writer).unwrap();
            let bytes = writer.finish().unwrap().to_vec().unwrap();
            let mut reader = Reader::new(&bytes, $version, false, DecodeLimits::default()).unwrap();
            let decoded = ReviewRequest::read(&mut reader).unwrap();
            reader.finish().unwrap();
            assert_eq!(decoded.r, 91);
            assert!(!decoded.type_);
            assert_eq!(decoded.uuid, [255; 16]);
            assert_eq!(decoded.payload, Some(&[0, 1, 255][..]));
            assert_eq!(decoded.text, "nondefault-π");
            assert_eq!(decoded.child.as_ref().unwrap().value, 123);
            assert!(decoded.default_child.is_none());
            assert_eq!(decoded.children.iter().next().unwrap().unwrap().value, 456);
            assert_eq!(decoded.strings.iter().map(Result::unwrap).collect::<Vec<_>>(), ["π", "", "x"]);
            assert_eq!(decoded.numbers.iter().map(Result::unwrap).collect::<Vec<_>>(), [i32::MIN, 0, i32::MAX]);
            let mut writer = Writer::new($version, false, EncodeLimits::default());
            decoded.write(&mut writer).unwrap();
            assert_eq!(bytes, writer.finish().unwrap().to_vec().unwrap());
        }};
    }
    check!(0, v0);
    check!(1, v1);
    check!(2, v2);
    check!(3, v2);
    // A schema-owned ID stays reserved when its typed value is elided as a default.
    let raw_tag = [1, 0, 2, 2, b'x'];
    let mut reader = Reader::new(&raw_tag, 2, true, DecodeLimits::default()).unwrap();
    let tags = TaggedFields::read(&mut reader).unwrap();
    let value = generated::review_request::v2::ReviewRequest { unknown_tags: tags, ..Default::default() };
    let mut writer = Writer::new(2, true, EncodeLimits::default());
    assert!(value.write(&mut writer).is_err());
    assert!(generated::review_request::decode(&[], 4, DecodeLimits::default()).is_err());
}
"#,
    );
}

#[test]
fn freshly_generated_nullable_struct_markers_match_java_generator_rules() {
    let message = message(
        "MarkerRequest",
        json!([
            {"name":"Child","type":"Child","versions":"0+","nullableVersions":"0+",
             "default":"null","taggedVersions":"1+","tag":0,
             "fields":[{"name":"Number","type":"int8","versions":"0+"}]}
        ]),
    );
    compile_and_execute(
        message,
        r#"
use wire::{Wire, Reader, Writer, DecodeLimits, EncodeLimits};
fn main() {
    use generated::marker_request::{v0, v1};
    let mut writer = Writer::new(0, false, EncodeLimits::default());
    v0::MarkerRequest::default().write(&mut writer).unwrap();
    assert_eq!(writer.finish().unwrap().to_vec().unwrap(), [255]);
    let value = v0::MarkerRequest { child: Some(v0::Child { number: 7, ..Default::default() }), ..Default::default() };
    let mut writer = Writer::new(0, false, EncodeLimits::default());
    value.write(&mut writer).unwrap();
    assert_eq!(writer.finish().unwrap().to_vec().unwrap(), [1, 7]);
    let value = v1::MarkerRequest { child: Some(v1::Child { number: 7, ..Default::default() }), ..Default::default() };
    let mut writer = Writer::new(1, true, EncodeLimits::default());
    value.write(&mut writer).unwrap();
    // Root tag count, ID, payload length, present marker, child number, child tags.
    assert_eq!(writer.finish().unwrap().to_vec().unwrap(), [1, 0, 3, 1, 7, 0]);
    for bytes in [&[0][..], &[2, 7]] {
        let mut reader = Reader::new(bytes, 0, false, DecodeLimits::default()).unwrap();
        assert!(v0::MarkerRequest::read(&mut reader).is_err());
    }
    let invalid_tag_marker = [1, 0, 1, 2];
    let mut reader = Reader::new(&invalid_tag_marker, 1, true, DecodeLimits::default()).unwrap();
    assert!(v1::MarkerRequest::read(&mut reader).is_err());
}
"#,
    );
}
