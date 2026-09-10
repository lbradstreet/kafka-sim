//! Reproducible offline regeneration; deliberately never runs from build.rs.
use kr_kafka_codegen::{SELECTION, emit, schema};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

fn main() {
    if let Err(error) = run() {
        eprintln!("kafka-codegen: {error}");
        std::process::exit(1);
    }
}
fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let check = match args.as_slice() {
        [] => false,
        [arg] if arg == "--check" => true,
        _ => return Err("usage: cargo run -p kr-kafka-codegen -- [--check]".into()),
    };
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let schemas_dir = root.join("schemas");
    let manifest_bytes = fs::read(schemas_dir.join("PROVENANCE.lock"))?;
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes)?;
    if manifest["format_version"] != 1 {
        return Err("unsupported schema provenance format".into());
    }
    let hashes = manifest["files"]
        .as_object()
        .ok_or("missing schema hashes")?;
    let mut sources = BTreeMap::new();
    for entry in fs::read_dir(&schemas_dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            let filename = path
                .file_name()
                .ok_or("invalid schema filename")?
                .to_str()
                .ok_or("non-UTF8 filename")?
                .to_string();
            let bytes = fs::read(&path)?;
            let expected = hashes
                .get(&filename)
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("untracked schema {filename}"))?;
            let actual = sha256(&bytes);
            if actual != expected {
                return Err(format!("schema checksum mismatch: {filename}").into());
            }
            let source = String::from_utf8(bytes)?;
            let parsed = schema::parse(&source).map_err(|err| format!("{filename}: {err}"))?;
            schema::validate(&parsed).map_err(|err| format!("{filename}: {err}"))?;
            if format!("{}.json", parsed.name) != filename {
                return Err(format!("schema name does not match filename: {filename}").into());
            }
            sources.insert(parsed.name.clone(), parsed);
        }
    }
    if sources.len() != hashes.len() {
        return Err("schema manifest lists missing inputs".into());
    }
    let mut messages = Vec::new();
    for &(name, versions) in SELECTION {
        messages.push(schema::compile(
            sources
                .get(name)
                .ok_or_else(|| format!("missing selected schema {name}"))?,
            versions,
        )?);
    }
    let fingerprint = sha256(&manifest_bytes);
    let generated = emit::emit(&messages, &fingerprint)?;
    let generated = format_rust(&generated)?;
    let banner = format!("// Schema input SHA-256: {fingerprint}\n");
    let fixture_tests =
        format_rust(&(banner.clone() + &kr_kafka_codegen::fixtures::emit(&messages)?))?;
    let registry = format_rust(&(banner + &kr_kafka_codegen::registry::emit(&messages)?))?;
    // Complete validation and emission before changing any generated artifact.
    output(
        &root.join("kafka/kr-kafka-protocol/src/generated.rs"),
        &generated,
        check,
    )?;
    output(
        &root.join("kafka/kr-kafka-protocol/tests/java_generated.rs"),
        &fixture_tests,
        check,
    )?;
    output(
        &root.join("kafka/kr-kafka-protocol/src/registry.rs"),
        &registry,
        check,
    )?;
    println!(
        "{} {} schemas; {} messages, {} layout classes",
        if check { "Verified" } else { "Compiled" },
        sources.len(),
        messages.len(),
        messages.iter().map(|m| m.layouts.len()).sum::<usize>()
    );
    Ok(())
}
fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn output(path: &Path, source: &str, check: bool) -> Result<(), Box<dyn Error>> {
    if check {
        if fs::read_to_string(path)? != source {
            return Err(format!(
                "generated output differs: {}; run cargo run -p kr-kafka-codegen",
                path.display()
            )
            .into());
        }
    } else {
        fs::write(path, source)?;
    }
    Ok(())
}
fn format_rust(source: &str) -> Result<String, Box<dyn Error>> {
    let mut child = Command::new("rustfmt")
        .args(["--edition", "2024", "--emit", "stdout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stdin = child.stdin.take().ok_or("missing rustfmt stdin")?;
    // Drain stdout concurrently: large generated programs can fill both pipes.
    let source = source.to_string();
    let writer = std::thread::spawn(move || stdin.write_all(source.as_bytes()));
    let output = child.wait_with_output()?;
    writer.join().map_err(|_| "rustfmt writer panicked")??;
    if !output.status.success() {
        return Err(format!(
            "rustfmt failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?)
}
