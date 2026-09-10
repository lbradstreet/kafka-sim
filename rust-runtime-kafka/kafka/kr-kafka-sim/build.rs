use sha2::{Digest, Sha256};
use std::{
    env, fs,
    path::{Path, PathBuf},
};
fn collect(path: &Path, files: &mut Vec<PathBuf>) {
    if path.is_dir() {
        let mut children: Vec<_> = fs::read_dir(path)
            .expect("source directory")
            .map(|entry| entry.expect("source entry").path())
            .collect();
        children.sort();
        for child in children {
            collect(&child, files);
        }
    } else if path.extension().is_some_and(|ext| ext == "rs")
        || path.file_name().is_some_and(|name| name == "Cargo.toml")
    {
        files.push(path.to_owned());
    }
}
fn main() {
    let base = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let root = base
        .parent()
        .and_then(Path::parent)
        .expect("workspace directory");
    let mut files = Vec::new();
    for relative in [
        "src",
        "io/kr-runtime-io/src",
        "trace/kr-runtime-trace-wire/src",
        "tools/trace-tool/src",
        "bytes/kr-shared-bytes/src",
        "kafka/kr-kafka-client/src",
        "kafka/kr-kafka-producer/src",
        "kafka/kr-kafka-protocol/src",
        "kafka/kr-kafka-record/src",
        "kafka/kr-kafka-broker-model/src",
        "kafka/kr-kafka-sim/src",
    ] {
        let source = root.join(relative);
        // Watch directory membership as well as individual contents so adding or
        // removing a source file also invalidates the replay fingerprint.
        println!("cargo:rerun-if-changed={}", source.display());
        collect(&source, &mut files);
        files.push(source.parent().expect("crate root").join("Cargo.toml"));
    }
    files.extend([
        root.join("Cargo.toml"),
        root.join("Cargo.lock"),
        root.join("rust-toolchain.toml"),
        base.join("build.rs"),
        base.join("Cargo.toml"),
    ]);
    files.sort();
    files.dedup();
    let mut digest = Sha256::new();
    for path in files {
        println!("cargo:rerun-if-changed={}", path.display());
        let relative = path
            .strip_prefix(root)
            .expect("workspace source")
            .to_string_lossy();
        let content = fs::read(&path).expect("workspace source read");
        digest.update((relative.len() as u64).to_be_bytes());
        digest.update(relative.as_bytes());
        digest.update((content.len() as u64).to_be_bytes());
        digest.update(content);
    }
    let hash: String = digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    println!("cargo:rustc-env=PRODUCER_SOURCE_SHA256={hash}");
}
