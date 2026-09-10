fn main() {
    // `--cfg kr_runtime_sim` is supplied through RUSTFLAGS by simulation builds,
    // matching the kr-runtime-tokio facade this crate's simulated transport uses.
    println!("cargo::rustc-check-cfg=cfg(kr_runtime_sim)");
}
