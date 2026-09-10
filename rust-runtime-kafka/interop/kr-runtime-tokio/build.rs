fn main() {
    // `--cfg kr_runtime_sim` is supplied through RUSTFLAGS by simulation builds, the
    // same way madsim supplies `--cfg madsim`. Declare it so `unexpected_cfgs`
    // stays meaningful for every other cfg in this crate.
    println!("cargo::rustc-check-cfg=cfg(kr_runtime_sim)");
}
