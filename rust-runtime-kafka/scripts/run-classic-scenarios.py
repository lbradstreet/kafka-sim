#!/usr/bin/env python3
"""Run the shared classic/Panama catalogue and retain complete comparison evidence."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def repository(path):
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=path)
    untracked = {}
    for name in git("ls-files", "--others", "--exclude-standard", "-z").split(b"\0"):
        if name:
            file = path / os.fsdecode(name)
            untracked[os.fsdecode(name)] = hashlib.sha256(file.read_bytes()).hexdigest()
    return {"path": str(path), "head": git("rev-parse", "HEAD").decode().strip(),
            "tracked_diff_sha256": hashlib.sha256(git("diff", "HEAD", "--binary")).hexdigest(),
            "status": git("status", "--short").decode(), "untracked_sha256": untracked}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka", type=Path, required=True, help="Kafka checkout containing the classicScenarios task")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--size", choices=["test", "full"], default="test")
    parser.add_argument("--profile", choices=["original", "common", "both"], default="both")
    parser.add_argument("--adapter", choices=["classic", "native", "both"], default="both")
    parser.add_argument("--scenario", default=".*")
    parser.add_argument("--variant", default=".*")
    parser.add_argument("--seed", default="0")
    parser.add_argument("--batch-target-mode", choices=["raw", "estimated-wire"],
                        help="explicit native policy override, recorded in the effective manifest")
    parser.add_argument("--request-batching-policy", choices=["single-partition", "sealed", "broker-ready"],
                        help="explicit request grouping policy, recorded in the effective manifest")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--library", type=Path, help="use an immutable simulation library with --skip-build")
    parser.add_argument("--no-viewer", action="store_true", help="omit the paired HTML export")
    args = parser.parse_args()
    if args.library and not args.skip_build:
        parser.error("--library requires --skip-build")
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, RUSTC_WRAPPER="")
    env.pop("KR_SIM_BATCH_TARGET_MODE", None)
    env.pop("KR_SIM_REQUEST_BATCHING_POLICY", None)
    if args.request_batching_policy:
        env["KR_SIM_REQUEST_BATCHING_POLICY"] = {
            "single-partition": "SinglePartition", "sealed": "Sealed", "broker-ready": "BrokerReady"
        }[args.request_batching_policy]
    if args.batch_target_mode:
        env["KR_SIM_BATCH_TARGET_MODE"] = {"raw": "Raw", "estimated-wire": "EstimatedWire"}[args.batch_target_mode]
    if not args.skip_build:
        subprocess.run(["cargo", "build", "--release", "-p", "kr-kafka-sim-ffi"], cwd=ROOT, env=env, check=True)
    extension = "dylib" if sys.platform == "darwin" else "so"
    library = args.library.resolve() if args.library else ROOT / "target" / "release" / f"libkr_kafka_sim_ffi.{extension}"
    with library.open("rb") as binary:
        library_hash = hashlib.file_digest(binary, "sha256").hexdigest()
    provenance = {"schema": "kr-classic-provenance/v1", "arguments": vars(args) | {
        "kafka": str(args.kafka.resolve()), "out": str(out), "library": str(library)},
        "rust": repository(ROOT), "kafka": repository(args.kafka.resolve()),
        "library": {"path": str(library), "sha256": library_hash,
                    "built_by_this_invocation": not args.skip_build},
        "java": subprocess.run(["java", "-version"], capture_output=True, text=True, check=True).stderr}
    (out / f"provenance-{args.adapter}-{args.profile}-{args.size}-{args.seed}.json").write_text(
        json.dumps(provenance, indent=2) + "\n")
    failures = []
    for profile in (["original", "common"] if args.profile == "both" else [args.profile]):
        for adapter in (["classic", "native"] if args.adapter == "both" else [args.adapter]):
            options = ["--profile", profile, "--adapter", adapter, "--size", args.size,
                       "--scenario", args.scenario, "--variant", args.variant,
                       "--seed", args.seed, "--out", str(out)]
            log = out / f"gradle-{adapter}-{profile}-{args.size}.log"
            command = ["./gradlew", ":clients-lab:classicScenarios", "--offline", "--console=plain",
                       f"-Dkr.sim.library={library}", "-PscenarioArgsJson=" + json.dumps(options)]
            print(f"Running {adapter}/{profile}/{args.size}; {log}", flush=True)
            with log.open("w") as output:
                result = subprocess.run(command, cwd=args.kafka.resolve(), env=env, stdout=output, stderr=subprocess.STDOUT)
            if result.returncode:
                failures.append(str(log))
            summary = out / f"summary-{adapter}-{profile}-{args.size}.json"
            if summary.exists():
                data = json.loads(summary.read_text())
                print(f"passed={data['passed']}, failures={len(data['failures'])}", flush=True)
    if failures:
        raise SystemExit("Failed runs: " + ", ".join(failures))
    if args.adapter == "both":
        subprocess.run([sys.executable, str(ROOT / "kafka/kr-kafka-experiments/analysis/classic_comparison.py"),
                        str(out), "--out", str(out / "comparison.json")], check=True)
        if not args.no_viewer:
            subprocess.run([sys.executable, "-B", str(ROOT / "kafka/kr-kafka-experiments/analysis/classic_visualization.py"),
                            str(out), "--out", str(out / "viewer")], check=True)


if __name__ == "__main__":
    main()
