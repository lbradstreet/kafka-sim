#!/usr/bin/env python3
"""Run isolated, sequential, open-loop clients and retain comparison provenance.

No package installation, broker mutation, cloud provisioning, or invented results.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time
from common import public_settings, validate_comparison
import allocation_profile

HERE = Path(__file__).resolve().parent


def run_isolated(argv, stdout, stderr, timeout):
    """The watchdog owns the entire process group, including time/java children."""
    process = subprocess.Popen(argv, stdout=stdout, stderr=stderr, start_new_session=True)
    try:
        return {"exit_code": process.wait(timeout=timeout)}
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        return {"timeout_seconds": timeout, "exit_code": process.returncode}


def command(argv):
    try:
        run = subprocess.run(argv, capture_output=True, text=True, timeout=30, check=False)
        return dict(argv=argv, exit_code=run.returncode, stdout=run.stdout, stderr=run.stderr)
    except (OSError, subprocess.TimeoutExpired) as error:
        return dict(argv=argv, unavailable=type(error).__name__)


def properties(profile, producer_class=None, warmup_records=0):
    result = dict(profile)
    result.update({"producer.bootstrap.servers": profile["bootstrap"], "producer.client.id": "java-open-loop",
                   "producer.key.serializer": "org.apache.kafka.common.serialization.ByteArraySerializer",
                   "producer.value.serializer": "org.apache.kafka.common.serialization.ByteArraySerializer",
                   "producer.acks": "all", "producer.enable.idempotence": "true",
                   "producer.max.in.flight.requests.per.connection": 5, "producer.max.block.ms": 0,
                   "producer.compression.type": "none" if profile["compression"] == "none" else "zstd",
                   "producer.linger.ms": profile["linger_us"] // 1000,
                   "producer.batch.size": profile["batch_bytes"], "producer.max.request.size": profile["request_bytes"],
                   "producer.buffer.memory": profile["input_bytes"],
                   "producer.delivery.timeout.ms": profile["delivery_timeout_ms"],
                   "producer.request.timeout.ms": profile["request_timeout_ms"]})
    if profile["compression"] != "none":
        result["producer.compression.zstd.level"] = int(profile["compression"][-1])
    result["warmup_records"] = warmup_records
    if producer_class:
        result["producer_class"] = producer_class
        # This is the explicit limited binding profile for both sides of the
        # Java/FFM comparison; no silent truncation of Kafka's retry default.
        result["producer.retries"] = 254
        if producer_class == "io.krkafka.producer.KrKafkaProducer":
            result["producer.kr.transport"] = profile["backend"]
    if profile["security"] != "plaintext":
        result.update({"producer.security.protocol": "SSL" if profile["security"] == "tls" else "SASL_SSL",
                       "producer.ssl.truststore.type": "PEM", "producer.ssl.truststore.location": profile["ca_pem"],
                       "producer.ssl.endpoint.identification.algorithm": "https"})
        if profile["security"] != "tls":
            result["producer.sasl.mechanism"] = {"plain": "PLAIN", "scram256": "SCRAM-SHA-256", "scram512": "SCRAM-SHA-512"}[profile["security"]]
    def escape(value):
        return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("=", "\\=").replace(":", "\\:").replace(" ", "\\ ")
    return "".join(f"{escape(key)}={escape(value)}\n" for key, value in sorted(result.items()) if value is not None)


def environment():
    commands = [["git", "rev-parse", "HEAD"], ["git", "status", "--porcelain=v1"], ["rustc", "-vV"],
                ["cargo", "--version"], ["uname", "-a"], ["lscpu"], ["java", "-version"], ["python3", "--version"]]
    files = {}
    for path in ["/proc/meminfo", "/proc/sys/kernel/io_uring_disabled", "/sys/kernel/mm/transparent_hugepage/enabled",
                 "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor", "/sys/devices/system/cpu/intel_pstate/no_turbo"]:
        try:
            files[path] = Path(path).read_text()
        except OSError:
            files[path] = None
    return dict(timestamp_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), platform=platform.platform(),
                affinity=sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
                commands=[command(cmd) for cmd in commands], files=files,
                missing_external_provenance=["broker version/config/topology", "NIC and network path", "host isolation/load", "NUMA/affinity policy for every process", "perf and allocation profiles"])


def watchdog_seconds(profile, warmup_records=0):
    workload_seconds = (profile["records"] + profile["rate"] - 1) // profile["rate"]
    # Java/FFM warmup shares one delivery timeout across its entire serial phase.
    # The process watchdog includes that phase without multiplying by its count.
    delivery_phases = 3 + int(warmup_records > 0)
    return workload_seconds * 4 + delivery_phases * profile["delivery_timeout_ms"] / 1000 + 120


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("profile", type=Path)
    parser.add_argument("output", type=Path, help="new result directory; existing results are never overwritten")
    parser.add_argument("--rust-binary", default="target/release/kr-kafka-bench")
    parser.add_argument("--ffi-library", default="target/release/libkr_kafka_ffi.so")
    parser.add_argument("--kafka-classpath", help="local pinned Kafka libs/*, required for the Java client")
    parser.add_argument("--java-binding-jar", type=Path, help="built kr-kafka-java jar, required for ffm")
    parser.add_argument("--warmup-records", type=int, default=0, help="in-process Java/FFM warmup outside measured interval")
    parser.add_argument("--clients", default="rust,java,librdkafka")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--allocations", action="store_true", help="separate extra heaptrack run per client; never used for baseline performance")
    parser.add_argument("--native-diagnostics", action="store_true", help="separate extra Rust completion/queue diagnostic run")
    parser.add_argument("--environment", type=Path, help="operator-supplied broker/host provenance JSON")
    args = parser.parse_args()
    profile = json.loads(args.profile.read_text())
    validate_comparison(profile)
    if profile.get("native_diagnostics", False):
        raise SystemExit("baseline profile must disable native_diagnostics; use --native-diagnostics for a separate pass")
    if platform.system() != "Linux":
        raise SystemExit("host I/O comparisons require Linux; primitive tests work on other hosts")
    clients = args.clients.split(",")
    if not clients or any(c not in ("rust", "java", "librdkafka", "ffi", "ffm") for c in clients) or len(set(clients)) != len(clients):
        raise SystemExit("invalid or duplicated clients")
    if any(c in clients for c in ("java", "ffm")) and not args.kafka_classpath:
        raise SystemExit("Java client requires --kafka-classpath")
    if "ffm" in clients and (args.java_binding_jar is None or not args.java_binding_jar.is_file()):
        raise SystemExit("FFM client requires --java-binding-jar")
    if not 0 <= args.warmup_records <= 100_000 or (args.warmup_records and any(c not in ("java", "ffm") for c in clients)):
        raise SystemExit("warmup-records requires 0..100000 and only java/ffm clients")
    if not 1 <= args.repetitions <= 100:
        raise SystemExit("repetitions must be 1..=100")
    args.output.mkdir(parents=True, exist_ok=False)
    copied_profile = args.output / "profile.json"
    copied_profile.write_text(json.dumps(profile, indent=2))
    java_profile = args.output / "profile.properties"
    java_profile.write_text(properties(profile, "org.apache.kafka.clients.producer.KafkaProducer" if "ffm" in clients else None,
                                       args.warmup_records), encoding="utf-8")
    ffm_profile = args.output / "ffm.properties"
    if "ffm" in clients:
        ffm_profile.write_text(properties(profile, "io.krkafka.producer.KrKafkaProducer", args.warmup_records), encoding="utf-8")
    manifest = environment()
    manifest.update(schema="kr-kafka-comparison/v1", profile_sha256=hashlib.sha256(args.profile.read_bytes()).hexdigest(),
                    matched_settings=public_settings(profile), clients=clients, repetitions=args.repetitions,
                    order="rotate client order each repetition; no concurrent clients", runs=[],
                    semantic_differences=["Java/librdkafka do not expose NotWritten versus Unknown", "buffer.memory/queue bytes are not equivalent hard whole-process budgets", "kr-kafka requires immutable topic IDs with Produce13", "disable broker auto.create.topics.enable for the Java metadata path"],
                    external_environment=json.loads(args.environment.read_text()) if args.environment else None)
    if any(c in clients for c in ("java", "ffm")):
        compiled = command(["javac", "-cp", args.kafka_classpath, "-d", str(args.output), str(HERE / "JavaOpenLoop.java")])
        manifest["java_compile"] = compiled
        if compiled.get("exit_code") != 0:
            (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2))
            raise SystemExit("Java benchmark compilation failed; see manifest")
    cp = str(args.output.resolve()) + os.pathsep + (args.kafka_classpath or "")
    if "ffm" in clients:
        manifest["java_binding_sha256"] = hashlib.sha256(args.java_binding_jar.read_bytes()).hexdigest()
        manifest["ffi_library_sha256"] = hashlib.sha256(Path(args.ffi_library).read_bytes()).hexdigest()
        manifest["java_ffm_retry_cap"] = 254
    manifest["warmup_records"] = args.warmup_records

    def invocation(client, result):
        return {
            "rust": [args.rust_binary, str(copied_profile), str(result)],
            "java": ["java", "-cp", cp, "JavaOpenLoop", str(java_profile), str(result)],
            "ffm": ["java", "--enable-native-access=ALL-UNNAMED", "-Dkr.kafka.library=" + str(Path(args.ffi_library).resolve()),
                    "-cp", cp + os.pathsep + str(args.java_binding_jar or ""), "JavaOpenLoop", str(ffm_profile), str(result)],
            "librdkafka": [sys.executable, str(HERE / "librdkafka_open_loop.py"), str(copied_profile), str(result)],
            "ffi": [sys.executable, str(HERE / "ffi_open_loop.py"), str(copied_profile), str(result), args.ffi_library],
        }[client]

    timeout = watchdog_seconds(profile, args.warmup_records)
    failed = False
    for repetition in range(args.repetitions):
        order = clients[repetition % len(clients):] + clients[:repetition % len(clients)]
        for client in order:
            stem = f"{repetition:02d}-{client}"
            result = args.output / f"{stem}.json"
            argv = invocation(client, result)
            record = dict(client=client, repetition=repetition, argv=argv, started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
            with (args.output / f"{stem}.stdout").open("w") as stdout, (args.output / f"{stem}.stderr").open("w") as stderr:
                record.update(run_isolated(["/usr/bin/time", "-v", "-o", str(args.output / f"{stem}.resource.txt"), *argv], stdout, stderr, timeout))
            if result.exists():
                report = json.loads(result.read_text())
                record["complete"] = report.get("complete", False)
                record["result_sha256"] = hashlib.sha256(result.read_bytes()).hexdigest()
            failed |= record.get("exit_code") != 0 or not record.get("complete", False)
            manifest["runs"].append(record)
            (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2))
            time.sleep(2)
    if args.native_diagnostics:
        diagnostic_profile = args.output / "diagnostic-profile.json"
        diagnostic_profile.write_text(json.dumps(dict(profile, native_diagnostics=True), indent=2))
        result = args.output / "native-diagnostics.json"
        argv = [args.rust_binary, str(diagnostic_profile), str(result)]
        record = dict(argv=argv, performance_comparable=False)
        with (args.output / "native-diagnostics.stdout").open("w") as stdout, (args.output / "native-diagnostics.stderr").open("w") as stderr:
            record.update(run_isolated(argv, stdout, stderr, timeout))
        record["complete"] = result.exists() and json.loads(result.read_text()).get("complete", False)
        failed |= record.get("exit_code") != 0 or not record["complete"]
        manifest["native_diagnostics"] = record
        (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2))
    if args.allocations:
        manifest["allocation_profiles"] = []
        for client in clients:
            directory = args.output / f"heaptrack-{client}"
            result = directory / "workload.json"
            argv = invocation(client, result)
            record = allocation_profile.run(argv, directory, timeout * 4, run_isolated, command)
            record["client"] = client
            record["workload_complete"] = result.exists() and json.loads(result.read_text()).get("complete", False)
            failed |= not record["complete"] or not record["workload_complete"]
            manifest["allocation_profiles"].append(record)
            (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2))
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
