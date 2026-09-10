#!/usr/bin/env python3
"""Compare the pinned KafkaProducer and Java FFM producer against fresh real logs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import select
import selectors
import shutil
import subprocess
import sys
import time

from real_broker import Cluster, Interrupted, command, install_interrupt_handlers, prepare, save, stop

HERE = Path(__file__).resolve().parent
PRODUCERS = ("org.apache.kafka.clients.producer.KafkaProducer", "io.krkafka.producer.KrKafkaProducer")


def digest(path):
    if path.is_file():
        return hashlib.sha256(path.read_bytes()).hexdigest()
    content = hashlib.sha256()
    for child in sorted(child for child in path.rglob("*") if child.is_file()):
        content.update(str(child.relative_to(path)).encode())
        content.update(hashlib.sha256(child.read_bytes()).digest())
    return content.hexdigest()


def properties(cluster, mode):
    if mode == "plaintext":
        return "security.protocol=PLAINTEXT\n"
    text = "security.protocol=" + ("SSL" if mode == "tls" else "SASL_SSL") + "\n"
    text += f"ssl.truststore.type=PEM\nssl.truststore.location={cluster.directory}/ca.pem\n"
    text += "ssl.endpoint.identification.algorithm=https\n"
    if mode != "tls":
        mechanism = {"plain": "PLAIN", "scram256": "SCRAM-SHA-256", "scram512": "SCRAM-SHA-512"}[mode]
        login = "plain.PlainLoginModule" if mode == "plain" else "scram.ScramLoginModule"
        text += f'sasl.mechanism={mechanism}\nsasl.jaas.config=org.apache.kafka.common.security.{login} required username="bench" password="fixture-password";\n'
    return text


def lifecycle_command(cluster, argv, folder, topic, suite, classpath):
    result = dict(argv=argv, phases=[], timeout_seconds=180)
    deadline = time.monotonic() + 180
    with (folder / "producer.stdout").open("wb") as stdout, (folder / "producer.stderr").open("wb") as stderr:
        process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr, start_new_session=True)
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        buffered, total = bytearray(), 0
        try:
            while selector.get_map():
                if time.monotonic() >= deadline:
                    result["timed_out"] = True
                    raise TimeoutError("restart producer watchdog expired")
                for key, _ in selector.select(.2):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    total += len(chunk)
                    if total > 8 * 1024 * 1024:
                        raise ValueError("producer stdout bound")
                    stdout.write(chunk)
                    stdout.flush()
                    buffered.extend(chunk)
                    while b"\n" in buffered:
                        line, _, buffered = buffered.partition(b"\n")
                        event = json.loads(line)
                        if event.get("kind") != "phase":
                            raise ValueError("unexpected producer output")
                        phases = ["before_recreate"] if suite == "recreate" else ["before_fault", "after_fault"]
                        if len(result["phases"]) >= len(phases) or event.get("phase") != phases[len(result["phases"])]:
                            raise ValueError("unexpected lifecycle phase")
                        expected = event["phase"]
                        result["phases"].append(event)
                        if expected == "before_recreate":
                            checkpoint = folder / "old-producer.json"
                            shutil.copyfile(folder / "producer.json", checkpoint)
                            command(["java", "-cp", classpath, "VerifyRecords", "--bootstrap", cluster.bootstrap(), "--ledger", str(checkpoint),
                                "--output", str(folder / "old-verification.json"), "--mode", "checkpoint"], folder / "old-verify", timeout=60)
                            result["old_log"] = json.loads((folder / "old-verification.json").read_text())
                            cluster.topic(topic, delete=True)
                            absent_by = time.monotonic() + 30
                            attempts = 0
                            while True:
                                attempts += 1
                                absent = cluster.cli("kafka-topics.sh", ["--bootstrap-server", cluster.bootstrap(), "--describe", "--topic", topic],
                                    folder / f"deletion-{attempts}", check=False)
                                if absent["exit_code"] != 0:
                                    break
                                if time.monotonic() >= absent_by:
                                    raise TimeoutError("topic deletion not observed")
                                time.sleep(.1)
                            cluster.cli("kafka-topics.sh", ["--bootstrap-server", cluster.bootstrap(), "--create", "--topic", topic,
                                "--partitions", "5", "--replication-factor", "1", "--config", "min.insync.replicas=1"])
                            until, attempts = time.monotonic() + 30, 0
                            while True:
                                attempts += 1
                                description = cluster.describe(topic, folder / f"recreated-topology-{attempts}")
                                rows = re.findall(r"Partition:\s*(\d+)\s+Leader:\s*(-?\d+)\s+Replicas:\s*([0-9,]+)\s+Isr:\s*([0-9,]*)", description)
                                if len(rows) == 5 and all(int(leader) >= 0 and isr for _, leader, _, isr in rows):
                                    result["recreated_topology"] = [dict(partition=int(p), leader=int(leader), isr=[int(n) for n in isr.split(',')]) for p, leader, _, isr in rows]
                                    break
                                if time.monotonic() >= until:
                                    raise TimeoutError("replacement topology not ready")
                                time.sleep(.1)
                        elif expected == "before_fault":
                            cluster.kill(0)
                            result["killed_pid"] = cluster.processes[0].pid
                        else:
                            if cluster.processes[0].poll() is None or event["accepted"] <= event["callbacks"]:
                                raise ValueError("outage pending cohort proof missing")
                            cluster.restore()
                            result["restored_topology"] = cluster.wait_replicated(topic, folder, stage="restored")
                        process.stdin.write(b"continue\n")
                        process.stdin.flush()
                    if len(buffered) > 65536:
                        raise ValueError("phase line bound")
            result["exit_code"] = process.wait(timeout=max(1, deadline - time.monotonic()))
            if len(result["phases"]) != (1 if suite == "recreate" else 2):
                raise ValueError("lifecycle phase proof incomplete")
        finally:
            selector.close()
            stop(process)
            result["exit_code"] = process.returncode
            save(folder / "producer.command.json", result)
            cluster.restore()
    return result


def run_case(cluster, folder, producer_class, mode, compression, args):
    folder.mkdir()
    topic = "kr-java-" + folder.name
    cluster.topic(topic)
    if args.suite == "limits":
        cluster.cli("kafka-configs.sh", ["--bootstrap-server", cluster.bootstrap(), "--alter", "--entity-type", "topics",
            "--entity-name", topic, "--add-config", "max.message.bytes=1024"], folder / "topic-limit")
    if args.suite == "authorization":
        for effect, operation in (("allow", "Describe"), ("deny", "Write")):
            cluster.cli("kafka-acls.sh", ["--bootstrap-server", cluster.bootstrap(), "--add", f"--{effect}-principal", "User:bench",
                "--operation", operation, "--topic", topic], folder / f"topic-{effect}")
    cluster.wait_replicated(topic, folder)
    settings = folder / "producer.properties"
    settings.write_text(properties(cluster, mode))
    ledger = folder / "producer.json"
    callers = 1 if args.suite in ("limits", "authorization") else args.callers
    result = dict(producer_class=producer_class, security=mode, compression=compression, topic=topic, callers=callers, passed=False)
    classpath = os.pathsep.join([str(cluster.directory / "java"), str(args.binding_classes), str(cluster.kafka / "libs/*")])
    launch = ["java", "--enable-native-access=ALL-UNNAMED", f"-Dkr.kafka.library={args.ffi_library}", "-cp", classpath, "VerifyRecords"]
    argv = launch + ["--producer-class", producer_class,
        "--bootstrap", cluster.bootstrap(mode), "--admin-bootstrap", cluster.bootstrap(), "--topic", topic,
        "--run-id", "java-" + mode + "-" + compression, "--records", str(args.records), "--callers", str(callers),
        "--compression", compression, "--transport", args.transport, "--properties", str(settings),
        "--output", str(ledger), "--timeout-ms", "30000", "--timestamp-ms", str(args.timestamp_ms), "--interceptor", str(args.suite == "positive").lower(),
        "--scenario", {"recovery": "restart", "limits": "broker_size", "authorization": "authorization", "recreate": "recreate"}.get(args.suite, "positive")]
    result["producer"] = (lifecycle_command(cluster, argv, folder, topic, args.suite, classpath) if args.suite in ("recovery", "recreate")
        else command(argv, folder / "producer", timeout=180, check=False))
    verification = folder / "verification.json"
    result["verification"] = command(["java", "-cp", classpath, "VerifyRecords", "--bootstrap", cluster.bootstrap(),
        "--ledger", str(ledger), "--output", str(verification), "--timeout-ms", "30000", "--generation", "1" if args.suite == "recreate" else "0"], folder / "verify", timeout=60, check=False)
    if verification.is_file():
        result["log"] = json.loads(verification.read_text())
    if ledger.is_file():
        produced = json.loads(ledger.read_text())
        result["callbacks"] = produced.get("callbacks")
        result["futures"] = produced.get("futures")
        result["error"] = produced.get("error")
    result["passed"] = (result["producer"].get("exit_code") == 0 and not result["producer"].get("timed_out")
        and result["verification"].get("exit_code") == 0 and result.get("log", {}).get("verified") is True)
    save(folder / "outcome.json", result)
    return result


def run(args):
    args.directory, args.kafka_home = args.directory.resolve(), args.kafka_home.resolve()
    args.binding_classes, args.ffi_library = args.binding_classes.resolve(), args.ffi_library.resolve()
    if not args.binding_classes.exists() or not args.ffi_library.is_file():
        raise ValueError("existing compiled binding classes/jar and production native library are required")
    if not 12 <= args.records <= 2000 or not 1 <= args.callers <= 16:
        raise ValueError("records12..2000 and callers1..16 required")
    if args.java_home:
        os.environ["JAVA_HOME"] = str(args.java_home.resolve())
        os.environ["PATH"] = str(args.java_home.resolve() / "bin") + os.pathsep + os.environ["PATH"]
    prepare(args.directory, args.kafka_home, args.base_port, 1)
    if args.suite == "authorization":
        config = args.directory / "node-1/server.properties"
        with config.open("a") as output:
            output.write("\nauthorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer\n"
                "super.users=User:ANONYMOUS\nallow.everyone.if.no.acl.found=true\n")
    sources = dict(binding=str(args.binding_classes), native=str(args.ffi_library))
    artifacts = args.directory / "artifacts"
    artifacts.mkdir()
    native = artifacts / args.ffi_library.name
    shutil.copyfile(args.ffi_library, native)
    binding = artifacts / ("binding.jar" if args.binding_classes.is_file() else "classes")
    if args.binding_classes.is_file():
        shutil.copyfile(args.binding_classes, binding)
    else:
        shutil.copytree(args.binding_classes, binding)
        inventory = binding / "META-INF/kr-kafka-symbols.txt"
        if not inventory.is_file():
            source = HERE.parent / "kr-kafka-java/src/main/resources/META-INF/kr-kafka-symbols.txt"
            inventory.parent.mkdir(exist_ok=True)
            shutil.copyfile(source, inventory)
            sources["symbol_inventory"] = str(source)
    args.binding_classes, args.ffi_library = binding, native
    args.timestamp_ms = int(time.time() * 1000)
    cluster = Cluster(args.directory)
    report = dict(schema="kr-kafka-java-comparison/v1", passed=False, cases=[], pairs=[], records=args.records,
        callers=1 if args.suite in ("limits", "authorization") else args.callers, transport=args.transport, suite=args.suite,
        native_sha256=digest(args.ffi_library), binding_sha256=digest(args.binding_classes),
        verifier_sha256=digest(HERE / "VerifyRecords.java"), harness_sha256=digest(Path(__file__)), artifact_sources=sources,
        timestamp_ms=args.timestamp_ms, started_unix=time.time())
    try:
        command(["java", "-version"], args.directory / "java-version")
        command([sys.executable, str(HERE.parents[1] / "scripts/kafka-java-native-symbols.py"), str(args.ffi_library)], args.directory / "production-symbols")
        command(["javac", "-cp", str(cluster.kafka / "libs/*"), "-d", str(args.directory / "java"), str(HERE / "VerifyRecords.java")], args.directory / "javac")
        command(["java", "-cp", os.pathsep.join([str(args.directory / "java"), str(cluster.kafka / "libs/*")]), "VerifyRecords", "--self-test"], args.directory / "verifier-self-test")
        cluster.provision()
        modes = (["plaintext", "tls", "plain", "scram256", "scram512"] if args.suite == "positive"
            else ["plain"] if args.suite == "authorization" else ["plaintext"])
        codecs = ["none"] if args.suite in ("smoke", "limits", "authorization") else ["none", "zstd"]
        for mode in modes:
            for compression in codecs:
                outcomes = []
                for label, producer_class in zip(("kafka", "native"), PRODUCERS):
                    folder = args.directory / f"{mode}-{compression}-{label}"
                    try:
                        outcome = run_case(cluster, folder, producer_class, mode, compression, args)
                    except Exception as error:
                        outcome = dict(producer_class=producer_class, security=mode, compression=compression, passed=False, error=repr(error))
                    outcomes.append(outcome)
                    report["cases"].append(outcome)
                    save(args.directory / "report.json", report)
                pair = dict(security=mode, compression=compression, passed=all(case["passed"] for case in outcomes))
                if pair["passed"]:
                    ledgers = [json.loads((args.directory / f"{mode}-{compression}-{label}" / "producer.json").read_text()) for label in ("kafka", "native")]
                    expected = [sorted(ledger["accepted"], key=lambda record: record["record_id"]) for ledger in ledgers]
                    pair["same_record_content_and_key_routing"] = expected[0] == expected[1]
                    pair["passed"] = pair["same_record_content_and_key_routing"]
                report["pairs"].append(pair)
                save(args.directory / "report.json", report)
        report["passed"] = all(pair["passed"] for pair in report["pairs"])
        if args.hold_ms and report["passed"]:
            save(args.directory / "report.json", report)
            print(json.dumps(dict(kind="phase", phase="comparison-complete", fixture=str(args.directory),
                bootstrap=cluster.bootstrap(), tls_bootstrap=cluster.bootstrap("tls"), sasl_bootstrap=cluster.bootstrap("plain"),
                ca_pem=str(args.directory / "ca.pem"), native_library=str(args.ffi_library), binding_classes=str(args.binding_classes), hold_ms=args.hold_ms)), flush=True)
            ready, _, _ = select.select([sys.stdin], [], [], args.hold_ms / 1000)
            if not ready or sys.stdin.readline(65).strip() != "continue":
                raise TimeoutError("owned broker hold expired without a bounded continue command")
    except Interrupted as error:
        report["interruption"] = error.details()
        raise
    except Exception as error:
        report["passed"] = False
        report["error"] = repr(error)
    finally:
        try:
            cluster.close()
        finally:
            report["finished_unix"] = time.time()
            save(args.directory / "report.json", report)
    return report["passed"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="new, single-use owned fixture directory")
    parser.add_argument("--kafka-home", type=Path, required=True)
    parser.add_argument("--binding-classes", type=Path, required=True)
    parser.add_argument("--ffi-library", type=Path, required=True)
    parser.add_argument("--java-home", type=Path)
    parser.add_argument("--suite", choices=("smoke", "positive", "recovery", "limits", "authorization", "recreate"), default="smoke")
    parser.add_argument("--transport", choices=("readiness", "uring"), default="readiness")
    parser.add_argument("--records", type=int, default=192)
    parser.add_argument("--callers", type=int, default=4)
    parser.add_argument("--base-port", type=int, default=29092)
    parser.add_argument("--hold-ms", type=int, default=0, help="after passing, retain owned brokers until stdin continue (at most600000ms)")
    args = parser.parse_args()
    if not 0 <= args.hold_ms <= 600000:
        parser.error("hold-ms must be0..600000")
    install_interrupt_handlers()
    try:
        return 0 if run(args) else 1
    except Interrupted as error:
        return error.exit_code


if __name__ == "__main__":
    sys.exit(main())
