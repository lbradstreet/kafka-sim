#!/usr/bin/env python3
"""Isolated local KRaft/TLS/SASL gate, using an explicitly supplied Kafka build.

`prepare` writes reviewable files only. `run` starts that broker, formats only the
new isolated log directory, creates one test topic, and executes native smoke runs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
from compare import run_isolated

HERE = Path(__file__).resolve().parent
PINNED_KAFKA_SOURCE = "7be741d08b3b06f6414ac868e57bf9b958f53a72"


def server_properties(directory, base):
    return f"""# Isolated test broker; never a production security configuration.
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:{base + 1}
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://127.0.0.1:{base},CONTROLLER://127.0.0.1:{base + 1},SSL://127.0.0.1:{base + 2},SASL_SSL://127.0.0.1:{base + 3}
advertised.listeners=PLAINTEXT://localhost:{base},SSL://localhost:{base + 2},SASL_SSL://localhost:{base + 3}
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,SSL:SSL,SASL_SSL:SASL_SSL
inter.broker.listener.name=PLAINTEXT
log.dirs={directory}/data
auto.create.topics.enable=false
num.partitions=16
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
min.insync.replicas=1
ssl.keystore.type=PKCS12
ssl.keystore.location={directory}/server.p12
ssl.keystore.password=fixture-password
ssl.key.password=fixture-password
ssl.truststore.type=PEM
ssl.truststore.location={directory}/ca.pem
ssl.client.auth=none
sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512
listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required user_bench=\"fixture-password\";
listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
"""


def prepare(directory, kafka_home, base):
    directory = directory.resolve()
    kafka_home = kafka_home.resolve()
    if not 1024 <= base <= 65532:
        raise ValueError("base-port must be 1024..=65532")
    # Properties paths are intentionally restricted instead of interpolating
    # ambiguous escapes or line breaks into a broker configuration.
    if any(ch in str(directory) for ch in "\n\r=\\"):
        raise ValueError("fixture path contains unsupported properties characters")
    for script in ["kafka-storage.sh", "kafka-server-start.sh", "kafka-topics.sh", "kafka-get-offsets.sh"]:
        if not (kafka_home / "bin" / script).is_file():
            raise ValueError(f"Kafka distribution is missing {script}")
    directory.mkdir(parents=True, exist_ok=False, mode=0o700)
    (directory / "server.properties").write_text(server_properties(directory, base))
    (directory / "certificate.cnf").write_text("""[req]
distinguished_name=dn
prompt=no
req_extensions=ext
[dn]
CN=localhost
[ext]
subjectAltName=DNS:localhost,IP:127.0.0.1
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
basicConstraints=critical,CA:FALSE
""")
    profile = json.loads((HERE / "profile.json").read_text())
    profile.update(records=1000, rate=1000, record_bytes=1024, delivery_timeout_ms=30000, request_timeout_ms=10000)
    for mode in ["plaintext", "tls", "plain", "scram256", "scram512"]:
        selected = dict(profile, security=mode, bootstrap=f"localhost:{base if mode == 'plaintext' else base + 2 if mode == 'tls' else base + 3}")
        if mode != "plaintext":
            selected.update(ca_der=str(directory / "ca.der"), ca_pem=str(directory / "ca.pem"))
        if mode not in ("plaintext", "tls"):
            selected.update(username_env="KR_FIXTURE_USER", password_env="KR_FIXTURE_PASSWORD")
        (directory / f"{mode}.json").write_text(json.dumps(selected, indent=2))
    metadata = dict(schema="kr-kafka-native-gate/v1", kafka_home=str(kafka_home), directory=str(directory),
                    source_protocol_pin=PINNED_KAFKA_SOURCE, base_port=base, topic=profile["topic"], partitions=profile["partitions"],
                    trust="self-signed isolated fixture CA", intended_kafka_version="4.3.0", executed=False)
    (directory / "fixture.json").write_text(json.dumps(metadata, indent=2))
    print(f"Prepared reviewable fixture at {directory}; no broker, storage format, or certificates created yet.")


def checked(argv, log, timeout=90):
    outcome = run_isolated(argv, log, log, timeout)
    if outcome.get("exit_code") != 0 or "timeout_seconds" in outcome:
        raise RuntimeError(f"fixture command failed: {Path(argv[0]).name}; see setup.log")


def run(directory, binary, backends):
    directory = directory.resolve()
    fixture = json.loads((directory / "fixture.json").read_text())
    if fixture["directory"] != str(directory) or fixture["executed"]:
        raise ValueError("fixture moved or already executed; prepare a new directory")
    if (directory / "data").exists() or (directory / "ca-key.pem").exists():
        raise ValueError("fixture contains prior runtime state")
    fixture["executed"] = True
    (directory / "fixture.json").write_text(json.dumps(fixture, indent=2))
    kafka = Path(fixture["kafka_home"]) / "bin"
    with (directory / "setup.log").open("w") as log:
        openssl = [
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(directory / "ca-key.pem"), "-out", str(directory / "ca.pem"), "-days", "2", "-subj", "/CN=kr-isolated-fixture-ca", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign"],
            ["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", str(directory / "server-key.pem"), "-out", str(directory / "server.csr"), "-config", str(directory / "certificate.cnf")],
            ["openssl", "x509", "-req", "-in", str(directory / "server.csr"), "-CA", str(directory / "ca.pem"), "-CAkey", str(directory / "ca-key.pem"), "-CAcreateserial", "-out", str(directory / "server.pem"), "-days", "2", "-extfile", str(directory / "certificate.cnf"), "-extensions", "ext"],
            ["openssl", "x509", "-in", str(directory / "ca.pem"), "-outform", "DER", "-out", str(directory / "ca.der")],
            ["openssl", "pkcs12", "-export", "-name", "localhost", "-inkey", str(directory / "server-key.pem"), "-in", str(directory / "server.pem"), "-certfile", str(directory / "ca.pem"), "-out", str(directory / "server.p12"), "-passout", "pass:fixture-password"],
        ]
        for argv in openssl:
            checked(argv, log)
        cluster = subprocess.run([str(kafka / "kafka-storage.sh"), "random-uuid"], capture_output=True, text=True, timeout=30, check=True).stdout.strip()
        checked([str(kafka / "kafka-storage.sh"), "format", "--standalone", "--cluster-id", cluster,
                 "--config", str(directory / "server.properties"),
                 "--add-scram", "SCRAM-SHA-256=[name=bench,password=fixture-password]",
                 "--add-scram", "SCRAM-SHA-512=[name=bench,password=fixture-password]"], log)
    os.environ["KR_FIXTURE_USER"] = "bench"
    os.environ["KR_FIXTURE_PASSWORD"] = "fixture-password"
    base = fixture["base_port"]
    outcomes = []
    with (directory / "broker.log").open("w") as log:
        broker = subprocess.Popen([str(kafka / "kafka-server-start.sh"), str(directory / "server.properties")], stdout=log, stderr=log, start_new_session=True)
        try:
            deadline = time.monotonic() + 90
            while True:
                if broker.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("broker exited or did not listen within 90s; inspect broker.log")
                try:
                    with socket.create_connection(("127.0.0.1", base), timeout=0.2):
                        break
                except OSError:
                    time.sleep(0.1)
            checked([str(kafka / "kafka-topics.sh"), "--bootstrap-server", f"localhost:{base}", "--create", "--topic", fixture["topic"], "--partitions", str(fixture["partitions"]), "--replication-factor", "1"], log)
            for backend in backends:
                for mode in ["plaintext", "tls", "plain", "scram256", "scram512"]:
                    for compression in ["none", "zstd1"]:
                        stem = f"{backend}-{mode}-{compression}"
                        profile = json.loads((directory / f"{mode}.json").read_text())
                        profile.update(backend=backend, compression=compression)
                        path = directory / f"{stem}.profile.json"
                        path.write_text(json.dumps(profile, indent=2))
                        result = directory / f"{stem}.result.json"
                        with (directory / f"{stem}.log").open("w") as output:
                            outcome = run_isolated([str(binary.resolve()), str(path), str(result)], output, output, 120)
                        outcome.update(case=stem, complete=False)
                        if result.exists():
                            report = json.loads(result.read_text())
                            outcome["complete"] = report["complete"] and report["acked"] == profile["records"] and report["rejected"] == 0
                            outcome["result_sha256"] = hashlib.sha256(result.read_bytes()).hexdigest()
                        outcomes.append(outcome)
                        (directory / "outcomes.json").write_text(json.dumps(outcomes, indent=2))
            # Independently inspect committed offsets, through Kafka's own tool.
            with (directory / "offsets.txt").open("w") as offsets:
                checked([str(kafka / "kafka-get-offsets.sh"), "--bootstrap-server", f"localhost:{base}", "--topic", fixture["topic"], "--time", "-1"], offsets)
            offsets = [int(line.rsplit(":", 1)[1]) for line in (directory / "offsets.txt").read_text().splitlines()
                       if line.startswith(fixture["topic"] + ":")]
            expected = len(outcomes) * 1000
            verified = len(offsets) == fixture["partitions"] and sum(offsets) == expected
            (directory / "log-verification.json").write_text(json.dumps(dict(
                expected_records=expected, partition_offsets=offsets, committed_records=sum(offsets), verified=verified), indent=2))
            if not verified:
                raise RuntimeError("independent broker offsets differ from offered smoke records")
        finally:
            if broker.poll() is None:
                os.killpg(broker.pid, signal.SIGTERM)
                try:
                    broker.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    os.killpg(broker.pid, signal.SIGKILL)
                    broker.wait()
    if any(o.get("exit_code") != 0 or not o["complete"] for o in outcomes):
        raise RuntimeError("native integration gate failed; inspect outcomes.json")


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare")
    prep.add_argument("directory", type=Path)
    prep.add_argument("--kafka-home", type=Path, required=True)
    prep.add_argument("--base-port", type=int, default=19092)
    execute = commands.add_parser("run")
    execute.add_argument("directory", type=Path)
    execute.add_argument("--binary", type=Path, default=Path("target/release/kr-kafka-bench"))
    execute.add_argument("--backends", default="readiness,uring")
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.directory, args.kafka_home, args.base_port)
    else:
        backends = args.backends.split(",")
        if not backends or any(b not in ["readiness", "uring"] for b in backends):
            raise ValueError("unknown native backend")
        run(args.directory, args.binary, backends)


if __name__ == "__main__":
    main()
