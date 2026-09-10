#!/usr/bin/env python3
"""Owned local Kafka correctness/security/recovery gate; never a benchmark.

No downloads. New fixture directories only. Broker and producer subprocess groups
have explicit owners; each command/case has a watchdog and retained diagnostics.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "benchmarks"))
from integration import server_properties  # fixture-only configuration, no execution

MAX_FRAME = 4 * 1024 * 1024
PUBLIC_PASSWORD = "fixture-password"
_interruption = None


class Interrupted(BaseException):
    """Exit the whole gate after owned cleanup, never treat a signal as a case failure."""
    def __init__(self, number):
        self.number = number
        self.exit_code = 128 + number
        super().__init__(signal.Signals(number).name)

    def details(self):
        return dict(signal=signal.Signals(self.number).name, signal_number=self.number, exit_code=self.exit_code)


def install_interrupt_handlers():
    def interrupt(number, _frame):
        global _interruption
        # Once unwinding, further TERM/INT signals must not interrupt cleanup.
        # An external watchdog can still enforce its final SIGKILL deadline.
        if _interruption is None:
            _interruption = Interrupted(number)
            raise _interruption
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def stop(process):
    interruption = None
    while True:
        try:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGCONT)
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=10)
            break
        except Interrupted as error:
            # A first signal can arrive during normal cleanup itself. Finish
            # this owned child before forwarding it to the next outer owner.
            interruption = error
    if interruption is not None:
        raise interruption


def command(argv, stem, timeout=90, env=None, check=True):
    result = {"argv": list(map(str, argv)), "timeout_seconds": timeout}
    process = None
    try:
        with stem.with_suffix(".stdout").open("wb") as stdout, stem.with_suffix(".stderr").open("wb") as stderr:
            process = subprocess.Popen(argv, stdout=stdout, stderr=stderr, env=env, start_new_session=True)
            result["pid"] = process.pid
            result["exit_code"] = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        result["timed_out"] = True
    except BaseException as error:
        result["error"] = repr(error)
        if isinstance(error, Interrupted):
            result["interruption"] = error.details()
        raise
    finally:
        try:
            if process is not None:
                stop(process)
                result["exit_code"] = process.returncode
        except Interrupted as error:
            result["interruption"] = error.details()
            if process is not None:
                result["exit_code"] = process.returncode
            raise
        finally:
            save(stem.with_suffix(".command.json"), result)
    if check and (result["exit_code"] != 0 or result.get("timed_out")):
        raise RuntimeError(f"command failed: {stem.name}; inspect its stdout/stderr")
    return result


class Cursor:
    def __init__(self, data):
        self.data, self.at = data, 0

    def take(self, count):
        if count < 0 or self.at + count > len(self.data):
            raise ValueError("truncated frame")
        value = self.data[self.at:self.at + count]
        self.at += count
        return value

    def integer(self, form):
        return struct.unpack(form, self.take(struct.calcsize(form)))[0]

    def varint(self):
        value = 0
        for shift in range(0, 35, 7):
            byte = self.integer(">B")
            if shift == 28 and byte > 15:
                raise ValueError("varint overflow")
            value |= (byte & 127) << shift
            if byte < 128:
                return value
        raise ValueError("varint overflow")

    def array(self):
        length = self.varint() - 1
        if not 0 <= length <= 4096:
            raise ValueError("array bound")
        return range(length)

    def nullable_string(self):
        size = self.varint()
        if size:
            self.take(size - 1).decode("utf-8")

    def tags(self):
        count, previous = self.varint(), -1
        if count > 4096:
            raise ValueError("tag bound")
        for _ in range(count):
            tag = self.varint()
            if tag <= previous:
                raise ValueError("tag order")
            previous = tag
            self.take(self.varint())


def successful_produce13(frame):
    """Independent pinned ProduceResponse13 parser, including full frame checks."""
    c = Cursor(frame)
    if c.integer(">i") != len(frame) - 4:
        raise ValueError("frame length")
    correlation = c.integer(">i")
    c.tags()  # flexible response header
    rows = []
    for _ in c.array():
        topic = c.take(16).hex()
        for _ in c.array():
            partition, error = c.integer(">i"), c.integer(">h")
            offset = c.integer(">q")
            c.integer(">q")
            c.integer(">q")
            for _ in c.array():
                c.integer(">i")
                c.nullable_string()
                c.tags()
            c.nullable_string()
            c.tags()
            rows.append(dict(topic_id=topic, partition=partition, error_code=error, offset=offset))
        c.tags()
    throttle = c.integer(">i")
    c.tags()
    if c.at != len(frame) or throttle < 0 or not rows:
        raise ValueError("incomplete Produce response")
    if any(row["error_code"] != 0 or row["offset"] < 0 or row["partition"] < 0 for row in rows):
        return None
    return dict(correlation=correlation, version=13, partitions=rows, throttle_ms=throttle)


def read_frame(stream):
    def exact(count):
        chunks = bytearray()
        while len(chunks) < count:
            chunk = stream.recv(count - len(chunks))
            if not chunk:
                if chunks:
                    raise ValueError("partial proxy frame")
                return None
            chunks.extend(chunk)
        return bytes(chunks)
    prefix = exact(4)
    if prefix is None:
        return None
    size = struct.unpack(">i", prefix)[0]
    if not 0 <= size <= MAX_FRAME:
        raise ValueError("proxy frame bound")
    body = exact(size)
    if body is None:
        raise ValueError("missing frame body")
    return prefix + body


class ResponseLossProxy:
    """Bounded fixture-only framing proxy, armed once for a real success reply."""
    def __init__(self, listen, upstream):
        self.upstream = upstream
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", listen))
        self.listener.listen(16)
        self.listener.settimeout(0.2)
        self.lock = threading.Lock()
        self.sockets, self.threads = set(), set()
        self.stopped, self.arm_path, self.evidence, self.errors = False, None, None, []
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def arm(self, path):
        with self.lock:
            if self.arm_path is not None:
                raise RuntimeError("proxy already armed")
            self.arm_path, self.evidence = path, None

    def accept(self):
        while not self.stopped:
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self.lock:
                if len(self.threads) >= 16:
                    client.close()
                    self.errors.append("proxy connection capacity exhausted")
                    continue
                self.sockets.add(client)
                worker = threading.Thread(target=self.connection, args=(client,), daemon=True)
                self.threads.add(worker)
            worker.start()

    def connection(self, client):
        upstream = None
        try:
            upstream = socket.create_connection(("127.0.0.1", self.upstream), timeout=5)
            upstream.settimeout(120)
            client.settimeout(120)
            with self.lock:
                self.sockets.add(upstream)
            requests, request_lock = {}, threading.Lock()

            def forward_requests():
                try:
                    while (frame := read_frame(client)) is not None:
                        if len(frame) < 12:
                            raise ValueError("short Kafka request")
                        api, version, correlation = struct.unpack(">hhi", frame[4:12])
                        with request_lock:
                            if len(requests) >= 32 or correlation in requests:
                                raise ValueError("request correlation bound")
                            requests[correlation] = (api, version)
                        upstream.sendall(frame)
                except (OSError, ValueError):
                    pass
                finally:
                    try:
                        upstream.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
            forward = threading.Thread(target=forward_requests, daemon=True)
            forward.start()
            try:
                while (frame := read_frame(upstream)) is not None:
                    if len(frame) < 8:
                        raise ValueError("short Kafka response")
                    correlation = struct.unpack(">i", frame[4:8])[0]
                    with request_lock:
                        request = requests.pop(correlation, None)
                    if request is None:
                        raise ValueError("unexpected proxy correlation")
                    with self.lock:
                        if self.arm_path is not None and self.evidence is None and request == (0, 13):
                            proof = successful_produce13(frame)
                            if proof is not None:
                                proof.update(schema="kr-kafka-withheld-success/v1", api_key=0,
                                             frame_sha256=hashlib.sha256(frame).hexdigest(), bytes=len(frame))
                                self.arm_path.with_suffix(".bin").write_bytes(frame)
                                save(self.arm_path.with_suffix(".json"), proof)
                                self.evidence = proof
                                break  # no byte of this frame is forwarded
                    client.sendall(frame)
            finally:
                try:
                    client.shutdown(socket.SHUT_RDWR)
                    upstream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                forward.join(timeout=5)
        except (OSError, ValueError) as error:
            if not self.stopped:
                with self.lock:
                    if len(self.errors) < 32:
                        self.errors.append(str(error))
        finally:
            for stream in (client, upstream):
                if stream is not None:
                    stream.close()
                    with self.lock:
                        self.sockets.discard(stream)
            with self.lock:
                self.threads.discard(threading.current_thread())

    def close(self):
        self.stopped = True
        self.listener.close()
        with self.lock:
            for stream in self.sockets:
                try:
                    stream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        self.thread.join(timeout=5)
        with self.lock:
            threads = list(self.threads)
        for thread in threads:
            thread.join(timeout=5)


def prepare(directory, kafka_home, base, nodes):
    directory, kafka_home = directory.resolve(), kafka_home.resolve()
    if nodes not in (1, 3) or not 1024 <= base <= 65490:
        raise ValueError("nodes must be1 or3; base port1024..65490")
    if any(char in str(directory) for char in "\r\n=\\"):
        raise ValueError("unsupported fixture path")
    for name in ("kafka-storage.sh", "kafka-server-start.sh", "kafka-topics.sh", "kafka-broker-api-versions.sh"):
        if not (kafka_home / "bin" / name).is_file():
            raise ValueError(f"missing Kafka distribution script: {name}")
    directory.mkdir(parents=True, mode=0o700, exist_ok=False)
    voters = ",".join(f"{node + 1}@localhost:{base + node * 10 + 1}" for node in range(nodes))
    for node in range(nodes):
        folder = directory / f"node-{node + 1}"
        folder.mkdir()
        config = server_properties(directory, base + node * 10)
        config = config.replace("node.id=1", f"node.id={node + 1}")
        config = re.sub(r"controller.quorum.bootstrap.servers=.*", "controller.quorum.voters=" + voters, config)
        config = config.replace(f"log.dirs={directory}/data", f"log.dirs={folder}/data")
        if nodes == 1:
            config = config.replace(f"advertised.listeners=PLAINTEXT://localhost:{base},", f"advertised.listeners=PLAINTEXT://localhost:{base + 4},")
        config = re.sub(r"(?m)^listeners=(.*)$", rf"listeners=\1,EXPIRED_SSL://127.0.0.1:{base + node * 10 + 5}", config)
        config = re.sub(r"(?m)^advertised.listeners=(.*)$", rf"advertised.listeners=\1,EXPIRED_SSL://localhost:{base + node * 10 + 5}", config)
        config = re.sub(r"(?m)^listener.security.protocol.map=(.*)$", r"listener.security.protocol.map=\1,EXPIRED_SSL:SSL", config)
        config += f"listener.name.expired_ssl.ssl.keystore.location={directory}/expired-server.p12\n"
        config += f"default.replication.factor={nodes}\n"
        config += "log.message.timestamp.type=CreateTime\nlog.retention.hours=24\n"
        if nodes == 3:
            config = config.replace("min.insync.replicas=1", "min.insync.replicas=2")
        (folder / "server.properties").write_text(config)
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
    save(directory / "fixture.json", dict(schema="kr-kafka-real-broker/v1", directory=str(directory),
         kafka_home=str(kafka_home), base_port=base, nodes=nodes, executed=False,
         expected_distribution="4.3.0", source_schema_pin="7be741d08b3b06f6414ac868e57bf9b958f53a72"))


class Cluster:
    def __init__(self, directory):
        self.directory = directory
        self.fixture = json.loads((directory / "fixture.json").read_text())
        self.kafka = Path(self.fixture["kafka_home"])
        self.base, self.nodes = self.fixture["base_port"], self.fixture["nodes"]
        self.processes, self.logs, self.starts = {}, [], [0] * self.nodes
        self.proxy = None
        self.sequence = 0
        self.case_deadline = None
        self.env = dict(os.environ, KAFKA_HEAP_OPTS="-Xms256m -Xmx512m")

    def bootstrap(self, mode="plaintext"):
        delta = 0 if mode == "plaintext" else 2 if mode == "tls" else 3
        return ",".join(f"localhost:{self.base + node * 10 + delta}" for node in range(self.nodes))

    def cli(self, name, args, stem=None, check=True):
        self.sequence += 1
        return command([str(self.kafka / "bin" / name), *args], stem or self.directory / f"command-{self.sequence:04}", timeout=self.remaining(90), env=self.env, check=check)

    def remaining(self, maximum):
        remaining = maximum if self.case_deadline is None else min(maximum, self.case_deadline - time.monotonic())
        if remaining <= 0:
            raise TimeoutError("case deadline expired")
        return remaining

    def provision(self):
        if self.fixture["executed"] or self.fixture["directory"] != str(self.directory):
            raise ValueError("fixture already executed or moved")
        self.fixture["executed"] = True
        save(self.directory / "fixture.json", self.fixture)
        self.cli("kafka-topics.sh", ["--version"], self.directory / "kafka-version")
        self.certificates()
        self.cli("kafka-storage.sh", ["random-uuid"], self.directory / "cluster-id")
        cluster = (self.directory / "cluster-id.stdout").read_text().strip()
        for node in range(self.nodes):
            self.cli("kafka-storage.sh", ["format", "--cluster-id", cluster, "--config", str(self.directory / f"node-{node + 1}/server.properties"),
                     "--add-scram", "SCRAM-SHA-256=[name=bench,password=fixture-password]",
                     "--add-scram", "SCRAM-SHA-512=[name=bench,password=fixture-password]"])
        if self.nodes == 1:
            self.proxy = ResponseLossProxy(self.base + 4, self.base)
        self.restore()
        self.cli("kafka-broker-api-versions.sh", ["--bootstrap-server", self.bootstrap()], self.directory / "api-versions")
        advertised = (self.directory / "api-versions.stdout").read_text()
        for name, key, wanted in [("Produce", 0, 13), ("Metadata", 3, 12), ("InitProducerId", 22, 4), ("ApiVersions", 18, 3), ("SaslHandshake", 17, 1), ("SaslAuthenticate", 36, 2)]:
            ranges = re.findall(rf"{name}\({key}\):\s*(\d+)\s+to\s+(\d+)", advertised)
            if len(ranges) != self.nodes or any(not int(low) <= wanted <= int(high) for low, high in ranges):
                raise RuntimeError(f"broker lacks verified advertised {name}{wanted}: inspect api-versions.stdout")

    def certificates(self):
        d = self.directory
        (d / "ca-issued").mkdir()
        (d / "ca-index.txt").write_text("")
        (d / "ca-serial.txt").write_text("1000\n")
        (d / "ca-sign.cnf").write_text(f"""[ca]
default_ca=fixture
[fixture]
database={d}/ca-index.txt
serial={d}/ca-serial.txt
new_certs_dir={d}/ca-issued
certificate={d}/ca.pem
private_key={d}/ca-key.pem
default_md=sha256
policy=subject
copy_extensions=copy
[subject]
commonName=supplied
""")
        commands = [
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(d / "ca-key.pem"), "-out", str(d / "ca.pem"), "-days", "2", "-subj", "/CN=kr-isolated-fixture-ca", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign"],
            ["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", str(d / "server-key.pem"), "-out", str(d / "server.csr"), "-config", str(d / "certificate.cnf")],
            ["openssl", "x509", "-req", "-in", str(d / "server.csr"), "-CA", str(d / "ca.pem"), "-CAkey", str(d / "ca-key.pem"), "-CAcreateserial", "-out", str(d / "server.pem"), "-days", "2", "-extfile", str(d / "certificate.cnf"), "-extensions", "ext"],
            ["openssl", "x509", "-in", str(d / "ca.pem"), "-outform", "DER", "-out", str(d / "ca.der")],
            ["openssl", "pkcs12", "-export", "-name", "localhost", "-inkey", str(d / "server-key.pem"), "-in", str(d / "server.pem"), "-certfile", str(d / "ca.pem"), "-out", str(d / "server.p12"), "-passout", "pass:fixture-password"],
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(d / "wrong-ca-key.pem"), "-out", str(d / "wrong-ca.pem"), "-days", "2", "-subj", "/CN=wrong-fixture-ca", "-addext", "basicConstraints=critical,CA:TRUE"],
            ["openssl", "x509", "-in", str(d / "wrong-ca.pem"), "-outform", "DER", "-out", str(d / "wrong-ca.der")],
            ["openssl", "ca", "-batch", "-notext", "-config", str(d / "ca-sign.cnf"), "-startdate", "20000101000000Z", "-enddate", "20000102000000Z", "-in", str(d / "server.csr"), "-out", str(d / "expired-server.pem")],
            ["openssl", "pkcs12", "-export", "-name", "localhost", "-inkey", str(d / "server-key.pem"), "-in", str(d / "expired-server.pem"), "-certfile", str(d / "ca.pem"), "-out", str(d / "expired-server.p12"), "-passout", "pass:fixture-password"],
        ]
        for index, argv in enumerate(commands):
            command(argv, d / f"certificate-{index}")

    def start(self, node):
        previous = self.processes.get(node)
        if previous is not None and previous.poll() is None:
            return
        self.starts[node] += 1
        log = (self.directory / f"node-{node + 1}/broker-{self.starts[node]}.log").open("wb")
        self.logs.append(log)
        self.processes[node] = subprocess.Popen([str(self.kafka / "bin/kafka-server-start.sh"), str(self.directory / f"node-{node + 1}/server.properties")], stdout=log, stderr=log, env=self.env, start_new_session=True)

    def restore(self):
        for node in range(self.nodes):
            self.start(node)
        deadline = time.monotonic() + self.remaining(90)
        for node in range(self.nodes):
            while True:
                if self.processes[node].poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("broker failed startup: inspect node logs")
                try:
                    with socket.create_connection(("127.0.0.1", self.base + node * 10), timeout=.2):
                        break
                except OSError:
                    time.sleep(.1)

    def kill(self, node):
        process = self.processes[node]
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)

    def topic(self, topic, delete=False):
        self.cli("kafka-topics.sh", ["--bootstrap-server", self.bootstrap(), "--delete" if delete else "--create", "--topic", topic] + ([] if delete else ["--partitions", "4", "--replication-factor", str(self.nodes), "--config", "min.insync.replicas=" + str(2 if self.nodes == 3 else 1)]))

    def describe(self, topic, stem):
        self.cli("kafka-topics.sh", ["--bootstrap-server", self.bootstrap(), "--describe", "--topic", topic], stem)
        return stem.with_suffix(".stdout").read_text()

    def wait_replicated(self, topic, folder, forbidden=None, required_isr=None, stage="initial"):
        required_isr = self.nodes if required_isr is None else required_isr
        deadline, iteration = time.monotonic() + self.remaining(60), 0
        while True:
            description = self.describe(topic, folder / f"{stage}-topology-{iteration}")
            rows = re.findall(r"Partition:\s*(\d+)\s+Leader:\s*(-?\d+)\s+Replicas:\s*([0-9,]+)\s+Isr:\s*([0-9,]*)", description)
            if len(rows) == 4 and all(int(leader) >= 0 and int(leader) != forbidden and len(isr.split(',')) >= required_isr for _, leader, _, isr in rows):
                return [dict(partition=int(p), leader=int(leader), isr=[int(n) for n in isr.split(',')]) for p, leader, _, isr in rows]
            if time.monotonic() >= deadline:
                raise TimeoutError("required live leader/ISR state not observed")
            iteration += 1
            time.sleep(.1)

    def close(self):
        interruption = None
        for process in self.processes.values():
            try:
                stop(process)
            except Interrupted as error:
                interruption = error
        try:
            if self.proxy:
                self.proxy.close()
        except Interrupted as error:
            interruption = error
            self.proxy.close()
        finally:
            for log in self.logs:
                log.close()
        if interruption is not None:
            raise interruption


def cases(suite, backends, nodes):
    output = []
    if suite == "bindings":
        return [dict(name=f"{backend}-ffi", backend=backend, mode="plaintext", compression="zstd1", scenario="basic", ffi=True) for backend in backends]
    if suite in ("baseline", "positive", "all"):
        for backend in backends:
            for mode in (["plaintext"] if suite == "baseline" else ["plaintext", "tls", "plain", "scram256", "scram512"]):
                for compression in (["none"] if suite == "baseline" else ["none", "zstd1"]):
                    output.append(dict(name=f"{backend}-{mode}-{compression}", backend=backend, mode=mode, compression=compression, scenario="basic"))
    if suite in ("extended", "all"):
        for backend in backends:
            for mode in ("plaintext", "tls"):
                output.append(dict(name=f"{backend}-{mode}-vectored", backend=backend, mode=mode, compression="zstd1", scenario="basic", write_mode="vectored"))
            for mode in ("tls", "scram512"):
                output.append(dict(name=f"{backend}-{mode}-restart", backend=backend, mode=mode, compression="zstd1", scenario="restart"))
            for scenario in ("lanes4", "pending", "stopped_polling", "restart", "recreate", "unavailable", "close_deadline"):
                output.append(dict(name=f"{backend}-{scenario}", backend=backend, mode="plaintext", compression="zstd1", scenario="basic" if scenario == "lanes4" else scenario, lanes=4 if scenario == "lanes4" else 1, input_mode="leased" if scenario in ("lanes4", "stopped_polling") else "copy"))
            if nodes == 3:
                output.append(dict(name=f"{backend}-leader-loss", backend=backend, mode="plaintext", compression="zstd1", scenario="leader_loss"))
            else:
                output.append(dict(name=f"{backend}-lost-success", backend=backend, mode="plaintext", compression="none", scenario="basic", response_loss=True))
            for negative, mode in [("wrong-ca", "tls"), ("wrong-hostname", "tls"), ("expired-certificate", "tls"), ("wrong-plain", "plain"), ("wrong-scram256", "scram256"), ("wrong-scram512", "scram512")]:
                output.append(dict(name=f"{backend}-{negative}", backend=backend, mode=mode, compression="none", scenario="basic", negative=negative))
    return output


def verifier(cluster, folder, ledger, generation=0, stem="verification", mode="final"):
    output = folder / f"{stem}.json"
    command(["java", "-Xmx256m", "-cp", str(cluster.directory / "java") + os.pathsep + str(cluster.kafka / "libs/*"), "VerifyRecords", "--bootstrap", cluster.bootstrap(), "--ledger", str(ledger), "--generation", str(generation), "--mode", mode, "--output", str(output), "--timeout-ms", "30000"], folder / stem, timeout=cluster.remaining(45))
    result = json.loads(output.read_text())
    if not result["verified"]:
        raise RuntimeError("independent consumed-log verification failed")
    return result


def backend_failure(requested, actual):
    if not isinstance(actual, str) or actual.lower() != requested:
        return f"reported backend {actual!r} differs from requested {requested!r}"
    return None


def leader_cohort(checkpoint, records):
    """Require real terminal acknowledgements for the complete down-node cohort."""
    third = records // 3
    accepted = {row["record_id"] for row in checkpoint["accepted"]}
    deliveries = {row["record_id"]: row for row in checkpoint["deliveries"]}
    if checkpoint.get("checkpoint") != "fault_settled" or accepted != set(range(2 * third)):
        raise ValueError("leader-dead checkpoint does not contain the complete warm/fault cohorts")
    if len(deliveries) != len(checkpoint["deliveries"]) or any(
        record not in deliveries or deliveries[record]["kind"] != "Acked" or deliveries[record]["offset"] is None
        for record in accepted
    ):
        raise ValueError("leader-dead cohort was not fully acknowledged")
    if not any(row["expected_partition"] == 0 and third <= row["record_id"] < 2 * third for row in checkpoint["accepted"]):
        raise ValueError("leader-dead fault cohort does not revisit partition0")
    return list(range(third, 2 * third))


def run_case(cluster, case, binary, records, ffi_binary=None):
    folder = cluster.directory / case["name"]
    folder.mkdir()
    outcome = dict(case=case, passed=False, phases=[])
    process = None
    cluster.case_deadline = time.monotonic() + 300
    try:
        topic = "kr-check-" + case["name"]
        cluster.topic(topic)
        cluster.wait_replicated(topic, folder)
        profile = json.loads((HERE.parent / "benchmarks/profile.json").read_text())
        profile.update(topic=topic, bootstrap=cluster.bootstrap(case["mode"]), partitions=4, records=records, rate=1000, record_bytes=256,
                       backend=case["backend"], security=case["mode"], compression=case["compression"], input_bytes=4 * 1024 * 1024,
                       delivery_timeout_ms=5000 if case["scenario"] in ("close_deadline", "unavailable") or case.get("negative") else 30000,
                       request_timeout_ms=3000)
        if case.get("ffi"):
            profile["records"] = 192  # three fixed64-record copy/native/foreign cohorts
        if case["mode"] != "plaintext":
            profile.update(ca_der=str(cluster.directory / "ca.der"), ca_pem=str(cluster.directory / "ca.pem"))
        if case["mode"] not in ("plaintext", "tls"):
            profile.update(username_env="KR_FIXTURE_USER", password_env="KR_FIXTURE_PASSWORD")
        config = dict(schema="kr-kafka-producer-check/v1", run_id=case["name"], profile=profile, lanes=case.get("lanes", 1),
                      input_mode=case.get("input_mode", "copy"), scenario=case["scenario"], barriers=True, phase_timeout_ms=90000, poll_pause_ms=250)
        if case.get("write_mode"):
            config["write_mode"] = case["write_mode"]
        if case.get("negative") == "wrong-ca":
            profile["ca_der"] = str(cluster.directory / "wrong-ca.der")
        elif case.get("negative") == "wrong-hostname":
            config["tls_server_name"] = "mismatched.invalid"
        elif case.get("negative") == "expired-certificate":
            profile["bootstrap"] = ",".join(f"localhost:{cluster.base + node * 10 + 5}" for node in range(cluster.nodes))
        elif case.get("negative", "").startswith("wrong-"):
            profile["password_env"] = "KR_FIXTURE_BAD_PASSWORD"
        config_path, ledger = folder / "config.json", folder / "producer.json"
        save(config_path, config)
        if case.get("response_loss"):
            cluster.proxy.arm(folder / "withheld-success")
        env = dict(os.environ, KR_FIXTURE_USER="bench", KR_FIXTURE_PASSWORD=PUBLIC_PASSWORD, KR_FIXTURE_BAD_PASSWORD="incorrect-fixture-password")
        argv = [str(ffi_binary), case["backend"], "localhost", str(cluster.base), topic, case["name"], str(ledger)] if case.get("ffi") else [str(binary), str(config_path), str(ledger)]
        outcome["argv"] = argv
        with (folder / "producer.stdout").open("wb") as stdout, (folder / "producer.stderr").open("wb") as stderr:
            process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr, env=env, start_new_session=True)
            selector = selectors.DefaultSelector()
            selector.register(process.stdout, selectors.EVENT_READ)
            buffered, count = bytearray(), 0
            deadline = cluster.case_deadline
            try:
                while selector.get_map():
                    if time.monotonic() >= deadline:
                        outcome["timed_out"] = True
                        raise TimeoutError("producer case watchdog expired")
                    for key, _ in selector.select(.2):
                        chunk = os.read(key.fileobj.fileno(), 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        count += len(chunk)
                        if count > 8 * 1024 * 1024:
                            raise ValueError("producer stdout bound")
                        stdout.write(chunk)
                        stdout.flush()
                        buffered.extend(chunk)
                        while b"\n" in buffered:
                            line, _, tail = buffered.partition(b"\n")
                            buffered = bytearray(tail)
                            event = json.loads(line)
                            if event.get("kind") != "phase":
                                continue
                            phase = event["phase"]
                            outcome["phases"].append(event)
                            save(folder / "outcome.json", outcome)
                            if phase == "before_recreate":
                                verifier(cluster, folder, ledger, stem="old-generation", mode="checkpoint")
                                cluster.topic(topic, delete=True)
                                # Kafka deletion is asynchronous. Observe absence
                                # before creating again; no blind fixed sleep.
                                until = time.monotonic() + 30
                                while True:
                                    cluster.sequence += 1
                                    stem = folder / f"deletion-{cluster.sequence}"
                                    result = cluster.cli("kafka-topics.sh", ["--bootstrap-server", cluster.bootstrap(), "--describe", "--topic", topic], stem, check=False)
                                    if result["exit_code"] != 0:
                                        break
                                    if time.monotonic() >= until:
                                        raise TimeoutError("topic deletion not observed")
                                    time.sleep(.1)
                                cluster.topic(topic)
                            elif phase == "before_fault":
                                if case["scenario"] == "leader_loss":
                                    description = cluster.describe(topic, folder / "leader-before")
                                    match = re.search(r"Partition:\s*0\s+Leader:\s*(\d+)", description)
                                    if not match:
                                        raise ValueError("partition0 leader not found")
                                    outcome["killed_node"] = int(match.group(1))
                                    cluster.kill(outcome["killed_node"] - 1)
                                else:
                                    for node in range(cluster.nodes):
                                        cluster.kill(node)
                            elif phase == "after_fault" and case["scenario"] not in ("unavailable", "close_deadline"):
                                if case["scenario"] == "leader_loss":
                                    outcome["topology_while_leader_dead"] = cluster.wait_replicated(topic, folder, forbidden=outcome["killed_node"], required_isr=2, stage="leader-dead")
                                    # Continue the application while the failed
                                    # node remains dead. It must flush the middle
                                    # cohort before its fault_settled checkpoint.
                                else:
                                    cluster.restore()
                                    outcome["topology_after_restore"] = cluster.wait_replicated(topic, folder, stage="restored")
                            elif phase == "fault_settled":
                                if case["scenario"] != "leader_loss" or cluster.processes[outcome["killed_node"] - 1].poll() is None:
                                    raise ValueError("unexpected fault_settled phase or failed leader already alive")
                                checkpoint = json.loads(ledger.read_text())
                                cohort = leader_cohort(checkpoint, records)
                                checkpoint_path = folder / "leader-dead-producer.json"
                                save(checkpoint_path, checkpoint)
                                proof = verifier(cluster, folder, checkpoint_path, stem="leader-dead-verification", mode="checkpoint")
                                if proof["acked"] != 2 * (records // 3) or cluster.processes[outcome["killed_node"] - 1].poll() is None:
                                    raise ValueError("leader-dead log proof incomplete or failed leader alive")
                                outcome["acked_while_leader_dead"] = dict(record_ids=cohort, count=len(cohort), node=outcome["killed_node"],
                                    checkpoint_sha256=hashlib.sha256(checkpoint_path.read_bytes()).hexdigest())
                                save(folder / "outcome.json", outcome)
                                cluster.restore()
                                outcome["topology_after_restore"] = cluster.wait_replicated(topic, folder, stage="restored")
                            if phase in ("before_fault", "after_fault", "fault_settled", "before_recreate", "recreated"):
                                process.stdin.write(b"continue\n")
                                process.stdin.flush()
                        if len(buffered) > 65536:
                            raise ValueError("phase line bound")
                outcome["exit_code"] = process.wait(timeout=max(1, deadline - time.monotonic()))
            finally:
                selector.close()
        cluster.restore()
        if not ledger.is_file() or ledger.stat().st_size > 32 * 1024 * 1024:
            raise ValueError("producer did not retain a bounded ledger")
        report = json.loads(ledger.read_text())
        outcome["ledger_sha256"] = hashlib.sha256(ledger.read_bytes()).hexdigest()
        outcome["producer_complete"] = report.get("complete", False)
        delivery_failure = None
        if case.get("negative"):
            if outcome["exit_code"] == 0 or any(p["phase"] == "ready" for p in outcome["phases"]):
                delivery_failure = "negative security setup unexpectedly succeeded"
            authentication = 16 in report.get("fatal", []) or any(row["code"] == 16 for row in report.get("topic_failures", []))
            if not authentication or not report.get("error") or report["accepted"]:
                delivery_failure = "negative case lacks explicit Authentication16 rejection before admission"
            if not (report.get("closed") and report.get("joined") and report.get("closed_unresolved") == 0) or any(
                row["held"] != 0 for row in report.get("final_credits", [])
            ):
                delivery_failure = "negative security case did not close, join and release all observable credits"
            outcome["authentication_rejection"] = authentication
        elif outcome["exit_code"] != 0:
            delivery_failure = "producer returned failure; see ledger"
        elif case["scenario"] == "leader_loss" and not outcome.get("acked_while_leader_dead"):
            delivery_failure = "producer lacked a verified acknowledged cohort while leader remained dead"
        outcome["reported_backend"] = report.get("backend")
        mismatch = backend_failure(case["backend"], report.get("backend"))
        if mismatch:
            delivery_failure = f"{delivery_failure}; {mismatch}" if delivery_failure else mismatch
        if delivery_failure:
            outcome["delivery_failure"] = delivery_failure
        verification = verifier(cluster, folder, ledger, generation=1 if case["scenario"] == "recreate" else 0,
                                mode="failed" if delivery_failure else "negative" if case.get("negative") else "unavailable" if case["scenario"] == "unavailable" else "final")
        outcome["committed"] = verification["committed"]
        outcome["log_verified"] = True
        if case.get("negative") and verification["committed"] != 0:
            raise ValueError("security-negative record committed")
        if case.get("response_loss"):
            if cluster.proxy.evidence is None or not any(d["attempts"] >= 2 for d in report["deliveries"]):
                raise ValueError("response-loss case did not prove successful-response suppression and retry")
            outcome["withheld_success"] = cluster.proxy.evidence
            cluster.proxy.arm_path = None
        outcome["passed"] = delivery_failure is None
    except Interrupted as error:
        outcome["interruption"] = error.details()
        outcome["passed"] = False
        raise
    except Exception as error:
        outcome["error"] = repr(error)
    finally:
        try:
            if process is not None:
                stop(process)
                outcome.setdefault("exit_code", process.returncode)
            cluster.case_deadline = None
            # On an interrupted outage, restarting brokers would both defeat
            # shutdown and consume the outer watchdog's cleanup grace period.
            if _interruption is None and "interruption" not in outcome:
                try:
                    cluster.restore()
                except Exception as error:
                    outcome["restore_error"] = repr(error)
                    outcome["passed"] = False
        except Interrupted as error:
            outcome["interruption"] = error.details()
            outcome["passed"] = False
            raise
        finally:
            cluster.case_deadline = None
            if process is not None:
                outcome.setdefault("exit_code", process.returncode)
            if case.get("response_loss"):
                with cluster.proxy.lock:
                    cluster.proxy.arm_path = None
            save(folder / "outcome.json", outcome)
    return outcome


def run(directory, binary, suite, backends, records, ffi_library=None, ffi_binary=None):
    directory, binary = directory.resolve(), binary.resolve()
    if not 6 <= records <= 2000 or (suite != "bindings" and not binary.is_file()):
        raise ValueError("records6..2000 and existing producer binary required")
    if suite == "bindings" and (ffi_library is None or not ffi_library.is_file()):
        raise ValueError("bindings suite requires --ffi-library pointing to the production shared library")
    cluster = Cluster(directory)
    report = dict(schema="kr-kafka-real-broker-report/v1", passed=False, cases=[], suite=suite,
                  producer_sha256=hashlib.sha256(binary.read_bytes()).hexdigest() if binary.is_file() else None, nodes=cluster.nodes,
                  verifier_sha256=hashlib.sha256((HERE / "VerifyRecords.java").read_bytes()).hexdigest())
    try:
        if suite == "bindings":
            ffi_library = ffi_library.resolve()
            if ffi_binary is None:
                ffi_binary = directory / "ffi_check"
                command(["cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-pthread", "-I", str(HERE.parent / "kr-kafka-ffi/include"),
                         str(HERE / "ffi_check.c"), str(ffi_library), "-Xlinker", "-rpath", "-Xlinker", str(ffi_library.parent), "-o", str(ffi_binary)], directory / "ffi-compile")
            ffi_binary = ffi_binary.resolve()
            if not ffi_binary.is_file():
                raise ValueError("missing ffi_check executable")
            report["ffi"] = dict(library=str(ffi_library), library_sha256=hashlib.sha256(ffi_library.read_bytes()).hexdigest(),
                                 driver=str(ffi_binary), driver_sha256=hashlib.sha256(ffi_binary.read_bytes()).hexdigest(),
                                 source_sha256=hashlib.sha256((HERE / "ffi_check.c").read_bytes()).hexdigest(), records_per_case=192)
        cluster.provision()
        command(["javac", "-cp", str(cluster.kafka / "libs/*"), "-d", str(directory / "java"), str(HERE / "VerifyRecords.java")], directory / "javac")
        for case in cases(suite, backends, cluster.nodes):
            try:
                outcome = run_case(cluster, case, binary, records, ffi_binary)
            except Interrupted as error:
                # Include the saved history, or an explicit interrupted entry
                # if the signal arrived before the case created its outcome.
                # Never let a missing early artifact replace the interruption.
                path = directory / case["name"] / "outcome.json"
                outcome = json.loads(path.read_text()) if path.is_file() else dict(case=case, phases=[])
                outcome.update(passed=False, interruption=error.details())
                report["cases"].append(outcome)
                raise
            report["cases"].append(outcome)
            save(directory / "report.json", report)
        report["passed"] = all(case["passed"] for case in report["cases"])
    except Interrupted as error:
        report["interruption"] = error.details()
        report["passed"] = False
        raise
    except Exception as error:
        report["error"] = repr(error)
    finally:
        try:
            cluster.close()
        except Interrupted as error:
            report["interruption"] = error.details()
            report["passed"] = False
            raise
        finally:
            if cluster.proxy:
                report["proxy_errors"] = cluster.proxy.errors
            save(directory / "report.json", report)
    return report["passed"]


class Primitives(unittest.TestCase):
    def test_interrupt_retires_separate_session_child_and_retains_command_result(self):
        # The helper owns a real subprocess in another session. Sending TERM or
        # INT to the helper alone must unwind command(), reap the child, retain
        # the reason and exit with the conventional signal status.
        helper = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import real_broker as gate
folder = Path(sys.argv[2])
child = "import os,signal,sys; from pathlib import Path; Path(sys.argv[1]).write_text(str(os.getpid())); signal.pause()"
gate.main = lambda: gate.command([sys.executable, "-c", child, str(folder / "child.pid")], folder / "owned", timeout=20)
raise SystemExit(gate.entrypoint())
"""
        for number in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signal=number), tempfile.TemporaryDirectory(prefix="kr-interrupt-check-") as temporary:
                folder, child_pid = Path(temporary), None
                owner = subprocess.Popen([sys.executable, "-c", helper, str(HERE), temporary],
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
                try:
                    until = time.monotonic() + 5
                    while not (folder / "child.pid").is_file():
                        if owner.poll() is not None or time.monotonic() >= until:
                            self.fail("owned child did not become ready")
                        time.sleep(.01)
                    child_pid = int((folder / "child.pid").read_text())
                    os.kill(owner.pid, number)
                    stdout, stderr = owner.communicate(timeout=5)
                    self.assertEqual(owner.returncode, 128 + number, stderr.decode())
                    self.assertEqual(stdout, b"")
                    result = json.loads((folder / "owned.command.json").read_text())
                    self.assertEqual(result["pid"], child_pid)
                    self.assertEqual(result["exit_code"], -signal.SIGTERM)
                    self.assertEqual(result["interruption"], Interrupted(number).details())
                    self.assertEqual(json.loads(stderr)["interruption"], Interrupted(number).details())
                    with self.assertRaises(ProcessLookupError):
                        os.kill(child_pid, 0)
                finally:
                    stop(owner)
                    owner.stdout.close()
                    owner.stderr.close()
                    if child_pid is not None:
                        try:
                            os.killpg(child_pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass

    def test_interrupted_case_skips_restore_and_next_case_but_saves_report(self):
        instances = []

        class FakeCluster:
            def __init__(self, directory):
                self.directory, self.nodes, self.kafka = directory, 1, directory
                self.proxy, self.case_deadline = None, None
                self.restores, self.topics, self.closed = 0, 0, False
                instances.append(self)

            def provision(self):
                pass

            def topic(self, _topic):
                self.topics += 1
                raise Interrupted(signal.SIGTERM)

            def restore(self):
                self.restores += 1

            def close(self):
                self.closed = True

        with tempfile.TemporaryDirectory(prefix="kr-interrupted-case-") as temporary:
            directory = Path(temporary)
            with patch.dict(run.__globals__, Cluster=FakeCluster, command=lambda *_args, **_kwargs: None):
                with self.assertRaises(Interrupted):
                    run(directory, Path(sys.executable), "baseline", ["readiness", "uring"], 192)
            report = json.loads((directory / "report.json").read_text())
            self.assertFalse(report["passed"])
            self.assertEqual(report["interruption"], Interrupted(signal.SIGTERM).details())
            self.assertEqual(len(report["cases"]), 1)
            self.assertFalse(report["cases"][0]["passed"])
            self.assertEqual(report["cases"][0]["interruption"], report["interruption"])
            self.assertEqual((instances[0].topics, instances[0].restores, instances[0].closed), (1, 0, True))

    def test_matrix_is_explicit_and_unique(self):
        positive = cases("positive", ["readiness", "uring"], 1)
        self.assertEqual(len(positive), 20)
        self.assertEqual(len({case["name"] for case in positive}), 20)
        self.assertTrue(any(c["scenario"] == "leader_loss" for c in cases("extended", ["uring"], 3)))
        self.assertTrue(any(c.get("response_loss") for c in cases("extended", ["uring"], 1)))
        extended = cases("extended", ["readiness", "uring"], 1)
        self.assertEqual(len(extended), 36)
        self.assertEqual(len({c["name"] for c in extended}), 36)
        for backend in ("readiness", "uring"):
            for mode in ("tls", "scram512"):
                self.assertTrue(any(c["backend"] == backend and c["mode"] == mode and c["scenario"] == "restart" for c in extended))
        bindings = cases("bindings", ["readiness", "uring"], 1)
        self.assertEqual([c["name"] for c in bindings], ["readiness-ffi", "uring-ffi"])
        self.assertTrue(all(c["ffi"] and c["mode"] == "plaintext" for c in bindings))

    def test_dead_leader_requires_terminal_fault_cohort_not_just_warm_acknowledgements(self):
        checkpoint = dict(checkpoint="fault_settled", accepted=[dict(record_id=i, expected_partition=i % 4) for i in range(8)],
                          deliveries=[dict(record_id=i, kind="Acked", offset=i) for i in range(8)])
        self.assertEqual(leader_cohort(checkpoint, 12), [4, 5, 6, 7])
        for bad in [dict(checkpoint, checkpoint="after_fault"), dict(checkpoint, deliveries=checkpoint["deliveries"][:4]),
                    dict(checkpoint, accepted=checkpoint["accepted"][:4]),
                    dict(checkpoint, deliveries=checkpoint["deliveries"][:7] + [dict(record_id=7, kind="Unknown", offset=None)])]:
            with self.assertRaises(ValueError):
                leader_cohort(bad, 12)

    def test_backend_labels_and_killed_partition_require_actual_evidence(self):
        for actual in ("Uring", "uring"):
            self.assertIsNone(backend_failure("uring", actual))
        for actual in ("Readiness", "readiness"):
            self.assertIsNone(backend_failure("readiness", actual))
        for requested, actual in [("uring", "Readiness"), ("readiness", "Uring"), ("uring", None), ("uring", "unknown")]:
            self.assertIsNotNone(backend_failure(requested, actual))
        warm_and_other_partitions = dict(checkpoint="fault_settled",
            accepted=[dict(record_id=i, expected_partition=i % 4) for i in range(4)],
            deliveries=[dict(record_id=i, kind="Acked", offset=i) for i in range(4)])
        with self.assertRaisesRegex(ValueError, "partition0"):
            leader_cohort(warm_and_other_partitions, 6)

    def test_success_response_parser_checks_full_frame_before_arming(self):
        # Independent literal flexible response: topicID1, partition0, offset42.
        body = struct.pack(">i", 19) + b"\0\2" + bytes(15) + b"\1\2" + struct.pack(">ihqqq", 0, 0, 42, -1, 0) + b"\1\0\0\0" + struct.pack(">i", 0) + b"\0"
        frame = struct.pack(">i", len(body)) + body
        self.assertEqual(successful_produce13(frame)["partitions"][0]["offset"], 42)
        for length in range(len(frame)):
            with self.assertRaises(ValueError):
                successful_produce13(frame[:length])
        with self.assertRaises(ValueError):
            successful_produce13(frame + b"\0")
        error = bytearray(frame)
        error[31:33] = struct.pack(">h", 6)
        self.assertIsNone(successful_produce13(bytes(error)))

    def test_proxy_withholds_one_complete_success_and_allows_retry(self):
        body = struct.pack(">i", 19) + b"\0\2" + bytes(15) + b"\1\2" + struct.pack(">ihqqq", 0, 0, 42, -1, 0) + b"\1\0\0\0" + struct.pack(">i", 0) + b"\0"
        reply = struct.pack(">i", len(body)) + body
        request = struct.pack(">ihhi", 8, 0, 13, 19)
        with socket.socket() as upstream, tempfile.TemporaryDirectory(prefix="kr-proxy-check-") as temporary:
            upstream.bind(("127.0.0.1", 0))
            upstream.listen(2)
            upstream.settimeout(5)
            requests, failures = [], []

            def broker():
                try:
                    for _ in range(2):
                        with upstream.accept()[0] as stream:
                            stream.settimeout(5)
                            requests.append(read_frame(stream))
                            stream.sendall(reply)
                except Exception as error:
                    failures.append(error)
            server = threading.Thread(target=broker)
            server.start()
            proxy = ResponseLossProxy(0, upstream.getsockname()[1])
            try:
                stem = Path(temporary) / "withheld"
                proxy.arm(stem)
                for attempt in range(2):
                    with socket.create_connection(proxy.listener.getsockname(), timeout=5) as client:
                        client.sendall(request)
                        self.assertEqual(read_frame(client), None if attempt == 0 else reply)
                server.join(timeout=5)
                self.assertFalse(server.is_alive())
                self.assertEqual(failures, [])
                self.assertEqual(requests, [request, request])
                self.assertEqual(stem.with_suffix(".bin").read_bytes(), reply)
                self.assertEqual(proxy.evidence["partitions"][0]["offset"], 42)
            finally:
                proxy.close()


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare")
    prep.add_argument("directory", type=Path)
    prep.add_argument("--kafka-home", type=Path, required=True)
    prep.add_argument("--base-port", type=int, default=19092)
    prep.add_argument("--nodes", type=int, choices=[1, 3], default=1)
    execute = commands.add_parser("run")
    execute.add_argument("directory", type=Path)
    execute.add_argument("--producer-binary", type=Path, default=Path("target/release/producer_check"))
    execute.add_argument("--suite", choices=["baseline", "positive", "extended", "all", "bindings"], default="baseline")
    execute.add_argument("--ffi-library", type=Path, help="production shared library for bindings suite; no test-support hooks")
    execute.add_argument("--ffi-binary", type=Path, help="optional precompiled ffi_check, otherwise cc links it against --ffi-library")
    execute.add_argument("--backends", default="readiness,uring")
    execute.add_argument("--records", type=int, default=192)
    commands.add_parser("self-test")
    args = parser.parse_args()
    if args.command == "self-test":
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Primitives)
        return unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
    if args.command == "prepare":
        prepare(args.directory, args.kafka_home, args.base_port, args.nodes)
        return True
    backends = args.backends.split(",")
    if len(set(backends)) != len(backends) or not backends or any(b not in ("readiness", "uring") for b in backends):
        raise ValueError("invalid backend list")
    return run(args.directory, args.producer_binary, args.suite, backends, args.records, args.ffi_library, args.ffi_binary)


def entrypoint():
    install_interrupt_handlers()
    try:
        return 0 if main() else 1
    except Interrupted as error:
        print(json.dumps(dict(interruption=error.details())), file=sys.stderr, flush=True)
        return error.exit_code


if __name__ == "__main__":
    raise SystemExit(entrypoint())
