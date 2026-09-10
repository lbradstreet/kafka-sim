#!/usr/bin/env python3
"""Capture/check Kafka wire fixtures using pinned Apache Kafka Java classes.

Needs Python 3 and JDK 17+. No Kafka server, Rust encoder, Maven or Gradle is used.
Artifacts are downloaded to --cache and checked against hard-coded SHA-512 pins.
The default --check mode does not write repository files.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import urllib.request


ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "kafka/kr-kafka-protocol/tests/fixtures/java-wire.json"
ARTIFACTS = {
    "kafka-clients-4.3.0.jar": (
        "https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/4.3.0/kafka-clients-4.3.0.jar",
        "7406d34fad25de6edd70ddfb4977d45bd775f8a520aa4c7d543511886e403cbaaa15c038bc217c2bb0c21c8c28998cbb5cf2b907cbf756c36d921edaffcc5a4f",
    ),
    "slf4j-api-2.0.17.jar": (
        "https://repo.maven.apache.org/maven2/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar",
        "9a3e79db6666a6096a3021bb2e1d918f30f589d8de51d6b600f8ebd92515a510ae2d8f87919cc2dfa8365d64f10194cac8dfa0fb950160eef0e9da06f6caaeb9",
    ),
}


def fetch(cache, name, url, expected):
    path = cache / name
    if not path.exists():
        with urllib.request.urlopen(url, timeout=60) as response:
            contents = response.read()
        if hashlib.sha512(contents).hexdigest() != expected:
            raise SystemExit(f"SHA-512 mismatch downloading {url}")
        path.write_bytes(contents)
    if hashlib.sha512(path.read_bytes()).hexdigest() != expected:
        raise SystemExit(f"SHA-512 mismatch in cached artifact {path}")
    return path


def check_schemas(cache, upstream=False):
    manifest = json.loads((ROOT / "schemas/PROVENANCE.lock").read_text())
    recorded = manifest["files"]
    local = {p.name: p for p in (ROOT / "schemas").glob("*.json")}
    if local.keys() != recorded.keys():
        raise SystemExit("Schema file inventory differs from PROVENANCE.lock")
    for name, expected in recorded.items():
        if hashlib.sha256(local[name].read_bytes()).hexdigest() != expected:
            raise SystemExit(f"Schema SHA-256 mismatch: {name}")
    if upstream:
        source = manifest["upstream"]
        archive = fetch(cache, f"kafka-{source['commit']}.tar.gz",
                        source["source_archive_url"], source["source_archive_sha512"])
        prefix = f"kafka-{source['commit']}/{source['directory']}/"
        with tarfile.open(archive, "r:gz") as tar:
            entries = {member.name[len(prefix):]: member for member in tar.getmembers()
                       if member.isfile() and member.name.startswith(prefix)
                       and member.name.endswith(".json")}
            if entries.keys() != recorded.keys():
                raise SystemExit("Upstream schema inventory differs from supplied schemas")
            for name, member in entries.items():
                if tar.extractfile(member).read() != local[name].read_bytes():
                    raise SystemExit(f"Upstream schema bytes differ: {name}")
        print(f"Verified {len(recorded)} schemas against upstream {source['commit']}")


def java_literal(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise ValueError("Fixture number is outside signed int64")
        return f"{value}L"
    if isinstance(value, str):
        # JSON escapes also represent these fixture strings correctly in Java.
        return json.dumps(value, ensure_ascii=True)
    if isinstance(value, list):
        return "list(" + ",".join(java_literal(item) for item in value) + ")"
    if isinstance(value, dict):
        entries = [java_literal(part) for item in value.items() for part in item]
        return "map(" + ",".join(entries) + ")"
    raise TypeError(type(value))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="check captured bytes (default)")
    mode.add_argument("--write", action="store_true", help="replace fixture hex with Java output")
    parser.add_argument("--verify-upstream", action="store_true",
                        help="also compare all schemas to pinned upstream source archive")
    parser.add_argument("--cache", type=Path,
                        default=Path(tempfile.gettempdir()) / "kafka-java-fixtures")
    args = parser.parse_args()
    args.cache.mkdir(parents=True, exist_ok=True)
    check_schemas(args.cache, args.verify_upstream)
    jars = [fetch(args.cache, name, url, digest)
            for name, (url, digest) in ARTIFACTS.items()]
    fixture = json.loads(FIXTURES.read_text())
    names = [case["name"] for case in fixture["cases"]]
    if len(names) != len(set(names)):
        raise SystemExit("Duplicate fixture case names")
    calls = ["capture(" + ",".join([
        java_literal(case["name"]), java_literal(case["message"]), str(case["version"]),
        java_literal(case["fields"])]) + ");" for case in fixture["cases"]]
    driver = ("class KafkaJavaFixtureDriver extends KafkaJavaFixtures {\n"
              "public static void main(String[] args) throws Exception {\n" +
              "\n".join(calls) + "\n}\n}\n")
    with tempfile.TemporaryDirectory(prefix="kafka-java-fixture-build-") as work:
        source = Path(work) / "KafkaJavaFixtureDriver.java"
        source.write_text(driver)
        classpath = os.pathsep.join(str(p) for p in jars)
        subprocess.run(["javac", "--release", "17", "-cp", classpath, "-d", work,
                        str(ROOT / "scripts/kafka-java-fixtures.java"), str(source)], check=True)
        result = subprocess.run(["java", "-cp", os.pathsep.join([work, classpath]),
                                 "KafkaJavaFixtureDriver"], check=True, capture_output=True, text=True)
    lines = [line.split("\t", 1) for line in result.stdout.splitlines()]
    if [line[0] for line in lines] != names:
        raise SystemExit("Java fixture output inventory/order mismatch")
    changed = []
    for case, (_, generated_hex) in zip(fixture["cases"], lines):
        if case["hex"] != generated_hex:
            changed.append(case["name"])
        case["hex"] = generated_hex
    if args.write:
        FIXTURES.write_text(json.dumps(fixture, ensure_ascii=False, indent=2) + "\n")
        print(f"Captured {len(names)} Java wire fixtures ({len(changed)} changed)")
    elif changed:
        raise SystemExit("Java fixture bytes differ: " + ", ".join(changed))
    else:
        print(f"Verified {len(names)} Java wire fixtures")


if __name__ == "__main__":
    main()
