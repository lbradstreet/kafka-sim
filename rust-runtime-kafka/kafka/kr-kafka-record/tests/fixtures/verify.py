#!/usr/bin/env python3
"""Verify or regenerate independent Java fixtures with pinned, verified jars."""
import argparse
import importlib.util
import pathlib
import subprocess
import tempfile
import sys
sys.dont_write_bytecode = True

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[3]
spec = importlib.util.spec_from_file_location("wire_fixtures", ROOT / "scripts/kafka-java-fixtures.py")
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--cache", type=pathlib.Path, default=pathlib.Path(tempfile.gettempdir()) / "kafka-java-fixtures")
parser.add_argument("--write", action="store_true")
args = parser.parse_args()
args.cache.mkdir(parents=True, exist_ok=True)
jars = []
for name, (url, digest) in helper.ARTIFACTS.items():
    path = args.cache / name
    helper.fetch(args.cache, name, url, digest)
    jars.append(str(path))
with tempfile.TemporaryDirectory(prefix="kafka-record-java-") as target:
    classpath = ":".join(jars)
    subprocess.run(["javac", "-cp", classpath, "-d", target, str(HERE / "Capture.java")], check=True)
    subprocess.run(["java", "-cp", target + ":" + classpath, "Capture", "--write" if args.write else "--check", str(HERE)], check=True)
