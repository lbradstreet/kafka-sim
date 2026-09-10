#!/usr/bin/env python3
"""Regenerate all checked-in FFM sources, or compare complete output with --check.

Pass --archive pointing at the pinned jextract tarball. Extraction is private and
temporary on every run: a mutable tool installation is not a reproducible input.
No download is implicit. See gradle/jextract-toolchain.json for official URLs.
"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parent.parent
PROJECT = ROOT / "kafka/kr-kafka-java"
HEADER = ROOT / "kafka/kr-kafka-ffi/include/kr_kafka.h"
MANIFEST = PROJECT / "gradle/jextract-toolchain.json"
INCLUDES = PROJECT / "gradle/jextract-includes.txt"
HASHES = PROJECT / "gradle/jextract-inputs.json"
GENERATED = PROJECT / "src/main/java/io/krkafka/ffi"
SYMBOLS = PROJECT / "src/main/resources/META-INF/kr-kafka-symbols.txt"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--archive", required=True, type=Path)
    args = parser.parse_args()
    manifest = json.loads(MANIFEST.read_text())
    os_name = {"Darwin": "macos", "Linux": "linux"}.get(platform.system())
    arch = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x64"}.get(platform.machine())
    key = f"{os_name}-{arch}"
    if key not in manifest["artifacts"]:
        raise SystemExit(f"No pinned generator for {key}")
    artifact = manifest["artifacts"][key]
    if sha(args.archive) != artifact["sha256"]:
        raise SystemExit("Generator archive SHA-256 mismatch")
    with tempfile.TemporaryDirectory(prefix="kr-java-bindings-") as temporary:
        work = Path(temporary)
        with tarfile.open(args.archive) as archive:
            archive.extractall(work, filter="data")
        tool = work / "jextract-25/bin/jextract"
        version = subprocess.check_output([tool, "--version"], text=True, stderr=subprocess.STDOUT)
        if f"JDK version {manifest['jdk']}" not in version or f"clang version {manifest['libclang']}" not in version:
            raise SystemExit("Generator runtime/libclang mismatch")
        dump = work / "includes.txt"
        subprocess.run([tool, "--dump-includes", dump, HEADER.relative_to(ROOT)], cwd=ROOT, check=True)
        # The pinned tool enumerates exact names; no wildcard assumptions.
        selected = []
        for line in dump.read_text().splitlines():
            if line.startswith("--include-") and line.split()[1].startswith(("kr_", "KR_")):
                selected.append(line.split("#", 1)[0].strip())
        # jextract's include inventory order is not stable across processes.
        # Keep exact names while canonicalizing only enumeration order.
        selected.sort()
        symbols = "\n".join(selected) + "\n"
        required = "\n".join(line.split()[1] for line in selected if line.startswith("--include-function ")) + "\n"
        if args.check:
            if INCLUDES.read_text() != symbols:
                raise SystemExit("Explicit include inventory differs; regenerate bindings")
            if SYMBOLS.read_text() != required:
                raise SystemExit("Required loader symbols differ; regenerate bindings")
        else:
            INCLUDES.write_text(symbols)
            SYMBOLS.parent.mkdir(parents=True, exist_ok=True)
            SYMBOLS.write_text(required)
        output = work / "generated"
        command = [tool, "@" + str(INCLUDES), "--target-package", "io.krkafka.ffi",
                   "--output", output, HEADER.relative_to(ROOT)]
        subprocess.run(command, cwd=ROOT, check=True)
        actual = output / "io/krkafka/ffi"
        # On supported LP64 platforms uint64_t aliases unsigned long (Linux)
        # or unsigned long long (macOS). Both emitted layouts are exactly
        # OfLong layouts; canonicalize only those qualified layout references. No
        # field, padding, descriptor, symbol or source inventory is inferred.
        shared = (actual / "kr_kafka_h$shared.java").read_text()
        for alias, spelling in (("C_LONG", "long"), ("C_LONG_LONG", "long long")):
            expected = 'public static final ValueLayout.OfLong ' + alias + ' = (ValueLayout.OfLong) Linker.nativeLinker().canonicalLayouts().get("' + spelling + '");'
            if expected not in shared:
                raise SystemExit("Generator target is not the pinned LP64 ABI")
        for path in actual.iterdir():
            path.write_text(re.sub(r"\bkr_kafka_h\.C_LONG\b", "kr_kafka_h.C_LONG_LONG", path.read_text()))
        inputs = {str(p.relative_to(ROOT)): sha(p) for p in [HEADER, INCLUDES, MANIFEST, Path(__file__).resolve()]}
        generated = {p.name: p.read_bytes() for p in actual.iterdir() if p.is_file()}
        if args.check:
            if json.loads(HASHES.read_text()) != inputs:
                raise SystemExit("Generation input hashes differ")
            expected = {p.name: p.read_bytes() for p in GENERATED.iterdir() if p.is_file()}
            if generated != expected:
                names = sorted(set(generated) | set(expected))
                raise SystemExit("Generated source drift: " + ", ".join(n for n in names if generated.get(n) != expected.get(n)))
        else:
            GENERATED.mkdir(parents=True, exist_ok=True)
            for path in GENERATED.iterdir():
                if path.is_file(): path.unlink()
            for path in actual.iterdir(): shutil.copy2(path, GENERATED / path.name)
            HASHES.write_text(json.dumps(inputs, indent=2) + "\n")
        print(f"{'Verified' if args.check else 'Generated'} {len(generated)} complete FFM source files ({key})")


if __name__ == "__main__":
    main()
