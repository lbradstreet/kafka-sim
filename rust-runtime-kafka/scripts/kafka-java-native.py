#!/usr/bin/env python3
"""Build and independently audit the two Linux production libraries for releaseJar.

Uses only Python's standard library; --verify-only is portable, including macOS.
Build requires Linux, rustup, cargo, bash and internet for the pinned Zig archive.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import shlex
import struct
import subprocess
import sys
import tarfile
import tempfile
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "kafka/kr-kafka-java"
PIN = PROJECT / "gradle/native-toolchain.json"
HEADER = ROOT / "kafka/kr-kafka-ffi/include/kr_kafka.h"
MACHINES = {"x86_64": 62, "aarch64": 183}


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def source_hash() -> str:
    """Hash compiler inputs, not HEAD: Java-only commits do not stale native code."""
    hasher = hashlib.sha256()
    extra = {HEADER.relative_to(ROOT).as_posix(), PIN.relative_to(ROOT).as_posix(), "scripts/kafka-java-native.py", "Cargo.lock"}
    for parent, dirs, files in os.walk(ROOT):
        dirs[:] = sorted(d for d in dirs if d not in {".git", ".gradle", ".codex", ".agents", "target", "build", "node_modules"})
        for name in sorted(files):
            path = Path(parent) / name
            relative = path.relative_to(ROOT).as_posix()
            if path.suffix not in {".rs", ".toml", ".h", ".c", ".S"} and relative not in extra:
                continue
            hasher.update(relative.encode() + b"\0" + path.read_bytes() + b"\0")
    return hasher.hexdigest()


def required_symbols() -> set[str]:
    return set(re.findall(r"\b(kr_[a-z][a-z0-9_]*)\s*\(", HEADER.read_text()))


def elf_audit(data: bytes, arch: str, glibc_max: str) -> dict:
    """Read ELF headers/tables directly so release validation needs no host binutils."""
    if len(data) < 64 or data[:7] != b"\x7fELF\x02\x01\x01":
        raise ValueError("native artifact must be ELF64 little-endian")
    if struct.unpack_from("<HH", data, 16) != (3, MACHINES[arch]):
        raise ValueError(f"native artifact architecture/type does not match {arch}")
    offset = struct.unpack_from("<Q", data, 40)[0]
    stride, count = struct.unpack_from("<HH", data, 58)
    if stride != 64 or count == 0 or offset + stride * count > len(data):
        raise ValueError("invalid ELF section table")
    sections = [struct.unpack_from("<IIQQQQIIQQ", data, offset + stride * i) for i in range(count)]
    def contents(section):
        start, length = section[4], section[5]
        if start + length > len(data):
            raise ValueError("ELF section exceeds artifact")
        return data[start:start + length]
    def string(table, offset):
        if offset >= len(table):
            raise ValueError("invalid ELF string offset")
        end = table.find(b"\0", offset)
        if end == -1:
            raise ValueError("unterminated ELF string")
        return table[offset:end].decode("ascii")
    symbols, versions, needed = set(), set(), set()
    soname = None
    for section in sections:
        kind, link, entry_size = section[1], section[6], section[9]
        if kind not in {6, 11, 0x6ffffffe}:
            continue
        if link >= len(sections):
            raise ValueError("invalid ELF linked string table")
        strings, rows = contents(sections[link]), contents(section)
        if kind == 11:
            if entry_size != 24 or len(rows) % 24:
                raise ValueError("invalid ELF dynamic symbol table")
            for at in range(0, len(rows), 24):
                name, info, other, defined, _, _ = struct.unpack_from("<IBBHQQ", rows, at)
                if defined and info >> 4 in {1, 2} and other & 3 == 0:
                    symbols.add(string(strings, name))
        elif kind == 6:
            if entry_size != 16 or len(rows) % 16:
                raise ValueError("invalid ELF dynamic table")
            for at in range(0, len(rows), 16):
                tag, value = struct.unpack_from("<qQ", rows, at)
                if tag in {15, 29}:
                    raise ValueError("native artifact contains a build-host RPATH/RUNPATH")
                if tag == 1:
                    needed.add(string(strings, value))
                if tag == 14:
                    soname = string(strings, value)
        else:
            at, visited = 0, set()
            while at < len(rows):
                if at in visited or at + 16 > len(rows):
                    raise ValueError("invalid ELF version requirement chain")
                visited.add(at)
                version, entries, _, auxiliary, following = struct.unpack_from("<HHIII", rows, at)
                if version != 1:
                    raise ValueError("unsupported ELF version requirement format")
                aux = at + auxiliary
                for _ in range(entries):
                    if aux + 16 > len(rows):
                        raise ValueError("invalid ELF version auxiliary table")
                    _, _, _, name, next_aux = struct.unpack_from("<IHHII", rows, aux)
                    text = string(strings, name)
                    if text.startswith("GLIBC_"):
                        versions.add(text.removeprefix("GLIBC_"))
                    aux += next_aux
                if following == 0:
                    break
                at += following
    if soname != "libkr_kafka_ffi.so":
        raise ValueError("native artifact must have portable SONAME libkr_kafka_ffi.so")
    exported = {symbol for symbol in symbols if symbol.startswith("kr_")}
    if exported != required_symbols():
        raise ValueError(f"production ABI symbols differ: missing={sorted(required_symbols() - exported)} extra={sorted(exported - required_symbols())}")
    if not versions or any(not re.fullmatch(r"\d+(\.\d+)+", value) for value in versions):
        raise ValueError("missing or private glibc symbol requirement")
    highest = max(versions, key=lambda value: tuple(map(int, value.split("."))))
    if tuple(map(int, highest.split("."))) > tuple(map(int, glibc_max.split("."))):
        raise ValueError(f"native artifact requires glibc {highest}, above {glibc_max}")
    allowed = {"libc.so.6", "libm.so.6", "libdl.so.2", "libpthread.so.0", "librt.so.1", "libutil.so.1", "libgcc_s.so.1", "ld-linux-aarch64.so.1", "ld-linux-x86-64.so.2"}
    if not needed or not needed <= allowed:
        raise ValueError(f"unexpected dynamic dependencies: {sorted(needed)}")
    return {"architecture": arch, "soname": soname, "glibc_required": highest, "needed": sorted(needed), "symbols": sorted(exported), "sha256": digest(data)}


def verify(output: Path) -> dict:
    pin = json.loads(PIN.read_text())
    manifest = json.loads((output / "META-INF/native/manifest.json").read_text())
    expected_pin = digest(PIN.read_bytes())
    if manifest.get("schema_version") != 1 or manifest.get("abi_version") != 4:
        raise ValueError("unsupported native manifest schema/ABI")
    if manifest.get("source_hash") != source_hash() or manifest.get("toolchain_hash") != expected_pin:
        raise ValueError("native artifacts are stale: compiler inputs or pinned toolchain changed")
    if manifest.get("rust") != pin["rust"] or manifest.get("zig") != pin["zig"]:
        raise ValueError("native compiler versions differ from the pinned toolchain")
    expected_files = {"META-INF/native/manifest.json"}
    for arch in pin["targets"]:
        base = f"META-INF/native/linux-{arch}/libkr_kafka_ffi.so"
        expected_files.update({base, base + ".sha256"})
    actual_files = {path.relative_to(output).as_posix() for path in output.rglob("*") if path.is_file()}
    if actual_files != expected_files:
        raise ValueError("native resource directory contains missing or unexpected files")
    if set(manifest.get("artifacts", {})) != set(pin["targets"]):
        raise ValueError("release requires exactly x86_64 and aarch64 production libraries")
    for arch in pin["targets"]:
        library = output / f"META-INF/native/linux-{arch}/libkr_kafka_ffi.so"
        observed = elf_audit(library.read_bytes(), arch, pin["glibc_max"])
        if observed != manifest["artifacts"][arch]:
            raise ValueError(f"{arch} artifact does not match its verified manifest")
        if library.with_suffix(".so.sha256").read_text().strip() != observed["sha256"]:
            raise ValueError(f"{arch} runtime extraction checksum differs")
    return manifest


def run(arguments, **kwargs):
    subprocess.run(list(map(str, arguments)), check=True, **kwargs)


def zig_toolchain(cache: Path, pin: dict) -> Path:
    arch = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x86_64", "amd64": "x86_64"}.get(platform.machine())
    if platform.system() != "Linux" or arch not in pin["downloads"]:
        raise ValueError("building native resources requires Linux aarch64 or x86_64; verification is portable")
    archive = cache / f"zig-{arch}-{pin['zig']}.tar.xz"
    expected = pin["downloads"][arch]
    cache.mkdir(parents=True, exist_ok=True)
    if not archive.exists() or digest(archive.read_bytes()) != expected["sha256"]:
        temporary = archive.with_suffix(".download")
        urllib.request.urlretrieve(expected["url"], temporary)
        if digest(temporary.read_bytes()) != expected["sha256"]:
            temporary.unlink()
            raise ValueError("downloaded Zig archive fails its pinned SHA-256")
        temporary.replace(archive)
    directory = cache / f"zig-{arch}-linux-{pin['zig']}"
    # Re-extract the checked archive so a modified cached compiler is not trusted.
    if directory.exists():
        shutil.rmtree(directory)
    with tarfile.open(archive) as bundle:
        bundle.extractall(cache, filter="data")
    zig = directory / "zig"
    if subprocess.check_output([zig, "version"], text=True).strip() != pin["zig"]:
        raise ValueError("Zig version does not match the pinned toolchain")
    return zig


def build(output: Path, jobs: int) -> dict:
    pin = json.loads(PIN.read_text())
    compiler_inputs = source_hash()
    target = ROOT / "target/kafka-java-native"
    zig = zig_toolchain(target / "toolchain", pin)
    manifest = {"schema_version": 1, "abi_version": 4, "source_hash": compiler_inputs,
                "toolchain_hash": digest(PIN.read_bytes()), "rust": pin["rust"], "zig": pin["zig"], "artifacts": {}}
    env = os.environ.copy()
    for key in list(env):
        if key in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CFLAGS", "CXXFLAGS", "LDFLAGS", "CARGO_BUILD_RUSTFLAGS"}:
            del env[key]
    env.update(RUSTC_WRAPPER="", CARGO_TARGET_DIR=str(target), CARGO_BUILD_JOBS=str(jobs), CARGO_PROFILE_RELEASE_STRIP="symbols")
    for arch in pin["targets"]:
        triple = f"{arch}-unknown-linux-gnu"
        run(["rustup", "target", "add", "--toolchain", pin["rust"], triple])
        cc, ar = target / f"cc-{arch}", target / f"ar-{arch}"
        cc.write_text("#!/bin/bash\nargs=()\nfor arg in \"$@\"; do\n  [[ \"$arg\" == --target=* ]] || args+=(\"$arg\")\ndone\nexec "
                      + shlex.quote(str(zig)) + f" cc -target {arch}-linux-gnu.{pin['glibc_max']} -mcpu=baseline \"${{args[@]}}\"\n")
        ar.write_text(f'#!/bin/sh\nexec "{zig}" ar "$@"\n')
        cc.chmod(0o755)
        ar.chmod(0o755)
        env[f"CC_{triple.replace('-', '_')}"] = str(cc)
        env[f"AR_{triple.replace('-', '_')}"] = str(ar)
        env[f"CARGO_TARGET_{triple.upper().replace('-', '_')}_LINKER"] = str(cc)
        run(["cargo", f"+{pin['rust']}", "rustc", "--locked", "--release", "-p", "kr-kafka-ffi", "--target", triple, "--", "-C", "link-arg=-Wl,-soname,libkr_kafka_ffi.so"], cwd=ROOT, env=env)
        data = (target / triple / "release/libkr_kafka_ffi.so").read_bytes()
        report = elf_audit(data, arch, pin["glibc_max"])
        destination = output / f"META-INF/native/linux-{arch}/libkr_kafka_ffi.so"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        destination.with_suffix(".so.sha256").write_text(report["sha256"] + "\n")
        manifest["artifacts"][arch] = report
    if source_hash() != compiler_inputs:
        raise ValueError("compiler inputs changed during the build; rebuild before packaging")
    (output / "META-INF/native/manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return verify(output)


def self_test(output: Path):
    """Corrupt real ELF/manifests to exercise release rejection, not a synthetic layout."""
    manifest = verify(output)
    for arch in MACHINES:
        data = (output / f"META-INF/native/linux-{arch}/libkr_kafka_ffi.so").read_bytes()
        variants = [data[:20], b"bad!" + data[4:]]
        changed = bytearray(data)
        struct.pack_into("<H", changed, 18, MACHINES["aarch64" if arch == "x86_64" else "x86_64"])
        variants.append(bytes(changed))
        original = b"kr_abi_version"
        variants.append(data.replace(original, b"kr_test_" + b"x" * (len(original) - 8)))
        version = ("GLIBC_" + manifest["artifacts"][arch]["glibc_required"]).encode()
        variants.append(data.replace(version, b"GLIBC_" + b"9" * (len(version) - 6)))
        for corrupted in variants:
            try:
                elf_audit(corrupted, arch, "2.28")
            except (ValueError, struct.error):
                pass
            else:
                raise AssertionError("release audit accepted a corrupted artifact")
    def rejected(mutator):
        with tempfile.TemporaryDirectory() as temporary:
            copied = Path(temporary)
            shutil.copytree(output, copied, dirs_exist_ok=True)
            mutator(copied)
            try:
                verify(copied)
            except ValueError:
                pass
            else:
                raise AssertionError("release audit accepted corrupted resources")
    def stale(copied):
        path = copied / "META-INF/native/manifest.json"
        changed = json.loads(path.read_text())
        changed["source_hash"] = "0" * 64
        path.write_text(json.dumps(changed))
    rejected(stale)
    rejected(lambda copied: (copied / "unexpected.so").write_bytes(b"extra"))
    for arch in MACHINES:
        relative = f"META-INF/native/linux-{arch}/libkr_kafka_ffi.so"
        rejected(lambda copied: (copied / (relative + ".sha256")).write_text("0" * 64))
        rejected(lambda copied: (copied / relative).unlink())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--output", type=Path, default=PROJECT / "build/generated/nativeResources")
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    report = verify(args.output) if args.verify_only or args.self_test else build(args.output, args.jobs)
    if args.self_test:
        self_test(args.output)
    print(json.dumps({"source_hash": report["source_hash"], "artifacts": {arch: {key: row[key] for key in ["architecture", "glibc_required", "sha256"]} for arch, row in report["artifacts"].items()}}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.CalledProcessError, struct.error) as error:
        sys.exit(f"Native packaging failed: {error}")
