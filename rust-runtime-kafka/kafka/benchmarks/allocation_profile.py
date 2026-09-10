"""Separate native allocator profiling; never mix profiled throughput into baselines.

Parser contract: KDE heaptrack v1.5.0 heaptrack_print summary labels.
https://github.com/KDE/heaptrack/blob/v1.5.0/src/analyze/print/heaptrack_print.cpp
https://github.com/KDE/heaptrack/blob/v1.5.0/src/track/heaptrack.sh.cmake
"""
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re

MAX_SUMMARY_BYTES = 16 * 1024 * 1024


def parse_summary(text):
    """Reject missing/duplicate/changed summaries; exact calls, rounded byte sizes."""
    if len(text.encode("utf-8")) > MAX_SUMMARY_BYTES:
        raise ValueError("heaptrack summary exceeds parser bound")
    def one(pattern):
        matches = re.findall(pattern, text, flags=re.MULTILINE)
        if len(matches) != 1:
            raise ValueError("missing, duplicate, or incompatible heaptrack summary")
        return matches[0]
    calls = int(one(r"^calls to allocation functions: ([0-9]+) \([0-9]+/s\)$"))
    temporary = int(one(r"^temporary memory allocations: ([0-9]+) \([0-9]+/s\)$"))
    if temporary > calls or calls > (1 << 63) - 1:
        raise ValueError("inconsistent allocation counts")
    def size(label):
        # heaptrack 1.5 writeBytes emits only the first unit character when
        # width is zero (the summary), and the complete unit with padding.
        # Its formatBytes scales by 1000, not 1024.
        amount, unit = one(r"^" + re.escape(label) + r": ([0-9]+(?:\.[0-9]+)?)(B|[KMGT]B?)$")
        value = Decimal(amount) * (1000 ** "BKMGT".index(unit[0]))
        if value > (1 << 63) - 1:
            raise ValueError("heaptrack byte count exceeds supported range")
        return {"display": amount + unit, "rounded_bytes": int(value), "exact": unit == "B"}
    return dict(native_allocation_calls=calls, temporary_allocation_calls=temporary,
                peak_heap=size("peak heap memory consumption"), retained_at_exit=size("total memory leaked"))


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def run(argv, directory, timeout, run_isolated, command):
    """One extra run with all argv passed as argv, no shell interpolation."""
    directory.mkdir(exist_ok=False)
    prefix = directory.resolve() / "allocations"
    # heaptrack 1.5 shell launcher expands its output template before quoting it.
    if re.search(r"\s|%", str(prefix)):
        raise ValueError("heaptrack output path must contain neither whitespace nor percent")
    record = {"schema": "kr-kafka-allocation-profile/v1", "argv": argv,
              "scope": "whole-process intercepted native allocator calls, including setup and harness; excludes managed JVM/Python object allocations and non-intercepted allocators",
              "performance_comparable": False,
              "versions": [command(["heaptrack", "--version"]), command(["heaptrack_print", "--version"])]}
    def invoke(command_args, stdout, stderr):
        try:
            return run_isolated(command_args, stdout, stderr, timeout)
        except OSError as error:
            return {"exit_code": None, "launch_error": type(error).__name__}
    with (directory / "stdout.txt").open("w") as stdout, (directory / "stderr.txt").open("w") as stderr:
        record.update(invoke(["heaptrack", "--record-only", "-o", str(prefix), "--", *argv], stdout, stderr))
    artifacts = [path for path in directory.glob("allocations.*") if path.suffix in (".gz", ".zst")]
    record["artifacts"] = [{"file": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path)} for path in artifacts]
    record["complete"] = False
    if record.get("exit_code") == 0 and len(artifacts) == 1:
        summary = directory / "summary.txt"
        with summary.open("w") as stdout, (directory / "summary.stderr").open("w") as stderr:
            analyzed = invoke(["heaptrack_print", "-f", str(artifacts[0]), "--print-peaks=false", "--print-allocators=false", "--print-temporary=false"], stdout, stderr)
        record["analysis"] = analyzed
        if analyzed.get("exit_code") == 0:
            try:
                if summary.stat().st_size > MAX_SUMMARY_BYTES:
                    raise ValueError("heaptrack summary exceeds parser bound")
                record["measurements"] = parse_summary(summary.read_text())
                record["summary_sha256"] = sha256_file(summary)
                record["complete"] = True
            except (UnicodeError, ValueError) as error:
                record["parse_error"] = str(error)
    (directory / "profile.json").write_text(json.dumps(record, indent=2))
    return record
