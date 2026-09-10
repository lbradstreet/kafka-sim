#!/usr/bin/env python3
"""Verify a packaged native artifact exports the header ABI and no test hooks."""
from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("library", type=pathlib.Path)
    parser.add_argument("--test-artifact", action="store_true", help="require the conformance hooks instead of checking a production library")
    parser.add_argument("--nm", default="nm", help="symbol inspection tool for the artifact's platform")
    args = parser.parse_args()
    library = args.library.resolve(strict=True)
    header = pathlib.Path(__file__).resolve().parents[1] / "kafka/kr-kafka-ffi/include/kr_kafka.h"
    declarations = re.findall(r"^\s*(?:int32_t|uint32_t|void)\s+(kr_\w+)\s*\(", header.read_text(), flags=re.MULTILINE)
    if not declarations:
        parser.error("the C header did not contain any ABI declarations")
    command = [args.nm, "-gU", str(library)] if sys.platform == "darwin" else [args.nm, "-D", "--defined-only", str(library)]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    exported = {line.split()[-1].lstrip("_").split("@")[0] for line in result.stdout.splitlines() if line.split()}
    missing = set(declarations) - exported
    hooks = {symbol for symbol in exported if symbol.startswith("kr_test_")}
    required_hooks = {
        "kr_test_producer_create", "kr_test_resume", "kr_test_metadata", "kr_test_abort",
        "kr_test_arm_call", "kr_test_call_state", "kr_test_wait_call", "kr_test_release_call", "kr_test_replay_delivery",
        "kr_test_cancel_since",
    }
    errors = []
    if missing:
        errors.append("missing ABI exports: " + ", ".join(sorted(missing)))
    if args.test_artifact:
        if required_hooks - hooks:
            errors.append("missing conformance hooks: " + ", ".join(sorted(required_hooks - hooks)))
    elif hooks:
        errors.append("production artifact exposes test hooks: " + ", ".join(sorted(hooks)))
    if errors:
        raise SystemExit("\n".join(errors))
    print(f"Verified {len(declarations)} ABI exports and {len(hooks)} test hooks in {library}")


if __name__ == "__main__":
    main()
