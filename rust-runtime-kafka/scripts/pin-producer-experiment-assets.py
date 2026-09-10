#!/usr/bin/env python3
"""Refresh or check the reviewed source hashes used by HTML export."""
import argparse
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--check", action="store_true")
args = parser.parse_args()
root = Path(__file__).resolve().parent.parent
files = ["trace-viewer-core.js", "producer-experiment-model.js", "trace-viewer-ui.js",
         "producer-comparison-model.js", "producer-comparison-viewer.js",
         "producer-experiment-viewer.js", "trace-viewer.css", "producer-experiment.css",
         "producer-experiment.html"]
pins = {name: hashlib.sha256((root / "tools/trace-tool" / name).read_bytes()).hexdigest()
        for name in files}
text = json.dumps(pins, sort_keys=True, indent=2) + "\n"
out = root / "kafka/kr-kafka-experiments/src/export/asset-hashes.json"
if args.check:
    if out.read_text() != text:
        raise SystemExit("Producer experiment assets differ from reviewed export hashes")
else:
    out.write_text(text)
