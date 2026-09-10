#!/usr/bin/env python3
"""Compare epoch recovery to the frozen Full review, including unchanged Java histories."""
import argparse
import copy
import hashlib
import json
from pathlib import Path

from classic_comparison import fingerprint, load_report, require, validate
from classic_outcome_review import review_failures


def input_contract(manifest):
    result = copy.deepcopy(manifest)
    # The implementation changed; every other manifest field must match.
    del result["versions"]["source_sha256"]
    return result


def comparable_java(report):
    return {k: input_contract(v) if k in ("manifest", "original_manifest") else v
            for k, v in report.items() if k != "artifacts"}


def evidence(path, report, probe_ids):
    checked = validate(report)
    require(not checked["coverage_gaps"], "missing configured fault exposure")
    probes = [d for d in report["deliveries"] if d["id"] in probe_ids]
    admissions = {e["id"] for e in report["external_history"]
                  if e["kind"] == "admission" and e["accepted"]}
    return {
        "header": str(path),
        "header_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "artifacts_sha256": {k: v["sha256"] for k, v in report["artifacts"].items()},
        "source_sha256": report["manifest"]["versions"]["source_sha256"],
        "records": {k: report[k] for k in ("offered", "accepted", "refused", "acked", "failed")},
        "failures": review_failures(report),
        "fatals": [e for e in report["external_history"]
                   if e["kind"] == "native-event" and e["event"].startswith("Fatal ")],
        "probes": {
            "ids": sorted(probe_ids),
            "accepted": len(probe_ids & admissions),
            "acked": sum(d["success"] for d in probes),
            "failed": sum(not d["success"] for d in probes),
            "first_terminal_ns": min((d["at_ns"] for d in probes), default=None),
            "last_terminal_ns": max((d["at_ns"] for d in probes), default=None),
        },
        "replay_verified": report["replay_verified"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    frozen = json.loads((args.before / "matrix.json").read_text())["identity"]
    provenance = json.loads((args.after / "provenance-both-both-full-0.json").read_text())
    require(provenance["kafka"]["head"] == frozen["kafka_head"], "Java revision changed")
    require(provenance["kafka"]["tracked_diff_sha256"] == frozen["kafka_diff_sha256"],
            "Java source changes differ")
    require(not provenance["kafka"]["status"], "Java checkout must be clean")
    results = []
    for new_path in sorted(args.after.glob("*--native--*--full--0.json")):
        scenario, variant, _, profile, size, seed = new_path.stem.split("--")
        require(scenario in ("hard.short-vs-long-outage", "resources.delivery-timeout-tuning"),
                "unexpected recovery case")
        old_path = args.before / scenario / new_path.name
        java_name = new_path.name.replace("--native--", "--classic--")
        paths = {"native_before": old_path, "native_after": new_path,
                 "java_before": args.before / scenario / java_name,
                 "java_after": args.after / java_name}
        reports = {k: load_report(p) for k, p in paths.items()}
        contract = input_contract(reports["native_before"]["manifest"])
        for r in reports.values():
            require(input_contract(r["manifest"]) == contract, "scenario environment changed")
            require(r["classic_config"] == reports["java_before"]["classic_config"],
                    "Java configuration changed")
        require(comparable_java(reports["java_before"]) == comparable_java(reports["java_after"]),
                "frozen Java result changed beyond source fingerprint/artifact paths")
        probe_ids = set()
        for load in contract["experiment"]["loads"]:
            shape = load["shape"].get("ClosedLoop")
            if shape and shape["start_ns"] >= contract["faults"]["isolations"][0]["end_ns"]:
                first = load["template"]["first_id"]
                probe_ids.update(range(first, first + shape["count"]))
        row = {"case": [scenario, variant, profile, size, seed],
               "input_contract_sha256": fingerprint(contract),
               "java_complete_result_unchanged": True,
               "runs": {k: evidence(paths[k], r, probe_ids) for k, r in reports.items()}}
        native, java = row["runs"]["native_after"], row["runs"]["java_after"]
        require(native["records"]["failed"] == java["records"]["failed"],
                "native terminal failures differ from Java")
        require(not native["fatals"], "native recovery became fatal")
        for r in (native, java):
            require(r["probes"]["accepted"] == r["probes"]["acked"] == len(probe_ids),
                    "a fixed recovery probe did not acknowledge")
            require(r["probes"]["failed"] == 0, "a recovery probe failed")
        results.append(row)
        print("Checked", "/".join(row["case"]), flush=True)
    require(len(results) == 14, "expected seven variants in both profiles")
    output = {"schema": "kr-kafka-epoch-recovery-review/v1",
              "baseline_library_sha256": frozen["library_sha256"],
              "after_provenance": provenance,
              "comparison_exclusions": ["manifest.versions.source_sha256",
                                        "original_manifest.versions.source_sha256",
                                        "artifact location/encoding metadata"],
              "pairs": results}
    args.out.write_text(json.dumps(output, indent=2) + "\n")


if __name__ == "__main__":
    main()
