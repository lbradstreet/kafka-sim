#!/usr/bin/env python3
"""Check full histories, then compare classic/Panama runs with identical manifests.

The common environment is an input contract. Equal outcomes, packetization,
random fault draws, and closed-loop offer counts are deliberately not required
between different producers. Replay within each implementation is required.
"""
import argparse
import hashlib
import json
from pathlib import Path
from classic_artifacts import read_artifact


def require(condition, message):
    if not condition:
        raise ValueError(message)


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def load_report(path):
    report = json.loads(path.read_text())
    for field, artifact in report.get("artifacts", {}).items():
        report[field] = read_artifact(artifact)
    return report


def comparison_limits(scenario, report):
    """Mechanisms for which matching workload inputs cannot make the knobs equal."""
    limits = []
    if scenario in {"baseline.partition-admission-skew", "hard.partition-admission-isolation"}:
        limits.append("PartitionPressure/Shared descriptor policy has no classic Java equivalent")
    if scenario == "resources.memory-bounded-overload":
        limits.append("native descriptor/input credits and Java buffer.memory account different resources")
    if scenario == "resources.stop-polling-backpressure":
        limits.append("Java callbacks release producer buffers while application consumption is paused; no delivery-event pool")
    if scenario == "resources.wire-window-vs-latency":
        limits.append("Java has no per-connection Produce wire-credit window")
    if scenario == "topology.delete-recreate":
        limits.append("classic Java addresses topics by name and has no native topic-handle UUID fence")
    p = report["manifest"]["producer"]
    if p["lanes"] != 1:
        limits.append("classic Java has no native lane scheduler")
    if p.get("linger_skip_below_rate") is not None:
        limits.append("classic Java has no native sparse-rate linger bypass")
    return limits


def validate(report):
    require(report["schema"] == "kr-classic-comparison/v1", "report schema")
    require(report["replay_verified"], "complete replay is required")
    require(report["offered"] == report["accepted"] + report["refused"], "admission population")
    require(report["accepted"] == report["acked"] + report["failed"], "terminal population")
    manifest = report["manifest"]
    origin = manifest["start_ns"]
    external = report["external_history"]
    entries = report["environment"]["history"]["entries"]
    hooks = []
    commits = []
    connections = {}
    controls = []
    for entry in entries:
        kind, body = next(iter(entry["event"].items()))
        at = entry["now_ns"] - origin
        if kind == "FaultDecision":
            require(body["hook"]["now_ns"] == at, "fault clock differs from history clock")
            hooks.append(body)
        elif kind == "ConnectionOpened":
            connections[body["connection"]] = body["broker"]
        elif kind == "BrokerCommit":
            require(body["connection"] in connections, "commit has no connection")
            commits.append((at, connections[body["connection"]], body["records"]))
        elif kind == "ScheduledControl":
            require(at == body["at_ns"], "control was not executed at its exact nanosecond boundary")
            controls.append({"at_ns": body["at_ns"], "action": body["action"]})
    require(controls == manifest["experiment"]["scheduled_actions"], "scheduled controls differ")
    require(len(hooks) == report["environment"]["fault_stats"]["decisions"], "missing fault decision history")
    require(sum(s["offered"] for s in report["source_evidence"]) == report["offered"], "source population")
    for source, spec in zip(report["source_evidence"], manifest["experiment"]["loads"], strict=True):
        require(source["reserved"] == source["offered"] + source["cancelled"], "source reservation")
        require(source["offered"] == source["accepted"] + source["refused"], "source admission")
        offers = [e for e in external if e["kind"] == "offer" and e["load"] == source["load"]]
        require(len(offers) == source["offered"], "missing source offers")
        shape, body = next(iter(spec["shape"].items()))
        first = spec["template"]["first_id"]
        for index, offer in enumerate(offers):
            require(offer["id"] == first + index, "source ID sequence")
            require(offer["at_ns"] >= offer["due_ns"], "offer precedes its due time")
            if shape == "OpenLoop":
                due = body["start_ns"] + index * 1_000_000_000 // body["rate_per_s"]
                require(offer["due_ns"] == due, "open-loop source schedule changed")
    for pause in manifest["experiment"]["polling_pauses"]:
        require(not any(e["kind"] == "consumed" and pause["start_ns"] <= e["at_ns"] < pause["end_ns"]
                        for e in external), "delivery consumed during polling pause")
    phases = []
    coverage_gaps = []
    for index, rule in enumerate(manifest["faults"].get("environment", [])):
        matches = [d for d in hooks if rule["start_ns"] <= d["hook"]["now_ns"] < rule["end_ns"]
                   and d["hook"]["phase"] == rule["phase"]
                   and (rule.get("broker") is None or d["hook"]["broker"] == rule["broker"])
                   and (rule.get("api") is None or d["hook"]["api"] == rule["api"])]
        if not matches:
            coverage_gaps.append(f"environment rule {index} has no matching opportunity")
        fired = report["environment"]["fault_stats"]["environment_firings"][index]
        require(0 <= fired <= len(matches), "fault firing denominator")
        if rule["probability_ppm"] == 1_000_000:
            require(fired == len(matches), "deterministic fault missed an opportunity")
        phases.append({"kind": "rule", "index": index, "opportunities": len(matches), "firings": fired})
    for index, window in enumerate(manifest["faults"].get("isolations", [])):
        start, end, broker = window["start_ns"], window["end_ns"], window["broker"]
        if report["environment"]["now_ns"] < start:
            phases.append({"kind": "isolation", "index": index, "status": "run closed before window"})
            continue
        affected = sum(n for at, b, n in commits if start <= at < end and b == broker)
        if manifest["faults"].get("crash_on_isolation", False):
            require(affected == 0, "append through crashed broker")
        during = sum(e["kind"] == "offer" and start <= e["at_ns"] < end for e in external)
        require(during > 0, f"isolation {index} has no during-window demand")
        phases.append({"kind": "isolation", "index": index, "offers_during": during,
                       "affected_appends": affected,
                       "healthy_appends": sum(n for at, b, n in commits if start <= at < end and b != broker)})
    for index, window in enumerate(manifest["faults"].get("link_outages", [])):
        start, end, broker = window["start_ns"], window["end_ns"], window["broker"]
        during = sum(e["kind"] == "offer" and start <= e["at_ns"] < end for e in external)
        require(during > 0, f"link window {index} has no during-window demand")
        phases.append({"kind": "link", "index": index, "offers_during": during,
                       "affected_appends": sum(n for at, b, n in commits if start <= at < end and b == broker)})
    offers = [e["id"] for e in external if e["kind"] == "offer"]
    require(len(offers) == len(set(offers)) == report["offered"], "duplicate/missing offered ID")
    accepted = {e["id"]: e["at_ns"] for e in external if e["kind"] == "admission" and e["accepted"]}
    require(len(accepted) == report["accepted"], "missing accepted ID")
    require(set(accepted) <= set(offers), "admission without offer")
    delivered = [d["id"] for d in report["deliveries"]]
    require(len(delivered) == len(set(delivered)) == report["accepted"] and set(delivered) == set(accepted),
            "missing/duplicate terminal delivery")
    require(sum(d["success"] for d in report["deliveries"]) == report["acked"], "ack population")
    consumed = {e["id"]: e["at_ns"] for e in external if e["kind"] == "consumed"}
    require(set(consumed) == set(accepted), "missing consumption evidence")
    stored = {r["id"]: r for r in report["environment"]["log"]}
    require(len(stored) == len(report["environment"]["log"]), "duplicated broker ID")
    require(set(stored) <= set(accepted), "unaccepted broker ID")
    ordinals, previous = {record_id: i for i, record_id in enumerate(accepted)}, {}
    for record in report["environment"]["log"]:
        route = (tuple(record["topic_id"]), record["partition"])
        ordinal = ordinals[record["id"]]
        require(previous.get(route, -1) < ordinal, "partition append order differs from admission")
        previous[route] = ordinal
    latencies = []
    for d in report["deliveries"]:
        require(d["id"] in accepted, "delivery for unaccepted record")
        require(d["at_ns"] >= accepted[d["id"]], "delivery precedes admission")
        require(d["at_ns"] == consumed[d["id"]], "delivery time differs from consumption evidence")
        require(d.get("callback_ns", d["at_ns"]) <= d["at_ns"], "consumption precedes callback")
        require(not any(pause["start_ns"] <= d["at_ns"] < pause["end_ns"]
                        for pause in manifest["experiment"]["polling_pauses"]), "delivery time inside polling pause")
        if d["success"]:
            r = stored.get(d["id"])
            require(r is not None and (r["partition"], r["offset"]) == (d["partition"], d["offset"]),
                    "acknowledged offset/route missing from broker log")
            latencies.append(d["at_ns"] - accepted[d["id"]])
    latencies.sort()
    return {"manifest_sha256": fingerprint(manifest), "phases": phases, "coverage_gaps": coverage_gaps,
            "offered": report["offered"], "accepted": report["accepted"], "refused": report["refused"],
            "acked": report["acked"], "failed": report["failed"],
            "ack_latency_p99_ns": latencies[(len(latencies) * 99 + 99) // 100 - 1] if latencies else None,
            "max_source_lag_ns": max((e["at_ns"] - e["due_ns"] for e in external if e["kind"] == "offer"), default=0),
            "sender_errors": sum(e["kind"] == "sender-error" for e in external)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    runs, errors = {}, []
    for path in sorted(args.directory.rglob("*.json")):
        if any(s in path.name for s in [".first.", ".second.", ".failure."]):
            continue
        if "--" not in path.name:
            continue
        report = json.loads(path.read_text())
        if report.get("schema") != "kr-classic-comparison/v1":
            continue
        report = load_report(path)
        scenario, variant, adapter, profile, size, seed = path.stem.split("--")
        key = (scenario, variant, profile, size, seed)
        try:
            result = validate(report)
            result["comparison_limits"] = comparison_limits(scenario, report)
            if scenario.startswith("baseline."):
                require(report["failed"] == 0, "baseline accepted records must all acknowledge")
                if not any("OpenLoop" in load["shape"] for load in report["manifest"]["experiment"]["loads"]):
                    require(report["refused"] == 0, "baseline closed-loop admission must retry pressure")
            runs.setdefault(key, {})[adapter] = result
        except ValueError as error:
            errors.append({"file": str(path), "error": str(error)})
    pairs = []
    for key, pair in runs.items():
        if set(pair) != {"classic", "native"}:
            errors.append({"case": key, "error": "missing classic/native counterpart"})
            continue
        if pair["classic"]["manifest_sha256"] != pair["native"]["manifest_sha256"]:
            errors.append({"case": key, "error": "different effective scenario inputs"})
            continue
        pairs.append({"case": key, "fault_exposure_comparable": not any(r["coverage_gaps"] for r in pair.values()), **pair})
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps({"schema": "kr-classic-pairs/v1", "pairs": pairs, "errors": errors}, indent=2) + "\n")
    print(f"{len(pairs)} checked pairs, {sum(not p['fault_exposure_comparable'] for p in pairs)} exposure gaps, {len(errors)} errors; {args.out}")
    if errors or not pairs:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
