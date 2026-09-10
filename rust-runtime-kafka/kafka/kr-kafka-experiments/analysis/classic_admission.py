#!/usr/bin/env python3
"""Measure admission isolation from complete shared-driver evidence and link its explorer."""
import argparse
from collections import Counter
import copy
import html
import json
from pathlib import Path

from classic_comparison import fingerprint, load_report, require, validate
from classic_visualization import compact, digest, reviewed_assets

SCENARIO = "hard.partition-admission-isolation"
NS = 1_000_000_000
START, END, WIDTH = 10 * NS, 13 * NS, NS // 10
PLOT_START, PLOT_END = 9 * NS, 14 * NS
METRICS = ("offered", "accepted", "refused", "acked", "failed")
NAMES = {"classic": "Classic Java", "shared": "Native Shared", "pressure": "Native PartitionPressure"}


def measure(report):
    """Fixed source destinations remain observable even when admission refuses them."""
    validate(report)
    m = report["manifest"]
    require(m["experiment"]["scheduled_actions"] == [], "isolation overview requires stable topology")
    require(m["faults"]["isolations"] == [{"broker": 1, "start_ns": START, "end_ns": END}], "isolation interval")
    require(m["topics"][0]["leaders"] == [1, 2, 3, 1, 2, 3], "isolation topology")
    loads = m["experiment"]["loads"]
    require(m['producer']['record_descriptors'] == 512 and m['producer']['input_bytes'] == 8 * 1024 * 1024
            and report['classic_config']['buffer.memory'] == 8 * 1024 * 1024, "overview capacity contract")
    destinations = []
    for load in loads:
        t = load["template"]
        require(t["topic"] == 0 and set(t["partitioning"]) == {"Fixed"}, "fixed source destination required")
        require(set(load["shape"]) == {"OpenLoop"}, "independent open sources required")
        require(load['shape']['OpenLoop']['start_ns'] == 0 and load['shape']['OpenLoop']['end_ns'] == 30 * NS,
                "Full thirty-second sources required")
        destinations.append(t["partitioning"]["Fixed"]["partition"])
    require(sorted(destinations) == list(range(6)), "one independent source per partition")
    rows = [{"partition": p, "healthy": p not in (0, 3),
             "outage": dict.fromkeys(METRICS, 0), "whole_run": dict.fromkeys(METRICS, 0),
             "plot": {k: [0] * 50 for k in METRICS}, "outage_ack_windows": [0] * 30,
             "outage_accepted_eventually_acked": 0} for p in range(6)]
    offers = {e["id"]: e for e in report["external_history"] if e["kind"] == "offer"}
    accepted = {e["id"]: e["at_ns"] for e in report["external_history"]
                if e["kind"] == "admission" and e["accepted"]}

    def record(record_id, at, metric):
        p = destinations[offers[record_id]["load"]]
        r = rows[p]
        r["whole_run"][metric] += 1
        if START <= at < END:
            r["outage"][metric] += 1
            if metric == "acked":
                r["outage_ack_windows"][(at - START) // WIDTH] += 1
        if PLOT_START <= at < PLOT_END:
            r["plot"][metric][(at - PLOT_START) // WIDTH] += 1
        return r

    for record_id, e in offers.items():
        record(record_id, e["at_ns"], "offered")
        if record_id not in accepted:
            record(record_id, e["at_ns"], "refused")
        else:
            record(record_id, accepted[record_id], "accepted")
    for d in report["deliveries"]:
        row = record(d["id"], d["at_ns"], "acked" if d["success"] else "failed")
        if d["success"]:
            require(d["partition"] == row["partition"], "ack differs from fixed source destination")
            if START <= accepted[d["id"]] < END:
                row["outage_accepted_eventually_acked"] += 1
    for metric in METRICS:
        require(sum(r["whole_run"][metric] for r in rows) == report[metric], "partition population")
    refused = [e for e in report['external_history'] if e['kind'] == 'refused']
    require(len(refused) == len({e['id'] for e in refused}) and
            {e['id'] for e in refused} == offers.keys() - accepted.keys(), "permanent refusal evidence")
    reasons, healthy_reasons, refusal_times = Counter(), Counter(), []
    for e in refused:
        require(e['at_ns'] == offers[e['id']]['at_ns'], "open-loop refusal clock")
        if START <= e['at_ns'] < END:
            reasons[e['error']] += 1
            if destinations[offers[e['id']]['load']] not in (0, 3):
                healthy_reasons[e['error']] += 1
            refusal_times.append(e['at_ns'])
    # Java's repeated executions under the two native policy labels must agree
    # in complete behavior, not just in aggregate counts.
    behavior = {field: report["artifacts"][field]["sha256"] if field in report.get("artifacts", {})
                else fingerprint(report[field]) for field in ("external_history", "deliveries", "environment")}
    normalized = copy.deepcopy(m)
    normalized["producer"]["descriptor_admission_policy"] = "Shared"
    return {"partitions": rows, "summary": {k: report[k] for k in METRICS},
            "healthy_outage_refused": sum(r["outage"]["refused"] for r in rows if r["healthy"]),
            "healthy_empty_ack_windows": sum(r["outage_ack_windows"].count(0) for r in rows if r["healthy"]),
            "behavior_sha256": behavior, "inputs_except_policy_sha256": fingerprint(normalized),
            "java_config_sha256": fingerprint(report["classic_config"]),
            "java_buffer_bytes": report["classic_config"]["buffer.memory"],
            "native_input_bytes": m["producer"]["input_bytes"],
            "native_descriptors": m["producer"]["record_descriptors"],
            "outage_first_refusal_ns": str(min(refusal_times)) if refusal_times else None,
            "outage_last_refusal_ns": str(max(refusal_times)) if refusal_times else None,
            "outage_refusal_reasons": [{'reason': k, 'count': v, 'healthy_count': healthy_reasons[k]}
                                      for k, v in sorted(reasons.items())],
            "policy": m["producer"]["descriptor_admission_policy"]}


def combine(runs):
    require(set(runs) == {"classic-shared", "classic-pressure", "native-shared", "native-pressure"}, "four replayed executions required")
    require(len({r["inputs_except_policy_sha256"] for r in runs.values()}) == 1, "inputs differ beyond native policy")
    require(len({r["java_config_sha256"] for r in runs.values()}) == 1, "Java configuration changed")
    for name, r in runs.items():
        require(r["policy"] == ("Shared" if name.endswith("-shared") else "PartitionPressure"), "policy label differs from effective policy")
    require(runs["classic-shared"]["behavior_sha256"] == runs["classic-pressure"]["behavior_sha256"], "Java behavior changed between native policy labels")
    return {"classic": runs["classic-shared"], "shared": runs["native-shared"], "pressure": runs["native-pressure"]}


def table(headers, rows):
    cell = lambda tag, value: f'<{tag}>{html.escape(str(value))}</{tag}>'
    return ('<div class="table-scroll"><table><thead><tr>' + ''.join(cell('th', h) for h in headers)
            + '</tr></thead><tbody>' + ''.join('<tr>' + ''.join(cell('td', c) for c in r) + '</tr>' for r in rows)
            + '</tbody></table></div>')


def plot(arms, metric, healthy=True):
    series = {name: [sum(p["plot"][metric][i] for p in r["partitions"] if p["healthy"] == healthy)
                     for i in range(50)] for name, r in arms.items()}
    peak = max(1, max(max(v) for v in series.values()))
    x = lambda i: 55 + i * 10.4
    y = lambda v: 185 - 140 * v / peak
    label = f'{"Healthy" if healthy else "Affected"} partitions: {metric} records per 100 ms'
    svg = f'<svg viewBox="0 0 600 225" role="img" aria-label="{label}"><title>{label}</title>'
    svg += f'<rect x="{x(10)}" y="35" width="312" height="150" fill="var(--muted)"/><text x="315" y="25" text-anchor="middle">Broker 1 isolated</text>'
    for value in (0, peak / 2, peak):
        svg += f'<path d="M55 {y(value)} H575" stroke="var(--border)"/><text x="48" y="{y(value)+4}" text-anchor="end">{value:g}</text>'
    for second in range(9, 15):
        svg += f'<text x="{x((second-9)*10)}" y="207" text-anchor="middle">{second}s</text>'
    for index, (name, values) in enumerate(series.items()):
        points = ' '.join(f'{x(i):.2f},{y(v):.2f} {x(i+1):.2f},{y(v):.2f}' for i, v in enumerate(values))
        dash = ['', '8 3', '2 3'][index]
        svg += f'<polyline points="{points}" fill="none" stroke="var(--variant-{index+1})" stroke-width="2.5" stroke-dasharray="{dash}"><title>{NAMES[name]}</title></polyline>'
    return f'<article><h3>{label}</h3>{svg}</svg></article>'


def render(bundle, links):
    assets = reviewed_assets()
    page = '<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Java and native admission isolation</title><style>'
    page += assets['trace-viewer.css'] + assets['producer-experiment.css']
    page += 'svg{width:100%;height:auto}svg text{fill:var(--foreground);font-size:12px}h2{margin-top:2rem}details{margin:1rem 0}summary{cursor:pointer}a{color:var(--selected)}</style><main>'
    page += '<p class="eyebrow">Full simulation · original profile · independent demand</p><h1>Does one failed broker block healthy partitions?</h1>'
    page += '<p>Six independent sources continue offering for 30 seconds. Broker 1, which leads partitions 0 and 3, is isolated during 10–13 seconds. Partitions 1, 2, 4 and 5 remain healthy. This is the workload that tests Shared versus PartitionPressure admission without closed-loop demand masking.</p>'
    page += '<p>Blue solid: Classic Java. Orange dashed: Native Shared. Green dotted: Native PartitionPressure. Every execution is replay verified. Java uses identical configuration and has identical complete execution evidence under both native policy labels.</p>'
    page += '<p>The native variants share 512 descriptors and 8 MiB of input credits. Java uses an 8 MiB buffer and has no 512-record descriptor limit. These are equal offered workloads and fault schedules; admission capacity and memory accounting differ. This trial does not give Java a partition admission policy.</p>'
    page += '<p>Counts below use exact 10 ≤ t &lt; 13 second boundaries. Each healthy partition has thirty 100 ms acknowledgment windows: 120 windows per execution. Refusals never enter delivery latency.</p>'
    overview = []
    for trial in bundle['trials']:
        for name, r in trial['arms'].items():
            overview.append([f"{trial['rate']:,}", trial['seed'], NAMES[name], f"{r['healthy_outage_refused']:,}",
                             r['healthy_empty_ack_windows'], f"{r['summary']['refused']:,}", f"{r['summary']['failed']:,}"])
    page += table(['Offers/s', 'Seed', 'Producer', 'Healthy outage refusals', 'Healthy windows without ACK / 120', 'Whole-run refusals', 'Accepted failures'], overview)
    for trial in reversed(bundle['trials']):
        arms, rate, seed = trial['arms'], trial['rate'], trial['seed']
        page += f'<h2>{rate:,} offers/s · seed {seed}</h2><div class="comparison-grid">'
        page += plot(arms, 'offered') + plot(arms, 'refused') + plot(arms, 'acked') + plot(arms, 'acked', False) + '</div>'
        page += '<p>Open the existing interactive explorer to zoom, select individual partitions, and inspect latency, outstanding records and requests: '
        page += ' · '.join(f'<a href="{html.escape(links[(rate, seed, policy)], quote=True)}">Java vs native {policy}</a>' for policy in ('shared', 'pressure')) + '.</p>'
        page += '<details><summary>Exact per-partition outage counts and recovery</summary>'
        rows = []
        for name, r in arms.items():
            for p in r['partitions']:
                o = p['outage']
                rows.append([NAMES[name], p['partition'], 'healthy' if p['healthy'] else 'affected',
                             *[f'{o[k]:,}' for k in ('offered', 'accepted', 'refused', 'acked')],
                             p['outage_ack_windows'].count(0), f"{p['outage_accepted_eventually_acked']:,} / {o['accepted']:,}"])
        page += table(['Producer', 'Partition', 'Destination', 'Offered', 'Accepted', 'Refused', 'ACKs during outage', 'No ACK / 30 windows', 'Outage accepts eventually ACKed'], rows)
        rows = [[NAMES[name], reason['reason'], reason['count'], reason['healthy_count']]
                for name, r in arms.items() for reason in r['outage_refusal_reasons']]
        page += table(['Producer', 'Outage refusal reason', 'All destinations', 'Healthy destinations'], rows) + '</details>'
    page += '<p><a href="admission-isolation.json">Exact measurements and evidence hashes</a> · <a href="index.html">All paired explorer pages</a></p></main></html>'
    require(len(page.encode()) <= 2 * 1024 * 1024, "overview page bound")
    return page


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--viewer', type=Path, required=True, help='existing classic_visualization output')
    args = parser.parse_args()
    groups = {}
    for path in sorted(args.directory.glob(f'{SCENARIO}--*--original--full--*.json')):
        _, variant, adapter, _, _, seed = path.stem.split('--')
        shape, raw_rate, policy = variant.split('-')
        require(shape == 'independent' and raw_rate.startswith('rate') and policy in ('shared', 'pressure'), "isolation variant")
        rate = int(raw_rate[4:])
        require(rate in (1000, 4000, 16000) and seed.isdecimal() and str(int(seed)) == seed and int(seed) < 2**64, "rate/seed")
        report = load_report(path)
        result = measure(report)
        require(sum(s['shape']['OpenLoop']['rate_per_s'] for s in report['manifest']['experiment']['loads']) == rate, "rate label")
        result['source'] = {'file': path.name, 'sha256': digest(path)}
        del report
        groups.setdefault((rate, seed), {})[f'{adapter}-{policy}'] = result
        print('Measured', path.name, flush=True)
    require(0 < len(groups) <= 48, "bounded nonempty trial set")
    links = {}
    for path in args.viewer.glob(f'{SCENARIO}--full--*.json'):
        bundle = json.loads(path.read_text())
        for i, pair in enumerate(bundle['pairs']):
            _, raw_rate, policy = pair['variant'].split('-')
            if pair['profile'] == 'original':
                links[(int(raw_rate[4:]), pair['seed'], policy)] = f'{path.stem}.html#pair={i}'
    trials = [{'rate': rate, 'seed': seed, 'arms': combine(runs),
               'java_control_source': runs['classic-pressure']['source']}
              for (rate, seed), runs in sorted(groups.items())]
    bundle = {'schema': 'kr-classic-admission-isolation/v1', 'scenario': SCENARIO,
              'outage_start_ns': str(START), 'outage_end_ns': str(END), 'bucket_ns': str(WIDTH),
              'plot_start_ns': str(PLOT_START), 'plot_end_ns': str(PLOT_END), 'trials': trials}
    page = render(bundle, links)
    (args.viewer / 'admission-isolation.json').write_text(compact(bundle) + '\n')
    (args.viewer / 'admission-isolation.html').write_text(page)
    print(args.viewer / 'admission-isolation.html')


if __name__ == '__main__':
    main()
