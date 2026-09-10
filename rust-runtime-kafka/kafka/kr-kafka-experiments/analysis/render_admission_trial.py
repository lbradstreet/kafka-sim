#!/usr/bin/env python3
"""Render the frozen 75% admission trial from complete, replay-checked analyses."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', nargs='?', type=Path, default=ROOT / 'target/experiments/admission-trial')
    parser.add_argument('--seeds', type=int, default=16)
    parser.add_argument('--baseline-inventory', type=Path, required=True,
                        help='fresh availability-runs.csv for the baseline catalogue')
    parser.add_argument('--out', type=Path, help='output directory; defaults to source/rendered')
    args = parser.parse_args()
    if not 1 <= args.seeds <= 16:
        parser.error('--seeds must be 1..16 (the acceptance campaign uses 16)')
    args.source = args.source.resolve()
    output = args.out.resolve() if args.out else args.source / 'rendered'
    output.mkdir(parents=True, exist_ok=True)
    paths = []
    for pattern in ['original/*/seed-*/analysis/*/*.json',
                    'independent/seed-*/analysis/*/*.json', 'skew/analysis/*/*.json']:
        paths.extend(args.source.glob(pattern))
    rows, details, fingerprints, keys = [], {}, set(), set()
    for path in sorted(paths):
        raw = path.read_bytes()
        d = json.loads(raw)
        assert d['schema'] == 'kr-kafka-availability-analysis/v1' and d['checkpoint_verified']
        assert tuple(d['source'][k] for k in ['manifest', 'history', 'driver', 'model']) == (7, 5, 5, 3)
        fingerprints.add(d['source']['source_sha256'])
        sid = d['scenario']['id']
        family = {'hard.crash-restart-open': 'original',
                  'hard.partition-admission-isolation': 'independent',
                  'baseline.partition-admission-skew': 'skew'}[sid]
        shape = d['variant']['name'].split('-rate')[0] if family == 'skew' else family
        rate = d['variant']['deltas']['rate_per_s']
        policy = d['config']['descriptor_admission_policy']
        seed = int(d['seed'])
        key = (family, shape, rate, policy, seed)
        assert key not in keys
        keys.add(key)
        healthy = [p for p in d['partitions'] if p['partition'] in [1, 2, 4, 5]]
        def during(p):
            return next(x for x in p['phases'] if x['name'] == 'band-0-during')
        is_crash = family != 'skew'
        if is_crash:
            assert sorted(p['partition'] for p in healthy) == [1, 2, 4, 5]
            assert all(len(p['outage_100ms_ack_counts']) == 30 for p in healthy)
        refused = sum(during(p)['intended_refusals'] for p in healthy) if is_crash else None
        missing = sum(sum(n == 0 for n in p['outage_100ms_ack_counts']) for p in healthy) if is_crash else None
        steady = [x for p in d['partitions'] for x in p['phases'] if x['name'] == 'steady']
        records = d['summary']['records']
        run_dir = path.parent.parent.parent
        report_path = run_dir / sid / f"{d['variant']['name']}-seed{seed}.json"
        report = json.loads(report_path.read_bytes())
        assert report['meta']['size'] == 'full' and report['meta']['replay_verified']
        assert report['meta']['source'] == d['source']
        assert report['meta']['seed'] == str(seed)
        assert report['summary']['records'] == records
        pools = report['buckets']['global']['credits']
        descriptor = pools['pools'].index('Descriptors')
        row = dict(family=family, shape=shape, rate=rate, policy=policy, seed=seed,
                   **records, healthy_outage_refused=refused, missing_healthy_ack_windows=missing,
                   steady_acked=sum(p['acked'] for p in steady) if steady else None,
                   ack_p99_ns=d['summary']['latency_acked']['p99'],
                   descriptor_capacity=pools['capacity'][descriptor],
                   descriptor_observed_peak=max(pools['held_observed_max'][descriptor]),
                   pressure_decisions=sum(d['descriptor_pressure']['counts'].values()),
                   active_partitions=sum(p['accepted'] > 0 for p in d['partitions']),
                   configured_partitions=len(report['topology']['partitions']),
                   source_sha256=d['source']['source_sha256'],
                   analysis_sha256=hashlib.sha256(raw).hexdigest(),
                   analysis_path=str(path.relative_to(ROOT)))
        rows.append(row)
        details[key] = d
    assert rows and len(fingerprints) == 1, 'missing analyses or mixed source revisions'
    regression = {}
    frozen_inventory = args.baseline_inventory.read_bytes()
    frozen_cases = {(r['scenario'], r['variant'])
                    for r in csv.DictReader(frozen_inventory.decode().splitlines())}
    new_families = {'hard.partition-admission-isolation', 'baseline.partition-admission-skew'}
    frozen_cases = {case for case in frozen_cases if case[0] not in new_families}
    assert len(frozen_cases) == 128
    for size in ['test', 'full']:
        index_path = args.source / f'regression-{size}' / 'index.json'
        if not index_path.exists():
            regression[size] = dict(passed=False, runs=0, original_runs=0)
            continue
        index = json.loads(index_path.read_bytes())
        assert index['schema'] == 'kr-kafka-experiment-index/v1'
        cases = {(r['scenario'], r['variant']['name']) for r in index['runs']}
        original = {case for case in cases if case[0] not in new_families}
        passed = (len(index['runs']) == len(cases) == 146 and original == frozen_cases
                  and all(r['status'] == 'passed' and r['replay_verified']
                          and r['size'] == size and r['seed'] == '0'
                          and r['versions']['source_sha256'] in fingerprints
                          for r in index['runs']))
        regression[size] = dict(passed=passed, runs=len(cases), original_runs=len(original),
                                missing_original=sorted(frozen_cases-original),
                                unexpected_original=sorted(original-frozen_cases),
                                index_sha256=hashlib.sha256(index_path.read_bytes()).hexdigest())
    expected = {(f, f, r, p, s) for f in ['original', 'independent'] for r in [1000, 4000, 16000]
                for p in ['Shared', 'PartitionPressure'] for s in range(args.seeds)}
    expected |= {('skew', shape, r, p, 0) for shape in ['hot', 'skew90', 'sparse1024']
                 for r in [8000, 32000] for p in ['Shared', 'PartitionPressure']}
    assert keys <= expected, 'unexpected trial inventory'
    complete = keys == expected
    rows.sort(key=lambda r: (r['family'], r['shape'], r['rate'],
                             r['policy'] != 'Shared', r['seed']))
    with (output / 'admission-trial-runs.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)
    lookup = {(r['family'], r['shape'], r['rate'], r['policy'], r['seed']): r for r in rows}
    pairs = []
    for shape in ['hot', 'skew90', 'sparse1024']:
        for rate in [8000, 32000]:
            shared = lookup.get(('skew', shape, rate, 'Shared', 0))
            pressure = lookup.get(('skew', shape, rate, 'PartitionPressure', 0))
            if shared and pressure:
                ratio = pressure['steady_acked'] / shared['steady_acked']
                pairs.append((shape, rate, shared['steady_acked'], pressure['steady_acked'], ratio, ratio >= .95))
    pressure_crash = [r for r in rows if r['family'] != 'skew' and r['policy'] == 'PartitionPressure']
    isolation = complete and all(r['healthy_outage_refused'] == 0 and r['missing_healthy_ack_windows'] == 0 for r in pressure_crash)
    recovery = complete and all(r['accepted'] == r['acked'] for r in rows)
    utilization = len(pairs) == 6 and all(p[-1] for p in pairs)
    baseline = all(lookup.get(('original', 'original', r, 'Shared', 0), {}).get('healthy_outage_refused') == n
                   for r, n in [(1000, 792), (4000, 7416), (16000, 33874)])
    gates = dict(complete_inventory=complete,
                 full_catalogue_regression=all(r['passed'] for r in regression.values()),
                 frozen_baseline_reproduced=baseline,
                 healthy_admission_and_progress=isolation, accepted_records_recover=recovery,
                 skew_throughput_within_five_percent=utilization)
    (output / 'admission-trial-gates.json').write_text(json.dumps(dict(
        schema='kr-kafka-admission-trial/v1', source_sha256=next(iter(fingerprints)),
        runs=len(rows), expected_runs=len(expected), seeds=args.seeds, gates=gates,
        frozen_inventory_sha256=hashlib.sha256(frozen_inventory).hexdigest(),
        catalogue=regression,
        missing_runs=sorted(expected-keys)), indent=2) + '\n')
    witness_rows = [dict(family=k[0], shape=k[1], rate=k[2], policy=k[3], seed=k[4],
                         variant=d['variant']['name'], source_sha256=d['source']['source_sha256'],
                         **d['descriptor_pressure'])
                    for k, d in details.items() if k[3] == 'PartitionPressure' and k[4] == 0]
    (output / 'admission-trial-witnesses.json').write_text(json.dumps(witness_rows, indent=2) + '\n')
    text = ['# Descriptor admission trial', '',
            f'**Healthy admission/progress: {"passed" if isolation else "failed or incomplete"}. '
            f'Skew throughput gate: {"passed" if utilization else "failed"}.** '
            'The policy remains opt-in; the 75% threshold was not tuned after measurement.', '',
            f'{len(rows)} of {len(expected)} planned Full comparisons were analyzed from complete histories, '
            f'with exact saved-checkpoint verification. Crash comparisons cover seeds 0–{args.seeds-1}; '
            'skew comparisons use seed 0. Every run uses fresh manifests and decision tapes.', '',
            '## Results', '',
            '| Gate | Result |', '|---|---|']
    text += [f'| {name.replace("_", " ")} | {"passed" if passed else "failed/incomplete"} |' for name, passed in gates.items()]
    text += ['', f'The {len(pressure_crash)} analyzed pressure-policy fault runs contain '
             f'{sum(r["healthy_outage_refused"] for r in pressure_crash):,} healthy outage refusals and '
             f'{sum(r["missing_healthy_ack_windows"] for r in pressure_crash):,} healthy 100 ms windows '
             f'without an acknowledgment, out of {len(pressure_crash) * 4 * 30:,} checked windows. '
             f'Across all paired runs, {sum(r["acked"] for r in rows):,} of '
             f'{sum(r["accepted"] for r in rows):,} accepted records were acknowledged.']
    text += ['', 'Seed-0 outage refusal counts below use intended destinations, including offers that were never admitted.', '',
             '| Source | Offers/s | Shared healthy refusals | Pressure healthy refusals |', '|---|---:|---:|---:|']
    for family in ['original', 'independent']:
        for rate in [1000, 4000, 16000]:
            a = lookup.get((family, family, rate, 'Shared', 0), {})
            b = lookup.get((family, family, rate, 'PartitionPressure', 0), {})
            text.append(f'| {family} | {rate:,} | {a.get("healthy_outage_refused", "missing")} | {b.get("healthy_outage_refused", "missing")} |')
    text += ['', 'Steady acknowledgment counts use the exact 1.0–10.2 second interval, excluding warmup. '
             'The throughput gate requires at least 95% of Shared acknowledgment throughput at the same offered rate. '
             'Descriptor peaks are observed maxima, shown as Shared / Pressure out of capacity 64.', '',
             '| Workload | Offers/s | Shared ACKs | Pressure ACKs | Ratio | Descriptor peaks | Gate |',
             '|---|---:|---:|---:|---:|---:|---|']
    for shape, rate, a, b, ratio, passed in pairs:
        shared = lookup[('skew', shape, rate, 'Shared', 0)]
        pressure = lookup[('skew', shape, rate, 'PartitionPressure', 0)]
        peaks = f'{shared["descriptor_observed_peak"]} / {pressure["descriptor_observed_peak"]}'
        text.append(f'| {shape} | {rate:,} | {a:,} | {b:,} | {ratio:.3%} | {peaks} | {"passed" if passed else "failed"} |')
    decision_example = ''
    witness_run = details.get(('original', 'original', 16000, 'PartitionPressure', 0))
    if witness_run and witness_run['descriptor_pressure']['witnesses']:
        w = witness_run['descriptor_pressure']['witnesses'][0]
        at = f'{w["at"] // 1_000_000_000}.{w["at"] % 1_000_000_000:09d}'
        decision_example = (
            f'For example, the original-source 16,000/s seed-0 run refused record {w["record_id"]} '
            f'for partition {w["partition"]} at {at} seconds. Its decision-time state was '
            f'H={w["total_held"]}, P={w["class_held"]}, C={w["capacity"]}: the next descriptor '
            f'would require {w["class_held"] + 1} ≤ {w["capacity"] - w["total_held"]}. '
            'This stops the large outstanding class from consuming the remaining descriptors.')
    text += ['', '## Contract and limits', '',
             'Admission shares the first floor(3C/4) descriptors freely. Above that point, a candidate with P outstanding '
             'descriptors needs P + 1 ≤ C − H, where H is total descriptor occupancy before the candidate. '
             'Ordinary global, lane, input and event-credit checks still apply. Charges last from acceptance to terminal settlement, '
             'including the time after encoding releases input. Delivery-event credits retain their separate lifetime. '
             'Records already accepted are never evicted to free capacity.', '',
             decision_example, '',
             'Ready explicit partitions and built-in keyed records use immutable topic/partition identities. '
             'Keyed records retain the partition selected from admission-time metadata. Unresolved, unkeyed and custom owner-selected '
             'routes share a conservative unclassified class until settlement. There is no per-partition guarantee inside that class.', '',
             'Idle partitions receive no reservations. Accounting has at most C live classes, independent of configured partition count. '
             'A lone busy partition can leave 25% unused under pressure. Thousands of simultaneously blocked destinations can still fill '
             'a small pool; input-byte isolation, metadata saturation, retry amplification and producer-wide terminal recovery remain deferred.', '',
             'The independent fixture divides total rate among six sources with quotient/remainder allocation and seed-rotated source order. '
             'Broker 1 owns partitions 0 and 3 and is isolated during 10–13 seconds. The sparse skew fixture has 1,024 configured '
             'partitions and six active destinations, 90% of traffic on partition 0. It retains 64 descriptors and one lane. '
             'Large-topology charts use coarser time buckets to stay within the unchanged 5 MiB report bound; acceptance gates use exact history.', '',
             'Simulation models service and encoder work; these results do not measure host CPU or establish a universal fairness bound. '
             'The frozen availability review remains unchanged.', '',
             '## Evidence and reproduction', '',
             f'Source fingerprint: `{next(iter(fingerprints))}`. Manifest/history/driver/model versions: 7/5/5/3.', '',
             '- [All paired run measurements](admission-trial-runs.csv)',
             '- [Machine-readable gates](admission-trial-gates.json)',
             '- [Decision-time pressure witnesses](admission-trial-witnesses.json)',
             '- [Original-source Shared pages](../../target/experiments/admission-trial/original/shared/seed-0/html/index.html)',
             '- [Original-source pressure pages](../../target/experiments/admission-trial/original/partition-pressure/seed-0/html/index.html)',
             '- [Independent-source pages](../../target/experiments/admission-trial/independent/seed-0/html/index.html)',
             '- [Skew and sparse-topology pages](../../target/experiments/admission-trial/skew/html/index.html)', '',
             f'The catalogue indexes contain {regression["test"]["runs"]} Test runs and '
             f'{regression["full"]["runs"]} Full runs, with {regression["test"]["original_runs"]} '
             f'and {regression["full"]["original_runs"]} original variants respectively. '
             'The catalogue gate requires every run to pass with replay verification and the same source fingerprint.', '',
             'From the repository root:', '', '```sh',
             'python3 scripts/rerun-admission-trial.py',
             'python3 kafka/kr-kafka-experiments/analysis/render_admission_trial.py --baseline-inventory target/experiments/availability-analysis/rendered/availability-runs.csv', '```', '',
             'The rerun script also executes the entire Test and Full catalogue, including the original 128 variants. '
             'Logs and indexes remain in `target/experiments/admission-trial/`; experiments execute sequentially to bound resident memory.']
    (output / 'ADMISSION_TRIAL.md').write_text('\n'.join(text) + '\n')
    print(json.dumps(gates, indent=2))
    return int(not all(gates.values()))


if __name__ == '__main__':
    raise SystemExit(main())
