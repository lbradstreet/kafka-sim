#!/usr/bin/env python3
"""Audit completed matrix pairs and refresh their existing visual explorer."""
import argparse
from collections import defaultdict
import csv
import hashlib
import json
from pathlib import Path
import time

from classic_artifacts import write_json
from classic_audit import audit
from classic_comparison import fingerprint
from classic_visualization import build_pair, digest, export_groups


def code_fingerprint():
    parent = Path(__file__).parent
    return fingerprint({name: digest(parent / name) for name in (
        'classic_audit.py', 'classic_comparison.py', 'classic_artifacts.py',
        'classic_visualization.py', 'classic_review_pipeline.py')})


def refresh(out, keys):
    groups, rows, inventory, errors = defaultdict(list), [], [], []
    for name in keys:
        meta_path = out / 'cache' / f'{name}.meta.json'
        if not meta_path.exists():
            continue
        meta = json.loads(meta_path.read_text())
        if meta.get('error'):
            errors.append({'case': name, 'error': meta['error']})
            continue
        pair = json.loads((out / 'cache' / f'{name}.pair.json').read_text())
        scenario, variant, profile, size, seed = name.split('--')
        groups[(scenario, size)].append(pair)
        inventory.append({'case': [scenario, variant, profile, size, seed],
                          'fault_exposure_comparable': pair['fault_exposure_comparable'],
                          'source_sha256': meta['sources']})
        for adapter in ('classic', 'native'):
            r = json.loads((out / 'runs' / f'{name}--{adapter}.json').read_text())
            gap = max(r['partitions'], key=lambda p: p['pending_gap']['duration_ns'], default=None)
            rows.append({'scenario': scenario, 'variant': variant, 'profile': profile, 'size': size, 'seed': seed,
                         'adapter': adapter, **{k: r['checks'][k] for k in ('offered', 'accepted', 'refused', 'acked', 'failed')},
                         'duration_ns': r['duration_ns'], 'ack_p50_ns': r['ack_latency_ns']['p50'],
                         'ack_p99_ns': r['ack_latency_ns']['p99'], 'ack_max_ns': r['ack_latency_ns']['max'],
                         'failed_max_ns': r['failed_latency_ns']['max'],
                         'first_observation_wait_p99_ns': r['first_observation_wait_ns']['p99'],
                         'first_observation_wait_max_ns': r['first_observation_wait_ns']['max'],
                         'broker_attempts_max': r['attempts_at_broker']['max'],
                         'broker_record_observations': r['record_observations_at_broker'],
                         'attempted_records': r['attempted_records'], 'produce_requests': r['produce_requests'],
                         'wire_bytes': r['wire_bytes'], 'request_records_p50': r['request_record_counts']['p50'],
                         'global_pending_gap_ns': r['global_pending_gap']['duration_ns'],
                         'partition_pending_gap_ns': gap['pending_gap']['duration_ns'] if gap else 0,
                         'gap_partition': gap['partition'] if gap else None,
                         'gap_end_kind': gap['pending_gap']['end_kind'] if gap else None,
                         'deadline_overshoot_count': r['deadline_overshoots']['count'],
                         'sender_errors': r['checks']['sender_errors'],
                         'max_source_lag_ns': r['checks']['max_source_lag_ns'],
                         'fault_exposure_comparable': pair['fault_exposure_comparable']})
    if groups:
        export_groups(groups, out / 'viewer')
    if rows:
        path = out / 'runs.csv'
        with path.with_suffix('.tmp').open('w') as file:
            writer = csv.DictWriter(file, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        path.with_suffix('.tmp').replace(path)
    write_json(out / 'inventory.json', {'schema': 'kr-classic-review-inventory/v1', 'pairs': inventory, 'errors': errors})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('matrix', type=Path)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--watch', action='store_true')
    args = parser.parse_args()
    for name in ('cache', 'runs'):
        (args.out / name).mkdir(parents=True, exist_ok=True)
    revision = code_fingerprint()
    while True:
        state = json.loads((args.matrix / 'matrix.json').read_text())
        identity = state['identity']
        keys, changed = [], False
        for entry in identity['catalogue']:
            scenario, variant = entry['scenario'], entry['variant']
            for profile in identity['profiles']:
                name = '--'.join((scenario, variant, profile, 'full', identity['seed']))
                keys.append(name)
                jobs = [state['jobs'].get(f'{scenario}/{profile}/{adapter}') for adapter in ('classic', 'native')]
                if not all(job and job.get('archived') for job in jobs):
                    continue
                paths = {adapter: args.matrix / scenario / f'{scenario}--{variant}--{adapter}--{profile}--full--{identity["seed"]}.json'
                         for adapter in ('classic', 'native')}
                meta_path = args.out / 'cache' / f'{name}.meta.json'
                sources = {k: digest(v) if v.exists() else None for k, v in paths.items()}
                signature = {'analysis_sha256': revision, 'sources': sources}
                if meta_path.exists():
                    previous = json.loads(meta_path.read_text())
                    if all(previous.get(k) == v for k, v in signature.items()):
                        continue
                try:
                    def on_report(adapter, report):
                        result = audit(report)
                        result['case'] = [scenario, variant, profile, 'full', identity['seed'], adapter]
                        result['source'] = {'path': str(paths[adapter].resolve()), 'sha256': sources[adapter]}
                        result['config'] = {'native': report['manifest']['producer'], 'java': report['classic_config'],
                                            'driver': report['manifest']['driver']}
                        write_json(args.out / 'runs' / f'{name}--{adapter}.json', result)
                    _, pair = build_pair(paths, on_report=on_report)
                    write_json(args.out / 'cache' / f'{name}.pair.json', pair)
                    write_json(meta_path, signature)
                    print('AUDITED', name, flush=True)
                except Exception as error:
                    write_json(meta_path, signature | {'error': repr(error)})
                    print('AUDIT ERROR', name, repr(error), flush=True)
                changed = True
        if changed or not (args.out / 'inventory.json').exists():
            refresh(args.out, keys)
        expected_jobs = len({e['scenario'] for e in identity['catalogue']}) * len(identity['profiles']) * 2
        if not args.watch or len(state['jobs']) == expected_jobs:
            break
        time.sleep(5)


if __name__ == '__main__':
    main()
