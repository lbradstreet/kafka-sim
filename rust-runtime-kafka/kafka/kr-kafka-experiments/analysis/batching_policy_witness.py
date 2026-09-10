#!/usr/bin/env python3
"""Extract policy-pair percentile witnesses and exact native batch packing."""
import argparse
from collections import Counter
import json
from pathlib import Path

from batching_policy_review import policy_contract
from classic_artifacts import write_json
from classic_audit import quantiles
from classic_comparison import load_report, require
from classic_request_witness import extract
from classic_visualization import digest


def selection(report):
    accepted = {e['id']: e['at_ns'] for e in report['external_history']
                if e['kind'] == 'admission' and e['accepted']}
    ordered = sorted((d['at_ns'] - accepted[d['id']], d['id'])
                     for d in report['deliveries'] if d['success'])
    return {name: {'id': ordered[(len(ordered) * rank + 99) // 100 - 1][1],
                   'latency_ns': ordered[(len(ordered) * rank + 99) // 100 - 1][0]}
            for name, rank in [('p50', 50), ('p90', 90), ('p99', 99), ('max', 100)]} if ordered else {}


def packing(report):
    batches, per_request, planned = {}, Counter(), 0
    for entry in report['environment']['history']['entries']:
        request = entry['event'].get('ClientRequestDispatched')
        if request is None or request['api'] != 0:
            continue
        per_request[len(request['batches'])] += 1
        planned += request['wire_bytes']
        for batch in request['batches']:
            previous = batches.setdefault(batch['batch_id'], batch)
            require(previous == batch, 'immutable cohort packing changed across retry')
    return {'produce_dispatches': sum(per_request.values()),
            'produce_dispatched_bytes': planned,
            'dispatches_by_batch_count': dict(per_request),
            'distinct_batch_cohorts': len(batches),
            'cohort_record_counts': quantiles(b['records'] for b in batches.values()),
            'cohorts_by_record_count': dict(Counter(b['records'] for b in batches.values())),
            'cohort_raw_bytes': quantiles(b['raw_bytes'] for b in batches.values()),
            'cohort_wire_bytes': quantiles(b['wire_bytes'] for b in batches.values())}


def first_divergence(raw, wire):
    result = {}
    for field in ('external_history', 'environment'):
        a, b = [r[field] if field == 'external_history' else r[field]['history']['entries']
                for r in (raw, wire)]
        index = next((i for i, (x, y) in enumerate(zip(a, b)) if x != y), min(len(a), len(b)))
        result[field] = {'identical_prefix_entries': index,
                         'raw': a[index] if index < len(a) else None,
                         'estimated-wire': b[index] if index < len(b) else None}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw', type=Path, required=True)
    parser.add_argument('--estimated-wire', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    paths = {'raw': args.raw, 'estimated-wire': args.estimated_wire}
    reports = {mode: load_report(path) for mode, path in paths.items()}
    require(policy_contract(reports['raw'], 'raw') == policy_contract(reports['estimated-wire'], 'estimated-wire'),
            'witness inputs differ beyond policy')
    selections = {mode: selection(report) for mode, report in reports.items()}
    ids = {row['id'] for selected in selections.values() for row in selected.values()}
    runs = {}
    for mode, report in reports.items():
        admitted = {d['id'] for d in report['deliveries']}
        runs[mode] = {'source': {'path': str(paths[mode].resolve()), 'sha256': digest(paths[mode])},
                      'selection': selections[mode], 'selected_ids_not_admitted': sorted(ids - admitted),
                      'packing': packing(report), 'requests': extract(report, ids & admitted)}
    output = {'schema': 'kr-batching-policy-witness/v1',
              'selection_rule': 'Nearest-rank successful latency, sorted by (latency_ns, record_id); include both modes\' selected IDs where admitted.',
              'packing_note': 'Distinct batch IDs denote topic/partition token cohorts. Dispatch bytes are planned Produce bytes, including retries, and differ from completed all-API write bytes.',
              'first_divergence': first_divergence(reports['raw'], reports['estimated-wire']),
              'analysis_tools_sha256': {name: digest(Path(__file__).parent / name) for name in
                  ('batching_policy_witness.py', 'batching_policy_review.py', 'classic_request_witness.py',
                   'classic_artifacts.py', 'classic_audit.py', 'classic_comparison.py', 'classic_visualization.py')},
              'runs': runs}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    write_json(args.out, output)
    print(json.dumps({mode: {'selection': r['selection'], 'packing': r['packing']} for mode, r in runs.items()}, indent=2))


if __name__ == '__main__':
    main()
