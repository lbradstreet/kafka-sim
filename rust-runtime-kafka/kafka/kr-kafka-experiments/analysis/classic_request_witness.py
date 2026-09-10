#!/usr/bin/env python3
"""Extract complete request lifetimes for selected accepted workload record IDs."""
import argparse
import json
from pathlib import Path

from classic_artifacts import write_json
from classic_comparison import load_report, require
from classic_visualization import digest


def extract(report, ids):
    ids = set(ids)
    origin = report['manifest']['start_ns']
    entries = report['environment']['history']['entries']
    tokens, accepted, selected_tokens = {}, {}, {}
    for e in entries:
        if 'Accepted' in e['event']:
            body = e['event']['Accepted']
            tokens[body['token']] = body['record_id']
            if body['record_id'] in ids:
                accepted[body['record_id']] = e['now_ns'] - origin
                selected_tokens[body['record_id']] = body['token']
    require(ids <= accepted.keys(), 'witness requires accepted workload IDs')
    requests, correlations = {}, set()
    first_dispatch, first_observed, dispatch_counts, observed_counts = {}, {}, {}, {}
    for e in entries:
        at = e['now_ns'] - origin
        kind, body = next(iter(e['event'].items()))
        if kind not in ('BrokerRequest', 'ClientRequestDispatched') or body['api'] != 0:
            continue
        field = 'records' if kind == 'BrokerRequest' else 'tokens'
        require(all(t in tokens for t in body[field]), 'unknown persisted admission token')
        selected = ids.intersection(tokens[t] for t in body[field])
        if not selected:
            continue
        correlations.add((body['connection'], body['correlation']))
        if kind == 'ClientRequestDispatched':
            requests[body['request_id']] = selected
            first, counts = first_dispatch, dispatch_counts
        else:
            first, counts = first_observed, observed_counts
        for record_id in selected:
            first[record_id] = min(at, first.get(record_id, at))
            counts[record_id] = counts.get(record_id, 0) + 1
    timeline = []
    for e in entries:
        kind, body = next(iter(e['event'].items()))
        selected = body.get('request_id') in requests
        if kind in ('BrokerRequest', 'BrokerFrameAbandoned'):
            selected |= (body['connection'], body['correlation']) in correlations
        elif kind == 'FaultDecision':
            selected |= (body['hook']['connection'], body['hook']['correlation']) in correlations
        if selected:
            timeline.append({'at_ns': e['now_ns'] - origin, 'ordinal': e['ordinal'], 'event': e['event']})
    deliveries = {d['id']: d for d in report['deliveries']}
    rows = []
    for i in sorted(ids):
        require(first_dispatch.get(i, accepted[i]) >= accepted[i], 'dispatch before acceptance')
        require(first_observed.get(i, accepted[i]) >= accepted[i], 'broker observation before acceptance')
        if i in first_dispatch and i in first_observed:
            require(first_dispatch[i] <= first_observed[i], 'broker observation before first dispatch')
        rows.append({'id': str(i), 'admission_token': str(selected_tokens[i]),
                     'accepted_ns': accepted[i], 'delivery': deliveries[i],
                     'first_dispatch_ns': first_dispatch.get(i), 'dispatches': dispatch_counts.get(i, 0),
                     'first_broker_observation_ns': first_observed.get(i),
                     'broker_observations': observed_counts.get(i, 0),
                     'pre_dispatch_wait_ns': first_dispatch[i] - accepted[i] if i in first_dispatch else None})
    return {'schema': 'kr-classic-request-witness/v1',
            'identity_note': 'Persisted BrokerRequest.records and ClientRequestDispatched.tokens are admission tokens; record IDs are decoded through Accepted. Java has no internal dispatch capture.',
            'records': rows, 'timeline': timeline}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    parser.add_argument('--id', type=int, action='append', required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    result = extract(load_report(args.report), args.id)
    result['source'] = {'path': str(args.report.resolve()), 'sha256': digest(args.report)}
    write_json(args.out, result)
    print(json.dumps(result['records'], indent=2))


if __name__ == '__main__':
    main()
