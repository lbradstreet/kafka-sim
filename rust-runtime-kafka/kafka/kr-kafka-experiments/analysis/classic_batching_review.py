#!/usr/bin/env python3
"""Compare the audited batching rerun with the frozen Full review."""
import argparse
from collections import Counter
import copy
import csv
import json
from pathlib import Path

from classic_comparison import fingerprint, load_report, require
from classic_visualization import digest


def input_contract(manifest, mode):
    result = copy.deepcopy(manifest)
    del result['versions']['source_sha256']
    require(result['producer'].pop('batch_target_mode', 'Raw') == mode,
            'unexpected batch target policy')
    return result


def java_result(header, mode):
    result = copy.deepcopy(header)
    for field in ('manifest', 'original_manifest'):
        result[field] = input_contract(result[field], mode)
    # These are hashes of complete decoded artifacts, already independently
    # checked by each review's audit. Paths and gzip representation may differ.
    result['artifacts'] = {name: value['sha256'] for name, value in result['artifacts'].items()}
    return result


def packing_summary(path):
    report = load_report(path)
    require(len(report['manifest']['topics']) == 1 and report['acked'] == 4096,
            'compression fixture population/topology changed')
    partitions = {row['id']: row['partition'] for row in report['deliveries']}
    requests = [row['event']['BrokerRequest'] for row in report['environment']['history']['entries']
                if row['event'].get('BrokerRequest', {}).get('api') == 0]
    counts = Counter(len({partitions[i] for i in request['records']}) for request in requests)
    return {'produce_requests': len(requests), 'requests_by_distinct_partition_count': dict(counts),
            'partition_appearances': sum(k * v for k, v in counts.items())}


def recovery_control(path, previous):
    report = load_report(path)
    before = previous['runs']['native_after']
    ids = set(before['probes']['ids'])
    probes = [row for row in report['deliveries'] if row['id'] in ids]
    admitted = {row['id'] for row in report['external_history']
                if row['kind'] == 'admission' and row['accepted']}
    require(ids <= admitted and len(probes) == len(ids) and all(row['success'] for row in probes),
            'a post-recovery probe failed or disappeared')
    require(not any(row['kind'] == 'native-event' and row['event'].startswith('Fatal ')
                    for row in report['external_history']), 'recovery became fatal')
    require(report['failed'] == before['records']['failed'], 'recovery failure count changed')
    return {'baseline_header_sha256': before['header_sha256'],
            'baseline_source_sha256': before['source_sha256'],
            'records_before': before['records'],
            'records_after': {k: report[k] for k in ('offered', 'accepted', 'refused', 'acked', 'failed')},
            'probes_before': before['probes'],
            'probes_after': {'ids': sorted(ids), 'accepted': len(ids), 'acked': len(probes), 'failed': 0,
                             'first_terminal_ns': min((row['at_ns'] for row in probes), default=None),
                             'last_terminal_ns': max((row['at_ns'] for row in probes), default=None)}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--before', type=Path, required=True)
    parser.add_argument('--after', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    states = {label: json.loads((root / 'matrix.json').read_text())
              for label, root in [('before', args.before), ('after', args.after)]}
    for field in ('kafka_head', 'kafka_diff_sha256', 'catalogue', 'seed', 'profiles'):
        require(states['before']['identity'][field] == states['after']['identity'][field],
                f'matrix inputs changed: {field}')
    tables = {}
    for label, root in [('before', args.before), ('after', args.after)]:
        state = states[label]
        require(sum(j['passed'] for j in state['jobs'].values()) == state['expected_runs'],
                'incomplete execution coverage')
        require(all(j['returncode'] == 0 and j['archived'] and not j['failures']
                    for j in state['jobs'].values()), 'failed matrix job')
        inventory = json.loads((root / 'analysis/inventory.json').read_text())
        require(not inventory['errors'], 'unresolved audit error')
        require(len(inventory['pairs']) * 2 == state['expected_runs'], 'incomplete audit coverage')
        rows = list(csv.DictReader((root / 'analysis/runs.csv').open()))
        tables[label] = {tuple(r[k] for k in ('scenario', 'variant', 'profile', 'size', 'seed', 'adapter')): r
                         for r in rows}
        require(len(tables[label]) == len(rows) == state['expected_runs'], 'CSV identity/population')
    require(tables['before'].keys() == tables['after'].keys(), 'comparison population changed')
    recovery_path = Path(__file__).parents[1] / 'EPOCH_RECOVERY_RESULTS.json'
    recovery = {tuple(row['case']): row for row in json.loads(recovery_path.read_text())['pairs']}
    results, packing = [], []
    for key in sorted(tables['after']):
        if key[-1] != 'native':
            continue
        scenario, variant, profile, size, seed, _ = key
        name = '--'.join((scenario, variant, profile, size, seed))
        headers, sources = {}, {}
        for label, root in [('before', args.before), ('after', args.after)]:
            meta = json.loads((root / 'analysis/cache' / f'{name}.meta.json').read_text())
            for adapter in ('classic', 'native'):
                path = root / scenario / f'{scenario}--{variant}--{adapter}--{profile}--{size}--{seed}.json'
                source_hash = digest(path)
                require(meta['sources'][adapter] == source_hash, 'source changed after audit')
                headers[label, adapter] = json.loads(path.read_text())
                sources[f'{label}_{adapter}'] = source_hash
        contract = input_contract(headers['before', 'classic']['manifest'], 'Raw')
        for (label, _), header in headers.items():
            mode = 'Raw' if label == 'before' else 'EstimatedWire'
            require(input_contract(header['manifest'], mode) == contract, 'scenario inputs changed')
            require(header['classic_config'] == headers['before', 'classic']['classic_config'],
                    'Java configuration changed')
            require(header['replay_verified'], 'missing replay verification')
        require(java_result(headers['before', 'classic'], 'Raw') ==
                java_result(headers['after', 'classic'], 'EstimatedWire'),
                f'complete Java result changed: {name}')
        old, new = tables['before'][key], tables['after'][key]
        metrics = ('offered', 'accepted', 'refused', 'acked', 'failed', 'produce_requests',
                   'wire_bytes', 'ack_p99_ns', 'ack_max_ns', 'partition_pending_gap_ns')
        results.append({'case': list(key[:-1]), 'sources': sources,
                        'input_contract_sha256': fingerprint(contract),
                        'complete_java_result_unchanged': True,
                        'native': {field: {'before': int(old[field]) if old[field] else None,
                                           'after': int(new[field]) if new[field] else None}
                                   for field in metrics}})
        if key[:-1] in recovery:
            previous = recovery[key[:-1]]
            require(fingerprint(contract) == previous['input_contract_sha256'],
                    'post-epoch control inputs changed')
            results[-1]['post_epoch_control'] = recovery_control(args.after / scenario /
                f'{scenario}--{variant}--native--{profile}--{size}--{seed}.json', previous)
        if scenario == 'baseline.compression':
            packing.append({'case': list(key[:-1]), 'runs': {
                adapter: packing_summary(args.after / scenario /
                    f'{scenario}--{variant}--{adapter}--{profile}--{size}--{seed}.json')
                for adapter in ('classic', 'native')}})
    if states['after']['identity']['seed'] == '0':
        require({tuple(row['case']) for row in results if 'post_epoch_control' in row} == set(recovery),
                'incomplete post-epoch control coverage')
    output = {'schema': 'kr-classic-batching-change/v1',
              'comparison_tools_sha256': {name: digest(Path(__file__).parent / name) for name in
                  ('classic_batching_review.py', 'classic_comparison.py', 'classic_artifacts.py',
                   'classic_visualization.py')},
              'matrix_sha256': {label: digest(root / 'matrix.json') for label, root in
                                [('before', args.before), ('after', args.after)]},
              'library_sha256': {label: state['identity']['library_sha256'] for label, state in states.items()},
              'comparison_exclusions': ['manifest.versions.source_sha256',
                                       'manifest.producer.batch_target_mode',
                                       'same fields in original_manifest',
                                       'artifact paths/encoding metadata; decoded hashes must match'],
              'historical_baseline_limit': 'The baseline predates epoch recovery as well as batching; failure changes do not isolate batching.',
              'post_epoch_control_sha256': digest(recovery_path),
              'supporting_measurements_sha256': {
                  name: digest(Path(__file__).parents[1] / name) for name in
                  ('COMPRESSION_NATIVE_CONTROL.jsonl', 'COMPRESSION_MEMORY_RESULTS.csv')},
              'pairs': results, 'compression_request_packing': packing}
    args.out.write_text(json.dumps(output, indent=2) + '\n')
    print(f'Compared {len(results)} pairs; every complete Java result is unchanged')


if __name__ == '__main__':
    main()
