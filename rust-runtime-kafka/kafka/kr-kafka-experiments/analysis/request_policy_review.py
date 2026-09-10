#!/usr/bin/env python3
"""Audit request gathering against current Sealed policy and the frozen default."""
import argparse
from collections import Counter
import copy
import csv
import json
from pathlib import Path
import time

from classic_artifacts import write_json
from classic_audit import audit
from classic_comparison import fingerprint, load_report, require
from classic_outcome_review import review_failures
from classic_visualization import digest

MODES = {'sealed': 'Sealed', 'broker-ready': 'BrokerReady'}
TOOL_FILES = ('request_policy_review.py', 'classic_artifacts.py', 'classic_audit.py',
              'classic_comparison.py', 'classic_outcome_review.py', 'classic_visualization.py')
METRICS = ('offered', 'accepted', 'refused', 'acked', 'failed', 'ack_p99_ns', 'ack_max_ns',
           'produce_requests', 'wire_bytes', 'partition_pending_gap_ns',
           'first_observation_wait_max_ns', 'broker_attempts_max', 'last_terminal_ns')


def policy_contract(header, mode):
    require(header['adapter'] == 'native-panama-sim', 'wrong adapter')
    require(header['replay_verified'], 'missing replay')
    result = {key: copy.deepcopy(header[key]) for key in
              ('manifest', 'original_manifest', 'adjustments', 'classic_config', 'profile', 'compatibility')}
    require(result['manifest']['producer'].pop('request_batching_policy') == MODES[mode], 'wrong policy')
    override = f'request batching policy override: {MODES[mode]}'
    require(result['adjustments'].count(override) == 1, 'missing or duplicate policy override')
    result['adjustments'].remove(override)
    return result


def complete_result(header, explicit):
    result = copy.deepcopy(header)
    for key in ('manifest', 'original_manifest'):
        del result[key]['versions']['source_sha256']
        require(result[key]['producer'].pop('request_batching_policy', 'Sealed') == 'Sealed',
                'previous-default control must use Sealed')
    if explicit:
        result['adjustments'].remove('request batching policy override: Sealed')
    result['artifacts'] = {key: value['sha256'] for key, value in result['artifacts'].items()}
    return result


def metrics(result):
    return {**{key: result['checks'][key] for key in ('offered', 'accepted', 'refused', 'acked', 'failed')},
            'ack_p99_ns': result['ack_latency_ns']['p99'], 'ack_max_ns': result['ack_latency_ns']['max'],
            'produce_requests': result['produce_requests'], 'wire_bytes': result['wire_bytes'],
            'partition_pending_gap_ns': max((p['pending_gap']['duration_ns'] for p in result['partitions']), default=0),
            'first_observation_wait_max_ns': result['first_observation_wait_ns']['max'],
            'broker_attempts_max': result['attempts_at_broker']['max'], 'last_terminal_ns': result['last_terminal_ns']}


def audit_one(path, case, mode, state):
    scenario, variant, profile, seed = case
    header = json.loads(path.read_text())
    contract = policy_contract(header, mode)
    provenance = json.loads((path.parent / f'provenance-native-{profile}-full-{seed}.json').read_text())
    require(provenance['library']['sha256'] == state['identity']['library_sha256'], 'wrong run library')
    require(provenance['kafka']['head'] == state['identity']['kafka_head']
            and not provenance['kafka']['status'], 'Java driver changed')
    require(provenance['arguments']['request_batching_policy'] == mode, 'runner policy provenance differs')
    report = load_report(path)
    require(all(value == 0 for value in report['teardown'].values()), 'incomplete teardown')
    result = audit(report)
    result.update({'case': case, 'mode': mode, 'contract_sha256': fingerprint(contract),
                   'source': {'path': str(path.resolve()), 'sha256': digest(path)},
                   'artifacts_sha256': {key: value['sha256'] for key, value in header['artifacts'].items()},
                   'failure_certainty': review_failures(report)})
    if scenario.startswith('baseline.'):
        require(report['failed'] == 0, 'baseline delivery failures')
        if not any('OpenLoop' in load['shape'] for load in report['manifest']['experiment']['loads']):
            require(report['refused'] == 0, 'finite baseline lost admission')
    return result


def summarize(pairs):
    return {field: dict(Counter(
        'missing_population' if None in (row['sealed'][field], row['broker-ready'][field]) else
        'decreased' if row['broker-ready'][field] < row['sealed'][field] else
        'increased' if row['broker-ready'][field] > row['sealed'][field] else 'unchanged'
        for row in pairs)) for field in METRICS}


def refresh(matrix, out, state, tool_hashes, baseline):
    pairs, errors, rows = [], [], []
    audited_runs = 0
    for case in state['identity']['cases']:
        scenario, variant, profile, seed = case
        name = '--'.join(case)
        runs, headers = {}, {}
        for mode in MODES:
            job = state['jobs'].get('/'.join((scenario, profile, seed, mode)), {})
            if not job.get('complete'):
                continue
            path = matrix / mode / seed / scenario / f'{scenario}--{variant}--native--{profile}--full--{seed}.json'
            cache = out / 'runs' / f'{name}--{mode}.json'
            meta_path = cache.with_suffix('.meta.json')
            signature = {'source_sha256': digest(path), 'tools_sha256': tool_hashes,
                         'library_sha256': state['identity']['library_sha256']}
            try:
                previous = json.loads(meta_path.read_text()) if meta_path.exists() else {}
                if any(previous.get(key) != value for key, value in signature.items()):
                    result = audit_one(path, case, mode, state)
                    write_json(cache, result)
                    write_json(meta_path, signature | {'audit_sha256': digest(cache)})
                    print('AUDITED', name, mode, flush=True)
                else:
                    require(digest(cache) == previous['audit_sha256'], 'cached audit changed')
                    result = json.loads(cache.read_text())
                runs[mode], headers[mode] = result, json.loads(path.read_text())
                audited_runs += 1
                rows.append({'scenario': scenario, 'variant': variant, 'profile': profile, 'seed': seed,
                             'mode': mode, **metrics(result)})
            except Exception as error:
                errors.append({'case': case, 'mode': mode, 'error': repr(error)})
                print('AUDIT ERROR', name, mode, repr(error), flush=True)
        if len(runs) != len(MODES):
            continue
        try:
            require(policy_contract(headers['sealed'], 'sealed') ==
                    policy_contract(headers['broker-ready'], 'broker-ready'),
                    'inputs differ beyond policy and its adjustment')
            pair = {'case': case, 'contract_sha256': runs['sealed']['contract_sha256'],
                    'same_offered_ids': [s['offered'] for s in runs['sealed']['sources']] ==
                                        [s['offered'] for s in runs['broker-ready']['sources']],
                    **{mode: metrics(run) for mode, run in runs.items()},
                    'evidence': {mode: {'source': run['source'], 'audit_sha256': digest(out / 'runs' / f'{name}--{mode}.json'),
                                       'artifacts_sha256': run['artifacts_sha256'],
                                       'fault_phases': run['checks']['phases'],
                                       'coverage_gaps': run['checks']['coverage_gaps'],
                                       'failures': run['failure_certainty'],
                                       'fatals': [e for e in run['native_events'] if e['event'].startswith('Fatal ')]}
                                 for mode, run in runs.items()}}
            if baseline and seed == '0':
                old = baseline / scenario / f'{scenario}--{variant}--native--{profile}--full--0.json'
                require(old.exists(), 'missing previous default control')
                prior = json.loads(old.read_text())
                require(complete_result(prior, False) == complete_result(headers['sealed'], True),
                        'Sealed control changed beyond harness metadata')
                pair['previous_default_control'] = {'source_sha256': digest(old), 'complete_result_unchanged': True}
            pairs.append(pair)
        except Exception as error:
            errors.append({'case': case, 'error': repr(error)})
            print('PAIR ERROR', name, repr(error), flush=True)
    summary = {'schema': 'kr-request-policy-review/v1', 'identity': state['identity'],
               'matrix_sha256': digest(matrix / 'matrix.json'), 'analysis_tools_sha256': tool_hashes,
               'expected_runs': state['expected_runs'], 'audited_runs': audited_runs,
               'expected_pairs': len(state['identity']['cases']), 'completed_pairs': len(pairs),
               'errors': errors, 'changes': summarize(pairs), 'pairs': pairs}
    if rows:
        with (out / 'results.csv').open('w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=list(rows[0]), lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)
        summary['results_sha256'] = digest(out / 'results.csv')
    write_json(out / 'comparison.json', summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('matrix', type=Path)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--previous-default', type=Path)
    parser.add_argument('--watch', action='store_true')
    args = parser.parse_args()
    (args.out / 'runs').mkdir(parents=True, exist_ok=True)
    tool_hashes = {name: digest(Path(__file__).parent / name) for name in TOOL_FILES}
    previous_state = None
    while True:
        state = json.loads((args.matrix / 'matrix.json').read_text())
        if state != previous_state:
            summary = refresh(args.matrix, args.out, state, tool_hashes, args.previous_default)
            print(f'Coverage: {summary["audited_runs"]}/{summary["expected_runs"]} runs; '
                  f'{summary["completed_pairs"]}/{summary["expected_pairs"]} pairs; '
                  f'{len(summary["errors"])} errors', flush=True)
            previous_state = state
        finished = sum(j.get('passed', 0) for j in state['jobs'].values() if j.get('complete')) == state['expected_runs']
        if not args.watch or finished:
            require(not summary['errors'], 'unresolved evidence errors')
            if finished:
                require(summary['completed_pairs'] == summary['expected_pairs'], 'incomplete audit')
            break
        time.sleep(5)


if __name__ == '__main__':
    main()
