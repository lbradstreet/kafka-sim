#!/usr/bin/env python3
"""Freeze complete audited policy matrices and a readable per-case appendix."""
import argparse
import csv
import json
from pathlib import Path
import shutil

from request_policy_review import MODES, metrics, policy_contract, summarize
from classic_comparison import require
from classic_visualization import digest


def checked(root):
    state = json.loads((root / 'matrix.json').read_text())
    report = json.loads((root / 'analysis/comparison.json').read_text())
    require(report['schema'] == 'kr-request-policy-review/v1', 'wrong request policy schema')
    require(report['identity'] == state['identity'], 'matrix identity changed')
    libraries = [p for p in root.glob('libkr_kafka_sim_ffi.*') if p.suffix in ('.dylib', '.so')]
    require(len(libraries) == 1 and digest(libraries[0]) == state['identity']['library_sha256'],
            'frozen simulation library changed')
    require(report['matrix_sha256'] == digest(root / 'matrix.json'), 'matrix changed after audit')
    require(not report['errors'] and report['completed_pairs'] == report['expected_pairs']
            and report['audited_runs'] == report['expected_runs'] == state['expected_runs'], 'incomplete audit')
    require(all(j['complete'] and j['returncode'] == 0 for j in state['jobs'].values())
            and sum(j['passed'] for j in state['jobs'].values()) == state['expected_runs'], 'incomplete execution/replay')
    require({tuple(p['case']) for p in report['pairs']} == {tuple(c) for c in state['identity']['cases']}
            and len(report['pairs']) == len(state['identity']['cases']), 'pair population mismatch')
    for name, expected in report['analysis_tools_sha256'].items():
        require(digest(Path(__file__).parent / name) == expected, 'audit tools changed')
    require(digest(root / 'analysis/results.csv') == report['results_sha256'], 'CSV changed')
    for pair in report['pairs']:
        contracts = []
        for mode in MODES:
            evidence = pair['evidence'][mode]
            source = Path(evidence['source']['path'])
            require(digest(source) == evidence['source']['sha256'], 'source changed after audit')
            cache = root / 'analysis/runs' / ('--'.join(pair['case']) + f'--{mode}.json')
            require(digest(cache) == evidence['audit_sha256'], 'audited result changed')
            require(metrics(json.loads(cache.read_text())) == pair[mode], 'frozen metrics differ from audit')
            contracts.append(policy_contract(json.loads(source.read_text()), mode))
        require(contracts[0] == contracts[1], 'policy pair inputs differ')
    return report


def appendix(full, followups):
    lines = ['# Request gathering: every variant', '',
             'Generated from complete replay and payload evidence. Compare Rust Sealed '
             'with Rust BrokerReady; both use EstimatedWire batch targets. '
             'Read [the analysis](REQUEST_GATHER_REVIEW.md) for interpretation.', '',
             'A/R/F means acknowledged / refused / failed. Successful p99 excludes refused and '
             'failed records. Closed-loop offers depend on completion timing; compare populations '
             'beside latency. Requests are broker-observed Produce requests. '
             'Common uses one lane, Shared admission and its recorded fault adjustments.', '']
    for title, report in [('Full catalogue, seed 0', full), ('Selected cases, additional seeds', followups)]:
        lines += [f'## {title}', '',
                  '| Scenario / variant / profile / seed | Sealed A/R/F | BrokerReady A/R/F | p99 ms Sealed → BrokerReady | Produce requests Sealed → BrokerReady |',
                  '| --- | ---: | ---: | ---: | ---: |']
        for pair in report['pairs']:
            raw, wire = pair['sealed'], pair['broker-ready']
            pop = lambda r: '/'.join(f'{r[k]:,}' for k in ('acked', 'refused', 'failed'))
            latency = lambda r: '—' if r['ack_p99_ns'] is None else f'{r["ack_p99_ns"] / 1e6:,.3f}'
            lines.append(f'| {" / ".join(pair["case"])} | {pop(raw)} | {pop(wire)} | '
                         f'{latency(raw)} → {latency(wire)} | {raw["produce_requests"]:,} → {wire["produce_requests"]:,} |')
        lines.append('')
    return '\n'.join(lines)


def combine(roots, reports, out):
    """Union disjoint profile matrices; each input was fully checked above."""
    first = reports[0]
    identity = {k: v for k, v in first['identity'].items() if k != 'cases'}
    pairs, rows, fields = [], [], None
    for root, report in zip(roots, reports, strict=True):
        require({k: v for k, v in report['identity'].items() if k != 'cases'} == identity,
                'profile matrices use different implementations/drivers')
        require(report['analysis_tools_sha256'] == first['analysis_tools_sha256'], 'different audit tools')
        pairs.extend(report['pairs'])
        with (root / 'analysis/results.csv').open() as file:
            reader = csv.DictReader(file)
            if fields is None:
                fields = reader.fieldnames
            require(reader.fieldnames == fields, 'different CSV contracts')
            rows.extend(reader)
    pairs.sort(key=lambda p: p['case'])
    require(len({tuple(p['case']) for p in pairs}) == len(pairs), 'overlapping profile matrices')
    destination = out / 'REQUEST_GATHER_RESULTS.csv'
    with destination.open('w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fields, lineterminator='\n')
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: tuple(r[k] for k in ('scenario', 'variant', 'profile', 'seed', 'mode'))))
    full = {'schema': 'kr-request-policy-review-set/v1',
            'identity': identity | {'cases': [p['case'] for p in pairs]},
            'matrices': [{'path': str(root.resolve()), 'matrix_sha256': report['matrix_sha256'],
                          'comparison_sha256': digest(root / 'analysis/comparison.json')}
                         for root, report in zip(roots, reports, strict=True)],
            'analysis_tools_sha256': first['analysis_tools_sha256'],
            'expected_runs': sum(r['expected_runs'] for r in reports),
            'audited_runs': sum(r['audited_runs'] for r in reports),
            'expected_pairs': sum(r['expected_pairs'] for r in reports),
            'completed_pairs': len(pairs), 'errors': [], 'changes': summarize(pairs),
            'results_sha256': digest(destination), 'pairs': pairs}
    (out / 'REQUEST_GATHER_COMPARISON.json').write_text(json.dumps(full, indent=2) + '\n')
    return full


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--full', type=Path, nargs='+', required=True, help='one complete matrix or disjoint profile matrices')
    parser.add_argument('--followups', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--witness', type=Path, action='append', default=[])
    args = parser.parse_args()
    full_reports, followups = [checked(root) for root in args.full], checked(args.followups)
    args.out.mkdir(parents=True, exist_ok=True)
    full = combine(args.full, full_reports, args.out)
    require(all(p.get('previous_default_control', {}).get('complete_result_unchanged')
                for p in full['pairs']), 'missing unchanged default control')
    for key in ('library_sha256', 'kafka_head', 'kafka_diff_sha256', 'tools_sha256'):
        require(full['identity'][key] == followups['identity'][key], 'follow-up implementation/driver changed')
    for prefix, root in [('REQUEST_GATHER_FOLLOWUP', args.followups)]:
        shutil.copyfile(root / 'analysis/comparison.json', args.out / f'{prefix}_COMPARISON.json')
        shutil.copyfile(root / 'analysis/results.csv', args.out / f'{prefix}_RESULTS.csv')
    (args.out / 'REQUEST_GATHER_VARIANTS.md').write_text(appendix(full, followups))
    inputs = {name: digest(args.out / name) for name in
              ('REQUEST_GATHER_COMPARISON.json', 'REQUEST_GATHER_RESULTS.csv',
               'REQUEST_GATHER_FOLLOWUP_COMPARISON.json', 'REQUEST_GATHER_FOLLOWUP_RESULTS.csv',
               'REQUEST_GATHER_VARIANTS.md')}
    java_coverage = json.loads((args.out / 'COMPRESSION_BATCHING_COVERAGE.json').read_text())
    require(digest(args.out / 'COMPRESSION_BATCHING_RESULTS.csv') == java_coverage['results_sha256'],
            'Java reference CSV changed')
    with (args.out / 'COMPRESSION_BATCHING_RESULTS.csv').open() as file:
        reference_cases = {tuple(r[k] for k in ('scenario', 'variant', 'profile', 'seed'))
                           for r in csv.DictReader(file) if r['adapter'] == 'classic'}
    require({tuple(p['case']) for p in full['pairs']} == reference_cases,
            'Full matrix does not cover the complete frozen Java reference population')
    inputs.update({name: digest(args.out / name) for name in
                   ('COMPRESSION_BATCHING_RESULTS.csv', 'COMPRESSION_BATCHING_COVERAGE.json')})
    if args.witness:
        witnesses = []
        sources = {tuple(pair['evidence'][mode]['source']['sha256'] for mode in MODES)
                   for report in (full, followups) for pair in report['pairs']}
        for path in args.witness:
            witness = json.loads(path.read_text())
            require(witness['schema'] == 'kr-request-policy-witness/v1', 'witness schema')
            require(tuple(witness['runs'][mode]['source']['sha256'] for mode in MODES) in sources,
                    'witness does not match a frozen policy pair')
            for name, expected in witness['analysis_tools_sha256'].items():
                require(digest(Path(__file__).parent / name) == expected, 'witness tools changed')
            witnesses.append({'name': path.stem, 'source_sha256': digest(path), 'evidence': witness})
        destination = args.out / 'REQUEST_GATHER_WITNESSES.json'
        destination.write_text(json.dumps({'schema': 'kr-request-policy-witnesses/v1',
                                          'witnesses': witnesses}, indent=2) + '\n')
        inputs[destination.name] = digest(destination)
    (args.out / 'REQUEST_GATHER_FREEZE.json').write_text(json.dumps({
        'schema': 'kr-request-policy-freeze/v1', 'artifacts_sha256': inputs,
        'freeze_tool_sha256': digest(Path(__file__)),
        'full_pairs': full['completed_pairs'], 'followup_pairs': followups['completed_pairs'],
        'replayed_runs': full['audited_runs'] + followups['audited_runs']}, indent=2) + '\n')
    print(f'Frozen {full["completed_pairs"]} full and {followups["completed_pairs"]} follow-up policy pairs')


if __name__ == '__main__':
    main()
