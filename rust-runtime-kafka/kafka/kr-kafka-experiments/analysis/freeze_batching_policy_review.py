#!/usr/bin/env python3
"""Freeze complete audited policy matrices and a readable per-case appendix."""
import argparse
import json
from pathlib import Path
import shutil

from batching_policy_review import MODES, metrics, policy_contract
from classic_comparison import require
from classic_visualization import digest


def checked(root):
    state = json.loads((root / 'matrix.json').read_text())
    report = json.loads((root / 'analysis/comparison.json').read_text())
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
    lines = ['# Current-code batching policies: every variant', '',
             'Generated from complete replay and payload evidence. Compare current Rust Raw '
             'with current Rust EstimatedWire; both include epoch recovery. '
             'Read [the analysis](BATCHING_POLICY_REVIEW.md) for interpretation.', '',
             'A/R/F means acknowledged / refused / failed. Successful p99 excludes refused and '
             'failed records. Closed-loop offers depend on completion timing; compare populations '
             'beside latency. Requests are broker-observed Produce requests. '
             'Common uses one lane, Shared admission and its recorded fault adjustments.', '']
    for title, report in [('Full catalogue, seed 0', full), ('Selected cases, additional seeds', followups)]:
        lines += [f'## {title}', '',
                  '| Scenario / variant / profile / seed | Raw A/R/F | EstimatedWire A/R/F | p99 ms Raw → EstimatedWire | Produce requests Raw → EstimatedWire |',
                  '| --- | ---: | ---: | ---: | ---: |']
        for pair in report['pairs']:
            raw, wire = pair['raw'], pair['estimated-wire']
            pop = lambda r: '/'.join(f'{r[k]:,}' for k in ('acked', 'refused', 'failed'))
            latency = lambda r: '—' if r['ack_p99_ns'] is None else f'{r["ack_p99_ns"] / 1e6:,.3f}'
            lines.append(f'| {" / ".join(pair["case"])} | {pop(raw)} | {pop(wire)} | '
                         f'{latency(raw)} → {latency(wire)} | {raw["produce_requests"]:,} → {wire["produce_requests"]:,} |')
        lines.append('')
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--full', type=Path, required=True)
    parser.add_argument('--followups', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--witness', type=Path, action='append', default=[])
    args = parser.parse_args()
    full, followups = checked(args.full), checked(args.followups)
    for key in ('library_sha256', 'kafka_head', 'kafka_diff_sha256', 'tools_sha256'):
        require(full['identity'][key] == followups['identity'][key], 'follow-up implementation/driver changed')
    args.out.mkdir(parents=True, exist_ok=True)
    for prefix, root in [('BATCHING_POLICY', args.full), ('BATCHING_POLICY_FOLLOWUP', args.followups)]:
        shutil.copyfile(root / 'analysis/comparison.json', args.out / f'{prefix}_COMPARISON.json')
        shutil.copyfile(root / 'analysis/results.csv', args.out / f'{prefix}_RESULTS.csv')
    (args.out / 'BATCHING_POLICY_VARIANTS.md').write_text(appendix(full, followups))
    inputs = {name: digest(args.out / name) for name in
              ('BATCHING_POLICY_COMPARISON.json', 'BATCHING_POLICY_RESULTS.csv',
               'BATCHING_POLICY_FOLLOWUP_COMPARISON.json', 'BATCHING_POLICY_FOLLOWUP_RESULTS.csv',
               'BATCHING_POLICY_VARIANTS.md')}
    if args.witness:
        witnesses = []
        sources = {tuple(pair['evidence'][mode]['source']['sha256'] for mode in MODES)
                   for report in (full, followups) for pair in report['pairs']}
        for path in args.witness:
            witness = json.loads(path.read_text())
            require(witness['schema'] == 'kr-batching-policy-witness/v1', 'witness schema')
            require(tuple(witness['runs'][mode]['source']['sha256'] for mode in MODES) in sources,
                    'witness does not match a frozen policy pair')
            for name, expected in witness['analysis_tools_sha256'].items():
                require(digest(Path(__file__).parent / name) == expected, 'witness tools changed')
            witnesses.append({'name': path.stem, 'source_sha256': digest(path), 'evidence': witness})
        destination = args.out / 'BATCHING_POLICY_WITNESSES.json'
        destination.write_text(json.dumps({'schema': 'kr-batching-policy-witnesses/v1',
                                          'witnesses': witnesses}, indent=2) + '\n')
        inputs[destination.name] = digest(destination)
    (args.out / 'BATCHING_POLICY_FREEZE.json').write_text(json.dumps({
        'schema': 'kr-batching-policy-freeze/v1', 'artifacts_sha256': inputs,
        'freeze_tool_sha256': digest(Path(__file__)),
        'full_pairs': full['completed_pairs'], 'followup_pairs': followups['completed_pairs'],
        'replayed_runs': full['audited_runs'] + followups['audited_runs']}, indent=2) + '\n')
    print(f'Frozen {full["completed_pairs"]} full and {followups["completed_pairs"]} follow-up policy pairs')


if __name__ == '__main__':
    main()
