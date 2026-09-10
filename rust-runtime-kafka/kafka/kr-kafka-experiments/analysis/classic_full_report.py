#!/usr/bin/env python3
"""Freeze the complete Full review's coverage and per-variant measurements."""
import argparse
from collections import defaultdict
import csv
import json
import os
import re
from pathlib import Path

from classic_comparison import load_report, require
from classic_outcome_review import review_failures
from classic_review_pipeline import code_fingerprint
from classic_visualization import digest


def freeze(matrix, out, prefix="CLASSIC_FULL"):
    require(re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", prefix) is not None, "report prefix")
    analysis = matrix / 'analysis'
    state = json.loads((matrix / 'matrix.json').read_text())
    inventory = json.loads((analysis / 'inventory.json').read_text())
    identity = state['identity']
    scenarios = {c['scenario'] for c in identity['catalogue']}
    expected = {(c['scenario'], c['variant'], p, 'full', identity['seed'])
                for c in identity['catalogue'] for p in identity['profiles']}
    require(len(state['jobs']) == len(scenarios) * len(identity['profiles']) * 2, 'unfinished matrix')
    require(not inventory['errors'], 'unresolved audit errors')
    require(all(j['archived'] and j['returncode'] == 0 and not j['failures'] for j in state['jobs'].values()),
            'execution failures require explicit review before freezing successful coverage')
    require(sum(j['passed'] for j in state['jobs'].values()) == len(expected) * 2 == state['expected_runs'],
            'execution count differs from catalogue')
    require({tuple(p['case']) for p in inventory['pairs']} == expected, 'audit coverage differs from catalogue')
    require(len(inventory['pairs']) == len(expected), 'duplicate audited pair')
    revision = code_fingerprint()
    groups, evidence, outcomes, extra = defaultdict(list), [], [], {}
    for item in inventory['pairs']:
        key = '--'.join(item['case'])
        meta = json.loads((analysis / 'cache' / f'{key}.meta.json').read_text())
        require(meta['analysis_sha256'] == revision and not meta.get('error'), 'stale audit')
        runs = {}
        for adapter in ('classic', 'native'):
            path = analysis / 'runs' / f'{key}--{adapter}.json'
            run = json.loads(path.read_text())
            require(run['case'] == item['case'] + [adapter], 'run identity differs')
            require(digest(Path(run['source']['path'])) == run['source']['sha256'] == meta['sources'][adapter],
                    'report changed after audit')
            runs[adapter] = run
            extra[tuple(run['case'])] = {'last_offer_ns': run['last_offer_ns'],
                                        'last_terminal_ns': run['last_terminal_ns'],
                                        'source_offer_gap_max_ns': max(s['max_offer_gap_ns'] for s in run['sources'])}
            if run['checks']['failed']:
                outcomes.append({'case': run['case'], 'source_sha256': run['source']['sha256'],
                                 'failures': review_failures(load_report(Path(run['source']['path'])))})
        groups[item['case'][0]].append((item, runs))
        evidence.append({'case': item['case'], 'fault_exposure_comparable': item['fault_exposure_comparable'],
                         'sources': meta['sources'],
                         'audit_sha256': {a: digest(analysis / 'runs' / f'{key}--{a}.json') for a in runs},
                         'coverage_gaps': {a: r['checks']['coverage_gaps'] for a, r in runs.items()}})
    out.mkdir(parents=True, exist_ok=True)
    with (analysis / 'runs.csv').open() as source:
        rows = list(csv.DictReader(source))
    require(len(rows) == len(expected) * 2, 'CSV population differs')
    for row in rows:
        key = tuple(row[k] for k in ('scenario', 'variant', 'profile', 'size', 'seed', 'adapter'))
        row.update(extra.pop(key))
    require(not extra, 'missing CSV identity')
    with (out / f'{prefix}_RESULTS.csv').open('w', newline='') as destination:
        writer = csv.DictWriter(destination, fieldnames=list(rows[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)
    coverage = {'schema': 'kr-classic-full-review/v1', 'identity': identity,
                'analysis_sha256': revision, 'matrix_sha256': digest(matrix / 'matrix.json'),
                'review_tools_sha256': {name: digest(Path(__file__).parent / name) for name in (
                    'classic_full_report.py', 'classic_outcome_review.py', 'classic_request_witness.py')},
                'families': len(scenarios), 'variants': len(identity['catalogue']),
                'runs': len(expected) * 2, 'pairs': len(expected),
                'results_sha256': digest(out / f'{prefix}_RESULTS.csv'),
                'evidence': evidence}
    (out / f'{prefix}_COVERAGE.json').write_text(json.dumps(coverage, indent=2) + '\n')
    (out / f'{prefix}_OUTCOMES.json').write_text(json.dumps({
        'schema': 'kr-classic-outcome-review/v1', 'runs_with_failures': outcomes}, indent=2) + '\n')
    lines = ['# Full producer comparison: every variant', '',
             'Generated from complete, replay-verified evidence by `analysis/classic_full_report.py`.', '',
             f'{len(scenarios)} families, {len(identity["catalogue"])} variants, {len(expected)} pairs, '
             f'{len(expected) * 2} executions, seed {identity["seed"]}. Each execution ran twice.', '',
             f'Read [the analysis]({prefix}_REVIEW.md) before interpreting these numbers. '
             'Java is the classic KafkaProducer; native is the Panama-driven native simulation actor. '
             'A/R/F means acknowledged / refused / failed. p99 measures accepted-to-consumed latency '
             'for successful records only, in milliseconds. Closed-loop offered populations depend on progress.', '',
             '**Common forces native Shared admission, including pressure-labeled variants.** '
             'It also changes lanes, linger bypass, simulated encoding cost and selected fault semantics. '
             'Use original for the admission-policy comparison. Both profiles retain unequal memory accounting.', '',
             'Links open the existing repository visualizer. CSV retains nanosecond precision, maximum waits, '
             'retry counts, wire bytes, pending-demand gaps and investigation flags. '
             'Coverage JSON pins each source report and audited result by SHA-256.', '']
    def population(r):
        return ' / '.join(f'{r["checks"][k]:,}' for k in ('acked', 'refused', 'failed'))
    def latency(r):
        n = r['ack_latency_ns']['p99']
        return '—' if n is None else f'{n / 1e6:,.3f}'
    for scenario, cases in sorted(groups.items()):
        ordered = sorted(cases, key=lambda x: (x[0]['case'][2] != 'common', x[0]['case'][1], int(x[0]['case'][4])))
        links = {tuple(item['case']): f'{Path(os.path.relpath(analysis / "viewer", out)).as_posix()}/{scenario}--full--{i // 16 + 1}.html#pair={i % 16}'
                 for i, (item, _) in enumerate(ordered)}
        lines += [f'## {scenario}', '', '| Variant · profile | Java A/R/F | Native A/R/F | p99 ms Java / native | Produce requests Java / native |',
                  '| --- | ---: | ---: | ---: | ---: |']
        for item, runs in sorted(cases, key=lambda x: (x[0]['case'][1], x[0]['case'][2] != 'original')):
            _, variant, profile, _, _ = item['case']
            j, n = runs['classic'], runs['native']
            flag = ' †' if not item['fault_exposure_comparable'] else ''
            lines.append(f'| [{variant} · {profile}]({links[tuple(item["case"])]}){flag} | '
                         f'{population(j)} | {population(n)} | {latency(j)} / {latency(n)} | '
                         f'{j["produce_requests"]:,} / {n["produce_requests"]:,} |')
        lines.append('')
    lines += ['† At least one fault rule had no matching opportunity; inspect the coverage gap before comparing fault effects.', '']
    (out / f'{prefix}_VARIANTS.md').write_text('\n'.join(lines))
    print(f'Frozen {len(expected)} checked pairs; {sum(not p["fault_exposure_comparable"] for p in inventory["pairs"])} exposure gaps')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('matrix', type=Path)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--prefix', default='CLASSIC_FULL')
    args = parser.parse_args()
    freeze(args.matrix, args.out, args.prefix)


if __name__ == '__main__':
    main()
