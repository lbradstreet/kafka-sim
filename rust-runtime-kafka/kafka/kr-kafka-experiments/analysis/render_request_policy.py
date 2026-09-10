#!/usr/bin/env python3
"""Render frozen request-policy metrics as a standalone, file:// dashboard."""
import argparse
import csv
import json
from pathlib import Path

from classic_comparison import require
from classic_visualization import ASSETS, compact, describe_setup, digest, environment
from request_policy_review import METRICS

ASSET_NAMES = ('trace-viewer.css', 'request-policy.css', 'trace-viewer-core.js',
               'producer-experiment-model.js', 'request-policy-model.js', 'request-policy-viewer.js')


def decimals(metrics):
    require(set(metrics) == set(METRICS), 'metric contract')
    require(all(v is None or type(v) is int and 0 <= v < 2**64 for v in metrics.values()), 'metric range')
    return {k: None if v is None else str(v) for k, v in metrics.items()}


def bundle(reports, java_rows, provenance):
    java = {tuple(row[k] for k in ('scenario', 'variant', 'profile', 'seed')): row
            for row in java_rows if row['adapter'] == 'classic'}
    previous_native = {tuple(row[k] for k in ('scenario', 'variant', 'profile', 'seed')): row
                       for row in java_rows if row['adapter'] == 'native'}
    cases = []
    for report in reports:
        for pair in report['pairs']:
            case = tuple(pair['case'])
            source = Path(pair['evidence']['sealed']['source']['path'])
            require(digest(source) == pair['evidence']['sealed']['source']['sha256'], 'setup source changed')
            h = json.loads(source.read_text())
            setup = describe_setup(h['manifest'], h['original_manifest'], h['classic_config'], h['adjustments'])
            events = environment(h['manifest'])
            description = setup['loads'] + setup['settings'] + [
                a for a in setup['adjustments'] if a != 'request batching policy override: Sealed']
            p, e = h['manifest']['producer'], h['manifest']['experiment']
            description += [f"Lanes per broker: {p['lanes']}; request target: {p['request_target_bytes']} bytes; request partition limit: {p['request_max_partitions']}.",
                            f"Native capacity: {p['record_descriptors']} record descriptors ({p['descriptor_admission_policy']} admission), {p['max_batches']} batches, {p['input_bytes']} input bytes, {p['compressed_bytes']} compressed-output bytes, {p['connection_wire_window_bytes']} wire bytes per connection; {p['codec_contexts']} codec contexts with {p['codec_workspace_bytes']} workspace bytes.",
                            f"Native encoding: {p['sim_encode_bytes_per_poll']} bytes per poll, modeled encode quantum {h['manifest']['driver']['encode_cost_ns']} ns. Java configuration: {compact(h['classic_config'])}.",
                            f"Offer deadline: {e['offer_deadline_ns']} ns; settle timeout: {e['settle_timeout_ns']} ns; close timeout: {e['close_timeout_ns']} ns. Scheduled gaps between source windows are idle time, not delivery stalls."]
            description += [f"At {b['start']}..{b['end']} ns: {b['label']}" for b in events['bands']]
            description += [f"At {m['at']} ns: {m['label']}" for m in events['markers']]
            runs = {mode: decimals(pair[mode]) for mode in ('sealed', 'broker-ready')}
            runs['java'] = None
            if case[3] == '0':
                require(case in java and case in previous_native, 'missing Java/native reference')
                values = lambda row: {k: int(row[k]) if row[k] else None for k in METRICS}
                require(pair['sealed'] == values(previous_native[case]), 'reference default metrics differ')
                runs['java'] = decimals(values(java[case]))
            cases.append({'case': pair['case'], 'same_offered_ids': pair['same_offered_ids'],
                          'description': description, 'runs': runs})
    return {'schema': 'kr-request-policy-dashboard/v1', 'provenance': provenance, 'cases': cases}


def render(data):
    serialized = compact(data).replace('<', '\\u003c').replace('\u2028', '\\u2028').replace('\u2029', '\\u2029')
    require(len(serialized.encode()) <= 8 * 1024 * 1024, 'dashboard size bound')
    page = (ASSETS / 'request-policy.html').read_text()
    for name in ASSET_NAMES:
        source = (ASSETS / name).read_text()
        if name.endswith('.css'):
            page = page.replace(f'<link rel="stylesheet" href="./{name}">', f'<style>\n{source}\n</style>')
        else:
            require('</script' not in source.lower(), 'script terminator in asset')
            page = page.replace(f'<script src="./{name}"></script>', f'<script>\n{source}\n</script>')
    return page.replace('<script src="./request-policy-data.js"></script>',
                        f'<script>globalThis.REQUEST_POLICY_DATA = {serialized};</script>')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('evidence', type=Path)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    freeze = json.loads((args.evidence / 'REQUEST_GATHER_FREEZE.json').read_text())
    require(freeze['schema'] == 'kr-request-policy-freeze/v1', 'freeze schema')
    for name, expected in freeze['artifacts_sha256'].items():
        require(Path(name).name == name and digest(args.evidence / name) == expected, 'frozen artifact changed')
    names = ('REQUEST_GATHER_COMPARISON.json', 'REQUEST_GATHER_FOLLOWUP_COMPARISON.json',
             'COMPRESSION_BATCHING_RESULTS.csv')
    provenance = [{'name': name, 'sha256': digest(args.evidence / name)} for name in names]
    reports = [json.loads((args.evidence / name).read_text()) for name in names[:2]]
    with (args.evidence / names[2]).open() as file:
        java = list(csv.DictReader(file))
    data = bundle(reports, java, provenance)
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / 'index.html').write_text(render(data))
    (args.out / 'data.json').write_text(json.dumps(data, indent=2) + '\n')
    (args.out / 'provenance.json').write_text(json.dumps({
        'schema': 'kr-request-policy-dashboard-export/v1', 'input_freeze_sha256': digest(args.evidence / 'REQUEST_GATHER_FREEZE.json'),
        'exporter_sha256': digest(Path(__file__)),
        'assets_sha256': {n: digest(ASSETS / n) for n in (*ASSET_NAMES, 'request-policy.html')},
        'output_sha256': {n: digest(args.out / n) for n in ('index.html', 'data.json')}}, indent=2) + '\n')
    print(f"Rendered {len(data['cases'])} policy pairs to {args.out / 'index.html'}")


if __name__ == '__main__':
    main()
