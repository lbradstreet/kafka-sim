#!/usr/bin/env python3
"""Replay current Rust under both batching policies with the frozen Panama driver."""
import argparse
from collections import defaultdict
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'kafka/kr-kafka-experiments/analysis'))
from classic_artifacts import archive_job, write_json
from classic_comparison import require
from classic_visualization import digest

MODES = {'raw': 'Raw', 'estimated-wire': 'EstimatedWire'}
REQUEST_MODES = {'sealed': 'Sealed', 'broker-ready': 'BrokerReady', 'single-partition': 'SinglePartition'}


def select_cases(catalogue, seeds, scenario='.*', variant='.*', explicit=None):
    available = {(e['scenario'], e['variant']) for e in catalogue}
    if explicit is not None:
        cases = [tuple(row) for row in explicit]
    else:
        cases = [(s, v, p, seed) for s, v in sorted(available)
                 if re.fullmatch(scenario, s) and re.fullmatch(variant, v)
                 for p in ('original', 'common') for seed in seeds]
    require(bool(cases) and len(set(cases)) == len(cases), 'empty or duplicate cases')
    for case in cases:
        require(len(case) == 4, 'case requires scenario, variant, profile, seed')
        s, v, p, seed = case
        require((s, v) in available and p in ('original', 'common'), 'unknown case')
        require(isinstance(seed, str) and re.fullmatch(r'0|[1-9][0-9]*', seed)
                and int(seed) < 2**64, 'unsigned 64-bit seed string required')
    return sorted(cases)


def verify_job(directory, scenario, variants, profile, seed, mode, field='batch_target_mode', modes=None):
    modes = modes or MODES
    label = 'batch target mode' if field == 'batch_target_mode' else 'request batching policy'
    summary = json.loads((directory / f'summary-native-{profile}-full.json').read_text())
    require(summary['passed'] == len(variants) and not summary['failures'], 'execution/replay failed')
    for variant in variants:
        path = directory / f'{scenario}--{variant}--native--{profile}--full--{seed}.json'
        header = json.loads(path.read_text())
        require(header['replay_verified'], 'missing complete replay')
        require(header['adapter'] == 'native-panama-sim', 'wrong adapter')
        require(header['manifest']['producer'][field] == modes[mode], 'wrong policy')
        require(f'{label} override: {modes[mode]}' in header['adjustments'], 'missing override provenance')
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kafka', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--seeds', nargs='+', default=['0'])
    parser.add_argument('--scenario', default='.*')
    parser.add_argument('--variant', default='.*')
    parser.add_argument('--cases', type=Path, help='JSON array of [scenario, variant, profile, seed]')
    parser.add_argument('--policy', choices=['batch-target', 'request-batching'], default='batch-target')
    parser.add_argument('--modes', nargs='+', help='policy arms; default Raw/EstimatedWire or Sealed/BrokerReady')
    args = parser.parse_args()
    available_modes = MODES if args.policy == 'batch-target' else REQUEST_MODES
    selected_modes = args.modes or (list(MODES) if args.policy == 'batch-target' else ['sealed', 'broker-ready'])
    require(len(set(selected_modes)) == len(selected_modes) and all(m in available_modes for m in selected_modes), 'invalid policy arms')
    modes = {m: available_modes[m] for m in selected_modes}
    field = 'batch_target_mode' if args.policy == 'batch-target' else 'request_batching_policy'
    argument = '--batch-target-mode' if args.policy == 'batch-target' else '--request-batching-policy'
    out, kafka = args.out.resolve(), args.kafka.resolve()
    out.mkdir(parents=True, exist_ok=True)
    state_path = out / 'matrix.json'
    require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=kafka), 'Java checkout must be clean')
    library = out / ('libkr_kafka_sim_ffi.dylib' if sys.platform == 'darwin' else 'libkr_kafka_sim_ffi.so')
    if not state_path.exists():
        subprocess.run(['cargo', 'build', '--release', '-p', 'kr-kafka-sim-ffi'], cwd=ROOT,
                       env=dict(os.environ, RUSTC_WRAPPER=''), check=True)
        shutil.copyfile(ROOT / 'target/release' / library.name, library)
    library_hash = digest(library)
    lib = ctypes.CDLL(str(library))
    lib.kr_sim_call.argtypes, lib.kr_sim_call.restype = [ctypes.c_char_p], ctypes.c_char_p
    catalogue = json.loads(lib.kr_sim_call(b'{"op":"catalogue"}'))['ok']
    cases = select_cases(catalogue, args.seeds, args.scenario, args.variant,
                         json.loads(args.cases.read_text()) if args.cases else None)
    identity = {'library_sha256': library_hash, 'kafka': str(kafka),
                'kafka_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=kafka).decode().strip(),
                'kafka_diff_sha256': hashlib.sha256(subprocess.check_output(['git', 'diff', 'HEAD', '--binary'], cwd=kafka)).hexdigest(),
                'cases': cases, 'modes': modes,
                'tools_sha256': {name: digest(ROOT / 'scripts' / name) for name in
                                 ('run-batching-policy-matrix.py', 'run-classic-scenarios.py')}}
    # JSON represents tuple case IDs as arrays.
    identity = json.loads(json.dumps(identity))
    if args.policy == 'request-batching':
        identity['policy_field'] = field
    state = json.loads(state_path.read_text()) if state_path.exists() else {
        'schema': 'kr-batching-policy-matrix/v1', 'identity': identity,
        'expected_runs': len(cases) * len(modes), 'jobs': {}}
    require(state['identity'] == identity, 'resume inputs changed; use a fresh output directory')
    write_json(state_path, state)
    grouped = defaultdict(list)
    for scenario, variant, profile, seed in cases:
        grouped[scenario, profile, seed].append(variant)
    for (scenario, profile, seed), variants in sorted(grouped.items()):
        for mode in modes:
            key = '/'.join((scenario, profile, seed, mode))
            if state['jobs'].get(key, {}).get('complete'):
                continue
            require(digest(library) == library_hash, 'simulation library changed')
            require(shutil.disk_usage(out).free >= 4 * 1024**3, 'less than 4 GiB free; resume after archiving evidence')
            directory = out / mode / seed / scenario
            directory.mkdir(parents=True, exist_ok=True)
            log = directory / f'job-native-{profile}.log'
            command = [sys.executable, '-B', str(ROOT / 'scripts/run-classic-scenarios.py'),
                       '--kafka', str(kafka), '--out', str(directory), '--size', 'full',
                       '--profile', profile, '--adapter', 'native', '--scenario', re.escape(scenario),
                       '--variant', '|'.join(re.escape(v) for v in variants), '--seed', seed,
                       argument, mode, '--skip-build', '--library', str(library), '--no-viewer']
            start = time.monotonic()
            print(f'RUN {key} ({len(variants)} variants)', flush=True)
            with log.open('w') as output:
                result = subprocess.run(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)
            archive_entries = archive_job(directory, scenario, 'native', profile, 'full', seed)
            job = {'returncode': result.returncode, 'archive_entries': archive_entries,
                   'directory': str(directory), 'log': str(log), 'variants': variants,
                   'elapsed_seconds': round(time.monotonic() - start, 3), 'complete': False}
            try:
                require(result.returncode == 0, 'runner failed; inspect job log')
                job['passed'] = verify_job(directory, scenario, variants, profile, seed, mode, field, modes)['passed']
                job['complete'] = True
            except Exception as error:
                job['error'] = repr(error)
            state['jobs'][key] = job
            write_json(state_path, state)
            print(f'DONE {key}: {job.get("passed", 0)} passed; complete={job["complete"]}', flush=True)
            require(job['complete'], f'job failed: {key}; evidence retained')
    passed = sum(j['passed'] for j in state['jobs'].values())
    require(passed == state['expected_runs'], 'incomplete matrix')
    print(f'Matrix finished: {passed}/{state["expected_runs"]} successful replayed runs', flush=True)


if __name__ == '__main__':
    main()
