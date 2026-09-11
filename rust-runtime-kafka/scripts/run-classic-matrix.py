#!/usr/bin/env python3
"""Run the full producer matrix in resumable, losslessly archived jobs."""
import argparse
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


def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kafka', type=Path, default=ROOT.parent,
                        help='Kafka checkout with the clients-dst comparison driver (default: this repository)')
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--seed', default='0')
    parser.add_argument('--scenario', default='.*')
    parser.add_argument('--profile', choices=['original', 'common', 'both'], default='both')
    args = parser.parse_args()
    if not re.fullmatch(r'0|[1-9][0-9]*', args.seed) or int(args.seed) >= 2**64:
        raise ValueError('unsigned 64-bit seed required')
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=True)
    subprocess.run(['cargo', 'build', '--release', '-p', 'kr-kafka-sim-ffi'], cwd=ROOT,
                   env=dict(os.environ, RUSTC_WRAPPER=''), check=True)
    library = ROOT / 'target/release' / ('libkr_kafka_sim_ffi.dylib' if sys.platform == 'darwin' else 'libkr_kafka_sim_ffi.so')
    library_hash = digest(library)
    lib = ctypes.CDLL(str(library))
    lib.kr_sim_call.argtypes = [ctypes.c_char_p]
    lib.kr_sim_call.restype = ctypes.c_char_p
    response = json.loads(lib.kr_sim_call(b'{"op":"catalogue"}'))
    catalogue = [e for e in response['ok'] if re.fullmatch(args.scenario, e['scenario'])]
    if not catalogue:
        raise ValueError('empty catalogue selection')
    profiles = ['original', 'common'] if args.profile == 'both' else [args.profile]
    scenarios = sorted({e['scenario'] for e in catalogue})
    state_path = args.out / 'matrix.json'
    identity = {'library_sha256': library_hash, 'kafka': str(args.kafka.resolve()),
                'kafka_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=args.kafka).decode().strip(),
                'kafka_diff_sha256': hashlib.sha256(subprocess.check_output(['git', 'diff', 'HEAD', '--binary'], cwd=args.kafka)).hexdigest(),
                'catalogue': catalogue, 'seed': args.seed, 'profiles': profiles}
    state = json.loads(state_path.read_text()) if state_path.exists() else {
        'schema': 'kr-classic-matrix/v1', 'identity': identity, 'expected_runs': len(catalogue) * len(profiles) * 2, 'jobs': {}}
    if state['identity'] != identity:
        raise ValueError('resume inputs changed; use a fresh output directory')
    write_json(state_path, state)
    total = len(scenarios) * len(profiles) * 2
    for scenario in scenarios:
        for profile in profiles:
            for adapter in ['classic', 'native']:
                key = f'{scenario}/{profile}/{adapter}'
                if state['jobs'].get(key, {}).get('archived'):
                    continue
                if digest(library) != library_hash:
                    raise ValueError('simulation library changed during matrix')
                if shutil.disk_usage(args.out).free < 4 * 1024**3:
                    raise RuntimeError('less than 4 GiB free; archived completed jobs can be resumed')
                directory = args.out / scenario
                directory.mkdir(exist_ok=True)
                log = directory / f'job-{adapter}-{profile}.log'
                command = [sys.executable, '-B', str(ROOT / 'scripts/run-classic-scenarios.py'),
                           '--kafka', str(args.kafka.resolve()), '--out', str(directory),
                           '--size', 'full', '--profile', profile, '--adapter', adapter,
                           '--scenario', re.escape(scenario), '--seed', args.seed, '--skip-build', '--no-viewer']
                start = time.monotonic()
                print(f'RUN {len(state["jobs"])+1}/{total} {key}', flush=True)
                with log.open('w') as output:
                    result = subprocess.run(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)
                summary_path = directory / f'summary-{adapter}-{profile}-full.json'
                summary = json.loads(summary_path.read_text()) if summary_path.exists() else {'passed': 0, 'failures': ['no execution summary; inspect job log']}
                count = archive_job(directory, scenario, adapter, profile, 'full', args.seed)
                state['jobs'][key] = {'returncode': result.returncode, 'passed': summary['passed'],
                                      'failures': summary['failures'], 'archived': True, 'archive_entries': count,
                                      'elapsed_seconds': round(time.monotonic() - start, 3), 'log': str(log)}
                write_json(state_path, state)
                print(f'DONE {len(state["jobs"])}/{total} {key}: passed={summary["passed"]} failures={len(summary["failures"])}', flush=True)
    print(f'Matrix finished: {sum(j["passed"] for j in state["jobs"].values())}/{state["expected_runs"]} successful replayed runs; {state_path}', flush=True)


if __name__ == '__main__':
    main()
