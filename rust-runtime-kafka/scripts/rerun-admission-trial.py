#!/usr/bin/env python3
"""Run the fixed descriptor-admission trial, retaining fresh replay evidence.

No dependencies beyond Python and the workspace toolchain. Runs are sequential
because complete Full histories can require gigabytes of resident memory.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, default=Path('target/experiments/admission-trial'))
    parser.add_argument('--stage', choices=['all', 'regression', 'trial'], default='all')
    parser.add_argument('--seeds', type=int, default=16)
    args = parser.parse_args()
    if not 1 <= args.seeds <= 16:
        parser.error('--seeds must be 1..16 (the acceptance campaign uses 16)')
    out = (ROOT / args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, RUSTC_WRAPPER='')
    subprocess.run(['cargo', 'build', '--release', '-p', 'kr-kafka-experiments',
                    '--bin', 'kafka-experiments', '--example', 'analyze_availability'],
                   cwd=ROOT, env=env, check=True)
    cli = ROOT / 'target/release/kafka-experiments'
    analyzer = ROOT / 'target/release/examples/analyze_availability'
    failures = []

    def execute(command, log):
        log.parent.mkdir(parents=True, exist_ok=True)
        print('Running', ' '.join(map(str, command)), flush=True)
        with log.open('w') as stream:
            result = subprocess.run(list(map(str, command)), cwd=ROOT, env=env,
                                    stdout=stream, stderr=subprocess.STDOUT)
        if result.returncode:
            failures.append(str(log.relative_to(out)))
            print('FAILED:', log, flush=True)
        return result.returncode == 0

    def measure(relative, scenario, seed, policy=None):
        directory = out / relative
        command = [cli, '--scenario', scenario, '--size', 'full', '--seed', str(seed), '--out', directory]
        if policy:
            command += ['--descriptor-admission', policy]
        if execute(command, directory / 'run.log'):
            execute([analyzer, directory, directory / 'analysis'], directory / 'analysis.log')
        # Export primary-seed pages. Every seed retains its reports and tapes.
        if seed == 0:
            execute([cli, '--out', directory, '--export-html', directory / 'html'], directory / 'export.log')

    if args.stage in ('all', 'regression'):
        for size in ('test', 'full'):
            directory = out / ('regression-' + size)
            execute([cli, '--all', '--size', size, '--out', directory], directory / 'run.log')
    if args.stage in ('all', 'trial'):
        # Characterize skew before the larger seed campaign; do not tune policy
        # thresholds or hide a utilization-gate failure after observing results.
        measure(Path('skew'), 'baseline.partition-admission-skew', 0)
        for seed in range(args.seeds):
            for policy in ('shared', 'partition-pressure'):
                measure(Path('original') / policy / f'seed-{seed}',
                        'hard.crash-restart-open', seed, policy)
            measure(Path('independent') / f'seed-{seed}',
                    'hard.partition-admission-isolation', seed)
    status = {'stage': args.stage, 'seeds': args.seeds, 'failures': failures}
    (out / f'{args.stage}-status.json').write_text(json.dumps(status, indent=2) + '\n')
    return int(bool(failures))


if __name__ == '__main__':
    sys.exit(main())
