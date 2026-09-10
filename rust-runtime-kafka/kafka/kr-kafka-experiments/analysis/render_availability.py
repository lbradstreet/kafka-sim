#!/usr/bin/env python3
"""Render fresh replay-verified availability audits without historical findings.

Arguments: analysis-directory [output-directory]. The input directory must
contain scenario/variant.json audits from one source snapshot and seed.
"""
import csv
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
SOURCE = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / 'target/experiments/availability-analysis'
OUT = Path(sys.argv[2]) if len(sys.argv) > 2 else SOURCE / 'rendered'

INTRO = """# Producer availability measurements

Generated from replay-verified input. These tables report observations, not a
proof of availability bounds. Refused offers are outside accepted-record
latency; source silence is distinct from a pending-record progress gap.
Times in table columns are milliseconds; interval endpoints are seconds.
A / R / N / U means acknowledged / refused / NotWritten / Unknown.

"""

TAIL = """
## Evidence

The adjacent CSV files contain run, partition and phase measurements.
`availability-witnesses.json` contains selected correlated request witnesses.
Generate and review conclusions from this run; no prior-run findings are reused.
"""


def write_csv(name, rows):
    with (OUT / name).open('w', newline='') as f:
        if not rows:
            return
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def ms(n):
    return '—' if n is None else f'{n / 1_000_000:,.3f}'


def max_gap(p):
    return p['pending_no_ack_spans'][0] if p['pending_no_ack_spans'] else dict(start=0, end=0, duration=0, ended_by='no-demand')


def main():
    paths = sorted(SOURCE.glob('*/*.json'))
    assert paths, 'no replay audits found'
    identities = set()
    runs, partition_rows, phase_rows, groups, selected = [], [], [], {}, []
    special = {'soft.degrading-broker-ramp/request200ms', 'soft.slow-broker-window/lanes1-i5', 'soft.throttle-window/i1', 'soft.one-way-loss-responses/outage3000ms-i5', 'resources.stop-polling-backpressure/events1024'}
    for path in paths:
        data = path.read_bytes()
        d = json.loads(data)
        assert d['schema'] == 'kr-kafka-availability-analysis/v1' and d['checkpoint_verified']
        identities.add((d['source']['source_sha256'], d['seed']))
        assert len(identities) == 1, 'input mixes source snapshots or seeds'
        sid, variant = d['scenario']['id'], d['variant']['name']
        key = f'{sid}/{variant}'
        groups.setdefault(sid, []).append(d)
        p = max(d['partitions'], key=lambda p: max_gap(p)['duration'])
        g = max_gap(p)
        s = d['summary']
        runs.append(dict(scenario=sid, variant=variant, **s['records'], duration_ns=d['duration'], closed_at_ns=d['closed_at'], runtime_after_closed_ns=d['runtime_after_closed'], ack_p99_ns=s['latency_acked']['p99'], all_max_latency_ns=s['latency_all_deliveries']['max'], max_pending_no_ack_ns=g['duration'], gap_topic=p['topic'], gap_partition=p['partition'], gap_start_ns=g['start'], gap_end_ns=g['end'], gap_ended_by=g['ended_by'], first_source_max_offer_gap_ns=max((x['duration'] for x in d['loads'][0]['offer_gaps']), default=0), max_open_loop_offer_lateness_ns=d['max_open_loop_offer_lateness'], analysis_sha256=hashlib.sha256(data).hexdigest()))
        for p in d['partitions']:
            g = max_gap(p)
            partition_rows.append(dict(scenario=sid, variant=variant, topic=p['topic'], partition=p['partition'], accepted=p['accepted'], acked=p['acked'], not_written=p['not_written'], unknown=p['unknown'], busy_ns=p['busy_ns'], ack_p50_ns=p['latency_acked']['p50'], ack_p99_ns=p['latency_acked']['p99'], all_max_latency_ns=p['latency_all']['max'], max_pending_no_ack_ns=g['duration'], gap_start_ns=g['start'], gap_end_ns=g['end'], gap_ended_by=g['ended_by'], max_raw_ack_gap_ns=max((x['duration'] for x in p['raw_ack_gaps']), default=0), max_first_dispatch_wait_ns=p['accept_to_first_dispatch']['max'], max_success_to_consumption_ns=p['success_to_consumption']['max']))
            for phase in p['phases']:
                q = phase['longest_pending_no_ack'] or {}
                phase_rows.append(dict(scenario=sid, variant=variant, topic=p['topic'], partition=p['partition'], phase=phase['name'], start_ns=phase['start'], end_ns=phase['end'], accepted=phase['accepted'], acked=phase['acked'], delivered=phase['delivered'], intended_offers=phase['intended_offers'], intended_refusals=phase['intended_refusals'], first_dispatches=phase['first_dispatches'], pending_before_start=phase['pending_before_start'], pending_before_end=phase['pending_before_end'], max_pending_no_ack_ns=q.get('duration', 0)))
        if key in special:
            selected.append({k: d[k] for k in ['scenario', 'variant', 'source', 'witnesses', 'witness_requests']})
    assert len({(r['scenario'], r['variant']) for r in runs}) == len(runs), 'duplicate audits'
    OUT.mkdir(parents=True, exist_ok=True)
    write_csv('availability-runs.csv', runs)
    write_csv('availability-partitions.csv', partition_rows)
    write_csv('availability-phases.csv', phase_rows)
    (OUT / 'availability-witnesses.json').write_text(json.dumps(selected, indent=2) + '\n')
    report = [INTRO]
    for number, sid in enumerate(sorted(groups), 1):
        group = groups[sid]
        report.append(f"## {number}. {group[0]['scenario']['title']}\n\n`{sid}` · {len(group)} variants\n\n")
        report.append('| Variant | A / R / N / U | ACK p99 | All max latency | P gap | Largest pending interval (seconds) |\n|---|---:|---:|---:|---:|---|\n')
        for d in group:
            p = max(d['partitions'], key=lambda p: max_gap(p)['duration'])
            g = max_gap(p)
            s = d['summary']
            counts = ' / '.join(str(s['records'][k]) for k in ['acked', 'refused', 'not_written', 'unknown'])
            ending = 'ACK' if g['ended_by'] == 'acked' else 'failure settlement'
            interval = f"P{p['partition']}: {g['start']/1e9:.6f}–{g['end']/1e9:.6f}; {ending}"
            report.append(f"| {d['variant']['name']} | {counts} | {ms(s['latency_acked']['p99'])} | {ms(s['latency_all_deliveries']['max'])} | {ms(g['duration'])} | {interval} |\n")
        report.append('\n')
    report.append(TAIL)
    (OUT / 'AVAILABILITY_REVIEW.md').write_text(''.join(report))
    print(f'Rendered {len(groups)} families, {len(runs)} variants, {len(partition_rows)} partitions and {len(phase_rows)} phase rows.')


if __name__ == '__main__':
    main()
