"""Check terminal certainty against the independent final broker log."""
from collections import Counter

from classic_comparison import require


def review_failures(report):
    stored = {r['id'] for r in report['environment']['log']}
    groups, witnesses = Counter(), {}
    for d in report['deliveries']:
        if d['success']:
            continue
        applied = d['id'] in stored
        outcome = d.get('outcome')
        if outcome is not None:
            require(outcome in (1, 2), 'native failure has no known terminal certainty')
            require(outcome != 1 or not applied, 'native NotWritten record exists in broker log')
            require(outcome != 2 or d['attempts'] > 0, 'native Unknown has no attempt')
            label = f'native outcome {outcome} reason {d["reason"]}'
        else:
            label = d['error']
        key = (label, applied)
        groups[key] += 1
        witnesses.setdefault(key, [])
        if len(witnesses[key]) < 8:
            witnesses[key].append(str(d['id']))
    require(sum(groups.values()) == report['failed'], 'failure certainty population differs')
    return [{'failure': label, 'stored_despite_failure': applied, 'count': count,
             'example_ids': witnesses[(label, applied)]}
            for (label, applied), count in sorted(groups.items())]
