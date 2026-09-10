"""Exact per-record availability and retry measurements for shared producer runs."""
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
import copy
from functools import lru_cache
from heapq import merge, nlargest

from classic_comparison import require, validate

NS = 1_000_000_000


@lru_cache(maxsize=65536)
def key_hash(number, width):
    data = (number.to_bytes(8, 'big') * ((width + 7) // 8))[:width]
    h, m = (0x9747b28c ^ len(data)), 0x5bd1e995
    cursor = 0
    while cursor + 4 <= len(data):
        k = int.from_bytes(data[cursor:cursor + 4], 'little')
        k = k * m & 0xffffffff
        k ^= k >> 24
        k = k * m & 0xffffffff
        h = ((h * m) ^ k) & 0xffffffff
        cursor += 4
    tail = data[cursor:]
    if tail:
        h ^= int.from_bytes(tail, 'little')
        h = h * m & 0xffffffff
    h ^= h >> 13
    h = h * m & 0xffffffff
    h ^= h >> 15
    return h & 0x7fffffff


def quantiles(values):
    values = sorted(values)
    return {name: values[(len(values) * rank + 99) // 100 - 1] if values else None
            for name, rank in [('p50', 50), ('p90', 90), ('p99', 99), ('max', 100)]}


def pending_gap(events):
    pending, start, best = 0, None, {'duration_ns': 0, 'start_ns': None, 'end_ns': None, 'end_kind': 'none'}
    for at, kind in events:
        if kind == 0:
            if not pending:
                start = at
            pending += 1
        else:
            require(pending > 0, 'settlement without pending demand')
            pending -= 1
            if kind == 1 or pending == 0:
                if at - start > best['duration_ns']:
                    best = {'duration_ns': at - start, 'start_ns': at, 'end_ns': at,
                            'end_kind': 'ack' if kind == 1 else 'failure'}
                    best['start_ns'] = start
                start = at if pending else None
    require(pending == 0, 'unsettled pending interval')
    return best


class Topology:
    def __init__(self, manifest):
        self.loads = manifest['experiment']['loads']
        topics = copy.deepcopy(manifest['topics'])
        self.times, self.states = [0], [copy.deepcopy(topics)]
        for control in manifest['experiment']['scheduled_actions']:
            action = control['action']
            if not isinstance(action, dict):
                continue
            kind, body = next(iter(action.items()))
            if kind == 'MoveLeader':
                topics[body['topic']]['leaders'][body['partition']] = body['broker']
            elif kind == 'AddPartitions':
                topics[body['topic']]['leaders'].extend(body['additional_leaders'])
            elif kind == 'RecreateTopic':
                topics[body['topic']]['id'] = body['new_id']
            else:
                continue
            self.times.append(control['at_ns'])
            self.states.append(copy.deepcopy(topics))
        self.topic_by_id = {bytes(t['id']).hex(): i for state in self.states for i, t in enumerate(state)}

    def state(self, at):
        return self.states[bisect_right(self.times, at) - 1]

    def intended(self, load, record_id, at):
        template = self.loads[load]['template']
        topic = self.state(at)[template['topic']]
        count = len(topic['leaders'])
        index = record_id - template['first_id']
        policy = template['partitioning']
        if policy == 'RoundRobin':
            partition = index % count
        elif 'Fixed' in policy:
            partition = policy['Fixed']['partition']
        else:
            keys = policy['Keyed']
            number = 0 if index * 618_033 % 1_000_000 < keys['skew_ppm'] else index % keys['keys']
            partition = key_hash(number, template['key_bytes']) % count
        return bytes(topic['id']).hex(), partition

    def broker(self, route, at):
        topic, partition = route
        index = self.topic_by_id.get(topic)
        if index is None or partition < 0:
            return None
        leaders = self.state(at)[index]['leaders']
        return leaders[partition] if partition < len(leaders) else None


def fault_bands(manifest):
    faults = manifest['faults']
    return [{'kind': kind, 'index': i, 'start_ns': w['start_ns'], 'end_ns': w['end_ns'],
             'broker': w.get('broker'), 'spec': w}
            for kind, windows in [('isolation', faults.get('isolations', [])),
                                  ('link', faults.get('link_outages', [])),
                                  ('rule', faults.get('environment', []))]
            for i, w in enumerate(windows)]


def audit(report):
    checked = validate(report)
    manifest = report['manifest']
    topology, bands = Topology(manifest), fault_bands(manifest)
    origin, duration = manifest['start_ns'], report['environment']['now_ns']
    offers, accepted = {}, {}
    errors, native_events, refusal_reasons = Counter(), [], Counter()
    for e in report['external_history']:
        kind = e['kind']
        if kind == 'offer':
            offers[e['id']] = e
        elif kind == 'admission' and e['accepted']:
            accepted[e['id']] = e['at_ns']
        elif kind == 'sender-error':
            errors[str(e.get('error', e))] += 1
        elif kind == 'native-event':
            native_events.append(e)
        elif kind == 'refused':
            refusal_reasons[e.get('error', 'unspecified')] += 1
    log = {r['id']: r for r in report['environment']['log']}
    routes = {record_id: (bytes(r['topic_id']).hex(), r['partition']) for record_id, r in log.items()}
    brokers, counts, first, tokens = {}, Counter(), {}, {}
    request_rows, commits, bytes_wire, api_counts = [], [], 0, Counter()
    for entry in report['environment']['history']['entries']:
        at = entry['now_ns'] - origin
        kind, body = next(iter(entry['event'].items()))
        if kind == 'ConnectionOpened':
            brokers[body['connection']] = body['broker']
        elif kind == 'Accepted':
            require(body['token'] not in tokens, 'duplicate admission token')
            tokens[body['token']] = body['record_id']
            routes.setdefault(body['record_id'], (bytes(body['topic']).hex(), body['partition']))
        elif kind == 'WriteCompleted':
            bytes_wire += body['bytes']
        elif kind == 'BrokerRequest':
            api_counts[body['api']] += 1
            if body['api'] == 0:
                # BrokerRequest.records contains native admission tokens. Java
                # uses record IDs as tokens, but native IDs diverge after a
                # refusal and for independent sources with disjoint ID ranges.
                require(all(token in tokens for token in body['records']), 'unknown broker request token')
                body = dict(body, records=[tokens[token] for token in body['records']])
                request_rows.append((at, body))
                for record_id in body['records']:
                    counts[record_id] += 1
                    first.setdefault(record_id, at)
        elif kind == 'BrokerCommit':
            commits.append((at, brokers[body['connection']], body['records']))
    for record_id, e in offers.items():
        intended = topology.intended(e['load'], record_id, e['at_ns'])
        if record_id in log and len(topology.states) == 1:
            require(routes[record_id] == intended, 'independent fixed-topology route oracle differs')
        if record_id not in routes:
            routes[record_id] = intended
    require(set(counts) <= set(accepted) and set(log) <= set(counts), 'broker request population differs')
    deliveries = {d['id']: d for d in report['deliveries']}
    partitions = {}
    timelines = defaultdict(list)
    latency, failed_latency, first_wait, overshoots = {}, {}, {}, []
    route_latencies, route_failed_latencies = defaultdict(list), defaultdict(list)
    route_first_wait, route_attempts = defaultdict(int), defaultdict(int)
    failure_reasons = Counter()
    deadline = manifest['producer']['delivery_timeout']
    pauses = manifest['experiment']['polling_pauses']
    for record_id, at in accepted.items():
        d = deliveries[record_id]
        route = routes[record_id]
        timelines[route].extend([(at, 0), (d['at_ns'], 1 if d['success'] else 2)])
        (latency if d['success'] else failed_latency)[record_id] = d['at_ns'] - at
        (route_latencies if d['success'] else route_failed_latencies)[route].append(d['at_ns'] - at)
        route_attempts[route] = max(route_attempts[route], counts[record_id])
        if record_id in first:
            first_wait[record_id] = first[record_id] - at
            require(first_wait[record_id] >= 0, 'broker observed before admission')
            route_first_wait[route] = max(route_first_wait[route], first_wait[record_id])
            if d['success']:
                require(first[record_id] <= d['at_ns'], 'ack before first broker observation')
        if not d['success']:
            failure_reasons[d.get('error') or f"native outcome {d.get('outcome')} reason {d.get('reason')}"] += 1
        callback = d.get('callback_ns', d['at_ns'])
        if callback - at > deadline + 5_000_000 and not any(at < p['end_ns'] and callback >= p['start_ns'] for p in pauses):
            overshoots.append((callback - at - deadline, record_id))
    for route, events in timelines.items():
        events.sort()
        partitions[route] = {'topic_id': route[0], 'partition': route[1], 'pending_gap': pending_gap(events),
                             'offered': 0, 'accepted': 0, 'refused': 0, 'acked': 0, 'failed': 0}
    for record_id in offers:
        route = routes[record_id]
        row = partitions.setdefault(route, {'topic_id': route[0], 'partition': route[1], 'pending_gap': pending_gap([]),
                                           'offered': 0, 'accepted': 0, 'refused': 0, 'acked': 0, 'failed': 0})
        row['offered'] += 1
        if record_id in accepted:
            row['accepted'] += 1
            row['acked' if deliveries[record_id]['success'] else 'failed'] += 1
        else:
            row['refused'] += 1
    for metric in ('offered', 'accepted', 'refused', 'acked', 'failed'):
        require(sum(r[metric] for r in partitions.values()) == report[metric], 'audit partition population')
    for route, row in partitions.items():
        row['ack_latency_ns'] = quantiles(route_latencies[route])
        row['failed_latency_ns'] = quantiles(route_failed_latencies[route])
        row['first_observation_wait_max_ns'] = route_first_wait[route]
        row['broker_attempts_max'] = route_attempts[route]
    sources = []
    for index, spec in enumerate(manifest['experiment']['loads']):
        events = [e for e in offers.values() if e['load'] == index]
        times = sorted(e['at_ns'] for e in events)
        source_ids = [e['id'] for e in events]
        gaps = [(b - a, a, b) for a, b in zip(times, times[1:])]
        gap = max(gaps, default=(0, None, None))
        sources.append({'load': index, 'shape': spec['shape'], 'template': spec['template'],
                        'offered': len(events), 'accepted': sum(i in accepted for i in source_ids),
                        'acked': sum(i in deliveries and deliveries[i]['success'] for i in source_ids),
                        'failed': sum(i in deliveries and not deliveries[i]['success'] for i in source_ids),
                        'max_offer_gap_ns': gap[0], 'offer_gap_start_ns': gap[1], 'offer_gap_end_ns': gap[2]})
    offer_order = sorted((e['at_ns'], i) for i, e in offers.items())
    offer_times = [at for at, _ in offer_order]
    delivery_order = sorted((d['at_ns'], i) for i, d in deliveries.items())
    delivery_times = [at for at, _ in delivery_order]
    phases = []
    for band in bands:
        start, end, target = band['start_ns'], band['end_ns'], band['broker']
        groups = {name: dict.fromkeys(('offered', 'accepted', 'refused', 'acked', 'failed'), 0)
                  for name in ('targeted', 'healthy')}
        def group(record_id, at):
            destination = topology.broker(routes[record_id], at)
            active = [b['broker'] for b in bands if b['start_ns'] <= at < b['end_ns']]
            return 'targeted' if target is None or destination is None or destination == target or None in active or destination in active else 'healthy'
        for at, record_id in offer_order[bisect_left(offer_times, start):bisect_left(offer_times, end)]:
            r = groups[group(record_id, at)]
            r['offered'] += 1
            r['accepted' if record_id in accepted else 'refused'] += 1
        for at, record_id in delivery_order[bisect_left(delivery_times, start):bisect_left(delivery_times, end)]:
            groups[group(record_id, at)]['acked' if deliveries[record_id]['success'] else 'failed'] += 1
        cohort = [i for i, at in accepted.items() if at < end <= deliveries[i]['at_ns']
                  and (target is None or topology.broker(routes[i], min(at, end - 1)) == target)]
        successes = [deliveries[i]['at_ns'] - end for i in cohort if deliveries[i]['success']]
        phases.append({**band, 'groups': groups, 'pending_at_end': len(cohort), 'cohort_acked': len(successes),
                       'cohort_failed': len(cohort) - len(successes),
                       'first_cohort_ack_after_end_ns': min(successes, default=None),
                       'last_cohort_ack_after_end_ns': max(successes, default=None)})
    all_latency = latency | failed_latency
    worst_ids = set(nlargest(8, all_latency, key=all_latency.get)) | set(nlargest(6, counts, key=counts.get)) | set(nlargest(6, first_wait, key=first_wait.get))
    request_evidence = defaultdict(list)
    for at, body in request_rows:
        for record_id in body['records']:
            if record_id in worst_ids:
                request_evidence[record_id].append({'at_ns': at, 'broker': brokers[body['connection']], 'correlation': body['correlation']})
    witnesses = []
    for i in sorted(worst_ids):
        d = deliveries.get(i)
        witnesses.append({'id': str(i), 'route': [routes[i][0], routes[i][1]], 'load': offers[i]['load'],
                          'offer_ns': offers[i]['at_ns'], 'accepted_ns': accepted.get(i),
                          'terminal_ns': d['at_ns'] if d else None, 'callback_ns': d.get('callback_ns') if d else None,
                          'success': d['success'] if d else None, 'error': d.get('error') if d else None,
                          'outcome': d.get('outcome') if d else None, 'reason': d.get('reason') if d else None,
                          'latency_ns': all_latency.get(i), 'attempts_at_broker': counts[i],
                          'first_observation_wait_ns': first_wait.get(i), 'requests': request_evidence[i]})
    return {'schema': 'kr-classic-audit/v1', 'checks': checked, 'duration_ns': duration,
            'last_offer_ns': max((e['at_ns'] for e in offers.values()), default=None),
            'last_terminal_ns': max((d['at_ns'] for d in deliveries.values()), default=None),
            'ack_latency_ns': quantiles(latency.values()), 'failed_latency_ns': quantiles(failed_latency.values()),
            'first_observation_wait_ns': quantiles(first_wait.values()),
            'attempts_at_broker': quantiles(counts.values()), 'attempted_records': len(counts),
            'record_observations_at_broker': sum(counts.values()), 'produce_requests': len(request_rows),
            'request_record_counts': quantiles(len(b['records']) for _, b in request_rows),
            'wire_bytes': bytes_wire, 'api_requests': dict(api_counts),
            'global_pending_gap': pending_gap(merge(*timelines.values())),
            'partitions': [partitions[k] for k in sorted(partitions)], 'sources': sources, 'phases': phases,
            'failure_reasons': dict(failure_reasons), 'refusal_reasons': dict(refusal_reasons),
            'sender_error_reasons': dict(errors), 'native_events': native_events,
            'deadline_overshoots': {'count': len(overshoots), 'worst': [{'id': str(i), 'overshoot_ns': n} for n, i in sorted(overshoots, reverse=True)[:8]]},
            'witnesses': witnesses}
