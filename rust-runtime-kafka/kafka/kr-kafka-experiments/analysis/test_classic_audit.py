"""Measurement oracles distinguish pending demand, failure settlement and admission loss."""
import unittest

from classic_audit import NS, audit, key_hash, pending_gap
from test_classic_admission import fixture as admission_fixture


def fixture():
    r = admission_fixture()
    r['manifest']['topics'][0]['id'] = [1] * 16
    r['manifest']['producer']['delivery_timeout'] = NS
    events = [{'event': {'ConnectionOpened': {'connection': b, 'broker': b}}, 'now_ns': r['manifest']['start_ns']}
              for b in (1, 2, 3)]
    tokens = {210: 1, 11: 2, 111: 3, 114: 4}
    for record_id, token in tokens.items():
        at = next(e['at_ns'] for e in r['external_history'] if e['kind'] == 'admission' and e['id'] == record_id)
        p = (record_id - 1) // 100
        events.append({'event': {'Accepted': {'token': token, 'record_id': record_id, 'partition': p, 'topic': [1] * 16}},
                       'now_ns': r['manifest']['start_ns'] + at})
    for at, record_id, broker in [(9 * NS + NS // 100, 210, 3), (10 * NS + NS // 10, 111, 2),
                                  (10 * NS + NS // 5, 111, 2), (13 * NS, 11, 1),
                                  (13 * NS + NS // 10, 114, 2)]:
        events.append({'now_ns': r['manifest']['start_ns'] + at, 'event': {
            'BrokerRequest': {'api': 0, 'records': [tokens[record_id]], 'connection': broker, 'correlation': len(events)}}})
    events.sort(key=lambda e: e['now_ns'])
    r['environment']['history']['entries'] = events
    return r


class FullAuditTest(unittest.TestCase):
    def test_failure_settlement_is_not_an_ack_or_an_idle_gap(self):
        self.assertEqual({'duration_ns': 8, 'start_ns': 0, 'end_ns': 8, 'end_kind': 'failure'},
                         pending_gap([(0, 0), (4, 0), (5, 2), (8, 2)]))
        self.assertEqual(2, pending_gap([(0, 0), (2, 1), (100, 0), (101, 1)])['duration_ns'])
        with self.assertRaises(ValueError):
            pending_gap([(0, 1)])

    def test_outage_refusals_wait_attempts_and_deadlines(self):
        r = audit(fixture())
        self.assertEqual(2, r['attempts_at_broker']['max'])
        self.assertEqual(5, r['record_observations_at_broker'])
        self.assertEqual(3 * NS, r['first_observation_wait_ns']['max'])
        self.assertEqual(11, r['phases'][0]['groups']['healthy']['refused'])
        self.assertEqual(1, r['phases'][0]['groups']['healthy']['acked'])
        self.assertEqual(1, r['phases'][0]['cohort_acked'])
        self.assertEqual(2, r['deadline_overshoots']['count'])
        self.assertEqual(3 * NS, max(p['pending_gap']['duration_ns'] for p in r['partitions']))
        bad = fixture()
        bad['environment']['history']['entries'].pop()
        with self.assertRaisesRegex(ValueError, 'request population'):
            audit(bad)
        r = fixture()
        r['manifest']['experiment']['polling_pauses'] = [{'start_ns': 11 * NS, 'end_ns': 12 * NS}]
        self.assertEqual(0, audit(r)['deadline_overshoots']['count'])

    def test_seed_zero_key_distribution_matches_record_materializer(self):
        # Full-width key encoding and Kafka's positive murmur2 routing, pinned
        # against the existing 4,096-record seed-0 six-partition audit.
        counts = [0] * 6
        for index in range(4096):
            counts[key_hash(index % 64, 8) % 6] += 1
        self.assertEqual([768, 960, 448, 320, 960, 640], counts)


if __name__ == '__main__':
    unittest.main()
