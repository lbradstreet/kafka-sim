"""Independent oracle for outage boundaries, refused destinations and cross-policy controls."""
import copy
import unittest

from classic_admission import END, NS, combine, measure, render


def fixture():
    report = {'schema': 'kr-classic-comparison/v1', 'replay_verified': True,
              'manifest': {'start_ns': 2**63 + 7, 'topics': [{'leaders': [1, 2, 3, 1, 2, 3]}],
                           'producer': {'record_descriptors': 512, 'input_bytes': 8 * 1024 * 1024,
                                        'descriptor_admission_policy': 'Shared'},
                           'experiment': {'scheduled_actions': [], 'polling_pauses': [], 'loads': []},
                           'faults': {'isolations': [{'broker': 1, 'start_ns': 10 * NS, 'end_ns': END}]}},
              'classic_config': {'buffer.memory': 8 * 1024 * 1024},
              'external_history': [], 'source_evidence': [], 'deliveries': [],
              'environment': {'now_ns': 30 * NS, 'history': {'entries': []},
                              'fault_stats': {'decisions': 0}, 'log': []}}
    # Affected acceptance recovers exactly at 13s. Healthy acceptance succeeds
    # one nanosecond before 13s; the next healthy offer occurs exactly at 13s.
    accepted = {(0, 10): END, (1, 10): END - 1, (1, 13): END + NS // 5, (2, 9): 9 * NS + NS // 10}
    for p in range(6):
        first = 100 * p + 1
        report['manifest']['experiment']['loads'].append({'template': {
            'first_id': first, 'topic': 0, 'partitioning': {'Fixed': {'partition': p}}},
            'shape': {'OpenLoop': {'start_ns': 0, 'end_ns': 30 * NS, 'rate_per_s': 1}}})
        offset = 0
        for i in range(30):
            record_id = first + i
            report['external_history'].append({'kind': 'offer', 'load': p, 'id': record_id,
                                               'at_ns': i * NS, 'due_ns': i * NS})
            if (p, i) in accepted:
                at = accepted[(p, i)]
                report['external_history'].extend([
                    {'kind': 'admission', 'id': record_id, 'at_ns': i * NS, 'accepted': True},
                    {'kind': 'consumed', 'id': record_id, 'at_ns': at}])
                report['deliveries'].append({'id': record_id, 'success': True, 'partition': p, 'offset': offset, 'at_ns': at})
                report['environment']['log'].append({'id': record_id, 'topic_id': [1] * 16, 'partition': p, 'offset': offset})
                offset += 1
            else:
                report['external_history'].append({'kind': 'refused', 'id': record_id, 'at_ns': i * NS,
                                                   'error': 'synthetic capacity refusal'})
        report['source_evidence'].append({'load': p, 'reserved': 30, 'offered': 30, 'cancelled': 0,
                                          'accepted': offset, 'refused': 30 - offset})
    report['external_history'].sort(key=lambda e: e['at_ns'])
    report.update(offered=180, accepted=4, acked=4, refused=176, failed=0)
    return report


class AdmissionComparisonTest(unittest.TestCase):
    def test_fixed_destinations_half_open_interval_and_recovery(self):
        result = measure(fixture())
        p0, p1 = result['partitions'][:2]
        self.assertEqual({'offered': 3, 'accepted': 1, 'refused': 2, 'acked': 1, 'failed': 0}, p1['outage'])
        self.assertEqual(0, p0['outage']['acked'])
        self.assertEqual(1, p0['outage_accepted_eventually_acked'])
        self.assertEqual(11, result['healthy_outage_refused'])
        self.assertEqual(119, result['healthy_empty_ack_windows'])
        self.assertEqual([{'reason': 'synthetic capacity refusal', 'count': 16, 'healthy_count': 11}],
                         result['outage_refusal_reasons'])
        self.assertEqual(1, p1['plot']['acked'][39])
        self.assertEqual(1, p0['plot']['acked'][40])
        bad = fixture()
        bad['manifest']['experiment']['loads'][0]['template']['partitioning'] = {'Keyed': {'keys': 1}}
        with self.assertRaisesRegex(ValueError, 'fixed source'):
            measure(bad)
        bad = fixture()
        bad['classic_config']['buffer.memory'] //= 2
        with self.assertRaisesRegex(ValueError, 'capacity'):
            measure(bad)

    def test_same_java_behavior_is_required_before_three_way_overlay(self):
        shared = fixture()
        pressure = copy.deepcopy(shared)
        pressure['manifest']['producer']['descriptor_admission_policy'] = 'PartitionPressure'
        runs = {f'{adapter}-{policy}': measure(report) for adapter in ('classic', 'native')
                for policy, report in [('shared', shared), ('pressure', pressure)]}
        arms = combine(runs)
        self.assertEqual({'classic', 'shared', 'pressure'}, set(arms))
        for field in ('behavior_sha256', 'inputs_except_policy_sha256', 'java_config_sha256', 'policy'):
            bad = copy.deepcopy(runs)
            bad['classic-pressure'][field] = 'changed'
            with self.assertRaises(ValueError):
                combine(bad)
        bundle = {'trials': [{'rate': 1000, 'seed': '18446744073709551615', 'arms': arms}]}
        links = {(1000, bundle['trials'][0]['seed'], policy): f'detail.html#pair={i}'
                 for i, policy in enumerate(('shared', 'pressure'))}
        page = render(bundle, links)
        self.assertIn('18446744073709551615', page)
        self.assertEqual(4, page.count('<svg '))
        self.assertEqual(4, page.count('</svg>'))
        self.assertNotIn('<script', page)
        self.assertIn('detail.html#pair=1', page)


if __name__ == '__main__':
    unittest.main()
