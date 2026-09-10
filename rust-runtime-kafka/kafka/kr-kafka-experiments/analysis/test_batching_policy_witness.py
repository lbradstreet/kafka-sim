import copy
import unittest

from batching_policy_witness import packing, selection


class PolicyWitnessTest(unittest.TestCase):
    def test_percentile_selection_excludes_failures_and_uses_nearest_rank(self):
        report = {'external_history': [{'kind': 'admission', 'accepted': True, 'id': i, 'at_ns': 0}
                                       for i in range(1, 103)],
                  'deliveries': [{'id': i, 'at_ns': i, 'success': i != 102} for i in range(1, 103)]}
        self.assertEqual({'id': 100, 'latency_ns': 100}, selection(report)['p99'])
        self.assertEqual({'id': 101, 'latency_ns': 101}, selection(report)['max'])
        report['deliveries'] = []
        self.assertEqual({}, selection(report))

    def test_retry_dispatches_do_not_inflate_distinct_batch_membership(self):
        batch = {'batch_id': 1, 'records': 8, 'raw_bytes': 4000, 'wire_bytes': 100}
        request = {'api': 0, 'wire_bytes': 150, 'batches': [batch]}
        report = {'environment': {'history': {'entries': [
            {'event': {'ClientRequestDispatched': copy.deepcopy(request)}} for _ in range(2)]}}}
        result = packing(report)
        self.assertEqual(2, result['produce_dispatches'])
        self.assertEqual(300, result['produce_dispatched_bytes'])
        self.assertEqual(1, result['distinct_batch_cohorts'])
        report['environment']['history']['entries'][1]['event']['ClientRequestDispatched']['batches'][0]['wire_bytes'] += 1
        with self.assertRaises(ValueError):
            packing(report)


if __name__ == '__main__':
    unittest.main()
