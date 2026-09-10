import unittest

from classic_request_witness import extract


class RequestWitnessTest(unittest.TestCase):
    def test_persisted_dispatch_and_broker_tokens_both_resolve_to_workload_ids(self):
        def e(at, kind, **body):
            return {'now_ns': 1000 + at, 'ordinal': at, 'event': {kind: body}}
        report = {'manifest': {'start_ns': 1000}, 'deliveries': [{'id': 900, 'at_ns': 30}],
                  'environment': {'history': {'entries': [
                      e(1, 'Accepted', token=7, record_id=900),
                      e(10, 'ClientRequestDispatched', request_id=4, connection=2, correlation=8, api=0, tokens=[7]),
                      e(12, 'BrokerRequest', connection=2, correlation=8, api=0, records=[7]),
                      e(13, 'FaultDecision', hook={'connection': 2, 'correlation': 8}),
                      e(15, 'ClientRequestFinished', request_id=4),
                      e(20, 'ClientRequestDispatched', request_id=5, connection=3, correlation=1, api=0, tokens=[7]),
                      e(21, 'BrokerRequest', connection=3, correlation=1, api=0, records=[7]),
                      e(22, 'ClientRequestFinished', request_id=5),
                  ]}}}
        result = extract(report, [900])
        row = result['records'][0]
        self.assertEqual('7', row['admission_token'])
        self.assertEqual((row['id'], row['pre_dispatch_wait_ns'], row['dispatches'], row['broker_observations']),
                         ('900', 9, 2, 2))
        self.assertEqual(len(result['timeline']), 7)
        with self.assertRaisesRegex(ValueError, 'accepted workload IDs'):
            extract(report, [7])
        report['environment']['history']['entries'][1]['event']['ClientRequestDispatched']['tokens'] = [900]
        with self.assertRaisesRegex(ValueError, 'unknown persisted admission token'):
            extract(report, [900])


if __name__ == '__main__':
    unittest.main()
