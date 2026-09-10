import copy
import unittest

from batching_policy_review import METRICS, complete_result, policy_contract, summarize


class PolicyReviewTest(unittest.TestCase):
    def header(self, mode):
        manifest = {'versions': {'source_sha256': 'same'},
                    'producer': {'batch_target_mode': mode, 'batch_target_bytes': 4096},
                    'faults': {'seed': 7}}
        original = copy.deepcopy(manifest)
        original['producer']['batch_target_mode'] = 'EstimatedWire'
        return {'adapter': 'native-panama-sim', 'replay_verified': True,
                'manifest': manifest, 'original_manifest': original,
                'adjustments': ['one lane', f'batch target mode override: {mode}'],
                'classic_config': {'batch.size': 4096}, 'profile': 'common', 'compatibility': {},
                'artifacts': {'history': {'path': 'old', 'sha256': 'same'}}, 'acked': 100}

    def test_only_policy_and_its_annotation_can_differ(self):
        raw, wire = self.header('Raw'), self.header('EstimatedWire')
        expected = policy_contract(raw, 'raw')
        self.assertEqual(expected, policy_contract(wire, 'estimated-wire'))
        for field in ('target', 'seed', 'source', 'java'):
            bad = copy.deepcopy(wire)
            if field == 'target': bad['manifest']['producer']['batch_target_bytes'] = 8192
            if field == 'seed': bad['manifest']['faults']['seed'] = 8
            if field == 'source': bad['manifest']['versions']['source_sha256'] = 'changed'
            if field == 'java': bad['classic_config']['batch.size'] = 8192
            self.assertNotEqual(expected, policy_contract(bad, 'estimated-wire'))
        with self.assertRaises(ValueError):
            policy_contract(raw, 'estimated-wire')

    def test_previous_default_control_requires_complete_unchanged_artifacts(self):
        old, new = self.header('EstimatedWire'), self.header('EstimatedWire')
        old['adjustments'].pop()
        for key in ('manifest', 'original_manifest'):
            new[key]['versions']['source_sha256'] = 'new harness'
        new['artifacts']['history']['path'] = 'new.gz'
        new['artifacts']['history']['encoding'] = 'gzip'
        self.assertEqual(complete_result(old, False), complete_result(new, True))
        new['artifacts']['history']['sha256'] = 'different'
        self.assertNotEqual(complete_result(old, False), complete_result(new, True))

    def test_empty_latency_populations_do_not_count_as_improvements(self):
        raw = dict.fromkeys(METRICS, 100)
        wire = raw | {'ack_p99_ns': None, 'acked': 0, 'failed': 100, 'produce_requests': 5}
        result = summarize([{'raw': raw, 'estimated-wire': wire}])
        self.assertEqual({'missing_population': 1}, result['ack_p99_ns'])
        self.assertEqual({'decreased': 1}, result['acked'])


if __name__ == '__main__':
    unittest.main()
