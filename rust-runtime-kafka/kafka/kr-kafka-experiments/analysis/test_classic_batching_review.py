import copy
import unittest
from unittest.mock import patch

from classic_batching_review import input_contract, java_result, recovery_control


class BatchingReviewTest(unittest.TestCase):
    def fixture(self):
        manifest = {'versions': {'source_sha256': 'old', 'codec': 'pinned'},
                    'producer': {'batch_target_bytes': 4096}, 'seed': 0}
        return {'manifest': manifest, 'original_manifest': copy.deepcopy(manifest),
                'artifacts': {'deliveries': {'path': '/old', 'sha256': 'unchanged'}},
                'acked': 4096}

    def test_java_comparison_excludes_only_declared_input_and_storage_changes(self):
        old = self.fixture()
        new = copy.deepcopy(old)
        for field in ('manifest', 'original_manifest'):
            new[field]['versions']['source_sha256'] = 'new'
            new[field]['producer']['batch_target_mode'] = 'EstimatedWire'
        new['artifacts']['deliveries']['path'] = '/new.gz'
        new['artifacts']['deliveries']['encoding'] = 'gzip'
        expected = java_result(old, 'Raw')
        self.assertEqual(expected, java_result(new, 'EstimatedWire'))
        for mutation in ('artifact', 'count', 'target', 'codec'):
            bad = copy.deepcopy(new)
            if mutation == 'artifact':
                bad['artifacts']['deliveries']['sha256'] = 'changed'
            elif mutation == 'count':
                bad['acked'] -= 1
            elif mutation == 'target':
                bad['manifest']['producer']['batch_target_bytes'] += 1
            else:
                bad['manifest']['versions']['codec'] = 'changed'
            self.assertNotEqual(expected, java_result(bad, 'EstimatedWire'), mutation)

    def test_unknown_or_missing_new_policy_cannot_enter_the_comparison(self):
        manifest = self.fixture()['manifest']
        with self.assertRaises(ValueError):
            input_contract(manifest, 'EstimatedWire')
        manifest['producer']['batch_target_mode'] = 'unknown'
        with self.assertRaises(ValueError):
            input_contract(manifest, 'Raw')

    def test_recovery_controls_reject_failed_missing_or_unadmitted_probes(self):
        report = {'offered': 3, 'accepted': 3, 'refused': 0, 'acked': 2, 'failed': 1,
                  'deliveries': [{'id': i, 'success': True, 'at_ns': 20 + i} for i in [1, 2]],
                  'external_history': [{'kind': 'admission', 'id': i, 'accepted': True} for i in [1, 2]]}
        previous = {'runs': {'native_after': {
            'header_sha256': 'header', 'source_sha256': 'source',
            'records': {'failed': 1}, 'probes': {'ids': [1, 2]}}}}
        with patch('classic_batching_review.load_report', return_value=report):
            self.assertEqual(2, recovery_control(None, previous)['probes_after']['acked'])
        for mutation in ('failed', 'missing', 'unadmitted', 'fatal', 'failure_count'):
            bad = copy.deepcopy(report)
            if mutation == 'failed':
                bad['deliveries'][0]['success'] = False
            elif mutation == 'missing':
                bad['deliveries'].pop()
            elif mutation == 'unadmitted':
                bad['external_history'][0]['accepted'] = False
            elif mutation == 'fatal':
                bad['external_history'].append({'kind': 'native-event', 'event': 'Fatal reason13'})
            else:
                bad['failed'] += 1
            with patch('classic_batching_review.load_report', return_value=bad):
                with self.assertRaises(ValueError, msg=mutation):
                    recovery_control(None, previous)


if __name__ == '__main__':
    unittest.main()
