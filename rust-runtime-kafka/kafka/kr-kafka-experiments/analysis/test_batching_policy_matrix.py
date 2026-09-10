import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

script = Path(__file__).resolve().parents[3] / 'scripts/run-batching-policy-matrix.py'
spec = importlib.util.spec_from_file_location('batching_matrix', script)
matrix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matrix)


class BatchingMatrixTest(unittest.TestCase):
    def test_request_policy_job_cannot_be_mislabeled_as_a_different_arm(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'summary-native-common-full.json').write_text(json.dumps({'passed': 1, 'failures': []}))
            path = root / 'baseline.compression--random0-zstd1--native--common--full--0.json'
            header = {'replay_verified': True, 'adapter': 'native-panama-sim',
                      'manifest': {'producer': {'request_batching_policy': 'BrokerReady'}},
                      'adjustments': ['request batching policy override: BrokerReady']}
            path.write_text(json.dumps(header))
            args = (root, 'baseline.compression', ['random0-zstd1'], 'common', '0')
            self.assertEqual(1, matrix.verify_job(*args, 'broker-ready',
                'request_batching_policy', matrix.REQUEST_MODES)['passed'])
            with self.assertRaises(ValueError):
                matrix.verify_job(*args, 'sealed', 'request_batching_policy', matrix.REQUEST_MODES)

    def test_selection_rejects_duplicate_unknown_or_out_of_range_cases(self):
        catalogue = [{'scenario': 'baseline.compression', 'variant': 'random0-zstd1'}]
        good = ['baseline.compression', 'random0-zstd1', 'common', '0']
        self.assertEqual(4, len(matrix.select_cases(catalogue, ['0', '7'])))
        for cases in [[good, good], [good[:3] + [str(2**64)]],
                      [good[:2] + ['unknown', '0']], [['missing', *good[1:]]], []]:
            with self.assertRaises(ValueError):
                matrix.select_cases(catalogue, [], explicit=cases)

    def test_success_summary_cannot_hide_missing_replay_or_wrong_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary = root / 'summary-native-common-full.json'
            summary.write_text(json.dumps({'passed': 1, 'failures': []}))
            path = root / 'baseline.compression--random0-zstd1--native--common--full--0.json'
            valid = {'replay_verified': True, 'adapter': 'native-panama-sim',
                     'manifest': {'producer': {'batch_target_mode': 'Raw'}},
                     'adjustments': ['batch target mode override: Raw']}
            path.write_text(json.dumps(valid))
            args = (root, 'baseline.compression', ['random0-zstd1'], 'common', '0', 'raw')
            self.assertEqual(1, matrix.verify_job(*args)['passed'])
            for field, value in [('replay_verified', False), ('adapter', 'classic'),
                                 ('adjustments', []),
                                 ('manifest', {'producer': {'batch_target_mode': 'EstimatedWire'}})]:
                path.write_text(json.dumps(valid | {field: value}))
                with self.assertRaises(ValueError):
                    matrix.verify_job(*args)


if __name__ == '__main__':
    unittest.main()
