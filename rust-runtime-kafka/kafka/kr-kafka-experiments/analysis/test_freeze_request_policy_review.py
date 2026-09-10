import copy
import csv
import json
from pathlib import Path
import tempfile
import unittest

from freeze_request_policy_review import combine
from request_policy_review import METRICS


class MatrixUnionTest(unittest.TestCase):
    def make(self, base, profile):
        root = base / profile
        (root / 'analysis').mkdir(parents=True)
        case = ['baseline.test', 'v', profile, '0']
        values = {k: 0 for k in METRICS}
        pair = {'case': case, 'sealed': values, 'broker-ready': values}
        report = {'identity': {'library_sha256': 'same', 'cases': [case]},
                  'matrix_sha256': '0' * 64, 'analysis_tools_sha256': {},
                  'pairs': [pair], 'expected_runs': 2, 'audited_runs': 2, 'expected_pairs': 1}
        (root / 'analysis/comparison.json').write_text(json.dumps(report))
        with (root / 'analysis/results.csv').open('w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=['scenario', 'variant', 'profile', 'seed', 'mode', *METRICS])
            writer.writeheader()
            for mode in ('sealed', 'broker-ready'):
                writer.writerow(dict(zip(('scenario', 'variant', 'profile', 'seed'), case)) | {'mode': mode} | values)
        return root, report

    def test_union_preserves_coverage_and_rejects_overlap_or_changed_implementation(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            common, a = self.make(base, 'common')
            original, b = self.make(base, 'original')
            out = base / 'out'
            out.mkdir()
            merged = combine([common, original], [a, b], out)
            self.assertEqual((2, 4), (merged['completed_pairs'], merged['audited_runs']))
            self.assertEqual(2, len(merged['matrices']))
            self.assertEqual(['common', 'original'], [p['case'][2] for p in merged['pairs']])
            with self.assertRaisesRegex(ValueError, 'overlapping'):
                combine([common, common], [a, a], out)
            changed = copy.deepcopy(b)
            changed['identity']['library_sha256'] = 'changed'
            with self.assertRaisesRegex(ValueError, 'different implementations'):
                combine([common, original], [a, changed], out)


if __name__ == '__main__':
    unittest.main()
