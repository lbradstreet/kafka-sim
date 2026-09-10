"""Archiving must preserve every decoded byte, including independent replay evidence."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from classic_artifacts import archive_job, read_artifact
from classic_comparison import load_report
from test_classic_comparison import fixture


class ArtifactArchiveTest(unittest.TestCase):
    def test_round_trip_replay_retention_and_corruption(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            name = 'test.scenario--v--classic--original--full--0'
            report = fixture()
            expected = copy.deepcopy(report)
            artifacts = {}
            for which in ('first', 'replay'):
                sidecars = directory / f'{name}.{which}'
                sidecars.mkdir()
                for field in ('environment', 'external_history', 'deliveries'):
                    path = sidecars / f'{field}.json'
                    data = json.dumps(expected[field]).encode()
                    path.write_bytes(data)
                    if which == 'first':
                        artifacts[field] = {'path': str(path), 'sha256': hashlib.sha256(data).hexdigest()}
            for field in artifacts:
                del report[field]
            report['artifacts'] = artifacts
            path = directory / f'{name}.json'
            path.write_text(json.dumps(report))
            original = path.read_bytes()
            count = archive_job(directory, 'test.scenario', 'classic', 'original', 'full', '0')
            self.assertEqual(7, count)
            restored = load_report(path)
            for field in expected:
                self.assertEqual(expected[field], restored[field])
            index = json.loads((directory / 'archive-classic-original-full-0.json').read_text())
            self.assertEqual(hashlib.sha256(original).hexdigest(), index['files'][path.name]['sha256'])
            for which in ('first', 'replay'):
                self.assertEqual([], list((directory / f'{name}.{which}').glob('*.json')))
                for field in artifacts:
                    self.assertEqual(expected[field], read_artifact(index['files'][f'{name}.{which}/{field}.json']))
            self.assertEqual(count, archive_job(directory, 'test.scenario', 'classic', 'original', 'full', '0'))
            bad = dict(restored['artifacts']['environment'], sha256='0' * 64)
            with self.assertRaisesRegex(ValueError, 'hash mismatch'):
                read_artifact(bad)
            with self.assertRaisesRegex(ValueError, 'encoding'):
                read_artifact(dict(bad, encoding='zip'))


if __name__ == '__main__':
    unittest.main()
