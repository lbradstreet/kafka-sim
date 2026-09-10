"""Portable presentation checks against full evidence, independent of SVG rendering."""
import copy
import json
import unittest

from classic_visualization import SAMPLE, derive_run, generate_sample, page_html, reviewed_assets
from test_classic_comparison import fixture


class ClassicVisualizationTest(unittest.TestCase):
    def test_population_and_latency_use_all_records_and_consumption_times(self):
        report = fixture()
        report["manifest"]["brokers"] = [{"id": 1}]
        result = derive_run(report, 1_000_000_000, 3)
        self.assertEqual([1, 1, 1], result["buckets"]["offered"])
        self.assertEqual([0, 1, 1], result["buckets"]["refused"])
        self.assertEqual([1, 0, 0], result["buckets"]["outstanding"])
        self.assertEqual([None, 1_200_000_000, None], result["buckets"]["p99"])
        self.assertEqual([0, 1, 0], result["brokers"][0]["committed"])
        bad = copy.deepcopy(report)
        bad["deliveries"][0]["at_ns"] -= 1
        with self.assertRaises(ValueError):
            derive_run(bad, 1_000_000_000, 3)

    def test_real_recovery_sample_is_deterministic_and_byte_pinned(self):
        first, second = generate_sample(), generate_sample()
        self.assertEqual(first, second)
        self.assertEqual(SAMPLE.read_text(), first)
        bundle = json.loads(first.split(" = ", 1)[1].removesuffix(";\n"))
        pair = bundle["pairs"][0]
        self.assertEqual("10000000000", str(pair["environment"]["bands"][0]["start"]))
        self.assertTrue(pair["fault_exposure_comparable"])
        self.assertEqual(96, pair["runs"]["classic"]["summary"]["acked"])
        self.assertEqual(96, pair["runs"]["native"]["summary"]["acked"])

    def test_standalone_page_reuses_reviewed_assets_and_escapes_trace_text(self):
        sample = generate_sample()
        bundle = json.loads(sample.split(" = ", 1)[1].removesuffix(";\n"))
        hostile = '</script><img src=x onerror="alert(1)">\u2028\u2029'
        bundle["pairs"][0]["limits"] = [hostile]
        page = page_html(reviewed_assets(), bundle)
        self.assertNotIn(hostile, page)
        self.assertIn("\\u003c/script>", page)
        self.assertNotIn('<script src=', page)
        self.assertIn("producer-comparison", page)


if __name__ == "__main__":
    unittest.main()
