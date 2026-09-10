import json
from pathlib import Path
import unittest
import tempfile
from common import Histogram, corpus, due_ns, route, validate_comparison
from compare import properties, watchdog_seconds
from integration import server_properties
from allocation_profile import parse_summary, run as allocation_run


class Primitives(unittest.TestCase):
    def test_heaptrack_parser_is_bounded_and_rejects_ambiguous_summaries(self):
        # Synthetic parser fixture, not a benchmark measurement.
        fixture = ("calls to allocation functions: 12345 (100/s)\n"
                   "temporary memory allocations: 123 (1/s)\n"
                   "peak heap memory consumption: 12.34MB\n"
                   "total memory leaked: 64B\n")
        parsed = parse_summary(fixture)
        self.assertEqual(parsed["native_allocation_calls"], 12345)
        self.assertEqual(parsed["peak_heap"], {"display": "12.34MB", "rounded_bytes": 12340000, "exact": False})
        self.assertTrue(parsed["retained_at_exit"]["exact"])
        for corrupt in [fixture + fixture, fixture.replace("12345", "10"), fixture.replace("MB", "MiB"), fixture[:100], fixture.replace("12345", "-1")]:
            with self.assertRaises(ValueError):
                parse_summary(corrupt)

    def test_heaptrack_default_width_uses_decimal_single_letter_units(self):
        # Literal summary shape observed from heaptrack_print 1.5.0; neither
        # repeated workloads nor guessed binary-unit scaling are needed.
        fixture = ("calls to allocation functions: 157506 (13208/s)\n"
                   "temporary memory allocations: 36869 (3091/s)\n"
                   "peak heap memory consumption: 53.73M\n"
                   "total memory leaked: 25.92M\n")
        parsed = parse_summary(fixture)
        self.assertEqual(parsed["peak_heap"], {"display": "53.73M", "rounded_bytes": 53_730_000, "exact": False})
        self.assertEqual(parsed["retained_at_exit"]["rounded_bytes"], 25_920_000)
        for unit, multiplier in [("K", 1000), ("M", 1000**2), ("G", 1000**3), ("T", 1000**4)]:
            actual = parse_summary(fixture.replace("53.73M", "1.25" + unit))
            self.assertEqual(actual["peak_heap"]["rounded_bytes"], multiplier * 5 // 4)

    def test_profiler_keeps_incomplete_launch_and_separate_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profile"
            def unavailable(argv, stdout, stderr, timeout):
                raise FileNotFoundError("synthetic missing profiler")
            result = allocation_run(["producer", "profile.json", "result.json"], output, 10, unavailable, lambda argv: {"argv": argv, "unavailable": True})
            self.assertFalse(result["complete"])
            self.assertFalse(result["performance_comparable"])
            self.assertEqual(result["launch_error"], "FileNotFoundError")
            self.assertEqual(result["artifacts"], [])
            self.assertEqual(json.loads((output / "profile.json").read_text()), result)

    def test_schedule_ignores_admission_refusal(self):
        for rate in [1, 3, 1000, 999983, 1000000000]:
            original = [due_ns(i, rate) for i in range(1000)]
            offered = []
            now = 0
            for index, due in enumerate(original):
                now = max(now, due) + (100000000 if index % 7 == 0 else 0)
                offered.append(due_ns(index, rate))
            self.assertEqual(original, offered)

    def test_payload_and_quantile_fixtures(self):
        self.assertEqual(corpus(8, 1, True)[0], bytes([108, 130, 165, 98, 203, 128, 141, 16]))
        histogram = Histogram()
        for value in range(10000):
            histogram.record(value)
        self.assertGreaterEqual(histogram.quantile(990), 9899)
        self.assertLessEqual(histogram.quantile(990), 9999)
        histogram.record((1 << 64) - 1)
        self.assertEqual(histogram.result()["max_ns"], (1 << 64) - 1)

    def test_comparison_rejects_unrepresentable_settings_and_never_writes_secrets(self):
        profile = json.loads((Path(__file__).parent / "profile.json").read_text())
        validate_comparison(profile)
        profile["linger_us"] = 500
        with self.assertRaises(ValueError):
            validate_comparison(profile)
        profile.update(security="plain", ca_pem="/tmp/ca.pem", username_env="BENCH_USER", password_env="BENCH_PASSWORD")
        text = properties(profile)
        self.assertIn("producer.max.block.ms=0", text)
        self.assertNotIn("sasl.jaas.config", text)
        self.assertIn("password_env=BENCH_PASSWORD", text)
        self.assertEqual(route(profile, 17), (1, None))

    def test_kraft_fixture_has_loopback_and_verified_security_routes(self):
        config = server_properties(Path("/tmp/isolated-kafka"), 19092)
        self.assertIn("CONTROLLER://127.0.0.1:19093", config)
        self.assertIn("SSL://127.0.0.1:19094", config)
        self.assertIn("SASL_SSL://127.0.0.1:19095", config)
        self.assertIn("auto.create.topics.enable=false", config)
        self.assertIn("SCRAM-SHA-256,SCRAM-SHA-512", config)
        self.assertIn("log.dirs=/tmp/isolated-kafka/data", config)

    def test_java_ffm_profiles_match_delivery_settings_and_explicit_warmup(self):
        profile = json.loads((Path(__file__).parent / "profile.json").read_text())
        java = properties(profile, "org.apache.kafka.clients.producer.KafkaProducer", 128)
        ffm = properties(profile, "io.krkafka.producer.KrKafkaProducer", 128)
        for option in ["producer.retries=254", "producer.acks=all", "producer.enable.idempotence=true",
                       "producer.max.block.ms=0", "warmup_records=128"]:
            self.assertIn(option, java)
            self.assertIn(option, ffm)
        self.assertIn("producer.kr.transport=readiness", ffm)
        self.assertNotIn("producer.kr.transport", java)

    def test_java_warmup_has_one_global_delivery_budget_in_the_watchdog(self):
        profile = json.loads((Path(__file__).parent / "profile.json").read_text())
        baseline = watchdog_seconds(profile)
        with_warmup = baseline + profile["delivery_timeout_ms"] / 1000
        self.assertEqual(watchdog_seconds(profile, 1), with_warmup)
        self.assertEqual(watchdog_seconds(profile, 100_000), with_warmup)


if __name__ == "__main__":
    unittest.main()
