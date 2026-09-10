import ctypes as c
import gc
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ffi_open_loop import Broker, Observations, configuration, load_library
from python_binding import KrEvent


def delivery(user, outcome):
    event = KrEvent()
    event.kind, event.user_token, event.outcome = 1, user, outcome
    event.attempts = 2
    return event


class ObservationTests(unittest.TestCase):
    def test_outcomes_conserve_only_the_accepted_prefix_and_close_follows_delivery(self):
        observed = Observations(1000, 3)
        for user in range(3):
            observed.admit(user, user * 1000000 + 7)
        for user in (2, 0, 1):
            observed.observe(delivery(user, user), 10000000)
        closed = KrEvent()
        closed.kind = 6
        observed.observe(closed, 10000001)
        self.assertTrue(observed.closed)
        self.assertEqual((observed.accepted, observed.acked, observed.not_written, observed.unknown), (3, 1, 1, 1))
        self.assertEqual(observed.attempts, 6)
        self.assertEqual(observed.offer_delivery.count, 3)
        for corruption in (delivery(0, 0), delivery(3, 0), closed):
            with self.assertRaises(RuntimeError):
                observed.observe(corruption, 10000002)

    def test_missing_delivery_invalid_outcome_and_capacity_fail(self):
        for corrupt in (KrEvent(kind=6), delivery(0, 3), delivery(1, 0)):
            observed = Observations(1000, 1)
            observed.admit(0, 0)
            with self.assertRaises(RuntimeError):
                observed.admit(1, 1)
            with self.assertRaises(RuntimeError):
                observed.observe(corrupt, 10000000)


@unittest.skipUnless(os.environ.get("KR_BENCH_FFI_LIBRARY"), "set KR_BENCH_FFI_LIBRARY for real ABI configuration checks")
class ConfigurationTests(unittest.TestCase):
    def test_real_abi_configuration_retains_spans_and_matches_each_security_profile(self):
        library = load_library(os.environ["KR_BENCH_FFI_LIBRARY"])
        profile = json.loads((Path(__file__).parent / "profile.json").read_text())
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, KR_BENCH_TEST_USER="bench", KR_BENCH_TEST_PASSWORD="public-fixture-password"):
            root = Path(directory) / "ca.der"
            root.write_bytes(b"configuration-only fixture, not a certificate")
            profile.update(ca_der=str(root), username_env="KR_BENCH_TEST_USER", password_env="KR_BENCH_TEST_PASSWORD")
            for backend in ("uring", "readiness"):
                for mode in ("plaintext", "tls", "plain", "scram256", "scram512"):
                    for compression in ("none", "zstd1", "zstd3"):
                        profile.update(backend=backend, security=mode, compression=compression)
                        config, keep = configuration(library, profile)
                        gc.collect()
                        brokers = c.cast(config.bootstrap, c.POINTER(Broker))
                        self.assertEqual(c.string_at(brokers[0].host.ptr, brokers[0].host.len), b"localhost")
                        self.assertEqual(config.transport, int(backend == "readiness"))
                        self.assertEqual(config.max_in_flight_per_connection, 5)
                        self.assertEqual(config.linger_max_ns, profile["linger_us"] * 1000)
                        self.assertEqual(config.input_bytes, profile["input_bytes"])
                        self.assertEqual(config.compression, int(compression != "none"))
                        self.assertEqual(config.security, 0 if mode == "plaintext" else 1 if mode == "tls" else 2)
                        if config.security == 2:
                            self.assertEqual(c.string_at(config.password.ptr, config.password.len), b"public-fixture-password")
                        self.assertTrue(keep)

    def test_integer_truncation_and_silent_unknown_modes_are_rejected(self):
        library = load_library(os.environ["KR_BENCH_FFI_LIBRARY"])
        profile = json.loads((Path(__file__).parent / "profile.json").read_text())
        for field, value in (("request_bytes", 1 << 32), ("input_bytes", 1 << 64), ("linger_us", -1000), ("routing", "typo"), ("backend", "auto")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                configuration(library, dict(profile, **{field: value}))


if __name__ == "__main__":
    unittest.main()
