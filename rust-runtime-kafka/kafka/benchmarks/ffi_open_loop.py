#!/usr/bin/env python3
"""Open-loop CPython/ctypes measurement of the production C ABI copy path.

Uses the same ABI layouts exercised by the C/Python conformance fixtures. No
test hook, foreign lease, or managed callback enters this benchmark.
"""
import ctypes as c
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import sys
import time
from collections import Counter
from common import Histogram, corpus, due_ns, public_settings, route, validate_comparison

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "kr-kafka-ffi" / "tests"))
from ctypes_smoke import Config, Span
from python_binding import KrEvent, KrRecord, KrSpan


class Broker(c.Structure):
    _fields_ = [("struct_size", c.c_uint32), ("host", Span), ("port", c.c_uint32)]


def load_library(path):
    library = c.CDLL(str(Path(path).resolve()))
    signatures = {
        "kr_abi_version": ([], c.c_uint32),
        "kr_producer_config_init": ([c.POINTER(Config), c.c_uint32], c.c_int32),
        "kr_producer_create": ([c.POINTER(Config), c.POINTER(c.c_void_p)], c.c_int32),
        "kr_topic_open": ([c.c_void_p, c.c_char_p, c.c_uint32, c.POINTER(c.c_uint32)], c.c_int32),
        "kr_submitv_copy": ([c.c_void_p, c.POINTER(KrRecord), c.c_uint32], c.c_uint32),
        "kr_poll_events": ([c.c_void_p, c.POINTER(KrEvent), c.c_uint32], c.c_uint32),
        "kr_last_error": ([c.c_void_p], c.c_int32),
        "kr_close": ([c.c_void_p, c.c_uint64], c.c_int32),
        "kr_destroy": ([c.c_void_p], None),
    }
    for name, (arguments, result) in signatures.items():
        function = getattr(library, name)
        function.argtypes, function.restype = arguments, result
    if library.kr_abi_version() != 2:
        raise ValueError("benchmark requires C ABI version 2")
    return library


def checked(result, operation):
    if result != 0:
        raise RuntimeError(f"{operation} failed: {result}")


def configuration(library, profile):
    for field in ("rate", "records", "record_bytes", "seed", "partitions", "linger_us", "batch_bytes", "request_bytes", "input_bytes", "delivery_timeout_ms", "request_timeout_ms"):
        if type(profile[field]) is not int:
            raise ValueError(f"{field} must be an integer")
    validate_comparison(profile)
    if profile.get("native_diagnostics", False):
        raise ValueError("C ABI benchmark has no native diagnostics toggle")
    for field, values in {
        "routing": ("hot", "many", "skewed", "unkeyed"),
        "pattern": ("compressible", "incompressible"),
        "compression": ("none", "zstd1", "zstd3"),
        "backend": ("uring", "readiness"),
        "security": ("plaintext", "tls", "plain", "scram256", "scram512"),
    }.items():
        if profile[field] not in values:
            raise ValueError(f"invalid {field}")
    if not 1 <= profile["partitions"] <= 65536 or not 1 <= profile["record_bytes"] <= 1048576:
        raise ValueError("partition/corpus bound exceeded")
    if not (0 <= profile["seed"] < 1 << 64 and 1 <= len(profile["topic"].encode()) <= 249
            and 1 <= profile["input_bytes"] <= 1 << 30 and 1024 <= profile["batch_bytes"] <= 1 << 20
            and profile["record_bytes"] + 128 <= profile["request_bytes"] < 1 << 32
            and 0 <= profile["linger_us"] <= 1000000 and 1 <= profile["request_timeout_ms"] <= 600000
            and profile["request_timeout_ms"] <= profile["delivery_timeout_ms"] <= 3600000):
        raise ValueError("benchmark profile bounds exceeded")
    config = Config()
    checked(library.kr_producer_config_init(c.byref(config), c.sizeof(config)), "config_init")
    # Keep every configuration allocation alive until create copies its spans.
    keep = []

    def span(data):
        owner = c.create_string_buffer(data)
        keep.append(owner)
        return Span(c.addressof(owner), len(data))

    def integer(field, value, bits=32):
        if type(value) is not int or not 0 <= value < 1 << bits:
            raise ValueError(f"{field} cannot fit the ABI")
        setattr(config, field, value)

    endpoints = profile["bootstrap"].split(",")
    if not 1 <= len(endpoints) <= config.brokers_max:
        raise ValueError("bootstrap count exceeds broker bound")
    brokers = (Broker * len(endpoints))()
    keep.append(brokers)
    for target, endpoint in zip(brokers, endpoints):
        host, port = endpoint.rsplit(":", 1)
        host, port = host.strip("[]"), int(port)
        if not host or not 1 <= port <= 65535:
            raise ValueError("invalid bootstrap endpoint")
        target.struct_size, target.host, target.port = c.sizeof(Broker), span(host.encode()), port
    config.bootstrap, config.bootstrap_count = c.addressof(brokers), len(brokers)
    config.client_id = span(b"kr-ffi-open-loop")
    config.transport = 0 if profile["backend"] == "uring" else 1
    config.compression = int(profile["compression"] != "none")
    config.compression_level = 1 if profile["compression"] == "none" else int(profile["compression"][-1])
    integer("linger_max_ns", profile["linger_us"] * 1000, 64)
    config.linger_skip_below_rate = 0
    integer("batch_target_bytes", profile["batch_bytes"])
    for field in ("request_target_bytes", "request_hard_bytes", "batch_hard_bytes"):
        integer(field, profile["request_bytes"])
    integer("input_bytes", profile["input_bytes"], 64)
    integer("delivery_timeout_ns", profile["delivery_timeout_ms"] * 1000000, 64)
    integer("request_timeout_ns", profile["request_timeout_ms"] * 1000000, 64)
    security = profile["security"]
    config.security = 0 if security == "plaintext" else 1 if security == "tls" else 2
    if security != "plaintext":
        roots = (Span * 1)(span(Path(profile["ca_der"]).read_bytes()))
        keep.append(roots)
        config.tls_roots, config.tls_root_count = c.addressof(roots), 1
        config.tls_system_roots = 0
    if config.security == 2:
        config.sasl_mechanism = {"plain": 0, "scram256": 1, "scram512": 2}[security]
        config.username = span(os.environ[profile["username_env"]].encode())
        config.password = span(os.environ[profile["password_env"]].encode())
    return config, keep


class Observations:
    """Bounded live admissions; reject duplicate, suffix, and reordered close events."""
    def __init__(self, rate, capacity):
        self.rate, self.capacity = rate, capacity
        self.pending = {}
        self.accepted = self.acked = self.not_written = self.unknown = self.attempts = 0
        self.closed = False
        self.error = None
        self.offer_delivery, self.submit_delivery, self.ack_delivery = [Histogram() for _ in range(3)]

    def admit(self, index, submitted):
        if index in self.pending or len(self.pending) >= self.capacity or self.closed:
            raise RuntimeError("invalid admission or descriptor bound exceeded")
        self.pending[index] = submitted
        self.accepted += 1

    def observe(self, event, now):
        if event.kind == 1:
            submitted = self.pending.pop(event.user_token, None)
            if submitted is None or self.closed or event.outcome not in (0, 1, 2):
                raise RuntimeError("duplicate/unaccepted delivery or invalid outcome/order")
            elapsed = now - due_ns(event.user_token, self.rate)
            self.offer_delivery.record(elapsed)
            self.submit_delivery.record(now - submitted)
            self.attempts += event.attempts
            if event.outcome == 0:
                self.acked += 1
                self.ack_delivery.record(elapsed)
            elif event.outcome == 1:
                self.not_written += 1
            else:
                self.unknown += 1
        elif event.kind == 6:
            if self.pending or self.closed:
                raise RuntimeError("Closed preceded delivery or was duplicated")
            self.closed = True
        elif event.kind in (5, 7):
            self.error = f"event {event.kind}: reason {event.reason}"
        elif event.kind not in (3, 4):
            raise RuntimeError("unexpected event on copy-only producer")


def run(profile, library_path):
    library = load_library(library_path)
    config, keep = configuration(library, profile)
    values = corpus(profile["record_bytes"], profile["seed"], profile["pattern"] == "incompressible")
    # c_char_p retains the immutable bytes, including embedded NULs. Explicit
    # lengths determine reads; all owners outlive each GIL-releasing copy call.
    pointers = [c.cast(c.c_char_p(value), c.c_void_p) for value in values]
    handle, topic = c.c_void_p(), c.c_uint32()
    observations = Observations(profile["rate"], config.record_descriptors)
    events = (KrEvent * 256)()
    for event in events:
        event.struct_size = c.sizeof(KrEvent)
    checked(library.kr_producer_create(c.byref(config), c.byref(handle)), "create")
    try:
        keep.clear()
        name = profile["topic"].encode()
        checked(library.kr_topic_open(handle, name, len(name), c.byref(topic)), "topic_open")
        deadline = time.monotonic_ns() + profile["delivery_timeout_ms"] * 1000000
        ready = False
        while not ready:
            count = library.kr_poll_events(handle, events, len(events))
            for event in events[:count]:
                if event.kind == 4 and event.topic == topic.value:
                    if event.count != profile["partitions"]:
                        raise RuntimeError("resolved partition count differs from profile")
                    ready = True
                elif event.kind in (5, 6, 7):
                    raise RuntimeError(f"startup event {event.kind}: reason {event.reason}")
            if time.monotonic_ns() >= deadline:
                raise RuntimeError("topic readiness timed out")
            if not ready:
                time.sleep(0.0001)
        lateness, admission, offered_admission = [Histogram() for _ in range(3)]
        rejections = Counter()
        cpu_start, start = time.process_time_ns(), time.monotonic_ns()

        def drain():
            count = library.kr_poll_events(handle, events, len(events))
            for event in events[:count]:
                observations.observe(event, time.monotonic_ns() - start)

        for index in range(profile["records"]):
            due = due_ns(index, profile["rate"])
            while (now := time.monotonic_ns() - start) < due:
                drain()
                time.sleep(min(due - now, 100000) / 1e9)
            drain()
            partition, key = route(profile, index)
            key_owner = c.c_char_p(key)
            record = KrRecord()
            record.struct_size, record.topic = c.sizeof(record), topic.value
            record.partition_hint, record.lane_hint = partition, -1
            record.key_is_null = int(key is None)
            record.key = KrSpan(c.cast(key_owner, c.c_void_p), 0 if key is None else len(key))
            record.value = KrSpan(pointers[index % len(values)], profile["record_bytes"])
            record.user_token = index
            before = time.monotonic_ns() - start
            lateness.record(before - due)
            accepted = library.kr_submitv_copy(handle, c.byref(record), 1)
            after = time.monotonic_ns() - start
            admission.record(after - before)
            if accepted == 1:
                observations.admit(index, before)
                offered_admission.record(after - due)
            elif accepted == 0:
                rejections[str(library.kr_last_error(handle))] += 1
            else:
                raise RuntimeError("ABI returned an impossible accepted prefix")
        offered_elapsed = time.monotonic_ns() - start
        close_error = library.kr_close(handle, profile["delivery_timeout_ms"])
        deadline = time.monotonic_ns() + (profile["delivery_timeout_ms"] + 10000) * 1000000
        while not observations.closed and time.monotonic_ns() < deadline:
            drain()
            if not observations.closed:
                time.sleep(0.0001)
        elapsed = time.monotonic_ns() - start
        cpu_ns = time.process_time_ns() - cpu_start
        report = dict(schema="kr-kafka-open-loop/v1", implementation="kr-kafka-ffi/ctypes-copy",
                      binding_version=platform.python_version(), abi_version=2,
                      library_sha256=hashlib.sha256(Path(library_path).read_bytes()).hexdigest(),
                      profile=profile, matched_settings=public_settings(profile),
                      complete=observations.closed and not observations.pending and not observations.error and close_error == 0,
                      error=observations.error, close_error=close_error,
                      offered=profile["records"], accepted=observations.accepted,
                      rejected=profile["records"] - observations.accepted, rejections_by_reason=dict(rejections),
                      acked=observations.acked, not_written=observations.not_written, unknown=observations.unknown,
                      unresolved=len(observations.pending), attempts_total=observations.attempts,
                      offered_elapsed_ns=offered_elapsed, delivery_elapsed_ns=elapsed,
                      acked_records_per_second=observations.acked * 1e9 / max(1, elapsed),
                      acked_raw_bytes_per_second=observations.acked * profile["record_bytes"] * 1e9 / max(1, elapsed),
                      process_cpu_ns=cpu_ns, cpu_ns_per_ack=cpu_ns / observations.acked if observations.acked else None,
                      process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                      scheduler_lateness=lateness.result(), admission_call=admission.result(),
                      offered_to_admission=offered_admission.result(), offered_to_delivery=observations.offer_delivery.result(),
                      submit_to_delivery=observations.submit_delivery.result(), offered_to_ack=observations.ack_delivery.result(),
                      corpus_bytes=sum(map(len, values)), input_path="one record per copy ABI call",
                      unavailable_metrics=["wire_bytes", "copies", "allocations", "codec_memory", "per_poll_work", "native_completion_diagnostics"])
    finally:
        # Exclusive teardown keeps the actual owner/provider retirement barrier.
        # The outer process-group watchdog bounds even a stuck destroy call.
        library.kr_close(handle, 0)
        destroy_start = time.monotonic_ns()
        library.kr_destroy(handle)
        destroy_ns = time.monotonic_ns() - destroy_start
    report["destroy_elapsed_ns"] = destroy_ns
    return report


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("usage: ffi_open_loop.py PROFILE.json RESULT.json LIBRARY")
    if platform.system() != "Linux":
        raise SystemExit("native benchmark requires Linux")
    result = run(json.loads(Path(sys.argv[1]).read_text()), sys.argv[3])
    Path(sys.argv[2]).write_text(json.dumps(result, indent=2))
    raise SystemExit(0 if result["complete"] else 1)
