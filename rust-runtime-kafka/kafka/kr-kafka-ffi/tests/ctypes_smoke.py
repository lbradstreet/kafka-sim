#!/usr/bin/env python3
"""Load the real shared library and validate ctypes layout / rejected startup.

Full foreign pinning proof is exercised separately in binding_lifetime.py.
"""
import ctypes as c
from pathlib import Path
import sys


class Span(c.Structure):
    _fields_ = [("ptr", c.c_void_p), ("len", c.c_uint32)]


class Config(c.Structure):
    _fields_ = [
        ("struct_size", c.c_uint32),
        ("delivery_timeout_ns", c.c_uint64),
        ("request_timeout_ns", c.c_uint64),
        ("linger_max_ns", c.c_uint64),
        ("metadata_max_age_ns", c.c_uint64),
        ("topic_resolve_timeout_ns", c.c_uint64),
        ("retry_backoff_min_ns", c.c_uint64),
        ("retry_backoff_max_ns", c.c_uint64),
        ("input_bytes", c.c_uint64),
        ("compressed_bytes", c.c_uint64),
        ("control_reserve_bytes", c.c_uint64),
        ("codec_workspace_bytes", c.c_uint64),
        ("max_in_flight_per_connection", c.c_uint32),
        ("lanes", c.c_uint32),
        ("codec_contexts", c.c_uint32),
        ("max_attempts", c.c_uint32),
        ("request_max_partitions", c.c_uint32),
        ("brokers_max", c.c_uint32),
        ("worker_jobs", c.c_uint32),
        ("connection_wire_window_bytes", c.c_uint32),
        ("batch_target_bytes", c.c_uint32),
        ("batch_hard_bytes", c.c_uint32),
        ("request_target_bytes", c.c_uint32),
        ("request_hard_bytes", c.c_uint32),
        ("record_descriptors", c.c_uint32),
        ("staging_bytes_per_connection", c.c_uint32),
        ("rx_bytes_per_connection", c.c_uint32),
        ("delivery_event_capacity", c.c_uint32),
        ("release_event_capacity", c.c_uint32),
        ("mailbox_capacity", c.c_uint32),
        ("max_open_topics", c.c_uint32),
        ("pending_records_per_topic", c.c_uint32),
        ("max_live_leases", c.c_uint32),
        ("max_batches", c.c_uint32),
        ("codec_window_log", c.c_uint32),
        ("output_chunk_bytes", c.c_uint32),
        ("progressive_threshold", c.c_uint32),
        ("tls_plaintext_bytes", c.c_uint32),
        ("tls_ciphertext_bytes", c.c_uint32),
        ("max_header_count", c.c_uint32),
        ("max_submissions_per_poll", c.c_uint32),
        ("max_submission_records", c.c_uint32),
        ("max_completions_per_poll", c.c_uint32),
        ("sim_encode_bytes_per_poll", c.c_uint32),
        ("target_poll_ms", c.c_uint32),
        ("coalesce_below_bytes", c.c_uint32),
        ("linger_skip_below_rate", c.c_uint32),
        ("unkeyed_policy", c.c_uint32),
        ("unkeyed_run_bytes", c.c_uint32),
        ("partitioner", c.c_uint32),
        ("compression", c.c_uint32),
        ("compression_level", c.c_uint32),
        ("transport", c.c_uint32),
        ("security", c.c_uint32),
        ("sasl_mechanism", c.c_uint32),
        ("tls_system_roots", c.c_uint32),
        ("client_id", Span),
        ("bootstrap", c.c_void_p),
        ("bootstrap_count", c.c_uint32),
        ("tls_roots", c.c_void_p),
        ("tls_root_count", c.c_uint32),
        ("tls_server_name", Span),
        ("username", Span),
        ("password", Span), ("batch_target_mode", c.c_uint32),
        ("request_batching_policy", c.c_uint32), ("reserved_request_policy", c.c_uint32),
    ]


def main():
    library = c.CDLL(str(Path(sys.argv[1]).resolve()))
    library.kr_abi_version.restype = c.c_uint32
    library.kr_producer_config_init.argtypes = [c.POINTER(Config), c.c_uint32]
    library.kr_producer_config_init.restype = c.c_int32
    library.kr_producer_create.argtypes = [c.POINTER(Config), c.POINTER(c.c_void_p)]
    library.kr_producer_create.restype = c.c_int32
    library.kr_last_error.argtypes = [c.c_void_p]
    library.kr_last_error.restype = c.c_int32
    library.kr_destroy.argtypes = [c.c_void_p]
    library.kr_destroy.restype = None
    assert library.kr_abi_version() == 4
    config = Config()
    assert library.kr_producer_config_init(c.byref(config), c.sizeof(config)) == 0
    assert config.struct_size == c.sizeof(config)
    assert config.delivery_timeout_ns > config.request_timeout_ns
    assert config.max_live_leases > 0
    assert config.bootstrap is None and config.bootstrap_count == 0
    assert config.password.ptr is None and config.password.len == 0
    handle = c.c_void_p(1)
    config.struct_size -= 1
    assert library.kr_producer_create(c.byref(config), c.byref(handle)) == -2
    assert handle.value is None
    config.struct_size += 1
    config.client_id = Span(None, 1)
    assert library.kr_producer_create(c.byref(config), c.byref(handle)) == -1
    assert handle.value is None
    assert library.kr_last_error(None) == -1
    library.kr_destroy(None)
    print("ctypes ABI smoke passed")


if __name__ == "__main__":
    main()
