"""Minimal CPython immutable-bytes lease binding exercised by lifetime tests.

Only this object drains events and destroys its producer. It pins immutable
bytes across ctypes calls, which release the GIL. No application callback is
installed in Rust. This is a binding conformance fixture, not a full Python SDK.
"""
import ctypes as c
from dataclasses import dataclass
import threading


class KrSpan(c.Structure):
    _fields_ = [
        ("ptr", c.c_void_p),
        ("len", c.c_uint32),
    ]


class KrHeader(c.Structure):
    _fields_ = [
        ("struct_size", c.c_uint32),
        ("key", KrSpan),
        ("value", KrSpan),
        ("value_is_null", c.c_uint32),
    ]


class KrRecord(c.Structure):
    _fields_ = [
        ("struct_size", c.c_uint32),
        ("topic", c.c_uint32),
        ("partition_hint", c.c_int32),
        ("lane_hint", c.c_int32),
        ("key", KrSpan),
        ("key_is_null", c.c_uint32),
        ("value", KrSpan),
        ("value_is_null", c.c_uint32),
        ("headers", c.c_void_p),
        ("header_count", c.c_uint32),
        ("timestamp_ms", c.c_int64),
        ("user_token", c.c_uint64),
        ("delivery_timeout_ns", c.c_uint64),
    ]


class KrEvent(c.Structure):
    _fields_ = [
        ("struct_size", c.c_uint32),
        ("kind", c.c_uint32),
        ("token", c.c_uint64),
        ("user_token", c.c_uint64),
        ("topic", c.c_uint32),
        ("topic_id", c.c_ubyte * 16),
        ("partition", c.c_int32),
        ("outcome", c.c_uint32),
        ("reason", c.c_uint32),
        ("base_offset", c.c_int64),
        ("base_offset_present", c.c_uint32),
        ("timestamp_ms", c.c_int64),
        ("timestamp_present", c.c_uint32),
        ("attempts", c.c_uint32),
        ("count", c.c_uint32),
    ]


@dataclass(frozen=True)
class PinnedBytes:
    data: bytes

    def __post_init__(self):
        if type(self.data) is not bytes:
            raise TypeError("foreign leases require immutable bytes")


@dataclass
class _Slot:
    owner: PinnedBytes
    lease: c.c_uint64
    view: object


class PyBuffer(c.Structure):
    # Stable ABI member layout; obj is the owned export reference, released only
    # by PyBuffer_Release (using py_object here would incorrectly double-own it).
    _fields_ = [
        ("buf", c.c_void_p), ("obj", c.c_void_p),
        ("len", c.c_ssize_t), ("itemsize", c.c_ssize_t),
        ("readonly", c.c_int), ("ndim", c.c_int), ("format", c.c_void_p),
        ("shape", c.c_void_p), ("strides", c.c_void_p),
        ("suboffsets", c.c_void_p), ("internal", c.c_void_p),
    ]


class Pins:
    def __init__(self, library):
        self.library = library
        self.handle = c.c_void_p()
        # Same fixed maximum as the injected producer, allocated before calls.
        self._pins = [None] * 4
        self._lock = threading.RLock()
        self.destroy_joined = False
        self._bytes_pointer = c.pythonapi.PyBytes_AsString
        self._bytes_pointer.argtypes = [c.py_object]
        self._bytes_pointer.restype = c.c_void_p
        self._get_buffer = c.pythonapi.PyObject_GetBuffer
        self._get_buffer.argtypes = [c.py_object, c.POINTER(PyBuffer), c.c_int]
        self._get_buffer.restype = c.c_int
        self._release_buffer = c.pythonapi.PyBuffer_Release
        self._release_buffer.argtypes = [c.POINTER(PyBuffer)]
        self._release_buffer.restype = None
        signatures = {
            "kr_test_producer_create": ([c.POINTER(c.c_void_p)], c.c_int32),
            "kr_test_resume": ([c.c_void_p], c.c_int32),
            "kr_topic_open": ([c.c_void_p, c.c_char_p, c.c_uint32, c.POINTER(c.c_uint32)], c.c_int32),
            "kr_lease_register": ([c.c_void_p, c.c_void_p, c.c_uint64, c.POINTER(c.c_uint64)], c.c_int32),
            "kr_buffer_release": ([c.c_void_p, c.c_uint64], c.c_int32),
            "kr_submitv_leased": ([c.c_void_p, c.c_uint64, c.POINTER(KrRecord), c.c_uint32], c.c_uint32),
            "kr_poll_events": ([c.c_void_p, c.POINTER(KrEvent), c.c_uint32], c.c_uint32),
            "kr_close": ([c.c_void_p, c.c_uint64], c.c_int32),
            "kr_destroy": ([c.c_void_p], None),
        }
        for name, (args, result) in signatures.items():
            function = getattr(library, name)
            function.argtypes = args
            function.restype = result
        assert library.kr_test_producer_create(c.byref(self.handle)) == 0
        self.topic = c.c_uint32()
        assert library.kr_topic_open(self.handle, b"events", 6, c.byref(self.topic)) == 0

    def register(self, owner):
        if not isinstance(owner, PinnedBytes):
            raise TypeError("register a PinnedBytes owner")
        with self._lock:
            index = self._pins.index(None)
            slot = _Slot(owner, c.c_uint64(), PyBuffer())
            self._pins[index] = slot  # pin before crossing the ABI boundary
            # PyBUF_SIMPLE == 0: contiguous bytes with a strong exporter ref.
            # https://docs.python.org/3/c-api/buffer.html
            assert self._get_buffer(owner.data, c.byref(slot.view), 0) == 0
            assert slot.view.readonly == 1 and slot.view.obj is not None
            pointer = slot.view.buf
            assert pointer == self._bytes_pointer(owner.data)
            result = self.library.kr_lease_register(
                self.handle, pointer, slot.view.len + 1, c.byref(slot.lease))
            if result != 0:
                self._release_buffer(c.byref(slot.view))
                self._pins[index] = None
                raise RuntimeError(result)
            # Any later Python exception leaves the preinstalled pin retained
            # until polling observes release or destroy has actually joined.
            return slot.lease.value, pointer, len(owner.data)

    def submit(self, lease, pointer, length, user):
        with self._lock:
            record = KrRecord()
            record.struct_size = c.sizeof(record)
            record.topic = self.topic.value
            record.partition_hint = record.lane_hint = -1
            record.key_is_null = 1
            record.value = KrSpan(pointer, length)
            record.user_token = user
            return self.library.kr_submitv_leased(self.handle, lease, c.byref(record), 1)

    def release(self, lease):
        with self._lock:
            assert self.library.kr_buffer_release(self.handle, lease) == 0
            # Keep the strong pin until the release event, irrespective of return.

    def poll(self):
        with self._lock:
            events = (KrEvent * 16)()
            for event in events:
                event.struct_size = c.sizeof(KrEvent)
            count = self.library.kr_poll_events(self.handle, events, len(events))
            out = list(events[:count])
            for event in out:
                if event.kind == 2:
                    for index, slot in enumerate(self._pins):
                        if slot is not None and slot.lease.value == event.token:
                            self._release_buffer(c.byref(slot.view))
                            self._pins[index] = None
                            break
            return out

    def resume_close(self):
        with self._lock:
            assert self.library.kr_close(self.handle, 0) == 0
            assert self.library.kr_test_resume(self.handle) == 0

    def destroy(self):
        with self._lock:
            if self.handle.value is not None:
                self.library.kr_destroy(self.handle)
                self.handle = c.c_void_p()
                self.destroy_joined = True
                for slot in self._pins:
                    if slot is not None and slot.view.obj is not None:
                        self._release_buffer(c.byref(slot.view))
                self._pins.clear()  # only after the actual owner join
