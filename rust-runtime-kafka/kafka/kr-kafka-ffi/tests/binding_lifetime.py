#!/usr/bin/env python3
"""Actual CPython GC / immutable-buffer pin tests against the Rust shared library."""
import ctypes as c
import gc
from pathlib import Path
import sys
import time
import weakref

from python_binding import PinnedBytes, Pins


def release_event_retires_python_pin(library):
    pins = Pins(library)
    finalized = []
    owner = PinnedBytes(bytes(bytearray(b"payload\x00with-null")))
    reference = weakref.ref(owner)
    weakref.finalize(owner, finalized.append, "released")
    lease, pointer, length = pins.register(owner)
    assert pointer == pins._bytes_pointer(owner.data)  # no ctypes payload copy
    assert pins.submit(lease, pointer, length, 1001) == 1
    del owner
    gc.collect()
    assert reference() is not None and not finalized
    pins.release(lease)
    assert pins.poll() == []  # accepted record still held by paused owner
    gc.collect()
    assert reference() is not None and not finalized
    pins.resume_close()
    events = []
    deadline = time.monotonic() + 3
    while not any(event.kind == 6 for event in events):
        assert time.monotonic() < deadline, "producer did not close"
        events.extend(pins.poll())
        time.sleep(0.001)
    gc.collect()
    assert reference() is None and finalized == ["released"]
    assert sum(event.kind == 2 and event.token == lease for event in events) == 1
    assert sum(event.kind == 1 and event.user_token == 1001 for event in events) == 1
    pins.destroy()


def destroy_keeps_python_pin_through_join(library):
    pins = Pins(library)
    finalized = []
    owner = PinnedBytes(bytes(bytearray(b"held-through-destroy")))
    reference = weakref.ref(owner)
    weakref.finalize(owner, lambda: finalized.append(pins.destroy_joined))
    lease, pointer, length = pins.register(owner)
    assert pins.submit(lease, pointer, length, 1002) == 1
    del owner
    gc.collect()
    assert reference() is not None
    pins.destroy()  # no explicit release/event drain; hook resumes the real owner
    gc.collect()
    assert reference() is None and finalized == [True]


def mutable_sources_are_rejected():
    try:
        PinnedBytes(bytearray(b"mutable"))
    except TypeError:
        pass
    else:
        raise AssertionError("mutable foreign source accepted")


def rejected_registration_releases_buffer_export(library):
    pins = Pins(library)
    owner = PinnedBytes(bytes(bytearray(64 * 1024)))
    reference = weakref.ref(owner)
    try:
        pins.register(owner)
    except RuntimeError as error:
        assert error.args == (-3,)  # full native InputBytes bound includes metadata
    else:
        raise AssertionError("oversized foreign registration admitted")
    del owner
    gc.collect()
    assert reference() is None
    pins.destroy()


if __name__ == "__main__":
    library = c.CDLL(str(Path(sys.argv[1]).resolve()))
    mutable_sources_are_rejected()
    release_event_retires_python_pin(library)
    destroy_keeps_python_pin_through_join(library)
    rejected_registration_releases_buffer_export(library)
    print("Python pinning/lifetime tests passed (4)")
