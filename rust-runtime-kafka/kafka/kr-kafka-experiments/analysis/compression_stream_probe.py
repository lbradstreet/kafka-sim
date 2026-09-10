#!/usr/bin/env python3
"""Bounded zstd API probe; synthetic bytes, not Kafka traffic or a CPU benchmark.

Uses an explicitly selected installed libzstd, without third-party Python modules.
Compares input chunking with forced flushes, recording emitted-byte visibility.
Every resulting frame is decoded and compared with its complete input.
"""
import argparse
import ctypes as c
import hashlib
import json
from pathlib import Path


class InBuffer(c.Structure):
    _fields_ = [("src", c.c_void_p), ("size", c.c_size_t), ("pos", c.c_size_t)]


class OutBuffer(c.Structure):
    _fields_ = [("dst", c.c_void_p), ("size", c.c_size_t), ("pos", c.c_size_t)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", type=Path, required=True)
    args = parser.parse_args()
    library = args.library.resolve(strict=True)
    lib = c.CDLL(str(library))

    def bind(name, result, *parameters):
        function = getattr(lib, name)
        function.restype = result
        function.argtypes = list(parameters)
        return function

    version = bind("ZSTD_versionString", c.c_char_p)
    create = bind("ZSTD_createCCtx", c.c_void_p)
    free = bind("ZSTD_freeCCtx", c.c_size_t, c.c_void_p)
    parameter = bind("ZSTD_CCtx_setParameter", c.c_size_t, c.c_void_p, c.c_int, c.c_int)
    stream = bind("ZSTD_compressStream2", c.c_size_t, c.c_void_p,
                  c.POINTER(OutBuffer), c.POINTER(InBuffer), c.c_int)
    sizeof = bind("ZSTD_sizeof_CCtx", c.c_size_t, c.c_void_p)
    is_error = bind("ZSTD_isError", c.c_uint, c.c_size_t)
    error_name = bind("ZSTD_getErrorName", c.c_char_p, c.c_size_t)
    decompress = bind("ZSTD_decompress", c.c_size_t,
                      c.c_void_p, c.c_size_t, c.c_void_p, c.c_size_t)

    def checked(result):
        if is_error(result):
            raise RuntimeError(error_name(result).decode())
        return result

    def run(payload, chunk_size, flush_each):
        context = create()
        if not context:
            raise MemoryError("ZSTD_createCCtx")
        output = c.create_string_buffer(256 * 1024)
        frames = []
        emitted = 0
        samples = []
        first_output_at = None
        peak_workspace = sizeof(context)
        try:
            # Stable zstd parameter IDs: level, windowLog, contentSizeFlag,
            # checksumFlag, nbWorkers. Match the native producer's defaults.
            for key, value in [(100, 1), (101, 20), (200, 0), (201, 0), (400, 0)]:
                checked(parameter(context, key, value))

            def step(data, directive):
                nonlocal emitted, peak_workspace
                source = c.create_string_buffer(data)
                input_buffer = InBuffer(c.cast(source, c.c_void_p), len(data), 0)
                for _ in range(1024):
                    out = OutBuffer(c.cast(output, c.c_void_p), len(output), 0)
                    remaining = checked(stream(context, c.byref(out), c.byref(input_buffer), directive))
                    frames.append(output.raw[:out.pos])
                    emitted += out.pos
                    peak_workspace = max(peak_workspace, sizeof(context))
                    if input_buffer.pos == input_buffer.size and (directive == 0 or remaining == 0):
                        return
                raise RuntimeError("bounded stream loop failed to finish")

            for offset in range(0, len(payload), chunk_size):
                part = payload[offset:offset + chunk_size]
                step(part, 0)  # ZSTD_e_continue
                if flush_each:
                    step(b"", 1)  # ZSTD_e_flush, retaining the same frame
                input_bytes = offset + len(part)
                if emitted and first_output_at is None:
                    first_output_at = input_bytes
                if input_bytes in {2048, 16384, 65536, 131072, len(payload)}:
                    samples.append({"input_bytes": input_bytes, "emitted_bytes": emitted})
            before_end = emitted
            step(b"", 2)  # ZSTD_e_end
            compressed = b"".join(frames)
            decoded = c.create_string_buffer(len(payload))
            compressed_buffer = c.create_string_buffer(compressed)
            length = checked(decompress(decoded, len(payload), compressed_buffer, len(compressed)))
            if length != len(payload) or decoded.raw != payload:
                raise RuntimeError("round-trip mismatch")
            return {"chunk_bytes": chunk_size, "flush_each_chunk": flush_each,
                    "bytes_before_end": before_end, "final_bytes": emitted,
                    "first_output_at_input_bytes": first_output_at,
                    "peak_context_bytes": peak_workspace,
                    "compressed_sha256": hashlib.sha256(compressed).hexdigest(),
                    "round_trip": True, "samples": samples}
        finally:
            checked(free(context))

    results = []
    for length in [64 * 1024, 256 * 1024]:
        repeated = b"event=payment region=ap-southeast-1 status=accepted\n"
        corpora = {
            "repeated": (repeated * (length // len(repeated) + 1))[:length],
            "sha256_blocks": b"".join(hashlib.sha256(i.to_bytes(8, "little")).digest()
                                       for i in range(length // 32)),
        }
        for name, payload in corpora.items():
            if len(payload) != length:
                raise RuntimeError("incorrect corpus length")
            runs = [run(payload, chunk, flush)
                    for chunk, flush in [(2048, False), (16384, False), (2048, True)]]
            results.append({"corpus": name, "input_bytes": length,
                            "input_sha256": hashlib.sha256(payload).hexdigest(), "runs": runs})
    print(json.dumps({"kind": "synthetic-zstd-stream-probe/v1", "library": str(library),
                      "zstd_version": version().decode(), "results": results}, indent=2))


if __name__ == "__main__":
    main()
