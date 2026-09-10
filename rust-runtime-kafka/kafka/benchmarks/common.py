"""Measurement primitives; intentionally independent of Kafka bindings."""
MASK = (1 << 64) - 1


def due_ns(index, rate):
    return index * 1_000_000_000 // rate


def corpus(size, seed, incompressible):
    if not 1 <= size <= 1024 * 1024:
        raise ValueError("record_bytes outside bounded corpus range")
    count = min(1024, max(1, 16 * 1024 * 1024 // size))
    state = seed
    result = []
    for _ in range(count):
        row = bytearray(size)
        for i in range(size):
            state = (state * 6364136223846793005 + 1442695040888963407) & MASK
            row[i] = state >> 56 if incompressible else 97
        result.append(bytes(row))
    return result


class Histogram:
    def __init__(self):
        self.bins = [0] * 1025
        self.count = self.total = self.maximum = 0

    def record(self, value):
        if not 0 <= value <= MASK:
            raise ValueError("histogram value out of range")
        exponent = value.bit_length() - 1
        index = value if value < 16 else 16 + (exponent - 4) * 16 + (value - (1 << exponent)) // (1 << (exponent - 4))
        self.bins[index] += 1
        self.count += 1
        self.total += value
        self.maximum = max(self.maximum, value)

    def quantile(self, fraction):
        rank = (self.count * fraction + 999) // 1000
        if rank == 0:
            return 0
        cumulative = 0
        for index, count in enumerate(self.bins):
            cumulative += count
            if cumulative >= rank:
                exponent, sub = divmod(index - 16, 16)
                upper = index if index < 16 else (1 << (exponent + 4)) + (sub + 1) * (1 << exponent) - 1
                return min(self.maximum, upper)
        raise AssertionError("histogram count mismatch")

    def result(self):
        return dict(count=self.count, mean_ns=self.total / self.count if self.count else 0,
                    p50_ns_upper=self.quantile(500), p99_ns_upper=self.quantile(990),
                    p999_ns_upper=self.quantile(999), max_ns=self.maximum)


def route(profile, index):
    mode = profile["routing"]
    if mode == "hot":
        return 0, None
    if mode == "many":
        return index % profile["partitions"], None
    if mode == "skewed":
        return -1, (index if index % 10 == 0 else 0).to_bytes(8, "big")
    return -1, None


def validate_comparison(profile):
    if profile["linger_us"] % 1000:
        raise ValueError("Java comparison requires whole-millisecond linger")
    if profile["input_bytes"] % 1024:
        raise ValueError("librdkafka input budget must be whole KiB")
    if not 0 < profile["rate"] <= 1_000_000_000 or not 0 < profile["records"] <= 100_000_000:
        raise ValueError("offered load out of bounds")
    if profile["delivery_timeout_ms"] < profile["request_timeout_ms"] + profile["linger_us"] // 1000:
        raise ValueError("Java delivery timeout must include request timeout and linger")


def public_settings(profile):
    return {"acks": "all", "enable.idempotence": True, "max.in.flight.requests.per.connection": 5,
            "compression": profile["compression"], "security": profile["security"],
            "linger_us": profile["linger_us"], "batch_bytes": profile["batch_bytes"],
            "request_bytes": profile["request_bytes"], "input_bytes": profile["input_bytes"]}
