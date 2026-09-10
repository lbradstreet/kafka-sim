#!/usr/bin/env python3
"""Open-loop benchmark of the actual confluent-kafka Python/librdkafka binding."""
import json
import os
import resource
import sys
import time
from collections import Counter
from common import Histogram, corpus, due_ns, public_settings, route, validate_comparison


def run(profile):
    import confluent_kafka
    validate_comparison(profile)
    settings = {
        "bootstrap.servers": profile["bootstrap"], "client.id": "librdkafka-open-loop",
        "acks": "all", "enable.idempotence": True, "max.in.flight.requests.per.connection": 5,
        "compression.type": "none" if profile["compression"] == "none" else "zstd",
        "linger.ms": profile["linger_us"] / 1000,
        "batch.size": profile["batch_bytes"], "message.max.bytes": profile["request_bytes"],
        "queue.buffering.max.kbytes": profile["input_bytes"] // 1024,
        "queue.buffering.max.messages": 1_000_000,
        "delivery.timeout.ms": profile["delivery_timeout_ms"],
        "request.timeout.ms": profile["request_timeout_ms"],
        "allow.auto.create.topics": False, "partitioner": "murmur2_random",
    }
    if profile["compression"] != "none":
        settings["compression.level"] = int(profile["compression"][-1])
    security = profile["security"]
    if security != "plaintext":
        settings.update({"security.protocol": "SSL" if security == "tls" else "SASL_SSL",
                         "ssl.ca.location": profile["ca_pem"],
                         "enable.ssl.certificate.verification": True,
                         "ssl.endpoint.identification.algorithm": "https"})
        if security != "tls":
            settings.update({"sasl.mechanism": {"plain": "PLAIN", "scram256": "SCRAM-SHA-256", "scram512": "SCRAM-SHA-512"}[security],
                             "sasl.username": os.environ[profile["username_env"]],
                             "sasl.password": os.environ[profile["password_env"]]})
    producer = confluent_kafka.Producer(settings)
    metadata = producer.list_topics(profile["topic"], timeout=profile["delivery_timeout_ms"] / 1000)
    topic = metadata.topics.get(profile["topic"])
    if topic is None or topic.error is not None or len(topic.partitions) != profile["partitions"]:
        raise RuntimeError("topic missing, errored, or partition count differs from profile")
    values = corpus(profile["record_bytes"], profile["seed"], profile["pattern"] == "incompressible")
    lateness, admission, offer_delivery, submit_delivery, ack_delivery = [Histogram() for _ in range(5)]
    offered_admission = Histogram()
    accepted = acked = failed = completed = 0
    reasons = Counter()
    peak_queue = 0
    cpu_start = time.process_time_ns()
    start = time.monotonic_ns()

    def callback(index, submitted):
        def delivered(error, _message):
            nonlocal acked, failed, completed
            now = time.monotonic_ns() - start
            elapsed = now - due_ns(index, profile["rate"])
            offer_delivery.record(elapsed)
            submit_delivery.record(now - submitted)
            completed += 1
            if error is None:
                acked += 1
                ack_delivery.record(elapsed)
            else:
                # Binding error codes do not prove all attempts uncommitted.
                failed += 1
        return delivered

    for index in range(profile["records"]):
        due = due_ns(index, profile["rate"])
        while (now := time.monotonic_ns() - start) < due:
            producer.poll(0)
            time.sleep(min(due - now, 100_000) / 1e9)
        producer.poll(0)
        partition, key = route(profile, index)
        before = time.monotonic_ns() - start
        lateness.record(before - due)
        try:
            producer.produce(profile["topic"], value=values[index % len(values)], key=key,
                             partition=partition, timestamp=0, on_delivery=callback(index, before))
            accepted += 1
            offered_admission.record(time.monotonic_ns() - start - due)
        except (BufferError, confluent_kafka.KafkaException) as error:
            reasons[type(error).__name__] += 1
        admission.record(time.monotonic_ns() - start - before)
        peak_queue = max(peak_queue, len(producer))
    offered_elapsed = time.monotonic_ns() - start
    left = producer.flush(profile["delivery_timeout_ms"] / 1000)
    elapsed = time.monotonic_ns() - start
    cpu_ns = time.process_time_ns() - cpu_start
    return dict(schema="kr-kafka-open-loop/v1", implementation="confluent-kafka-python/librdkafka",
                binding_version=confluent_kafka.version(), native_version=confluent_kafka.libversion(),
                profile=profile, matched_settings=public_settings(profile), complete=left == 0 and completed == accepted,
                offered=profile["records"], accepted=accepted, rejected=profile["records"] - accepted,
                rejections_by_reason=dict(reasons), acked=acked, failed_unclassified=failed, unresolved=accepted - completed,
                offered_elapsed_ns=offered_elapsed, delivery_elapsed_ns=elapsed,
                acked_records_per_second=acked * 1e9 / max(1, elapsed),
                acked_raw_bytes_per_second=acked * profile["record_bytes"] * 1e9 / max(1, elapsed),
                process_cpu_ns=cpu_ns, cpu_ns_per_ack=cpu_ns / acked if acked else None,
                process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                scheduler_lateness=lateness.result(), admission_call=admission.result(),
                offered_to_admission=offered_admission.result(),
                offered_to_delivery=offer_delivery.result(), submit_to_delivery=submit_delivery.result(),
                offered_to_ack=ack_delivery.result(), sampled_queue_peak=peak_queue,
                corpus_bytes=sum(map(len, values)),
                unavailable_metrics=["not_written_vs_unknown", "wire_bytes", "copies", "allocations", "codec_memory", "per_poll_work"])


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: librdkafka_open_loop.py PROFILE.json RESULT.json")
    with open(sys.argv[1], encoding="utf-8") as source:
        report = run(json.load(source))
    with open(sys.argv[2], "w", encoding="utf-8") as output:
        json.dump(report, output, indent=2)
    raise SystemExit(0 if report["complete"] else 1)
