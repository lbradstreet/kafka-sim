#define _DEFAULT_SOURCE
#define _DARWIN_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#include "kr_kafka.h"
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

/* Real production ABI only. Run against an isolated four-partition topic.
 * Application-owned mappings become unreadable immediately at InputReleased;
 * they remain mapped until destroy returns, detecting premature release/UAF.
 * The Java verifier independently compares every accepted record with Kafka. */
enum { RECORDS = 192, COHORT = 64, HEADERS = 5, PAYLOAD = 256, SLAB = 1024 };
static const char *header_keys[HEADERS] = {
    "kr-check-run", "kr-check-id", "duplicate", "duplicate", "binary"
};
typedef struct {
    uint8_t key[128], value[PAYLOAD], binary[3];
    char id[24];
    uint32_t key_len, value_len;
    bool key_null, value_null;
} payload;
typedef struct {
    bool accepted, delivered, released, release_requested;
    uint64_t lease, release_index, delivery_index;
    void *foreign;
    kr_event delivery;
} history;
typedef struct {
    kr_producer *producer;
    uint32_t topic;
    uint8_t topic_id[16];
    bool ready, close_requested, destroyed;
    unsigned accepted, deliveries, releases, closed, flush_done;
    uint64_t flush_token, event_index, retries, destroy_ns;
    int64_t timestamp;
    size_t page_bytes;
    const char *backend, *host, *topic_name, *run_id, *output;
    uint32_t port;
    char error[256];
    history rows[RECORDS];
} harness;

static uint64_t monotonic_ns(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) abort();
    return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}
static void pause_poll(void) {
    struct timespec delay = {.tv_sec = 0, .tv_nsec = 1000000};
    while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {}
}
static bool fail(harness *h, const char *message) {
    if (!h->error[0]) snprintf(h->error, sizeof(h->error), "%s", message);
    return false;
}
static bool okay(harness *h, int32_t code, const char *operation) {
    if (code == KR_OK) return true;
    if (!h->error[0]) snprintf(h->error, sizeof(h->error), "%s returned %" PRId32, operation, code);
    return false;
}
static bool safe_name(const char *text, size_t maximum) {
    size_t length = strlen(text);
    if (length == 0 || length > maximum) return false;
    for (size_t i = 0; i < length; ++i) {
        unsigned char c = (unsigned char)text[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.')) return false;
    }
    return true;
}
static payload expected(const harness *h, unsigned id) {
    payload p = {0};
    p.key_null = id % 11 == 0;
    p.value_null = id % 17 == 0;
    snprintf(p.id, sizeof(p.id), "%u", id);
    if (!p.key_null && id % 13 != 0) {
        int n = snprintf((char *)p.key, sizeof(p.key), "ffi-key-%s-%u", h->run_id, id);
        if (n < 0 || (size_t)n >= sizeof(p.key)) abort();
        p.key_len = (uint32_t)n;
    }
    if (!p.value_null && id % 19 != 0) {
        p.value_len = PAYLOAD;
        for (unsigned i = 0; i < PAYLOAD; ++i) p.value[i] = (uint8_t)(id * 31 + i * 17);
    }
    p.binary[0] = 0; p.binary[1] = 255; p.binary[2] = (uint8_t)id;
    return p;
}
static kr_span append(uint8_t *slab, size_t *used, const void *bytes, size_t length) {
    if (length > SLAB - *used) abort();
    kr_span span = {.ptr = slab + *used, .len = (uint32_t)length};
    if (length) memcpy(slab + *used, bytes, length);
    *used += length;
    return span;
}
static kr_record input(const harness *h, unsigned id, uint8_t *slab, kr_header headers[HEADERS], uint32_t *used_out) {
    payload p = expected(h, id);
    size_t used = 0;
    kr_record r = {0};
    r.struct_size = sizeof(r); r.topic = h->topic;
    r.partition_hint = (int32_t)(id % 4); r.lane_hint = 0;
    r.key = append(slab, &used, p.key, p.key_len); r.key_is_null = p.key_null;
    r.value = append(slab, &used, p.value, p.value_len); r.value_is_null = p.value_null;
    for (unsigned i = 0; i < HEADERS; ++i) {
        headers[i] = (kr_header){.struct_size = sizeof(kr_header)};
        headers[i].key = append(slab, &used, header_keys[i], strlen(header_keys[i]));
        const void *bytes = NULL; size_t length = 0;
        if (i == 0) {bytes = h->run_id; length = strlen(h->run_id);}
        if (i == 1) {bytes = p.id; length = strlen(p.id);}
        if (i == 4) {bytes = p.binary; length = sizeof(p.binary);}
        headers[i].value = append(slab, &used, bytes, length);
        headers[i].value_is_null = i == 2;
    }
    r.headers = headers; r.header_count = HEADERS;
    r.timestamp_ms = h->timestamp + (int64_t)id; r.user_token = id;
    *used_out = (uint32_t)used;
    return r;
}
static bool tick(harness *h) {
    kr_event events[64] = {0};
    for (unsigned i = 0; i < 64; ++i) events[i].struct_size = sizeof(kr_event);
    uint32_t count = kr_poll_events(h->producer, events, 64);
    if (!okay(h, kr_last_error(h->producer), "poll events")) return false;
    if (count > 64) return fail(h, "event count exceeded output capacity");
    for (uint32_t i = 0; i < count; ++i) {
        kr_event e = events[i]; ++h->event_index;
        if (e.kind == KR_EVENT_TOPIC_READY) {
            if (e.topic != h->topic || e.count != 4 ||
                (h->ready && memcmp(h->topic_id, e.topic_id, 16))) return fail(h, "topic identity/count changed");
            h->ready = true; memcpy(h->topic_id, e.topic_id, 16);
        } else if (e.kind == KR_EVENT_DELIVERY) {
            if (e.user_token >= RECORDS) return fail(h, "delivery for unknown record");
            history *row = &h->rows[e.user_token];
            if (!row->accepted || row->delivered || e.topic != h->topic || e.token == 0 ||
                e.partition != (int32_t)(e.user_token % 4) || e.outcome != KR_DELIVERY_ACKED ||
                e.reason != KR_REASON_NONE || e.attempts == 0 ||
                !e.base_offset_present || e.base_offset < 0 || memcmp(e.topic_id, h->topic_id, 16))
                return fail(h, "duplicate, foreign, or non-Acked delivery");
            for (unsigned j = 0; j < RECORDS; ++j)
                if (h->rows[j].delivered && h->rows[j].delivery.token == e.token) return fail(h, "duplicate record token");
            row->delivered = true; row->delivery = e; row->delivery_index = h->event_index; ++h->deliveries;
        } else if (e.kind == KR_EVENT_INPUT_RELEASED) {
            history *row = NULL;
            for (unsigned j = COHORT; j < RECORDS; ++j) if (h->rows[j].lease == e.token) row = &h->rows[j];
            if (!row || !row->release_requested || row->released) return fail(h, "duplicate or unrequested input release");
            row->released = true; row->release_index = h->event_index; ++h->releases;
            if (row->foreign && mprotect(row->foreign, h->page_bytes, PROT_NONE) != 0)
                return fail(h, "foreign release mprotect failed");
        } else if (e.kind == KR_EVENT_FLUSH_DONE) {
            if (e.token != h->flush_token || ++h->flush_done != 1 || h->deliveries != RECORDS)
                return fail(h, "duplicate, premature or unrequested flush");
        } else if (e.kind == KR_EVENT_CLOSED) {
            if (!h->close_requested || ++h->closed != 1 || e.count != 0 ||
                h->deliveries != RECORDS || h->releases != 2 * COHORT || h->flush_done != 1)
                return fail(h, "premature/duplicate close or unresolved healthy records");
        } else {
            return fail(h, "unexpected topic failure, fatal or event kind");
        }
    }
    return true;
}
static bool submit(harness *h, unsigned id) {
    history *row = &h->rows[id];
    uint8_t copy[SLAB], *slab = copy;
    uint64_t deadline = monotonic_ns() + UINT64_C(30000000000);
    if (id >= 2 * COHORT) {
        row->foreign = mmap(NULL, h->page_bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (row->foreign == MAP_FAILED) {row->foreign = NULL; return fail(h, "mmap foreign input failed");}
        slab = row->foreign;
    } else if (id >= COHORT) {
        while (true) {
            int32_t code = kr_buffer_acquire(h->producer, SLAB, &slab, &row->lease);
            if (code == KR_OK) break;
            if (code != KR_ERR_EXHAUSTED) return okay(h, code, "acquire native input");
            if (monotonic_ns() >= deadline || !tick(h)) return fail(h, "native acquire deadline");
            ++h->retries; pause_poll();
        }
    }
    kr_header headers[HEADERS]; uint32_t used;
    kr_record record = input(h, id, slab, headers, &used);
    if (id >= 2 * COHORT) {
        if (mprotect(slab, h->page_bytes, PROT_READ) != 0) return fail(h, "mprotect immutable foreign input failed");
        if (!okay(h, kr_lease_register(h->producer, slab, h->page_bytes, &row->lease), "register foreign input")) return false;
    } else if (id >= COHORT) {
        if (!okay(h, kr_buffer_commit(h->producer, row->lease, used), "commit native input")) return false;
    }
    while (true) {
        uint32_t count = id < COHORT ? kr_submitv_copy(h->producer, &record, 1)
            : kr_submitv_leased(h->producer, row->lease, &record, 1);
        if (count == 1) break;
        if (count != 0) return fail(h, "singleton admission exceeded one record");
        int32_t code = kr_last_error(h->producer);
        if (code != KR_ERR_EXHAUSTED) return okay(h, code == KR_OK ? KR_ERR_FAILED : code, "submit");
        if (monotonic_ns() >= deadline || !tick(h)) return fail(h, "admission deadline");
        ++h->retries; pause_poll();
    }
    row->accepted = true; ++h->accepted;
    if (id < COHORT) memset(copy, 0xee, sizeof(copy)); /* copy must own its input now */
    else {
        if (!okay(h, kr_buffer_release(h->producer, row->lease), "release input")) return false;
        row->release_requested = true;
    }
    return tick(h);
}
static bool exercise(harness *h) {
    kr_producer_config config;
    if (!okay(h, kr_producer_config_init(&config, sizeof(config)), "config init")) return false;
    config.transport = strcmp(h->backend, "uring") == 0 ? 0 : 1;
    config.security = 0; config.compression = 1; config.compression_level = 1;
    config.lanes = 1; config.codec_contexts = 1; config.brokers_max = 4;
    config.record_descriptors = 256; config.delivery_event_capacity = 256;
    config.pending_records_per_topic = 256; config.max_batches = 64;
    config.max_live_leases = 128; config.release_event_capacity = 128;
    config.max_open_topics = 4; config.mailbox_capacity = 32; config.max_submission_records = 64;
    config.request_max_partitions = 4; config.input_bytes = 4 * 1024 * 1024;
    config.compressed_bytes = 8 * 1024 * 1024;
    config.delivery_timeout_ns = UINT64_C(30000000000); config.request_timeout_ns = UINT64_C(3000000000);
    config.metadata_max_age_ns = UINT64_C(100000000);
    kr_broker broker = {.struct_size = sizeof(kr_broker), .host = {(const uint8_t *)h->host, (uint32_t)strlen(h->host)}, .port = h->port};
    config.bootstrap = &broker; config.bootstrap_count = 1;
    config.client_id = (kr_span){(const uint8_t *)h->run_id, (uint32_t)strlen(h->run_id)};
    if (!okay(h, kr_producer_create(&config, &h->producer), "create producer")) return false;
    if (!okay(h, kr_topic_open(h->producer, h->topic_name, (uint32_t)strlen(h->topic_name), &h->topic), "open topic")) return false;
    uint64_t deadline = monotonic_ns() + UINT64_C(60000000000);
    while (!h->ready) {
        if (!tick(h)) return false;
        if (monotonic_ns() >= deadline) return fail(h, "topic readiness deadline");
        if (!h->ready) pause_poll();
    }
    uint8_t id[16];
    if (!okay(h, kr_topic_id(h->producer, h->topic, id), "read topic ID")) return false;
    if (memcmp(id, h->topic_id, 16)) return fail(h, "topic ID query differs from event");
    for (unsigned i = 0; i < RECORDS; ++i) if (!submit(h, i)) return false;
    if (!okay(h, kr_flush(h->producer, &h->flush_token), "flush")) return false;
    if (!okay(h, kr_close(h->producer, 30000), "close")) return false;
    h->close_requested = true;
    deadline = monotonic_ns() + UINT64_C(60000000000);
    while (!h->closed) {
        if (!tick(h)) return false;
        if (monotonic_ns() >= deadline) return fail(h, "close deadline");
        if (!h->closed) pause_poll();
    }
    return tick(h); /* A duplicate terminal event must not escape the check. */
}
static void quoted(FILE *out, const char *text) {
    fputc('"', out);
    for (const unsigned char *p = (const unsigned char *)text; *p; ++p) {
        if (*p == '"' || *p == '\\') {fputc('\\', out); fputc(*p, out);}
        else if (*p < 32) fprintf(out, "\\u%04x", *p);
        else fputc(*p, out);
    }
    fputc('"', out);
}
static void hex(FILE *out, const void *bytes, size_t length, bool absent) {
    if (absent) {fputs("null", out); return;}
    const uint8_t *p = bytes; fputc('"', out);
    for (size_t i = 0; i < length; ++i) fprintf(out, "%02x", p[i]);
    fputc('"', out);
}
static bool ledger(harness *h) {
    FILE *out = fopen(h->output, "w");
    if (!out) return fail(h, "open ledger output failed");
    fprintf(out, "{\"schema\":\"kr-kafka-producer-check/v1\",\"run_id\":"); quoted(out, h->run_id);
    fputs(",\"topic\":", out); quoted(out, h->topic_name);
    fputs(",\"config\":{\"run_id\":", out); quoted(out, h->run_id);
    fputs(",\"scenario\":\"basic\",\"lanes\":1,\"input_mode\":\"mixed_ffi\",\"profile\":{\"topic\":", out); quoted(out, h->topic_name);
    fprintf(out, ",\"partitions\":4,\"records\":%u,\"security\":\"plaintext\"}},\"complete\":%s,\"closed\":%s,\"joined\":%s,\"closed_unresolved\":0,\"error\":", RECORDS,
        !h->error[0] && h->destroyed ? "true" : "false", h->closed == 1 ? "true" : "false", h->destroyed ? "true" : "false");
    if (h->error[0]) quoted(out, h->error); else fputs("null", out);
    fputs(",\"backend\":", out); quoted(out, h->backend);
    fprintf(out, ",\"write_mode\":\"staging\",\"checkpoint\":\"closed\",\"final_credits\":[],\"limitations\":[\"The C ABI exposes no credit-pool snapshot; exact release/close events and destroy return are checked.\",\"Record tokens first become observable in Delivery; acceptance is recorded independently from submit counts.\"],\"destroy_ns\":%" PRIu64 ",\"rejected_attempts\":%" PRIu64 ",\"topic_ids\":{", h->destroy_ns, h->retries);
    if (h->ready) {fputs("\"0\":", out); hex(out, h->topic_id, 16, false);} fputs("},\"accepted\":[", out);
    bool comma = false;
    for (unsigned i = 0; i < RECORDS; ++i) if (h->rows[i].accepted) {
        history *row = &h->rows[i]; payload p = expected(h, i);
        if (comma) fputc(',', out);
        comma = true;
        fprintf(out, "{\"record_id\":%u,\"token\":%" PRIu64 ",\"topic_handle\":%u,\"generation\":0,\"expected_partition\":%u,\"lane\":0,\"input_mode\":\"%s\",\"timestamp_ms\":%" PRId64 ",\"key_hex\":", i, row->delivery.token, h->topic, i % 4, i < COHORT ? "copy" : i < 2 * COHORT ? "native" : "foreign", h->timestamp + i);
        hex(out, p.key, p.key_len, p.key_null); fputs(",\"value_hex\":", out); hex(out, p.value, p.value_len, p.value_null);
        fputs(",\"headers\":[", out);
        for (unsigned j = 0; j < HEADERS; ++j) {
            if (j) fputc(',', out);
            fputs("{\"key\":", out); quoted(out, header_keys[j]); fputs(",\"value_hex\":", out);
            if (j == 0) hex(out, h->run_id, strlen(h->run_id), false);
            else if (j == 1) hex(out, p.id, strlen(p.id), false);
            else if (j == 4) hex(out, p.binary, sizeof(p.binary), false);
            else hex(out, NULL, 0, j == 2);
            fputc('}', out);
        }
        fputs("]}", out);
    }
    fputs("],\"deliveries\":[", out); comma = false;
    for (unsigned i = 0; i < RECORDS; ++i) if (h->rows[i].delivered) {
        history *row = &h->rows[i]; kr_event e = row->delivery;
        if (comma) fputc(',', out);
        comma = true;
        fprintf(out, "{\"record_id\":%u,\"token\":%" PRIu64 ",\"topic_handle\":%u,\"partition\":%" PRId32 ",\"topic_id\":", i, e.token, e.topic, e.partition);
        hex(out, e.topic_id, 16, false);
        fprintf(out, ",\"kind\":\"Acked\",\"reason\":\"None\",\"offset\":%" PRId64 ",\"timestamp_ms\":", e.base_offset);
        if (e.timestamp_present) fprintf(out, "%" PRId64, e.timestamp_ms); else fputs("null", out);
        fprintf(out, ",\"attempts\":%u,\"event_index\":%" PRIu64 "}", e.attempts, row->delivery_index);
    }
    fputs("],\"input_releases\":[", out); comma = false;
    for (unsigned i = COHORT; i < RECORDS; ++i) if (h->rows[i].lease) {
        history *row = &h->rows[i]; if (comma) fputc(',', out); comma = true;
        fprintf(out, "{\"lease\":%" PRIu64 ",\"record_id\":%u,\"accepted\":%s,\"release_requested\":%s,\"released_event\":", row->lease, i, row->accepted ? "true" : "false", row->release_requested ? "true" : "false");
        if (row->released) fprintf(out, "%" PRIu64, row->release_index); else fputs("null", out);
        fputc('}', out);
    }
    fprintf(out, "],\"release_count\":%u,\"closed_count\":%u,\"flush_count\":%u}\n", h->releases, h->closed, h->flush_done);
    bool success = !ferror(out);
    if (fclose(out) != 0) success = false;
    return success || fail(h, "write ledger failed");
}
int main(int argc, char **argv) {
    if (argc != 7 || (strcmp(argv[1], "uring") && strcmp(argv[1], "readiness")) ||
        strlen(argv[2]) == 0 || strlen(argv[2]) > 253 || !safe_name(argv[4], 200) || !safe_name(argv[5], 64)) {
        fprintf(stderr, "usage: ffi_check <uring|readiness> <host> <port> <topic> <run_id> <ledger.json>\n"); return 2;
    }
    char *end = NULL; errno = 0; unsigned long port = strtoul(argv[3], &end, 10);
    if (errno || !end || *end || !port || port > 65535) {fprintf(stderr, "invalid port\n"); return 2;}
    harness h = {.backend = argv[1], .host = argv[2], .port = (uint32_t)port, .topic_name = argv[4], .run_id = argv[5], .output = argv[6]};
    long page = sysconf(_SC_PAGESIZE); struct timespec now;
    if (page < SLAB || (uint64_t)page > UINT32_MAX || clock_gettime(CLOCK_REALTIME, &now) != 0) {fprintf(stderr, "clock/page setup failed\n"); return 2;}
    h.page_bytes = (size_t)page; h.timestamp = (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
    bool success = exercise(&h);
    if (h.producer) {
        if (!h.close_requested) (void)kr_close(h.producer, 0);
        uint64_t started = monotonic_ns();
        kr_destroy(h.producer); /* Exclusive call; foreign storage stays mapped. */
        h.destroy_ns = monotonic_ns() - started; h.destroyed = true; h.producer = NULL;
    }
    for (unsigned i = 2 * COHORT; i < RECORDS; ++i)
        if (h.rows[i].foreign && munmap(h.rows[i].foreign, h.page_bytes) != 0) {fail(&h, "munmap failed"); success = false;}
    if (!ledger(&h)) success = false;
    if (!success || h.error[0]) {fprintf(stderr, "FFI check failed: %s\n", h.error); return 1;}
    fprintf(stderr, "FFI %s: %u Acked, %u InputReleased, %u Closed; destroy returned\n", h.backend, h.deliveries, h.releases, h.closed);
    return 0;
}
