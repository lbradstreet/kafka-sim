#define _DEFAULT_SOURCE
#include "kr_kafka.h"
#include <assert.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

/* These two hooks exist only in the explicit binding-test-hooks build. */
extern int32_t kr_test_producer_create(kr_producer **);
extern int32_t kr_test_resume(kr_producer *);

static kr_record record(uint32_t topic, const uint8_t *bytes, uint64_t user) {
    kr_record out;
    memset(&out, 0, sizeof(out));
    out.struct_size = sizeof(out);
    out.topic = topic;
    out.partition_hint = -1;
    out.lane_hint = -1;
    out.key_is_null = 1;
    out.value.ptr = bytes;
    out.value.len = 7;
    out.user_token = user;
    return out;
}

static void release_event_is_the_reclamation_boundary(void) {
    kr_producer *producer = NULL;
    assert(kr_test_producer_create(&producer) == KR_OK);
    uint32_t topic = 0;
    assert(kr_topic_open(producer, "events", 6, &topic) == KR_OK);
    size_t length = (size_t)sysconf(_SC_PAGESIZE);
    uint8_t *bytes = mmap(NULL, length, PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANON, -1, 0);
    assert(bytes != MAP_FAILED);
    memset(bytes, 0, length);
    memcpy(bytes, "payload", 7);
    uint64_t lease = 0;
    assert(kr_lease_register(producer, bytes, length, &lease) == KR_OK);
    assert(lease != 0);
    /* A registered foreign owner must never acquire Rust mutable access. */
    assert(mprotect(bytes, length, PROT_READ) == 0);
    kr_record records[2] = {record(topic, bytes, 901), record(topic, bytes, 902)};
    records[1].value.ptr = bytes + length - 1;
    records[1].value.len = 2;
    assert(kr_submitv_leased(producer, lease, records, 2) == 1);
    assert(kr_last_error(producer) == KR_ERR_INVALID);
    assert(kr_buffer_release(producer, lease) == KR_OK);
    assert(kr_submitv_leased(producer, lease, records, 1) == 0);
    kr_event events[16] = {0};
    for (size_t i = 0; i < 16; i++) events[i].struct_size = sizeof(kr_event);
    assert(kr_poll_events(producer, events, 16) == 0); /* owner still paused */
    assert(kr_close(producer, 0) == KR_OK);
    assert(kr_test_resume(producer) == KR_OK);
    unsigned releases = 0, deliveries = 0, closed = 0;
    for (unsigned tries = 0; tries < 3000 && !closed; tries++) {
        uint32_t count = kr_poll_events(producer, events, 16);
        for (uint32_t i = 0; i < count; i++) {
            if (events[i].kind == KR_EVENT_INPUT_RELEASED) {
                assert(events[i].token == lease);
                assert(++releases == 1);
                /* Any subsequent stale read faults, including during destroy. */
                assert(mprotect(bytes, length, PROT_NONE) == 0);
            } else if (events[i].kind == KR_EVENT_DELIVERY) {
                assert(events[i].user_token == 901);
                assert(events[i].outcome == KR_DELIVERY_NOT_WRITTEN);
                deliveries++;
            } else if (events[i].kind == KR_EVENT_CLOSED) {
                closed++;
            }
        }
        if (!closed) usleep(1000);
    }
    assert(releases == 1 && deliveries == 1 && closed == 1);
    kr_destroy(producer);
    assert(munmap(bytes, length) == 0);
}

static void destroy_joins_before_foreign_memory_can_be_freed(void) {
    kr_producer *producer = NULL;
    assert(kr_test_producer_create(&producer) == KR_OK);
    uint32_t topic = 0;
    assert(kr_topic_open(producer, "events", 6, &topic) == KR_OK);
    uint8_t bytes[8] = "payload";
    uint64_t lease = 0;
    assert(kr_lease_register(producer, bytes, sizeof(bytes), &lease) == KR_OK);
    kr_record input = record(topic, bytes, 903);
    assert(kr_submitv_leased(producer, lease, &input, 1) == 1);
    /* No explicit release or event drain. Destroy resumes the test owner,
       fences every lease and waits for actual retirement before returning. */
    kr_destroy(producer);
    memset(bytes, 0, sizeof(bytes));
}

int main(void) {
    release_event_is_the_reclamation_boundary();
    destroy_joins_before_foreign_memory_can_be_freed();
    return 0;
}
