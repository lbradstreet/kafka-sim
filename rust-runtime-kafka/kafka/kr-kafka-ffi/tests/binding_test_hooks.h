#ifndef KR_KAFKA_BINDING_TEST_HOOKS_H
#define KR_KAFKA_BINDING_TEST_HOOKS_H
#include "kr_kafka.h"

/* Explicit binding-test-hooks artifacts only. None belong to the public ABI. */
int32_t kr_test_producer_create(kr_producer **);
int32_t kr_test_resume(kr_producer *);
int32_t kr_test_metadata(kr_producer *, uint32_t topic, uint32_t partitions, uint32_t identity);
int32_t kr_test_abort(kr_producer *);

#define KR_TEST_SUBMIT 1u
#define KR_TEST_FLUSH 2u
#define KR_TEST_CANCEL_ACCEPTED 1u
#define KR_TEST_WAIT_PUBLICATION 2u
#define KR_TEST_PAUSE_RETURN 4u
#define KR_TEST_ARMED 1u
#define KR_TEST_ENTERED 2u
#define KR_TEST_PUBLISHED 4u
#define KR_TEST_RELEASED 8u
#define KR_TEST_FINISHED 16u
#define KR_TEST_TIMED_OUT 32u

/* Arm one return point. The caller retains the handle until all calls return.
   Observation/release never changes producer-global last_error. The real owner
   still performs cancellation, terminal classification and event publication.
   Returns can pause for at most ten seconds; use wait_call for observable waits. */
int32_t kr_test_arm_call(kr_producer *, uint32_t operation, uint32_t flags);
uint32_t kr_test_call_state(kr_producer *);
int32_t kr_test_wait_call(kr_producer *, uint32_t required_phase, uint32_t timeout_ms);
int32_t kr_test_release_call(kr_producer *);
/* One intentionally malformed replay of the last real drained delivery.
   A zero user token means exact duplicate; nonzero overrides that token. */
int32_t kr_test_replay_delivery(kr_producer *, uint64_t user_token);
/* Real cancellation, at most256 native admission tokens per call. Initialize
   cursor to0; partial progress is retained on control-credit exhaustion. No
   synthetic delivery and no last_error mutation. Retain handle/cursor to return. */
int32_t kr_test_cancel_since(kr_producer *, uint64_t *cursor);
#endif
