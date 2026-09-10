#ifndef KR_KAFKA_H
#define KR_KAFKA_H
#include <stdint.h>
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
#define KR_ABI_VERSION 4u
#define KR_OK 0
#define KR_ERR_INVALID -1
#define KR_ERR_VERSION -2
#define KR_ERR_EXHAUSTED -3
#define KR_ERR_NOT_READY -4
#define KR_ERR_CLOSED -5
#define KR_ERR_UNSUPPORTED -6
#define KR_ERR_FAILED -7
#define KR_ERR_TIMEOUT -8
#define KR_EVENT_DELIVERY 1u
#define KR_EVENT_INPUT_RELEASED 2u
#define KR_EVENT_FLUSH_DONE 3u
#define KR_EVENT_TOPIC_READY 4u
#define KR_EVENT_TOPIC_FAILED 5u
#define KR_EVENT_CLOSED 6u
#define KR_EVENT_FATAL 7u
#define KR_DELIVERY_ACKED 0u
#define KR_DELIVERY_NOT_WRITTEN 1u
#define KR_DELIVERY_UNKNOWN 2u
#define KR_REASON_NONE 0u
#define KR_REASON_DEADLINE 1u
#define KR_REASON_CANCELLED 2u
#define KR_REASON_TOPIC_DELETED 3u
#define KR_REASON_TOPIC_RESOLUTION 4u
#define KR_REASON_PARTITION_FAILED 5u
#define KR_REASON_COMPRESSED_TOO_LARGE 6u
#define KR_REASON_INVALID_RECORD 7u
#define KR_REASON_BROKER_REJECTED 8u
#define KR_REASON_PRODUCER_FENCED 9u
#define KR_REASON_PROTOCOL_VIOLATION 10u
#define KR_REASON_TRANSPORT 11u
#define KR_REASON_RUNTIME_FAILED 12u
#define KR_REASON_SEQUENCE_UNRESOLVED 13u
#define KR_REASON_CLOSED 14u
#define KR_REASON_RESOURCE_EXHAUSTED 15u
#define KR_REASON_AUTHENTICATION 16u
#define KR_OWNER_RUNNING 0u
#define KR_OWNER_CLOSED 1u
#define KR_OWNER_ABORTED 2u
#define KR_TOPIC_RESOLVING 0u
#define KR_TOPIC_READY 1u
#define KR_TOPIC_FAILED 2u
#define KR_TOPIC_DELETED 3u
#define KR_TOPIC_CLOSING 4u
#define KR_TOPIC_RETIRED 5u
#define KR_TOPIC_STALE 6u
#define KR_METADATA_REPLICAS 0u
#define KR_METADATA_ISR 1u
#define KR_METADATA_OFFLINE 2u
#define KR_METADATA_HOST 0u
#define KR_METADATA_RACK 1u
/* Snapshot generation changes on every successful refresh, including changes
 * to replicas, ISR and broker endpoints. RETIRED means its name may be reopened;
 * accepted old records retain their original UUID and delivery obligations. */
typedef struct {
 uint32_t struct_size,status;uint64_t generation;uint8_t topic_id[16];
 uint32_t partition_count,reason;
} kr_topic_status;
typedef struct {
 uint32_t struct_size,status;uint64_t generation;uint8_t topic_id[16];
 uint32_t partition_count,reason;uint64_t snapshot;uint32_t broker_count;
} kr_metadata_snapshot;
typedef struct {
 uint32_t struct_size;int32_t id;uint32_t port,host_len,rack_len,rack_present;
} kr_metadata_broker;
typedef struct {
 uint32_t struct_size;int32_t partition,leader,leader_epoch,error_code;
 uint32_t replica_count,isr_count,offline_count;
} kr_metadata_partition;
typedef struct kr_producer kr_producer;
typedef struct {const uint8_t *ptr;uint32_t len;} kr_span;
typedef struct {uint32_t struct_size;kr_span host;uint32_t port;} kr_broker;
typedef struct {uint32_t struct_size;kr_span key,value;uint32_t value_is_null;} kr_header;
typedef struct {
 uint32_t struct_size,topic;int32_t partition_hint,lane_hint;
 kr_span key;uint32_t key_is_null;kr_span value;uint32_t value_is_null;
 const kr_header *headers;uint32_t header_count;int64_t timestamp_ms;uint64_t user_token,delivery_timeout_ns;
} kr_record;
/* Delivery base_offset is this record's absolute offset. Delivery timestamp_ms,
 * when present, is the broker's batch log_append_time_ms. On duplicate retries
 * it may be the batch maximum timestamp, not this record's encoded CreateTime. */
typedef struct {
 uint32_t struct_size,kind;uint64_t token,user_token;uint32_t topic;uint8_t topic_id[16];int32_t partition;
 uint32_t outcome,reason;int64_t base_offset;uint32_t base_offset_present;int64_t timestamp_ms;
 uint32_t timestamp_present,attempts,count;
} kr_event;
typedef struct {
 uint32_t struct_size;
 uint64_t delivery_timeout_ns;
 uint64_t request_timeout_ns;
 uint64_t linger_max_ns;
 uint64_t metadata_max_age_ns;
 uint64_t topic_resolve_timeout_ns;
 uint64_t retry_backoff_min_ns;
 uint64_t retry_backoff_max_ns;
 uint64_t input_bytes;
 uint64_t compressed_bytes;
 uint64_t control_reserve_bytes;
 uint64_t codec_workspace_bytes;
 uint32_t max_in_flight_per_connection;
 uint32_t lanes;
 uint32_t codec_contexts;
 uint32_t max_attempts;
 uint32_t request_max_partitions;
 uint32_t brokers_max;
 uint32_t worker_jobs;
 uint32_t connection_wire_window_bytes;
 uint32_t batch_target_bytes;
 uint32_t batch_hard_bytes;
 uint32_t request_target_bytes;
 uint32_t request_hard_bytes;
 uint32_t record_descriptors;
 uint32_t staging_bytes_per_connection;
 uint32_t rx_bytes_per_connection;
 uint32_t delivery_event_capacity;
 uint32_t release_event_capacity;
 uint32_t mailbox_capacity;
 uint32_t max_open_topics;
 uint32_t pending_records_per_topic;
 uint32_t max_live_leases;
 uint32_t max_batches;
 uint32_t codec_window_log;
 uint32_t output_chunk_bytes;
 uint32_t progressive_threshold;
 uint32_t tls_plaintext_bytes;
 uint32_t tls_ciphertext_bytes;
 uint32_t max_header_count;
 uint32_t max_submissions_per_poll;
 uint32_t max_submission_records;
 uint32_t max_completions_per_poll;
 uint32_t sim_encode_bytes_per_poll;
 uint32_t target_poll_ms;
 uint32_t coalesce_below_bytes;
 uint32_t linger_skip_below_rate;
 uint32_t unkeyed_policy;
 uint32_t unkeyed_run_bytes;
 uint32_t partitioner;
 uint32_t compression;
 uint32_t compression_level;
 uint32_t transport;
 uint32_t security;
 uint32_t sasl_mechanism;
 uint32_t tls_system_roots;
 kr_span client_id;
 const kr_broker * bootstrap;
 uint32_t bootstrap_count;
 const kr_span * tls_roots;
 uint32_t tls_root_count;
 kr_span tls_server_name;
 kr_span username;
 kr_span password;
 /* 0 = estimated wire bytes including batch header; 1 = legacy raw bytes. */
 uint32_t batch_target_mode;
 /* 0 = sealed (default), 1 = single partition, 2 = broker ready. */
 uint32_t request_batching_policy;
 /* Must be zero; ABI 4 has a distinct configuration size. */
 uint32_t reserved_request_policy;
} kr_producer_config;
/* Initialize defaults into an output of exactly sizeof(kr_producer_config).
 * Empty bootstrap selects localhost:9092. NULL client_id selects kr-kafka.
 * transport:0=uring,1=readiness,2=Auto (uring-or-fail until native gate).
 * security:0=plaintext,1=TLS,2=SASL+TLS; sasl:0=PLAIN,1=SHA256,2=SHA512.
 * compression:0=none,1=zstd; partitioner:0=builtin,1=external partition hints.
 * unkeyed_policy:0=uniform bytes,1=adaptive. All time fields use nanoseconds.
 * Credentials/config spans are copied before create returns. */
uint32_t kr_abi_version(void);
int32_t kr_producer_config_init(kr_producer_config*,uint32_t struct_size);
int32_t kr_producer_create(const kr_producer_config*,kr_producer**);
int32_t kr_topic_open(kr_producer*,const char*,uint32_t,uint32_t*);
int32_t kr_topic_id(kr_producer*,uint32_t,uint8_t id_out[16]);
int32_t kr_topic_close(kr_producer*,uint32_t);
/* Owner terminal status is published only after terminal record/fence events.
 * ABORTED emits no CLOSED event. Provider retirement may still require destroy.
 * These functions, snapshot reads/release and event draining remain usable after
 * owner failure. All outputs are caller-owned; initialize every struct_size.
 * Snapshot handles never repeat. At most max_open_topics snapshots may be held.
 * Acquiring missing/resolving metadata returns NOT_READY without a handle.
 * A snapshot remains immutable across refresh, topic close and name reuse.
 * Release once; stale/released handles return INVALID. Destroy releases all.
 * Row pages cap at 1024; string/node pages cap at 65536 bytes / 1024 IDs.
 * start equal to count returns an empty page; start beyond count is INVALID.
 * String bytes are UTF-8 without a terminator. Rack may be absent.
 * Refresh coalesces until the owner consumes it; STALE invalidates the cached
 * metadata until a successful refresh. Held snapshots remain readable. */
int32_t kr_owner_status(kr_producer*,uint32_t *status);
int32_t kr_topic_get_status(kr_producer*,uint32_t topic,kr_topic_status*);
int32_t kr_topic_refresh(kr_producer*,uint32_t topic);
int32_t kr_metadata_acquire(kr_producer*,uint32_t topic,kr_metadata_snapshot*);
int32_t kr_metadata_release(kr_producer*,uint64_t snapshot);
int32_t kr_metadata_brokers(kr_producer*,uint64_t snapshot,uint32_t start,kr_metadata_broker*,uint32_t capacity,uint32_t *written);
int32_t kr_metadata_partitions(kr_producer*,uint64_t snapshot,uint32_t start,kr_metadata_partition*,uint32_t capacity,uint32_t *written);
int32_t kr_metadata_nodes(kr_producer*,uint64_t snapshot,uint32_t partition,uint32_t kind,uint32_t start,int32_t*,uint32_t capacity,uint32_t *written);
int32_t kr_metadata_string(kr_producer*,uint64_t snapshot,uint32_t broker,uint32_t kind,uint32_t start,uint8_t*,uint32_t capacity,uint32_t *written);
int32_t kr_buffer_acquire(kr_producer*,uint32_t,uint8_t**,uint64_t*);
int32_t kr_buffer_commit(kr_producer*,uint64_t,uint32_t);
int32_t kr_buffer_release(kr_producer*,uint64_t);
/* Register immutable foreign storage without copying. ptr/length describe the
 * full pinned byte allocation (1..UINT32_MAX bytes), including capacity slack.
 * Caller prevents writes, movement and deallocation across all threads until
 * InputReleased for this lease or until destroy returns. Rejection retains no
 * pointer. C/Python examples are tested: Python uses PyBUF_SIMPLE and keeps the
 * exporter until PyBuffer_Release after the event. Go/JVM bindings are currently
 * unavailable: Go requires runtime.Pinner for every retained Go allocation;
 * JVM requires retained direct ByteBuffer storage. Enable those bindings only
 * after their own pinning/lifetime tests; movable managed heap storage is invalid. */
int32_t kr_lease_register(kr_producer*,const uint8_t*,uint64_t,uint64_t*);
/* Accepted prefix. Check kr_last_error for why a nonempty suffix was rejected.
 * Copy inputs remain valid/immutable throughout the call; no retention afterwards.
 * Leased spans must lie in committed native memory. Mutation stops at commit.
 * key/value_is_null distinguish NULL from present empty; hints -1 mean absent.
 * Each struct_size must exactly match its current definition. */
uint32_t kr_submitv_copy(kr_producer*,const kr_record*,uint32_t);
uint32_t kr_submitv_leased(kr_producer*,uint64_t,const kr_record*,uint32_t);
/* Initialize struct_size on every output slot. This drains events only.
 * event.token is record/lease/flush token according to kind; event.count is
 * TopicReady partition count or Closed unresolved count. Other unused fields
 * are zero. Delivery reason uses KR_REASON_*, optional values use *_present. */
uint32_t kr_poll_events(kr_producer*,kr_event*,uint32_t);
int32_t kr_flush(kr_producer*,uint64_t*);
/* Relative delivery close timeout in milliseconds. */
int32_t kr_close(kr_producer*,uint64_t timeout_ms);
int32_t kr_last_error(kr_producer*);
/* Exclusive final call: no concurrent calls or writes into acquired buffers.
 * Releases outstanding acquired/native lease handles, then joins actual I/O
 * retirement. Best effort after close deadline; deadline never frees live I/O. */
void kr_destroy(kr_producer*);
#ifdef __cplusplus
}
#endif
#endif
