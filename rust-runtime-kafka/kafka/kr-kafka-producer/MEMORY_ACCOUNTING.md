# Memory report contract

`ProducerConfig::validate().memory` reports configured capacity for one producer
engine/client pair. It is **incomplete**. `require_complete_core_bound()` returns
`IncompleteMemoryAccounting` for every current configuration. Neither
`configured_byte_pools` nor `configured_capacity_subtotal` is a complete bound on
client-owned allocation capacity, and neither represents RSS.

The six byte pools retain their existing meanings: input, compressed output,
codec workspace, transport payload, control reserve and request metadata payload.
Input and compressed admission charge whole retained payload capacity, including
views and unused capacity. Headers charged to InputBytes are not counted again
in fixed metadata. Shared payload references do not create another payload charge.

The checked fixed metadata subtotal uses the target's actual Rust element layout:

| Term | Configured element backing included |
| --- | --- |
| `engine_object_pools` | Batch, connection and request arena entries plus free-slot indices |
| `engine_queues` | Intrusive order slots, engine event envelopes, flush fences and terminal-batch slots |
| `client_queues` | Command data/control lanes and the application event ring |
| `input_registry` | Lease arena entries/free indices and passive release-event envelopes; close uses fixed cursor state |
| `routing_scratch` | Preallocated snapshot rows/cumulative weights and bounded ingress/policy scratch |
| `metrics` | Three HDR banks, counters, labels and preallocated scope-index nodes; requested backing is checked before construction |

The three event-storage owners are separate allocations even though an event's
credit moves between them. All are included. The inline value of a Vec, Arc or
tree handle is included when it occupies a counted element; its separately
allocated referents are not. No standard-library B-tree node layout or Arc header
size is assumed. `Pool`, command/application-event mailboxes, engine event/order/
flush/terminal queues and the input-release ring check that `try_reserve_exact` returned exactly
the configured capacity before publishing a handle. Excess reported capacity
returns the existing allocation-failure error; zero-sized mailbox elements need
no backing allocation and retain the same logical queue limits. Thus
`engine_object_pools`, `client_queues`, `input_registry` and `engine_queues`
describe exact retained element backing on successful
construction. Queue and pool limits are unchanged.

Routing's twelve working vectors, four collector vectors and two actor ingress/
pending vectors apply the same check, so `routing_scratch` is also exact retained
element backing after successful construction. The collector's callback view
array remains temporary: its requested length is bounded by one routing bulk,
and its actual capacity is measured separately. Its simultaneous peak remains
in transient working storage; it is not folded into retained routing scratch.

This check preserves lazy pool materialization and empty queue storage: it does
not initialize every configured slot or add a full-capacity teardown scan. In
particular, the default million-event reservation does not gain a startup page-
touch pass. Allocator rounding not exposed as capacity remains a scope exclusion.
Rejected reservations and simultaneous partially constructed owners are startup
transients whose full peak is still unaccounted. Other fixed reservations may
still return extra capacity; their slack remains an explicit gap.

Configuration rejects checked arithmetic overflow in these terms before creating
resources. Tests also observe actual fixed backing capacity across full
admission, rejection, drain and reuse cycles. Exact-capacity constructor checks
establish the retained guarantee for the listed containers independently of the
allocator's reservation policy. Tests do not establish an allocation layout for
opaque containers or remove the report's incomplete result.

# Remaining proof work

The `unaccounted` list is machine-readable and includes collection reservation
slack, ordered indexes, configuration/metadata ownership, record/credit metadata,
codec/output metadata, actor/transport metadata, shared owners and transient work.
Completing CFG09 requires compositional capacity bounds for all of them.

Terminal publication retains at most one partition/unrouted index entry per
accepted descriptor and at most one held result per terminal descriptor.
Handles with unrouted records also retain one captured-UUID entry and one
minimum-token summary under that UUID. These summaries keep retired handles in
the same physical partition's publication order after name reopen, are bounded
by live unrouted descriptors, and disappear with the handle's last unrouted
record. Capturing identity and advancing a minimum use tree operations without
scanning retained handles or records.
Descriptor and delivery-event credits remain charged until ordered publication;
input bytes and leases are released at terminal cleanup independently. A blocked
pass visits one result per work item and parks until routing or publication
changes. These B-tree nodes and held obligation metadata remain in
`OrderedIndexes` and `RecordAndCreditMetadata`, not the fixed backing subtotal.

Topic settlement keeps one index entry per live accepted descriptor and at most
one settlement book per handle with live records. Closing empty handles creates
no jobs; closed generations remain separate from reopened handles. These
cardinality bounds do not establish the layout of their B-tree nodes or shared
write-fence allocations. They remain in `OrderedIndexes` and
`SharedOwnerAllocations`. Each send plan retains at most one fence reference per
selected partition; that vector's backing remains in
`ActorAndTransportMetadata`, while its inline handle is included in counted
order slots.

Closed-topic reclamation adds at most one cleanup job per retained UUID with an
engine partition. Request state retains one partition identity per selected
batch so provider ownership survives batch-slot removal. Terminal ownership uses
inline per-partition counters. These preserve the same cardinality limits and
remain in the ordered-index and request-metadata gaps until their full backing
is composed. Used sequence history is reclaimed only after a quiescent identity
refresh; close/reopen does not silently reset sequences.

Metrics registration uses a preallocated owned-node AVL index. Its vector and
each bank's label/distribution-descriptor vectors reject excess reservation
capacity, so their included backing is exact. HDR logical bin counts determine the
requested counter storage; `distinct_values()` reports length, and does not
expose the private counter Vec's capacity. A correct bin formula therefore does
not establish exact retained histogram backing. Platform mutex backing, Arc control blocks and
reservation slack remain unaccounted producer-core terms. They are distinct
from allocator rounding that is not exposed as capacity and from injected
provider state, which are scope exclusions.

Encoder readiness adds ordered memberships bounded by live owners: at most one
partition work entry, raw-deficit member and ready-lane entry per nonempty
partition, at most one codec-ready/resource-wait membership per live batch, and
at most one actual-context, broker/lane seal and topic seal membership per open
batch. A pending-blocked partition has at most two route memberships (wildcard
and explicit partition); a batch-slot-blocked partition has one slot waiter.
Reverse maps and active group sets have the same respective cardinality bounds.
Every group owns one coalescing cursor, so repeated invalidation does not queue
more jobs. These indexes use opaque B-tree allocation layouts and remain in
`OrderedIndexes`; their known cardinalities do not complete CFG09. Four raw lane
states, seven reconsideration categories and three service phases are fixed
inline state. Idle partition memberships and terminal batch memberships are
removed through owner transitions, including producer failure.

Wire scheduling likewise retains at most one dirty, ready-priority, ready-round-
robin, destination-candidate, topic-invalidation and identity-invalidation entry
per nonempty partition. A blocked partition belongs to one causal waiter group;
active groups own a single cursor with a coalesced restart. Four lane deficits,
three service phases and the fixed resource-release fingerprint are inline.
Every batch-owner transition removes obsolete membership, and final partition
cleanup forgets its destination candidate. A selected request additionally uses
at most `request_max_partitions` keys, exact per-partition wire charges and topic
size entries while framing is built. These temporary collection backings remain
in `TransientWork`; ordered index node capacities remain in `OrderedIndexes`.
The request payload/metadata credit is independently retained through the real
provider completion and is not replaced by these cardinality limits.

The smallest useful migrations are independent:

1. Extend exact-capacity checks, normalization or actual-capacity charging to
   remaining fixed reservations (including control queues and private HDR counter
   storage). Core object pools, ingress/event/release/order/flush/terminal queues,
   routing scratch and metrics label/distribution/index vectors now reject excess
   reported capacity at startup. Compose partial
   constructor and rejected-reservation peaks separately.
2. Common single-credit guards now use inline storage, and release, shrink and
   lane transfer allocate no temporary token vectors. Public duplicate claims
   retain their original semantics. Multi-claim backing is exposed through
   `HeldCredits::metadata_capacity_bytes()` and still needs to be composed into
   the full bound, including rare splits into two multi-claim owners. Guard,
   event-envelope and record-descriptor sizes are unchanged on the tested target.
3. Replace producer-owned B-tree indexes with bounded arenas whose node layouts
   the producer owns, preserving the lifecycle work-per-poll guarantees. Merely
   multiplying entries by `size_of<(K, V)>` does not bound a B-tree allocation.
4. Give topic/config copies, decode trees, record/obligation vectors, request
   segment vectors and temporary admission/control work explicit byte capacity
   owners. Include simultaneous old/new storage during growth or transformation.
5. Compose the record crate's measured output/codec metadata and bounded
   maintenance storage into startup maxima. Codec payload workspace is already
   covered and must not be charged twice.
6. Bound producer-owned shared and boxed owner allocations, including composed
   lifetime guards. `SharedBytes::retain_guard` creates one shared pair allocation
   when a previous guard exists; producer chunks must bound the number of such
   compositions. A reference-counted handle's inline size is not its heap layout.

Only after these terms compose without omissions may the completeness gate
succeed. A counted resource limit is not by itself a proof of backing capacity.

# Scope exclusions

`exclusions` distinguishes missing producer accounting from memory outside the
producer-core contract. Allocator headers/rounding not exposed as capacity,
stacks, executable pages, kernel memory and RSS effects are excluded. Injected
runtime, connector, I/O-provider and TLS-library private state is also excluded:
those traits can retain arbitrary independent objects. Their producer-credited
payload buffers remain included in the configured byte pools. Host supervision,
language-binding registries and application memory not transferred into an input
lease require additional embedding-specific accounting before a broader bound
can be claimed.

Full metadata snapshots retain response broker rows/endpoints/racks and owned
replica/ISR/offline arrays. Their element/string/node backing is charged to
InputBytes; broker snapshots from one response are shared among its topics.
Refresh candidates coexist with old pinned snapshots and may encounter input
credit exhaustion. The FFI caps concurrently pinned snapshot handles at
max_open_topics and charges its preallocated handle table. Release/retirement
returns backing credits only when the final owner drops it. Arc allocator
headers, B-tree cache nodes and allocation rounding remain in the existing
shared-owner/metadata accounting gaps; this does not claim a complete bound.
