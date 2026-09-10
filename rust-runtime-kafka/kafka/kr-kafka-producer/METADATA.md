# Metadata owner work

`ProducerActor` retains at most one decoded metadata response. Its control-reserve
lease remains attached through normalization, per-topic notification and final
discard. Another control request is prepared only after that retained response
has finished. A stopped control connection can release its own lease while the
engine still owns the response; abort drains the engine's owned rows before the
last lease disappears.

`ProducerEngine::begin_metadata` transfers owned arrays without copying them.
`on_deadline` charges one item for each handle, broker, topic or partition visit,
generation restart, commit or retired owner. Zero items do no work. Metadata
failure bookkeeping uses the same retained-handle path. Passive callers can use
`apply_metadata` as a synchronous convenience; poll-driven owners must use the
bounded entry point and drain `take_metadata_notice` after maintenance. There is
one notice slot, so publication pauses instead of growing a notification queue.

The first traversal validates the complete normalized response before changing
any broker or topic cache: counts, selector uniqueness, broker/controller IDs,
immutable nonzero identities on successful topics, dense partition indexes,
leader ranges and retained vector/string capacities. Topic partition rows then
normalize incrementally into one owned candidate. Every comparison validates the
handle and generation again. A generation change restarts comparison; a newer
KIP-951 epoch cannot be overwritten by an older response. After all comparisons,
one topic commits atomically by moving its owned array. Closed handles cannot
rebind a reopened name, and failed/deleted handles remain terminal. Topics in one
response can become visible at different maintenance steps; this is not a
transaction across unrelated topics.

Broker reconciliation visits one indexed old node at a time. Requests and live
routes retain omitted endpoints until they release ownership. Retiring an unused
node also removes its throttle deadline. New endpoints recheck current broker
capacity because response processing may interleave with Produce responses.
Structural errors publish no cache changes; a later resource failure can occur
after earlier valid topics or brokers have applied and fails the actor closed.

The retained response and largest normalization candidate have checked actual
vector/string capacity bounded by `control_reserve_bytes`. The actor control
codec applies its smaller working-arena limit before admitting the response.
Validation indexes contain at most `brokers_max` broker IDs and
`max_open_topics` selector, identity and handle entries; they are dismantled one
entry per maintenance item. Their opaque B-tree node backing remains part of
CFG09's explicit incomplete metadata/index accounting, not a proved full heap
bound.

Control frame encoding and parsing are still a whole-frame quantum bounded by
configured frame, byte and cardinality limits. The codec's sorts and structural
checks are not divided into per-row actor work items. This change bounds retained
queue preparation, engine normalization and cache application; it does not claim
a bound on wall-clock time for decoding one maximum-sized metadata frame.
