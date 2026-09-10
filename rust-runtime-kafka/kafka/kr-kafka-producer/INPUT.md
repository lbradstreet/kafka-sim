# Native and registered input ownership

`InputLeases` is the producer's bounded native input registry. Construct it with
the same `SharedCredits` authority and validated configuration as `Admission`.
The producer client serializes admission/publication, routing and lease release
under its outer admission lock. Lease IDs are generation-checked and scoped to
that registry. Raw foreign pointer validity is established only in the audited
C ABI; the safe core accepts an immutable `SharedBytes` owner.

`register_shared(bytes, lane)` accepts uniquely owned, unguarded immutable
storage without copying. It reserves the full retained backing capacity and one
release event, then attaches the same allocation guard used by native acquire.
The `ByteOwner` interface cannot expose mutable access through `SharedBytes`.
Registration failure retains no pointer and creates no release event. A binding
must keep any underlying foreign memory pinned and immutable until
`InputReleased`, or until its exclusive `kr_destroy` call returns. See the
[C ABI pinning contract](../kr-kafka-ffi/README.md).

`acquire(bytes, lane)` reserves the full allocation capacity and one release-event
credit before exposing memory. The serializer writes through
`InputBuffer::as_mut_slice`; `commit(used)` consumes mutable ownership and retains
only an immutable visible prefix. The original pointer and physical allocation
are preserved. Capacity outside the committed prefix remains charged until the
last owner releases it. Invalid commit returns the same still-owned buffer.

`Admission::prepare_leased` accepts key/value/header ranges within the committed
prefix. Null and empty are distinct. It retains only the accepted prefix of
descriptors and does not copy or recharge payload bytes. Header keys are checked
as UTF-8. The full allocation remains charged to the lane that acquired it;
record descriptor/header/delivery credits may atomically move to the final
partition lane using `AdmittedRecord::set_lane`.

`release(lease)` prohibits subsequent submission with that generation. Existing
records, encoder state and provider spans retain immutable ownership. Every
record carries an explicit input reference until its FIFO descriptor is encoded
or failed, even if all its payload fields are null. Actual payload views carry
the same allocation guard, so an outstanding provider reference cannot return
byte credits early. Keeping a committed registry reference permits future
submissions and therefore also prevents release.

When the final reference disappears, its small internal guard releases the
allocation's byte credit and queues exactly one `InputReleased` envelope in
preallocated storage. It invokes no application code, waker or provider.
`pop_released` moves this envelope to the engine's event queue and retires the
generation; its event credit remains owned until application drain. A stopped
consumer therefore exhausts release-event admission without unbounded queueing.
The engine drains this passive queue after input/client transitions, encoder
progress, failures and provider completions. Client wrappers must arrange a
normal actor notification for an uncommitted buffer dropped on another thread;
the allocation guard itself cannot wake foreign code under provider state.

`close()` prevents acquisition and future submissions and drops registry
references. Caller-owned uncommitted buffers and admitted encoder/provider work
must still terminate. `InputStatus` exposes live/acquired/committed/released
slots, pending notifications and native capacity by acquiring lane. The owner
must drain all release notifications and wait for `live == 0` before publishing
Closed. The already-created event envelopes may remain queued for application
drain after that.

Both copy and leased admission charge the backing capacity of each per-record
`OwnedHeader` vector to `InputBytes`; empty payloads do not make header metadata
free. Copy preflight also acquires transient byte credits for its borrowed-header
scratch vector before allocating it, then returns them at method exit. Retained
header/input credits return after the encoder reports the FIFO descriptor
consumed, independently of delivery-event credits.

Tests exercise pointer identity, capacity slack, stale generations, partial
acceptance, null/empty spans, unconsumed event backpressure, cross-thread provider
release, close with a live mutable buffer, atomic lane relabeling, and actual
uncompressed/zstd encoding under 1-, 7- and 127-byte quanta.
