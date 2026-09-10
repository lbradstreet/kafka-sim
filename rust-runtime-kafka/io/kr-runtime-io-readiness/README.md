# Native readiness networking

`ReadinessNet` provides the owned `NetworkProviderSubmit`, `ByteStreamSubmit`
and `ByteStreamVectoredSubmit` contracts on Linux using rustix epoll and native
sendmsg. One thread owns all sockets and operations. It creates no executor,
per-record task, flattened vectored payload or unbounded command queue.

`ReadinessConfig` bounds streams, listeners, operation counts, independent read
and write bytes, segment metadata, per-turn transfer bytes and socket buffer
requests. `connect_timeout` is nonzero and at most one day. The reactor derives
its epoll wait from the nearest admitted connect deadline and expires connects
even when no socket event arrives. Expiry drops the descriptor and stream slot
before completing its waiter; it reports MayHaveApplied because a TCP SYN may
have escaped. A queued command that expires before socket creation is NotApplied.

Caller allocations and byte/count reservations stay owned until the operation
is terminal and its output consumed or abandoned. `attach_lifetime_guard` adds
one passive caller budget obligation; it follows buffers through closed handle
and unconsumed terminal output. Guards must not invoke application callbacks.
Drop requests a bounded close; the reactor drains retained work. Read capacity
is independent of blocked or exhausted sends.

Linux-target all-target checking and strict Clippy pass locally. The shared
warm/cold conformance suite, native timeout regressions and budget-guard test
are present and typechecked. Native execution is pending explicit approval to
copy the reviewed source/build manifest into the dedicated local Linux VM.
