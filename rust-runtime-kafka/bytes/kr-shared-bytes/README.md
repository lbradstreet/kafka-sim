# Shared bytes

`SharedBytes` is a `no_std + alloc` immutable span shared by protocol, record,
runtime I/O and producer ownership boundaries. `new(Arc<[u8]>)` retains an
existing slice allocation. `From<Vec<u8>>` wraps the existing vector in a small
`Arc<Vec<u8>>`; it preserves the payload pointer and performs no payload copy.

Clones and `slice` retain the original backing allocation and optional resource
guard. `len` reports visible initialized bytes. `allocation_len` and
`retained_capacity` report the full payload capacity, including unused vector
capacity and bytes outside a view. Shared-allocation admission counts this
capacity once across aliases. Capacity slack is never exposed for reading or
mutation.

`try_as_mut` succeeds only with unique strong ownership and no weak references,
and returns only the visible initialized range. `attach_guard` must run before
publishing any cloned spans; existing clones do not acquire a guard retroactively.
The last guarded view releases the backing bytes before dropping its guard.
Guards must never invoke application callbacks or reenter providers, because a
provider may drop the final view under its internal state lock.
