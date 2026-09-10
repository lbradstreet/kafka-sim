# Kafka host

`kr-kafka-host` supplies native connection setup, TLS, SASL and bounded control
workers for `kr-kafka-client`. It has no dependency on the producer or record
encoder. Dedicated producer ownership and calibration live in
[`kr-kafka-producer-host`](../kr-kafka-producer-host/README.md).

`native::HostConnector::new(handle, connection_config, control_codec, setup_budget)`
accepts explicit connection bounds, a shared control codec, and an opaque
`SetupBudget` authority. It checks buffer arithmetic, TLS/security settings and
native ring capacity before creating provider resources. Required io_uring fails
on unavailable kernels. Explicit readiness uses the native epoll provider.
**Auto fallback is currently disabled:** its execution release gate remains
separate from successful Linux typechecking.

Constructing a connect future admits nothing. First poll reserves 128 KiB from
`SetupBudget` for a data setup; the control connection uses its existing arena
guard when supplied. DNS and SCRAM work run on a separate two-worker fleet, with
bounded job and retained-byte admission. DNS retains at most sixteen candidate
addresses and attempts them in order. System resolver internals, root-store
parsing, crypto-provider state, thread stacks and kernel socket buffers remain
host/runtime overhead, separately from workload byte pools.

Setup verifies TLS with supplied/system roots, discovers validated advertised
API ranges, then performs SASL when configured. SASL requires advertised
Handshake1 and Authenticate2. Workload-specific requirements are checked by the
caller: the producer actor requires its strict Produce13 profile. PLAIN bytes
require the actual verified-TLS proof; SCRAM requires a fresh OS-CSPRNG nonce and
verified server signature. Setup uses negative correlation IDs. The exact driver
used to read negotiation responses moves into the caller, preserving buffered
input and admitted operations. Failure retires that driver and drains owned
operations through actual terminal results. Connection/setup guards follow
provider buffers and control workers through abandonment and unconsumed output.

The injected codec's owned response and authentication limits may be at most
64 KiB and 16 KiB respectively, preserving the fixed setup allowance. Retained
advertised API ranges share that owned-response allowance with later SASL
parsing. Their immutable backing retains the setup guard independently of the
driver. Arc control blocks and container descriptors remain metadata outside the
payload allowance, as in the existing memory-accounting exclusions.

TLS plaintext/ciphertext budgets include every fixed adapter buffer. For example,
32 KiB plaintext and 64 KiB ciphertext comprise a 16 KiB plaintext stage, two
14 KiB transport arrays, and passive-client allocations of 16 KiB plaintext plus
36 KiB ciphertext. The standalone transform defaults below are independent.

## TLS ownership and progress

`tls::TlsClient` uses [rustls 0.23.43 unbuffered mode][unbuffered]. Construct it
with a verified `ClientConfig`, server name and `TlsLimits`. Configuration,
trust roots and crypto-provider allocations are shared caller-owned control
resources; `retained_capacity()` reports the adapter's two fixed allocations.
Rustls's bounded record/handshake parsing and cryptographic state are additional
library overhead, not included in that accessor. Early data is disabled.

The default plaintext allocation is 16 KiB and the ciphertext allocation is
256 KiB, split into independent 128 KiB receive/transmit regions. No data-path
call grows these buffers. Encryption accepts at most one 16 KiB plaintext chunk;
larger Kafka batches are split at this transform boundary. This intentionally
copies into encrypted storage and leaves immutable compressed retry bytes alone.

The owner follows these transitions:

1. `drive()` reports `Transmit`, `Receive`, `Plaintext`, `Ready` or `PeerClosed`.
2. On `Transmit`, submit `outbound_ciphertext()` through the owned cold stream.
   Retain the actual submitted request/future until terminal, including when an
   application waiter is abandoned. Call `consume_outbound(n)` only for the
   terminal's known successful prefix; retry the remaining ciphertext on the
   same connection without encrypting it again. Any transport failure fences
   the connection through `transport_failed()` and requires a new handshake.
3. On a completed read, call `receive_ciphertext()` and retain any unaccepted
   suffix in the driver's bounded receive staging. Drive and consume existing
   data before supplying more. The adapter does not authorize discarding an
   unaccepted suffix.
4. On `Plaintext`, parse Kafka bytes and `consume_plaintext(n)`. Consumed
   plaintext is zeroized. On `Ready`, `encrypt()` prepares the next chunk.
5. `close_notify()` encrypts a clean shutdown after outstanding writes drain.
   `transport_eof()` reports truncation unless the peer's authenticated
   close_notify was processed.

The stream's owned staging allocation is charged separately while the kernel
owns it. The passive adapter itself has no user future to abandon. It marks a
handshake flight transmitted only after every byte was acknowledged. Receive
storage remains independent of a blocked send. A new connection encrypts retry
bytes into fresh records using production cryptographic entropy.

`stream::TlsStream<S>` provides the complete owned-I/O adapter without a hidden
task. Its `TlsTransport` bound requires completion-cell guards on underlying
read/control futures. Ciphertext write spans and read/control completion cells
retain the matching plaintext payload and connection-credit guard. These guards
contain no TLS core or stream reference: after the last TLS observer drops,
provider terminal completion releases them without another TLS poll. Each lower
operation adds one bounded guard allocation, which belongs in the adapter's
metadata allowance. `retire()` remains the normal observed close/drain path.

`peer_verified()` returns a private-constructor `VerifiedTls` capability only
after TLS reaches authenticated application traffic. `sasl::plain_response`
requires that capability, so selecting TLS in configuration does not by itself
permit sending a clear password. Injecting rustls's dangerous certificate
verifier would bypass the caller's trust policy; production configuration must
use the library's verified root-certificate builder.

## SASL and control jobs

Supported mechanisms are PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512. PLAIN accepts
bounded nonempty UTF-8 credentials without NUL. SCRAM implements the explicit
ASCII-only credential profile permitted by [RFC 5802][scram], rejecting
non-ASCII credentials instead of applying an incomplete normalization scheme.
It escapes username commas/equals and validates canonical Base64, duplicate
attributes, mandatory extensions, nonce extension, and iteration limits before
derivation. The accepted iteration range is 4096 through the configured cap
(default one million; the configuration itself is capped at ten million).

`ScramClient::start` obtains a fresh 192-bit nonce from ring's OS CSPRNG.
`on_server_first` consumes that exchange and yields an owned `ScramWork` without
running PBKDF2. Submit it with `ControlJobs::scram`; send the resulting
`ScramProof.response`, and require `ScramProof.verifier.verify(server_final)`
to succeed before the connection becomes authenticated. Sending a proof alone
is never authentication success. Server signatures use ring's constant-time
HMAC verification. Password/proof buffers are zeroized and error messages never
contain credentials or challenge contents.

`ControlJobs` acquires both job and conservative byte credits before calling the
runtime's unbounded `HostBlocking::submit`. Pending abandonment and unconsumed
terminal results retain admission through the common completion guard; panic
produces a terminal `WorkerPanicked` response. Its counters are separate from
data jobs. Provision its `HostBlocking` capability on a separate control runtime
when the process also runs compression jobs: a byte reserve alone cannot
prioritize jobs in a shared FIFO worker queue. Generic `submit` permits other
bounded control work, including DNS, when the caller supplies an honest maximum
input/output/scratch byte charge.

## Verification

Run `RUSTC_WRAPPER= cargo test -p kr-kafka-host --offline` and strict Clippy with
`--all-targets --all-features`. The portable suite covers startup mapping and the security cases below.
Linux-only connector tests cover same-driver reuse, generic capability discovery
without producer policy, and real TLS plus Kafka PLAIN. This extraction is
verified with local execution and Linux cross-typechecking; it does not execute
native broker tests. Tests cover:

- The [RFC 7677][sha256] complete SHA-256 exchange and an independent SHA-512
  fixture computed from the same inputs using Python `hashlib.pbkdf2_hmac`,
  `hashlib.sha512` and `hmac.digest` following the RFC's proof equations.
- Malformed, replayed, duplicate, oversized, noncanonical and over-budget auth
  inputs; mandatory server signature verification; production nonce freshness.
- Pending/terminal control-credit retention and worker panic recovery.
- Actual TLS 1.3 records over both memory and simulated cold streams, at 1-, 7-
  and 4096-byte partial progress, with real ring ECDH, certificate validation,
  full-size plaintext, close_notify, tamper, name and validity rejection.
- A byte-identical TLS replay using injected fixed time/randomness and the
  published [RFC 7748 section 6.1][x25519] ECDH public keys/shared secret as
  recorded ephemeral material. That test still uses actual rustls transcript
  hashing, Ed25519 signatures, HKDF and AEAD; normal-provider tests independently
  execute real ephemeral key generation/agreement. This provider exists only
  in the integration test and is never exported by the production library.

`tests/fixtures/` contains deliberately public, **TEST ONLY** Ed25519 credentials
for `localhost`. The portable security tests inject epoch 1,800,000,000 for certificate
validation; native connector integration uses the production clock. These keys must never be trusted or used by a
deployed broker. Production SCRAM has no injected-nonce entry point unless the
explicit `test-support` feature is enabled.

[unbuffered]: https://docs.rs/rustls/0.23.43/rustls/unbuffered/index.html
[scram]: https://www.rfc-editor.org/rfc/rfc5802.html
[sha256]: https://www.rfc-editor.org/rfc/rfc7677.html
[x25519]: https://www.rfc-editor.org/rfc/rfc7748.html

Opt-in native diagnostics are available through
`HostConnector::attach_diagnostics` and a cloneable `HostDiagnostics` handle.
The producer wrapper exposes this through `HostProducer::start_with_diagnostics`
and `diagnostics()`.
They measure actual terminal publication-to-drain delay using bounded counters,
and sample runtime/provider pressure through weak observers that cannot postpone
shutdown. This adds clock reads and mutex overhead to instrumented runs only.
See `../benchmarks/README.md` for precise intervals, scopes, and profiler passes.
