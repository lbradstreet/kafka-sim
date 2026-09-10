# Public TLS test fixtures

These Ed25519 credentials are deliberately public and must never be used for a
real service. They identify only `Kafka TEST ONLY CA` and `localhost`.

Certificate validity is fixed from 2000-01-01 through 2100-01-01; those dates are
synthetic, not creation timestamps. Serial numbers are fixed at 1 and 2.
The test keys are derived from the SHA-256 bytes of the UTF-8 labels
`kr-kafka TEST ONLY CA fixture v1` and
`kr-kafka TEST ONLY localhost fixture v1`.

The TLS tests use an injected time inside that interval and separately reject a
time before it. Regeneration must preserve the CA constraints, localhost SAN,
server-auth usage and matching leaf key, then run the host TLS tests.
