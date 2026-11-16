# Test support

This unpublished workspace crate is a **dev-dependency** for integration tests.
Production crates must not depend on it.

- `int64_schema`, `int64_batch`, and `unordered_options` build common Arrow inputs
  and portable write options. Tests keep their backend-specific setup locally.
- `http::serve` accepts a request-aware callback over the original URI and attempt
  number, so tests can route paths, inspect signed queries, and inject retries.
- `Reply` controls HTTP status, header delay, body gates, bytes, or a generated
  constant-byte body. Generated bodies use at most 64 KiB per chunk.
- `Server` owns the listening task and stops it on drop.

Deliberately malformed HTTP framing remains in the owning failure tests: an axum
response cannot represent a truncated chunk header or a false Content-Length.
Lifecycle-specific mocks also stay near their tests rather than growing a shared
mock with switches for every backend and layer.

`sort_batch(batch, keys)` and `concat_sorted(schema, batches, keys)` normalize
unordered readback without dropping duplicate rows. Include enough key columns
to break ties when tied rows have different values. Backend scan setup remains
in the backend tests; this crate does not depend on Lance or another sink.
`seeded_bytes(seed, len)` generates reproducible payloads for rolling/compression
fixtures without duplicating a PRNG loop.

`conformance::SinkFixture` lets a backend supply an isolated destination factory,
its sink/request/options, a representative batch, a published-data reader, and an
abort-state assertion. `assert_conforms::<Fixture>()` runs each shared contract on
a fresh fixture: multi-batch round trip, readable empty commit, explicit abort,
and schema-error termination. Iceberg can supply Append while file backends use
Create. Keep backend-specific checks (snapshot identity, row-group layout,
partitions, indexes, compression metadata, cleanup of existing data) beside the
backend. These checks complement the core adapter's direct failure/cancellation
and publication-boundary tests.
