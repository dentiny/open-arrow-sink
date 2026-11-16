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
