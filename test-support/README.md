# Test support

This unpublished workspace crate is a **dev-dependency** for integration tests.
Production crates must not depend on it.

- `int64_schema`, `int64_batch`, and `unordered_options` build common Arrow inputs
  and portable write options. Tests keep their backend-specific setup locally.
- `http::serve` accepts a request-aware callback over the original URI,
  so tests can route paths, inspect signed queries, and inject retries.
- `Reply` controls HTTP status, body gates, bytes, or a generated
  constant-byte body. Generated bodies use at most 64 KiB per chunk.
- `Server` owns the listening task, stops it on drop, and exposes the received
  request count through `requests()`.

Deliberately malformed HTTP framing remains in the owning failure tests: an axum
response cannot represent a truncated chunk header or a false Content-Length.
`parallel_sink` supplies the event-recording parallel backend shared by layer and
session-adapter tests. `Config`, `State`, `Event`, and `TestSink` support failure
injection, dispatch gates, and lifecycle assertions without `#[path]` imports.
Backend-specific mocks stay with their owning tests.

`sort_batch(batch, keys)` and `concat_sorted(schema, batches, keys)` normalize
unordered readback without dropping duplicate rows. Include enough key columns
to break ties when tied rows have different values. Backend scan setup remains
in the backend tests; this crate does not depend on Lance or another sink.
`seeded_bytes(seed, len)` generates reproducible payloads for rolling/compression
fixtures without duplicating a PRNG loop.

`conformance::SinkFixture` supplies isolated destinations, sink/request/options,
representative input, published-data readback, and abort/rolling/sibling assertions.
`conformance::assert_conforms::<Fixture>()` runs the complete portable suite.
`conformance::assert_case::<Fixture>(Case::Rolling)` runs one named contract;
backend integration tests use named rstest cases so failures identify the contract.
The individual contract functions are also public for tests supplying a custom
fixture instance.

The suite covers multi-batch round trips, target-size rolling with full readback,
unsupported-mode rejection without changing committed rows, readable empty commits,
abort with sibling preservation, schema-error termination, abort before input, and
poisoned commit cleanup. Iceberg supplies Append while file backends use Create.
Keep snapshot identity, row-group layout, partitions, indexes, compression metadata,
and preservation of existing data beside their backend.

`write_batches(sink, request, batches, options)` opens and commits a successful
session, checking the input schema and committed row count. It accepts any sink
and a nonempty slice of batches, and is shared by Lance and Parquet tests. Tests
for empty sessions, failures, cancellation, and intermediate state use the suite
or the writer directly.

`memory_storage(base)` constructs an isolated memory operator rooted at an
explicit namespace. `read_parquet_dir(storage, directory)` reads all Parquet
files in path order through shared storage, for memory and filesystem fixtures.
Its directory argument is a resolved `ObjectPath`. `path(storage, relative)` resolves
a fixture-relative path; `sorted_list(storage, directory)` makes listing comparisons
deterministic. `within(future)` bounds asynchronous fixture work to five seconds.
