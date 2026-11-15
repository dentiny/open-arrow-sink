# Proposal: one HTTP request budget for access and sink extensions

Status: design proposal; no runtime behavior changes in this PR.

## Evidence

The [sink limiter](https://github.com/dentiny/open-arrow-sink/blob/3e60048fa9c5a7ed8406a54275dbc12daeb0b1cf/storage/src/http_limit.rs)
and [access limiter](https://github.com/dentiny/open-arrow-access/blob/bbd6284585dbf60f802bab1cbe13acfbb37ce8ae/storage/src/http_limit.rs)
are each 107 lines. Their only textual difference is the core crate used for the
constructor's error result. Both implement transport wrapping, body-lifetime
permits, cancellation, and the same semaphore limit validation.

The implementations are separate Rust types with private semaphores. Cloning a
sink limiter cannot supply the access library's `with_http_concurrency_limit`,
and independently constructing two limits does not produce one combined budget.
An application can work around this at the transport level, but then has to
coordinate the separate default limiters and avoid accidentally wrapping the
same request budget twice. Each cancellation or body-lifetime fix also needs to
be maintained and tested in two repositories.

## Proposed common component

Extract the transport-level limiter into a small, independently published crate
with dependencies on `opendal`, `http`, `futures`, and Tokio's semaphore support.
The crate name and release ownership remain open. It should expose the existing
`HttpConcurrencyLimitLayer::new` and `wrap` operations with a small independent
invalid-limit error. It should not depend on either library's Arrow contracts.

Both repositories can depend on the same compatible crate version. Preserve
their current public constructors and domain-specific errors with thin wrappers;
add conversion from the common limiter into each wrapper. The conversions must
retain the same shared semaphore, rather than construct a new budget.

The intended application code would create one common limiter, clone it into
`OpenDalAccessor::with_http_concurrency_limit`, and move another clone into
`Storage::with_http_concurrency_limit`. These conversions are proposed APIs,
not available APIs today. The combined number of active source and destination
HTTP bodies would then obey the one configured limit.

Keep retry and timeout configuration, URL namespaces, Arrow schemas, and sink
publication/cleanup outside this initial extraction. They have different domain
contracts and do not need to move to establish shared request admission.
Existing per-instance and download-pool defaults remain separate policies; this
proposal adds an explicit shared budget and does not introduce a global default
across the two libraries.

## Migration and verification

1. Move the limiter and its lifecycle tests to the common crate. Cover zero and
   oversized limits, fetch failure, pending-fetch cancellation, body EOF, body
   error, body drop, and cloned transport admission.
2. Adopt the crate in access and sink through compatibility wrappers. Retain a
   small integration test in each repository for operator-context replay and
   replacing a budget without nesting it. Keep native-SDK adapter tests local.
3. Test one access operator and one sink operator using clones of a one-request
   common budget. Hold the source response body and assert that a destination
   request waits; dropping or exhausting that body must admit the destination.
4. Verify that retries acquire their own request permits and that replacing an
   HTTP transport preserves caller layers. Never install the same common budget
   twice on a single transport stack.

This removes the duplicated implementation and its repeated contract tests while
keeping the adapter tests that prove each library actually uses the common code.
It also lets extension authors share resource policy without depending on both
libraries' internal storage implementations.
