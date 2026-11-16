# Development and release documentation

Use a Rust toolchain supporting edition 2024 and the resolved dependencies in
`Cargo.lock`. No toolchain or minimum Rust version is pinned in this workspace.
Lance's dependencies require `protoc` (`protobuf-compiler` on Ubuntu).
Install Taplo with `cargo install taplo-cli --version 0.10.0 --locked`.

The [CI workflow](../.github/workflows/ci.yml) runs the checks below on pull
requests and pushes to `main`, and can also be dispatched manually. It uses
stable Rust on Ubuntu, installs Protobuf and native build dependencies, and
tests both default workspace features and all features. Download tests use
local loopback listeners and require no cloud credentials. CI disables debug
information and incremental compilation and uses the LLVM linker to reduce
Lance's memory and disk requirements on hosted runners.

## Repository checks

Run from the repository root:

```sh
cargo fmt --all --check
taplo fmt --check
taplo lint --no-schema
cargo metadata --no-deps --locked --format-version 1 > /dev/null
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo doc --workspace --all-features --no-deps --locked
```

`cargo test --workspace` exercises member crates, including backend dependencies,
even though the facade defaults are empty. It does not enable every optional
codec/cloud feature. For that coverage also run:

```sh
cargo test --workspace --all-features --locked
```

Download tests bind local loopback listeners. Run in an environment permitting
local sockets. The lightweight extension examples need no service credentials:

```sh
cargo run --locked -p open-arrow-sink-layer --example custom_sink
cargo run --locked -p open-arrow-sink-layer --example custom_layer
```

For the quick start, use a fresh directory on each run. Existing destinations
are intentionally rejected by Create. Native backend examples and readback
commands are listed in [usage](usage.md#runnable-examples).

## Keep documentation aligned with code

When changing a public option, check `core/src/request.rs`, capabilities,
`core/src/validation.rs`, each backend's validation, option-override layers,
[options](options.md), and the [support matrix](support.md). A schema-changing
layer also needs both input/output schema and metadata behavior documented.
When adding a backend or decorator, update the appropriate extension guide,
component README, facade feature table, and a runnable example.

README and guides are Markdown files, not automatically included Rust doctests.
Compile/run complete code blocks against the stated features; contextual snippets
marked `ignore` require the surrounding schema, request, runtime, and error type.
Verify local Markdown paths and anchors whenever moving content. Preserve deep
links used by component READMEs where practical.

## Before publishing a release

1. Run the checks and examples above against the commit being released. Verify
   both default-feature and explicitly enabled codec behavior.
2. Confirm workspace package versions and local dependency versions match.
   Inspect packaged file lists with `cargo package --list -p <crate>`; include
   READMEs, the license, and required source files.
3. Publish in dependency order: core → storage → layer → backend crates → facade.
   Backend crates are optional for the facade at build time but their versioned
   dependencies must resolve when packaging/publishing it.
4. Check the intended versions are available before replacing README's repository
   dependency with a crates.io version. The workspace currently declares `0.1.0`;
   that declaration alone is not proof of publication.
5. Record supported feature combinations and material limits in release notes:
   explicit unordered writes, Iceberg Append, fixed schemas, backend-specific
   publication, and uncertain-commit handling.

These are maintainer instructions, not an automated publishing workflow. See the
[extension contracts](extending-sinks.md) for guarantees a new backend must satisfy.
