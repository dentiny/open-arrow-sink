# Releasing

The first release is **v0.1.0**. All seven public crates inherit the version in
`[workspace.package]`; internal dependency versions must match it.
`open-arrow-sink-test-support` is private (`publish = false`).

## Validate

Use Rust/Cargo 1.97.1 and install `protoc` for the Lance dependencies.
From a clean checkout, run:

```sh
cargo fmt --all --check
cargo test --workspace --all-features --locked
cargo publish --workspace --all-features --locked --dry-run
```

Cargo's workspace publish command packages and verifies the crates in dependency
order, including dependencies that have not yet been published. It skips the
private test support crate. Keep Cargo.lock committed so that release checks use
the reviewed dependency versions.

## Publish

1. Merge the release PR and check out the reviewed commit on `main`:

   ```sh
   git switch main
   git pull --ff-only
   ```

2. Run the validation commands above, then authenticate with `cargo login` using
   a crates.io API token allowed to create/publish all seven public crate names.
   Never commit the token.
3. Publish the workspace:

   ```sh
   cargo publish --workspace --all-features --locked
   ```

   Cargo handles dependency order and waits for registry availability.
4. After successful publication, tag the same reviewed commit:

   ```sh
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```

Publication is manual. Opening the PR or pushing a tag does not publish crates
or create a GitHub Release.

The public crates are `open-arrow-sink-core`, `open-arrow-sink-storage`,
`open-arrow-sink-layer`, `open-arrow-sink-parquet`, `open-arrow-sink-iceberg`,
`open-arrow-sink-lance`, and the `open-arrow-sink` facade.
