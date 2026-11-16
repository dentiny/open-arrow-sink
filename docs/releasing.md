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

The Release workflow runs these checks on pull requests, release tags, and manual
dispatches. Manual dispatch only validates packages; it does not publish them.

## Publish

1. Configure the repository Actions secret `CARGO_REGISTRY_TOKEN` with a crates.io
   API token allowed to publish all seven public crates. For a first release, the
   token must also allow creating those crate names. Never commit the token.
2. Merge the release PR and ensure its checks pass.
3. Tag the reviewed commit on `main` and push the tag:

   ```sh
   git switch main
   git pull --ff-only
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```

The Release workflow checks that the tag matches the workspace version and that
the commit belongs to `main`, then runs `cargo publish --workspace` with all
features and the lockfile. Cargo handles dependency order and waits for registry
availability. No GitHub Release or tag is created by opening the PR.

For a local release instead, run the validation above, authenticate with
`cargo login`, then run `cargo publish --workspace --all-features --locked` from
the same reviewed commit. Use one publishing method per release.

The public crates are `open-arrow-sink-core`, `open-arrow-sink-storage`,
`open-arrow-sink-layer`, `open-arrow-sink-parquet`, `open-arrow-sink-iceberg`,
`open-arrow-sink-lance`, and the `open-arrow-sink` facade.
