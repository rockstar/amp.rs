Developing
==

Layout
--

This repository is a Cargo workspace. The root `Cargo.toml` lists the member
crates and holds the package fields and dependency versions they share; each
crate lives in its own directory (for example `serde_amp/`) with its own
`Cargo.toml`, `README.md`, and copies of the license files so that
`cargo publish` packages them. All `cargo` commands below run from the
repository root and cover every crate.

Pre-commit hooks
--

Formatting, build, clippy, and semver checks run as git pre-commit hooks via
[prek](https://prek.j178.dev), configured in [prek.toml](prek.toml).
Install prek, then enable the hook once per clone:

```
cargo install prek                        # or: brew install prek
cargo install cargo-semver-checks --locked # or: cargo binstall cargo-semver-checks
prek install
```

The `cargo semver-checks` hook compares each crate's public API against the
latest version published on crates.io and fails if a change needs a bigger
version bump than that crate's `Cargo.toml` currently has. It is the slowest
hook since it builds rustdoc for both versions, and it needs network access to
fetch the baseline.

The hook runs automatically on `git commit` against the staged files. To run
the checks by hand:

```
prek run --all-files                    # whole repository
prek run --files serde_amp/src/ser.rs   # specific files
```

A bare `prek` only checks staged files, so it reports "no files to check" when
nothing is staged.

`prek install` also installs a `commit-msg` hook that rejects any commit whose
message is not a [conventional commit](https://www.conventionalcommits.org/)
(`type(scope)?!?: subject`, with `type` one of `feat`, `fix`, `docs`,
`refactor`, `perf`, `test`, `build`, `ci`, `chore`, or `revert`). The pre-commit
hooks run before git opens your editor, so this check runs separately after the
message is written. To test a message by hand:

```
prek run --hook-stage commit-msg --commit-msg-filename path/to/message.txt
```

Minimum supported Rust version
--

Every crate declares the same `rust-version`, set once in the root
`Cargo.toml` under `[workspace.package]`. CI checks the library targets of the
whole workspace on exactly that toolchain. The workspace uses resolver 3, so
`cargo` skips dependency versions that need a newer compiler whenever an older
release satisfies the requirement. To run the same check locally:

```
rustup toolchain install 1.85 --profile minimal
cargo +1.85 check --workspace --all-features
```

Only library targets are checked, since dev-dependencies do not affect
consumers and may need a newer toolchain. Raising the floor is a minor version
bump for the crates that need it; the comment next to `rust-version` records
what currently sets it, so update that when you move it.

Benchmarks
--

`amp-protocol` has a [divan](https://docs.rs/divan) benchmark for decoding,
covering a small box, a large box, and the large box fed through the codec a
TCP segment at a time:

```
cargo bench -p amp-protocol --all-features
```

The hooks compile it but do not run it, since timings are too noisy to gate a
commit. Run it by hand before and after touching `AmpBox::decode` or the
codec, and compare. The chunked case is the one that has regressed before:
each call must only look at bytes the previous call did not, or a large box
takes seconds instead of milliseconds.

Releasing
--

```
./release.sh --dry-run   # show what would be bumped, tagged and pushed
./release.sh             # do it
```

Each crate is released on its own, tagged `<crate>-<version>`. The script
patch-bumps every crate that changed since its last tag, releases as-is any
crate whose version was already bumped by hand (because `cargo semver-checks`
demanded it), commits the bumps, tags, and pushes. Pushing a tag starts the
[release workflow](.github/workflows/release.yml), which publishes that crate
to crates.io. The script refuses to run unless `main` is clean, identical to
`origin/main`, and green in CI; it tells you what to install if a tool is
missing. The header of [release.sh](release.sh) has the full rules.
