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

Formatting, build, clippy, test, semver, and WebAssembly checks run as git
pre-commit hooks via [prek](https://prek.j178.dev), configured in
[prek.toml](prek.toml). Install prek, the wasm targets, and
[wasmtime](https://wasmtime.dev), then enable the hook once per clone:

```
cargo install prek                        # or: brew install prek
cargo install cargo-semver-checks --locked # or: cargo binstall cargo-semver-checks
rustup target add wasm32-unknown-unknown wasm32-wasip2
brew install wasmtime                     # or: curl https://wasmtime.dev/install.sh -sSf | bash
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

WebAssembly
--

`tower-amp`'s client builds for `wasm32-unknown-unknown`, the target a
browser runs, and `wasm32-wasip2`, the WASI target, so CI and the hooks check
the library targets of the whole workspace on both, and run the tests of the
whole workspace on WASI under wasmtime:

```
cargo clippy --workspace --all-features --target wasm32-unknown-unknown -- -D warnings
cargo clippy --workspace --all-features --target wasm32-wasip2 -- -D warnings
cargo test --workspace --all-features --target wasm32-wasip2 --tests
```

[.cargo/config.toml](.cargo/config.toml) names wasmtime as the runner for
that target, so `cargo test` invokes it on each test binary. `--tests`
leaves out the examples, which need TCP; doctests are skipped on any
non-host target anyway. A panic aborts a wasm test binary, so one failing
test there takes the rest of its binary down with it; run the failing test
alone to see its message. The tests in `tower-amp/tests/roundtrip.rs` drive
a served connection, so that file is compiled out on wasm, and
`tower-amp/tests/client.rs` covers the client against a hand-rolled peer
instead. A test that needs both sides goes in the first; one that only
needs the client goes in the second, and must not spawn, since on
`wasm32-unknown-unknown` there is no runtime to spawn on.

Nothing runs the tests in a browser. That would take Node and the
`wasm-bindgen-test` runner, and swapping `#[tokio::test]` for
`#[wasm_bindgen_test]` on that target, since there is no tokio runtime to
block on there.

The server is compiled out on every WebAssembly target: tokio refuses to
build its `net` feature, or `fs`, `io-std`, `process`, `rt-multi-thread`,
and `signal`, for any wasm target. `Cargo.toml` therefore enables those only
off wasm, in a `[target.'cfg(not(target_family = "wasm"))']` table, and the
same split applies to the dev-dependencies; a new tokio feature goes there
unless it is one of `io-util`, `macros`, `rt`, `sync`, or `time`. (With
`--cfg tokio_unstable` and Rust 1.97 or newer, a server does run on
`wasm32-wasip2` under wasmtime with sockets enabled. It is left out because
that cfg would then be required of every consumer building for WASI, client
or not.)

The two wasm targets differ in whether tokio's runtime can run. Under WASI it
can, so `wasm32-wasip2` gets `rt` and `time` and the client works as it does
natively. In a browser it cannot, so on `wasm32-unknown-unknown` the client
spawns through `wasm-bindgen-futures` and times out through `gloo-timers`.
Everything that differs there is a small `#[cfg]`'d helper in `client.rs`
(`spawn`, `timeout`, the `Send` bound on the I/O type, and the `Service`
future type), keyed on `all(target_arch = "wasm32", target_os = "unknown")`,
so a change to the client should only ever need one of those to change.
Only the WASI tests run, so a change to the browser-only helpers is checked
by clippy alone; test the browser path by hand in a page.

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
