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
`tower-amp/tests/client.rs` covers the client, and a `Connection` that
serves, against a hand-rolled peer instead. A test that needs both sides
goes in the first; one that only needs the client goes in the second, and
must not spawn, since on `wasm32-unknown-unknown` there is no runtime to
spawn on.

CI also runs `tower-amp/tests/client.rs` on `wasm32-unknown-unknown`, in
Node, where there is no tokio runtime to block on: each test there is a
`#[wasm_bindgen_test]` instead of a `#[tokio::test]`, through a pair of
`cfg_attr`s, and the file's own timeouts wait on the host's timer. This is
not a hook, since it needs Node and [wasm-pack](https://github.com/wasm-bindgen/wasm-pack),
which installs the `wasm-bindgen-test-runner` matching `Cargo.lock`. To run
it by hand, with Node installed:

```
cargo install wasm-pack                   # or: brew install wasm-pack
wasm-pack test --node tower-amp --all-features --test client
```

Only that one test file runs there: the others need a server, and the unit
tests in `client.rs` a tokio runtime.

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
Everything that differs there is a small `#[cfg]`'d helper, keyed on
`all(target_arch = "wasm32", target_os = "unknown")`: in `connection.rs`,
`spawn` and `call`, which elsewhere runs a handler on a task of its own to
survive its panic; in `client.rs`, `timeout`, the `Send` bound on the I/O
type, and the `Service` future type. A change to the client or the
connection should only ever need one of those to change. The client tests
above exercise `spawn` and `timeout` on that target, the latter through
`an_unanswered_call_times_out`, and `call` through
`a_connection_answers_the_peer_and_calls_it`.

Twisted interoperability
--

AMP's reference implementation is Twisted's, and the point of these crates
is to talk to it, so `tower-amp/tests/twisted.rs` runs this crate's server
against Twisted's client and this crate's client against Twisted's server,
over TCP on loopback. The Python side is `tower-amp/tests/twisted/peer.py`,
whose commands are the `Sum` and `Divide` from Twisted's AMP documentation,
and `Relay`, which each side answers by calling `Sum` on the other, so that
both runs also cover a command going the other way on the same connection.

The tests are marked `#[ignore]`, since they need a Python with Twisted
installed, so `cargo test` and the hooks skip them and only CI runs them.
To run them by hand:

```
python3 -m venv .venv && .venv/bin/pip install twisted
PYTHON="$PWD/.venv/bin/python" cargo test -p tower-amp --test twisted -- --ignored
```

`PYTHON` names the interpreter and defaults to `python3`. It must be an
absolute path or a name on `PATH`: cargo runs the test binary from the
crate's own directory, not the repository root, so a relative
`.venv/bin/python` is not found. The tests kill
the Python server they start when they finish, and a peer that does not
answer within thirty seconds fails the test rather than hanging it.

Benchmarks
--

`amp-protocol` has a [divan](https://docs.rs/divan) benchmark for decoding,
covering a small box, a large box, and the large box fed through the codec a
TCP segment at a time:

```
cargo bench -p amp-protocol --all-features
```

`serde_amp` has one for building a box through `to_box`, covering a
four-field struct and a 10,000-entry map:

```
cargo bench -p serde_amp
```

`tower-amp` has one for calls through a served connection, one at a time
and pipelined, over an in-memory pipe and over loopback TCP, reported as
calls per second as well as time. The pipe times the two sides' work alone;
TCP adds the system calls, so it shows how many writes a batch of calls
costs:

```
cargo bench -p tower-amp
```

The hooks compile them but do not run them, since timings are too noisy to
gate a commit. Run them by hand before and after touching `AmpBox::decode`,
the codec, how `AmpBox` stores and looks up pairs, or how a connection reads,
dispatches and answers a request, and compare. The chunked
decode case is the one that has regressed before: each call must only look at
bytes the previous call did not, or a large box takes seconds instead of
milliseconds. The `to_box` cases pull in opposite directions: inserting scans
the existing pairs, which is quadratic in the large case but beats hashing in
the small one, so an index that speeds up the first must not slow the second.
The sequential calls case shows anything added to a call's critical path; the
pipelined case shows only what does not overlap with the other calls in
flight, so a cost that moves between them has not gone away.

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
to crates.io and then creates a GitHub release for the tag. The script refuses
to run unless `main` is clean, identical to `origin/main`, and green in CI; it
tells you what to install if a tool is missing. The header of
[release.sh](release.sh) has the full rules.

There is no changelog in the tree. The release notes are generated by
[release-notes.sh](release-notes.sh) from the commits since the crate's
previous tag that touched its directory (or the root `Cargo.toml`), grouped
by conventional-commit type, which is why the commit-msg hook insists on
that format. A `!` after the type, or a `BREAKING CHANGE:` footer, files the
commit under breaking changes. Write subjects for the consumer who will read
them there. `./release.sh --dry-run` prints the notes each release would get;
`./release-notes.sh tower-amp-0.2.2` prints them for a past release.
