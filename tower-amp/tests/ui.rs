#![expect(unused_crate_dependencies)]
//! The `#[command]` attribute's error messages, and a case it must accept.
//! Each `.rs` under `tests/ui` has the compiler's output in a `.stderr`
//! next to it; run with `TRYBUILD=overwrite` to regenerate those.

// trybuild runs cargo, which is not available inside WebAssembly.
#![cfg(not(target_family = "wasm"))]

#[test]
fn command_attribute() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/generic_struct.rs");
    t.compile_fail("tests/ui/name_twice.rs");
    t.compile_fail("tests/ui/response_twice.rs");
    t.compile_fail("tests/ui/error_twice.rs");
    t.compile_fail("tests/ui/unknown_argument.rs");
}
