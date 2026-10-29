//! Building a box through serde, small and large.
//!
//! Run with `cargo bench -p serde_amp`. `AmpBox::insert` scans the existing
//! pairs for the key, so building a box is quadratic in its size. That is the
//! right trade for the typical four-pair request, where a scan beats hashing,
//! and the small case guards that. The large case is the one that would make
//! someone with a big box reach for an index: watch both when touching how
//! `AmpBox` stores or looks up pairs.
#![expect(
    clippy::unwrap_used,
    reason = "a bench that cannot set up should fail loudly"
)]

use std::collections::BTreeMap;

use divan::{Bencher, black_box};
use serde::Serialize;

fn main() {
    divan::main();
}

/// Pairs in the large box.
const LARGE_KEYS: u32 = 10_000;

/// A typical request: four short fields.
#[derive(Serialize)]
struct Sum {
    _ask: u32,
    _command: &'static str,
    a: i32,
    b: i32,
}

#[divan::bench]
fn small_struct(bencher: Bencher) {
    let sum = Sum {
        _ask: 1,
        _command: "Sum",
        a: 13,
        b: 81,
    };
    bencher.bench(|| serde_amp::to_box(black_box(&sum)).unwrap());
}

#[divan::bench]
fn large_map(bencher: Bencher) {
    let map: BTreeMap<String, String> = (0..LARGE_KEYS)
        .map(|i| (format!("key{i:05}"), "v".repeat(50)))
        .collect();
    bencher.bench(|| serde_amp::to_box(black_box(&map)).unwrap());
}
