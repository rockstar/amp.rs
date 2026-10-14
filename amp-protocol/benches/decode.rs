//! Decoding throughput, whole and as a stream.
//!
//! Run with `cargo bench -p amp-protocol --all-features`. The chunked case is
//! the one to watch: it used to re-parse the buffer on every chunk, which
//! made a large box take seconds instead of milliseconds.

use amp_protocol::{AmpBox, codec::AmpCodec};
use bytes::BytesMut;
use divan::{Bencher, black_box};
use tokio_util::codec::Decoder;

fn main() {
    divan::main();
}

/// A typical request: four short pairs.
const SMALL: &[u8] =
    b"\x00\x04_ask\x00\x011\x00\x08_command\x00\x03Sum\x00\x01a\x00\x0213\x00\x01b\x00\x0281\x00\x00";

/// Pairs in the large box, about 1.2 MB on the wire. More than the default
/// codec allows, so the codec cases that read it raise the pair limit.
const LARGE_KEYS: u32 = 20_000;

/// A TCP segment's worth of payload, so the chunked case looks like a read
/// loop over a socket.
const CHUNK: usize = 1460;

fn large() -> Vec<u8> {
    let mut amp_box = AmpBox::new();
    for i in 0..LARGE_KEYS {
        amp_box
            .insert(format!("key{i:05}"), vec![b'v'; 50])
            .unwrap();
    }
    amp_box.encode()
}

#[divan::bench]
fn small_box(bencher: Bencher) {
    bencher.bench(|| AmpBox::decode(black_box(SMALL)).unwrap());
}

#[divan::bench]
fn small_box_through_codec(bencher: Bencher) {
    let mut codec = AmpCodec::new();
    let mut buf = BytesMut::new();
    bencher.bench_local(|| {
        buf.extend_from_slice(black_box(SMALL));
        codec.decode(&mut buf).unwrap().unwrap()
    });
}

#[divan::bench]
fn large_box(bencher: Bencher) {
    let bytes = large();
    bencher.bench(|| AmpBox::decode(black_box(&bytes)).unwrap());
}

#[divan::bench]
fn large_box_through_codec_in_chunks(bencher: Bencher) {
    let bytes = large();
    bencher.bench(|| {
        let mut codec = AmpCodec::with_limits(16 * 1024 * 1024, LARGE_KEYS as usize);
        let mut buf = BytesMut::new();
        let mut decoded = None;
        for chunk in black_box(&bytes).chunks(CHUNK) {
            buf.extend_from_slice(chunk);
            if let Some(amp_box) = codec.decode(&mut buf).unwrap() {
                decoded = Some(amp_box);
            }
        }
        decoded.unwrap()
    });
}
