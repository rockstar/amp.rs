amp-protocol
==

![build-and-check](https://github.com/rockstar/amp.rs/actions/workflows/build-and-check.yml/badge.svg) ![crates.io](https://img.shields.io/crates/v/amp-protocol.svg) ![docs.rs](https://docs.rs/amp-protocol/badge.svg)

The wire format and message types of [Asynchronous Messaging Protocol](https://amp-protocol.net/)
(AMP), the key/value remoting protocol from Twisted.

This crate knows how AMP packets ("boxes") look on the wire and which keys the
protocol reserves. It has no opinion about how your arguments map to Rust
types; that is what [serde_amp](https://crates.io/crates/serde_amp) is for.

* `AmpBox` is one packet: ordered key/value pairs of bytes, with the
  protocol's length limits enforced on insert, and `encode`/`decode` to and
  from bytes. Decoding reports how many bytes it consumed, so a stream can be
  framed.
* `Request`, `Answer`, and `ErrorResponse` carry the protocol's reserved keys
  (`_command`, `_ask`, `_answer`, `_error`, `_error_code`,
  `_error_description`) alongside the user's own key/value pairs.
  `Message::from_box` classifies an incoming box into one of them.
* The `tokio` feature adds `codec::AmpCodec`, a `tokio_util::codec` encoder
  and decoder for framing boxes on a socket. It refuses a box longer than a
  limit, 16 MiB by default, so a peer that never finishes one cannot fill
  memory, and one with more pairs than a limit, 1,024 by default, so a box of
  millions of tiny pairs cannot cost many times its size to decode. Both are
  checked as the box arrives, before any of it is decoded.

Usage
--

```rust
use amp_protocol::{AmpBox, Message, Request};

fn main() -> Result<(), amp_protocol::Error> {
    // Build a request and put it on the wire.
    let mut arguments = AmpBox::new();
    arguments.insert("a", "13")?;
    arguments.insert("b", "81")?;
    let request = Request::new("Sum", arguments).with_ask("1");
    let bytes = request.into_box()?.encode();

    // Read it back off the wire.
    let (amp_box, consumed) = AmpBox::decode(&bytes)?;
    assert_eq!(consumed, bytes.len());
    let request = match Message::from_box(amp_box)? {
        Message::Request(request) => request,
        other => panic!("expected a request, got {other:?}"),
    };
    assert_eq!(request.command, b"Sum");
    assert_eq!(request.arguments.get("a"), Some(&b"13"[..]));

    // Reply, echoing the request's `_ask` as `_answer`.
    let mut results = AmpBox::new();
    results.insert("total", "94")?;
    let answer = request.answer(results).expect("request wanted an answer");
    let bytes = answer.into_box()?.encode();
    assert_eq!(
        bytes,
        b"\x00\x05total\x00\x0294\x00\x07_answer\x00\x011\x00\x00"
    );
    Ok(())
}
```

License
--

amp-protocol is licensed under the MIT license ([LICENSE-MIT](LICENSE-MIT) or
<http://opensource.org/licenses/MIT>).
