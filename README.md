serde_amp
==

![build-and-check](https://github.com/rockstar/amp.rs/actions/workflows/build-and-check.yml/badge.svg)

Rust tooling for [Asynchronous Messaging Protocol](https://amp-protocol.net/)
(AMP), the key/value remoting protocol from Twisted.

This repository is a Cargo workspace. Each crate has its own README with usage
details.

| Crate                         | Description                                                    |
|-------------------------------|----------------------------------------------------------------|
| [amp-protocol](amp-protocol/) | The wire format, box type, and request/response message types |
| [serde_amp](serde_amp/)  | [serde](https://serde.rs) serialization and deserialization of AMP boxes |
| [tower-amp](tower-amp/)  | AMP servers and clients on [tower](https://github.com/tower-rs/tower) and [tokio](https://tokio.rs) |
| [tower-amp-macros](tower-amp-macros/) | The `#[command]` attribute, re-exported by tower-amp |

License
--

Every crate in this workspace is licensed under the MIT license ([LICENSE-MIT](LICENSE-MIT) or
<http://opensource.org/licenses/MIT>).
