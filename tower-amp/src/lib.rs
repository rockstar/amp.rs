#![cfg_attr(test, expect(unused_crate_dependencies))]
#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod client;
mod command;
mod connection;
mod error;
mod handle;
mod log;
mod router;
// A server listens on TCP, and tokio has no networking on WebAssembly; only
// the client builds there.
#[cfg(not(target_family = "wasm"))]
mod server;

/// The box type that carries a command's arguments and results, re-exported
/// from `amp-protocol` so that services use the version this crate was built
/// against.
pub use amp_protocol::AmpBox;
/// The request type that services accept, re-exported from `amp-protocol`
/// for the same reason as [`AmpBox`].
pub use amp_protocol::Request;
/// The codec a connection is framed by, re-exported from `amp-protocol` so
/// that its limits can be set without depending on it directly.
/// See [`Server::codec`], [`Connection::with_codec`], and
/// [`Client::with_codec`].
pub use amp_protocol::codec::AmpCodec;
pub use client::Client;
pub use command::Command;
pub use connection::Connection;
pub use error::Error;
pub use handle::{Handle, HandleWith};
pub use router::Router;
#[cfg(not(target_family = "wasm"))]
pub use server::{Server, serve, serve_connection};
/// Declares a struct as a command's arguments. See [`Command`].
///
/// The generated code refers to this crate as `::tower_amp`, so it must be a
/// dependency under its own name. Renaming it with `package = "tower-amp"` in
/// Cargo.toml is not supported.
pub use tower_amp_macros::command;
