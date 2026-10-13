#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod client;
mod command;
mod error;
mod handle;
mod router;
// A server listens on TCP and spawns on the tokio runtime, neither of which
// exists on wasm32-unknown-unknown; only the client builds there.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod server;

/// The box type that carries a command's arguments and results, re-exported
/// from `amp-protocol` so that services use the version this crate was built
/// against.
pub use amp_protocol::AmpBox;
/// The request type that services accept, re-exported from `amp-protocol`
/// for the same reason as [`AmpBox`].
pub use amp_protocol::Request;
/// The codec a connection is framed by, re-exported from `amp-protocol` so
/// that the frame-size limit can be set without depending on it directly.
/// See [`serve_with_codec`], [`serve_connection_with_codec`], and
/// [`Client::with_codec`].
pub use amp_protocol::codec::AmpCodec;
pub use client::Client;
pub use command::Command;
pub use error::Error;
pub use handle::{Handle, HandleWith};
pub use router::Router;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use server::{serve, serve_connection, serve_connection_with_codec, serve_with_codec};
/// Declares a struct as a command's arguments. See [`Command`].
pub use tower_amp_macros::command;
