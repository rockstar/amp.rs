#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(test, expect(unused_crate_dependencies))]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod amp_box;
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub mod codec;
mod error;
mod message;

pub use amp_box::{AmpBox, Iter};
pub use error::Error;
pub use message::{Answer, ErrorResponse, Message, Request};

/// The longest key a box may hold, in bytes. Keys share the 16-bit length
/// prefix used by values, but the protocol restricts them to 8 bits. A key of
/// length zero is the box terminator.
pub const MAX_KEY_LENGTH: usize = 0xFF;

/// The longest value a box may hold, in bytes.
pub const MAX_VALUE_LENGTH: usize = 0xFFFF;

// The keys the protocol reserves for itself. A box is a request if it carries
// `_command`, an answer if it carries `_answer`, and an error response if it
// carries `_error`. User arguments and results must not use any of these.
const KEY_ASK: &str = "_ask";
const KEY_COMMAND: &str = "_command";
const KEY_ANSWER: &str = "_answer";
const KEY_ERROR: &str = "_error";
const KEY_ERROR_CODE: &str = "_error_code";
const KEY_ERROR_DESCRIPTION: &str = "_error_description";
const RESERVED_KEYS: [&str; 6] = [
    KEY_ASK,
    KEY_COMMAND,
    KEY_ANSWER,
    KEY_ERROR,
    KEY_ERROR_CODE,
    KEY_ERROR_DESCRIPTION,
];

fn is_reserved_key(key: &[u8]) -> bool {
    RESERVED_KEYS
        .iter()
        .any(|reserved| reserved.as_bytes() == key)
}
