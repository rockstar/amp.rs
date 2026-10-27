use std::fmt;

use crate::{MAX_KEY_LENGTH, MAX_VALUE_LENGTH};

/// Everything that can go wrong while building, encoding, decoding, or
/// classifying a box.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A key was empty. An empty key is the box terminator on the wire, so a
    /// box cannot hold one.
    EmptyKey,
    /// A key was longer than 255 bytes.
    KeyTooLong {
        /// The offending key's length in bytes.
        len: usize,
    },
    /// A value was longer than 65,535 bytes.
    ValueTooLong {
        /// The offending value's length in bytes.
        len: usize,
    },
    /// The input ended before the box did. More bytes are needed.
    Incomplete,
    /// A box was missing a key the message type requires.
    MissingKey(&'static str),
    /// User data used one of the keys the protocol reserves: `_ask`,
    /// `_command`, `_answer`, `_error`, `_error_code`, or `_error_description`.
    ReservedKey(Vec<u8>),
    /// A box carried none of `_answer`, `_error`, or `_command`, so it is
    /// neither a request nor a response.
    UnrecognizedBox,
    /// A box on a stream was longer than the codec allows. See
    /// [`AmpCodec::with_max_frame_length`](crate::codec::AmpCodec::with_max_frame_length).
    #[cfg(feature = "tokio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    FrameTooLong {
        /// The limit in bytes.
        limit: usize,
    },
    /// A box had more pairs than the decoder allows. See
    /// [`AmpBox::decode_bounded`](crate::AmpBox::decode_bounded) and, with
    /// the `tokio` feature, `AmpCodec::with_limits`.
    TooManyPairs {
        /// The limit in pairs.
        limit: usize,
    },
    /// The underlying stream failed.
    #[cfg(feature = "tokio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::EmptyKey => write!(f, "keys must not be empty"),
            Error::KeyTooLong { len } => {
                write!(
                    f,
                    "key is {len} bytes, longer than the limit of {MAX_KEY_LENGTH}"
                )
            }
            Error::ValueTooLong { len } => write!(
                f,
                "value is {len} bytes, longer than the limit of {MAX_VALUE_LENGTH}"
            ),
            Error::Incomplete => write!(f, "input ended before the box did"),
            Error::MissingKey(key) => write!(f, "box is missing the required key {key}"),
            Error::ReservedKey(key) => write!(
                f,
                "key {} is reserved for the protocol",
                String::from_utf8_lossy(key)
            ),
            Error::UnrecognizedBox => {
                write!(f, "box has none of _answer, _error, or _command")
            }
            #[cfg(feature = "tokio")]
            Error::FrameTooLong { limit } => {
                write!(f, "box is longer than the limit of {limit} bytes")
            }
            Error::TooManyPairs { limit } => {
                write!(f, "box has more than the limit of {limit} pairs")
            }
            #[cfg(feature = "tokio")]
            Error::Io(err) => write!(f, "i/o error: {err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            #[cfg(feature = "tokio")]
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}
