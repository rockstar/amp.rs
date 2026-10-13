use std::fmt;

use serde::{de, ser};

/// Everything that can go wrong converting between a Rust value and a box.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A `Serialize` or `Deserialize` implementation reported a problem, such
    /// as a missing or unknown field.
    Message(String),
    /// A key or value broke the protocol's length limits.
    Protocol(amp_protocol::Error),
    /// The top-level value is not a struct or map, so it cannot be a box.
    NotABox,
    /// The value has no representation in AMP. The payload says which kind of
    /// value, for example a nested struct or an enum variant carrying data.
    Unsupported(&'static str),
    /// A value with no key of its own, such as a map key, could not be read
    /// as the requested type. The payload says what was expected.
    InvalidValue(&'static str),
    /// The value under `key` could not be read as the requested type.
    InvalidValueForKey {
        /// The key, converted lossily from UTF-8.
        key: String,
        /// What the value was expected to be.
        expected: &'static str,
        /// The value's raw bytes.
        value: Vec<u8>,
    },
}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Message(message) => f.write_str(message),
            Error::Protocol(err) => write!(f, "{err}"),
            Error::NotABox => f.write_str("a box must be a struct or map at the top level"),
            Error::Unsupported(what) => write!(f, "{what} cannot be represented in AMP"),
            Error::InvalidValue(expected) => write!(f, "expected {expected}"),
            Error::InvalidValueForKey {
                key,
                expected,
                value,
            } => {
                write!(f, "key {key:?}: expected {expected}, got ")?;
                write_truncated(f, value)
            }
        }
    }
}

/// Writes `value` as quoted text, lossily, cut short so that a large value
/// does not swamp the message.
fn write_truncated(f: &mut fmt::Formatter<'_>, value: &[u8]) -> fmt::Result {
    const LIMIT: usize = 64;
    let text = String::from_utf8_lossy(value);
    let mut chars = text.chars();
    let shown: String = chars.by_ref().take(LIMIT).collect();
    write!(f, "{shown:?}")?;
    if chars.next().is_some() {
        f.write_str("...")?;
    }
    Ok(())
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Protocol(err) => Some(err),
            _ => None,
        }
    }
}
