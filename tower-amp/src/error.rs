use std::fmt;
use std::sync::Arc;

use amp_protocol::ErrorResponse;

/// Why a command failed.
///
/// A handler fails a command by returning [`Error::new`] with the code and
/// description the peer should see, or by returning any other variant, which
/// goes on the wire with the code `UNKNOWN`. Of those, only
/// [`Error::Deserialize`] takes its message with it, since it is about the
/// peer's own request; the rest are answered with the description
/// `internal error`, so that nothing about this side's internals reaches
/// the peer. The error itself is logged when the `tracing` feature is on. A
/// client receives the peer's code and description back as
/// [`Error::Command`].
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The command failed with an AMP error code and description.
    Command {
        /// The error code, by convention all-caps `SNAKE_CASE`.
        code: String,
        /// A human-readable description of the failure.
        description: String,
    },
    /// The connection failed.
    Io(std::io::Error),
    /// A box broke the protocol, or a peer sent a box that is neither a
    /// request nor a response.
    Protocol(amp_protocol::Error),
    /// Arguments or results of this side's own type could not be turned
    /// into a box to send. The type is at fault, not the peer: a nested
    /// struct, for example, has no AMP representation.
    Serialize(serde_amp::Error),
    /// A box from the peer did not match the type it was read as: on a
    /// server, the arguments of a command; on a client, the results of a
    /// call. The two sides disagree about the command's schema, or the peer
    /// sent something malformed. A server answers the peer with this as an
    /// `UNKNOWN` error.
    Deserialize(serde_amp::Error),
    /// The connection closed before the command was answered, or before it
    /// could be sent.
    ConnectionClosed {
        /// What ended the connection: the I/O error, or the box from the
        /// peer that broke the protocol. `None` when the peer closed the
        /// connection cleanly. Shared, since one failure fails every call
        /// that was waiting on the connection.
        cause: Option<Arc<Error>>,
    },
    /// The call was not answered within the client's timeout. See
    /// [`Client::with_timeout`](crate::Client::with_timeout).
    Timeout,
}

/// The description a peer sees for an error that is about this side rather
/// than its request.
pub(crate) const INTERNAL_ERROR: &str = "internal error";

impl Error {
    /// Fails a command with `code` and `description`.
    pub fn new(code: impl Into<String>, description: impl Into<String>) -> Self {
        Error::Command {
            code: code.into(),
            description: description.into(),
        }
    }

    /// The error for a call on a connection that has ended because of
    /// `cause`, or cleanly.
    pub(crate) fn closed(cause: Option<Arc<Error>>) -> Self {
        Error::ConnectionClosed { cause }
    }

    /// The error for a request naming a command this side does not serve.
    pub(crate) fn unhandled(command: &[u8]) -> Self {
        Error::new(
            ErrorResponse::UNHANDLED,
            format!("unhandled command: {}", String::from_utf8_lossy(command)),
        )
    }

    /// The code that goes on the wire.
    pub(crate) fn code(&self) -> &str {
        match self {
            Error::Command { code, .. } => code,
            _ => ErrorResponse::UNKNOWN,
        }
    }

    /// The description that goes on the wire.
    ///
    /// A command error carries the description the handler chose, and a
    /// deserialize error its message, which tells the peer its request did
    /// not match the command's schema. Any other error is about this side,
    /// not the request: an I/O error names hosts, a panic message says what
    /// the handler was doing. A peer, who may be anyone, gets a fixed
    /// description instead, and the error goes to the log.
    pub(crate) fn description(&self) -> String {
        match self {
            Error::Command { description, .. } => description.clone(),
            Error::Deserialize(_) => self.to_string(),
            _ => INTERNAL_ERROR.to_string(),
        }
    }

    /// A second copy of an error that ended a connection, for the client
    /// handles of a connection whose driver gets the original.
    ///
    /// Only an I/O error or a protocol error ends a connection. A protocol
    /// error is plain data. An I/O error is rebuilt from its OS error code
    /// when it has one, which keeps its kind and message, and otherwise
    /// from its kind and message, which loses only its source. Anything
    /// else keeps its message.
    pub(crate) fn duplicate(&self) -> Error {
        use amp_protocol::Error as Protocol;

        match self {
            Error::Io(err) => Error::Io(duplicate_io(err)),
            Error::Protocol(err) => Error::Protocol(match err {
                Protocol::EmptyKey => Protocol::EmptyKey,
                Protocol::KeyTooLong { len } => Protocol::KeyTooLong { len: *len },
                Protocol::ValueTooLong { len } => Protocol::ValueTooLong { len: *len },
                Protocol::Incomplete => Protocol::Incomplete,
                Protocol::MissingKey(key) => Protocol::MissingKey(key),
                Protocol::ReservedKey(key) => Protocol::ReservedKey(key.clone()),
                Protocol::UnrecognizedBox => Protocol::UnrecognizedBox,
                Protocol::FrameTooLong { limit } => Protocol::FrameTooLong { limit: *limit },
                Protocol::Io(err) => Protocol::Io(duplicate_io(err)),
                // A variant amp-protocol has since added.
                other => Protocol::Io(std::io::Error::other(other.to_string())),
            }),
            other => Error::Io(std::io::Error::other(other.to_string())),
        }
    }
}

/// See [`Error::duplicate`].
fn duplicate_io(err: &std::io::Error) -> std::io::Error {
    match err.raw_os_error() {
        Some(code) => std::io::Error::from_raw_os_error(code),
        None => std::io::Error::new(err.kind(), err.to_string()),
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Command { code, description } => write!(f, "{code}: {description}"),
            Error::Io(err) => write!(f, "connection failed: {err}"),
            Error::Protocol(err) => write!(f, "protocol error: {err}"),
            Error::Serialize(err) => write!(f, "could not serialize: {err}"),
            Error::Deserialize(err) => write!(f, "could not deserialize: {err}"),
            Error::ConnectionClosed { cause: Some(cause) } => {
                write!(f, "connection closed: {cause}")
            }
            Error::ConnectionClosed { cause: None } => f.write_str("connection closed"),
            Error::Timeout => f.write_str("timed out"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            Error::Protocol(err) => Some(err),
            Error::Serialize(err) => Some(err),
            Error::Deserialize(err) => Some(err),
            Error::ConnectionClosed { cause: Some(cause) } => Some(cause.as_ref()),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<amp_protocol::Error> for Error {
    fn from(err: amp_protocol::Error) -> Self {
        match err {
            amp_protocol::Error::Io(err) => Error::Io(err),
            other => Error::Protocol(other),
        }
    }
}

/// Lets a service wrapped in tower middleware, whose error type is a boxed
/// error, be served. An `Error` inside the box comes back out unchanged;
/// anything else, such as a timeout, becomes an `UNKNOWN` error carrying its
/// message.
impl From<tower::BoxError> for Error {
    fn from(err: tower::BoxError) -> Self {
        match err.downcast::<Error>() {
            Ok(err) => *err,
            Err(err) => Error::new(ErrorResponse::UNKNOWN, err.to_string()),
        }
    }
}
