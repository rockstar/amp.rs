use crate::Error;

/// A command's schema: its name on the wire, the type of its results, and
/// the type its handler fails with.
///
/// Implemented on the struct that holds the command's arguments, normally by
/// the [`command`](macro@crate::command) attribute:
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use tower_amp::command;
///
/// #[derive(Deserialize)]
/// #[command(response = Quotient)]
/// struct Divide {
///     numerator: i64,
///     denominator: i64,
/// }
///
/// #[derive(Serialize)]
/// struct Quotient {
///     result: f64,
/// }
/// ```
///
/// The name defaults to the struct's name and can be set with
/// `name = "..."`. The response defaults to `()`, an empty box on the wire,
/// for a command with nothing to report. The error type defaults to
/// [`Error`], and `error = Type` sets it to a type of this side's own, so
/// that handlers fail with an enum of the failures they share rather than
/// with a code and description spelled out at each one:
///
/// ```
/// use std::fmt;
/// use serde::{Deserialize, Serialize};
/// use tower_amp::{command, Error, Handle};
///
/// /// Every way a command on this server can fail, with its code on the
/// /// wire. One such type serves every command.
/// #[derive(Debug)]
/// enum DivideError {
///     ZeroDivision,
/// }
///
/// impl DivideError {
///     fn code(&self) -> &'static str {
///         match self {
///             DivideError::ZeroDivision => "ZERO_DIVISION",
///         }
///     }
/// }
///
/// impl fmt::Display for DivideError {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         match self {
///             DivideError::ZeroDivision => f.write_str("cannot divide by zero"),
///         }
///     }
/// }
///
/// impl std::error::Error for DivideError {}
///
/// /// What the peer sees: the code, and the message as the description.
/// impl From<DivideError> for Error {
///     fn from(err: DivideError) -> Self {
///         Error::new(err.code(), err.to_string())
///     }
/// }
///
/// #[derive(Deserialize)]
/// #[command(response = Quotient, error = DivideError)]
/// struct Divide {
///     numerator: i64,
///     denominator: i64,
/// }
///
/// #[derive(Serialize)]
/// struct Quotient {
///     result: f64,
/// }
///
/// impl Handle for Divide {
///     async fn handle(self) -> Result<Quotient, DivideError> {
///         if self.denominator == 0 {
///             return Err(DivideError::ZeroDivision);
///         }
///         Ok(Quotient {
///             result: self.numerator as f64 / self.denominator as f64,
///         })
///     }
/// }
/// ```
///
/// Arguments travel as a box, a flat set of keys, so the attribute only
/// accepts structs. An enum or union is rejected at compile time:
///
/// ```compile_fail
/// use tower_amp::command;
///
/// #[command]
/// enum Divide {
///     Exact,
///     Rounded,
/// }
/// ```
///
/// The schema is what a client and server share. A server implements
/// [`Handle`](crate::Handle) or [`HandleWith`](crate::HandleWith) on the
/// struct as well, and needs `Deserialize` on it and `Serialize` on the
/// response. A client needs the reverse, and calls the command with
/// [`Client::call`](crate::Client::call). The error type is the server's
/// alone: a client receives whatever the peer answered as
/// [`Error::Command`].
pub trait Command {
    /// The command's name, the value of the request's `_command` key.
    const NAME: &'static str;

    /// The type of the command's results.
    type Response;

    /// The type a handler of the command fails with. It is converted into
    /// [`Error`] on the way to the peer, so a type of this side's own
    /// chooses its code and description in its `From` impl.
    type Error: Into<Error>;
}
