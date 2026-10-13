/// A command's schema: its name on the wire and the type of its results.
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
/// for a command with nothing to report.
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
/// [`Client::call`](crate::Client::call).
pub trait Command {
    /// The command's name, the value of the request's `_command` key.
    const NAME: &'static str;

    /// The type of the command's results.
    type Response;
}
