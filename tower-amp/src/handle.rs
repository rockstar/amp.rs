use std::future::Future;

use crate::Command;
#[cfg(doc)]
use crate::Error;

/// A command that handles itself without any state.
///
/// Implemented on the command's argument struct. The handler consumes the
/// arguments and returns the command's response, or the command's error for
/// the peer: [`Error`] unless the command was declared with `error = Type`,
/// and then that type. Every `Handle` is also a [`HandleWith`] for any
/// state, so a stateless command can be registered on a router that has
/// state.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use tower_amp::{command, Error, Handle};
///
/// #[derive(Deserialize)]
/// #[command(response = Total)]
/// struct Sum {
///     a: i64,
///     b: i64,
/// }
///
/// #[derive(Serialize)]
/// struct Total {
///     total: i64,
/// }
///
/// impl Handle for Sum {
///     async fn handle(self) -> Result<Total, Error> {
///         Ok(Total { total: self.a + self.b })
///     }
/// }
/// ```
pub trait Handle: Command + Sized {
    /// Runs the command.
    fn handle(self) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send;
}

/// A command that handles itself with a clone of the router's state.
///
/// Implemented on the command's argument struct, for the state type the
/// router was built with by [`Router::with_state`](crate::Router::with_state).
/// It fails as [`Handle`] does.
///
/// ```
/// use std::sync::{Arc, Mutex};
/// use serde::{Deserialize, Serialize};
/// use tower_amp::{command, Error, HandleWith};
///
/// #[derive(Clone, Default)]
/// struct Counter(Arc<Mutex<u64>>);
///
/// #[derive(Deserialize)]
/// #[command(response = Count)]
/// struct Bump {
///     by: u64,
/// }
///
/// #[derive(Serialize)]
/// struct Count {
///     count: u64,
/// }
///
/// impl HandleWith<Counter> for Bump {
///     async fn handle(self, counter: Counter) -> Result<Count, Error> {
///         let mut count = counter.0.lock().unwrap();
///         *count += self.by;
///         Ok(Count { count: *count })
///     }
/// }
/// ```
pub trait HandleWith<S>: Command + Sized {
    /// Runs the command with `state`.
    fn handle(self, state: S) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send;
}

impl<C: Handle, S> HandleWith<S> for C {
    fn handle(self, _state: S) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send {
        Handle::handle(self)
    }
}
