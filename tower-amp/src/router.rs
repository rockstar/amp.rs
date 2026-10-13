use std::collections::HashMap;
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use amp_protocol::{AmpBox, Request};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tower::Service;

use crate::{Error, HandleWith};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type Handler<S> = dyn Fn(S, AmpBox) -> BoxFuture<Result<AmpBox, Error>> + Send + Sync;

/// Dispatches requests to commands by name.
///
/// A router is a tower [`Service`] from a [`Request`] to the results box, so
/// it can be wrapped in tower middleware and passed to [`serve`](crate::serve).
/// A request naming a command the router does not know fails with the
/// `UNHANDLED` error code.
///
/// `S` is the state handed to every command. A router made with
/// [`Router::new`] has no state and serves commands that implement
/// [`Handle`](crate::Handle); one made with [`Router::with_state`] serves
/// commands that implement [`HandleWith`] for its state.
pub struct Router<S = ()> {
    // Shared so that cloning the router, which `serve` does per connection
    // and `serve_connection` does per request, bumps a refcount instead of
    // copying the table and every command name in it.
    handlers: Arc<HashMap<Vec<u8>, Arc<Handler<S>>>>,
    state: S,
}

impl Router<()> {
    /// Creates a router with no state.
    pub fn new() -> Self {
        Router::with_state(())
    }
}

impl Default for Router<()> {
    fn default() -> Self {
        Router::new()
    }
}

impl<S> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Creates a router whose commands receive a clone of `state`.
    pub fn with_state(state: S) -> Self {
        Router {
            handlers: Arc::new(HashMap::new()),
            state,
        }
    }

    /// Serves the command `C`, replacing any earlier command with the same
    /// name.
    pub fn command<C>(mut self) -> Self
    where
        C: HandleWith<S> + DeserializeOwned + Send + 'static,
        C::Response: Serialize,
    {
        let handler = |state: S, arguments: AmpBox| -> BoxFuture<Result<AmpBox, Error>> {
            match serde_amp::from_box::<C>(&arguments) {
                Ok(command) => Box::pin(async move {
                    let response = HandleWith::handle(command, state).await?;
                    serde_amp::to_box(&response).map_err(Error::Serde)
                }),
                Err(err) => Box::pin(ready(Err(err.into()))),
            }
        };
        // The table is only shared once the router has been cloned, so this
        // copies nothing in the usual builder chain.
        Arc::make_mut(&mut self.handlers).insert(C::NAME.as_bytes().to_vec(), Arc::new(handler));
        self
    }
}

impl<S: Clone> Clone for Router<S> {
    fn clone(&self) -> Self {
        Router {
            handlers: Arc::clone(&self.handlers),
            state: self.state.clone(),
        }
    }
}

impl<S> Service<Request> for Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    type Response = AmpBox;
    type Error = Error;
    type Future = BoxFuture<Result<AmpBox, Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        match self.handlers.get(&request.command) {
            Some(handler) => handler(self.state.clone(), request.arguments),
            None => Box::pin(ready(Err(Error::unhandled(&request.command)))),
        }
    }
}
