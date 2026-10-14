use std::collections::HashMap;
use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use amp_protocol::codec::AmpCodec;
use amp_protocol::{AmpBox, Request};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, watch};
use tower::Service;

use crate::connection::{DEFAULT_MAX_IN_FLIGHT, Owner, spawn};
use crate::{Command, Connection, Error};

/// Calls commands on a peer over one connection.
///
/// A client is cheap to clone, and every clone shares the connection. Calls
/// from any number of tasks may be in flight at once; the protocol matches
/// each answer to its request. A connection made by [`Client::new`] closes
/// when the last clone is dropped; one driven by a [`Connection`] lives as
/// long as that future does.
///
/// A call waits until it is answered or the connection ends, unless the
/// client has a [`timeout`](Client::with_timeout). Dropping a call before it
/// is answered, whether through the timeout or by the caller, forgets it: a
/// reply that arrives later is discarded, and a request that had not yet been
/// written is not sent.
///
/// A client is also a tower [`Service`] from a [`Request`] to the results
/// box, which is the untyped form of [`call`](Client::call): a request with
/// `_ask` waits for its answer, and one without is sent and resolves at once
/// with an empty box. The client assigns every `_ask` itself; see the
/// [`Service`] impl.
///
/// Requests the peer sends to a client made by [`Client::new`] are refused
/// with the `UNHANDLED` error code. A [`Connection`] serves them instead,
/// and hands out clients for the same connection.
///
/// Once the connection has ended, every call fails with
/// [`Error::ConnectionClosed`], which carries what ended it: an I/O error,
/// or a box from the peer that broke the protocol. A connection the peer
/// closed cleanly has no cause. A client that would rather not wait for a
/// call to fail, such as one that reconnects, or one that only sends
/// notifications and so never hears back, waits on
/// [`closed`](Client::closed) instead.
#[derive(Clone)]
pub struct Client {
    outgoing: mpsc::Sender<Outgoing>,
    next_ask: Arc<AtomicU64>,
    pub(crate) pending: Arc<Mutex<Pending>>,
    pub(crate) cause: Arc<Cause>,
    /// Nothing is ever sent on it: its sender is held by whatever drives
    /// the connection, and dropped once the connection has ended and the
    /// cause is recorded, which is what [`closed`](Client::closed) waits
    /// for.
    closed: watch::Receiver<()>,
    timeout: Option<Duration>,
}

/// A request queued for the connection's writer.
pub(crate) struct Outgoing {
    pub(crate) request: Request,
    /// Answered with an empty box once the request has been written. Only a
    /// notification has one; a call's reply sender waits in [`Pending`] for
    /// the peer's answer instead.
    pub(crate) written: Option<oneshot::Sender<Result<AmpBox, Error>>>,
}

/// Calls waiting for a reply, keyed by ask.
///
/// A call registers itself here before its request is queued, so a reply can
/// never arrive first, and removes itself if it is dropped before the reply
/// arrives, so the map only holds calls somebody is still waiting on.
///
/// NOTE: this is a `std::sync::Mutex` by design! We don't want the mutex to
/// be held across an await, and using the `std::sync` version, rather than
/// the `tokio::sync` version, ensures we can't mess that up.
pub(crate) type Pending = HashMap<Vec<u8>, oneshot::Sender<Result<AmpBox, Error>>>;

/// What ended the connection, recorded by [`Connection::run`] before it
/// lets go of anything that fails a call. `None` while the connection is
/// up, and once it has ended cleanly.
pub(crate) type Cause = Mutex<Option<Arc<Error>>>;

/// The constructors, with the bound on the I/O type passed in: the task that
/// drives the connection may move between threads on the tokio runtime, so
/// `io` must be `Send` there, but on wasm32-unknown-unknown it runs on the
/// host's single thread, and the I/O types available there are not `Send`.
macro_rules! constructors {
    ($($bound:tt)+) => {
        impl Client {
            /// Starts a client over `io`, typically a connected `TcpStream`,
            /// or in WebAssembly anything that reads and writes a byte
            /// stream, such as a WebSocket adapter. The connection is driven
            /// by a task spawned on the current tokio runtime, or on
            /// wasm32-unknown-unknown on the host's event loop through
            /// `wasm-bindgen-futures`, and closes when the last clone of
            /// the client is dropped.
            ///
            /// Requests the peer sends are refused. For a connection that
            /// answers them as well, see [`Connection`].
            ///
            /// Set `TCP_NODELAY` on a `TcpStream` before passing it, with
            /// `stream.set_nodelay(true)`. Each request is written as it is
            /// made, and with Nagle's algorithm on, a request made while
            /// the one before it is unacknowledged waits for that
            /// acknowledgement, up to the server's delayed-ACK timer. The
            /// server sets it on the connections it accepts.
            ///
            /// The connection uses a default [`AmpCodec`], which refuses a
            /// box longer than 16 MiB or with more than 1,024 pairs and
            /// closes the connection; [`with_codec`](Client::with_codec)
            /// takes another.
            pub fn new<T>(io: T) -> Client
            where
                T: $($bound)+,
            {
                Self::with_codec(io, AmpCodec::new())
            }

            /// [`new`](Client::new) with the codec the connection is framed
            /// by, for setting the largest box the peer may send with
            /// [`AmpCodec::with_max_frame_length`], or that and the most
            /// pairs it may hold with [`AmpCodec::with_limits`].
            pub fn with_codec<T>(io: T, codec: AmpCodec) -> Client
            where
                T: $($bound)+,
            {
                let connection = Connection::with_codec(io, codec);
                let client = connection.client();
                // What ended the connection reaches the client through
                // `cause`, so the error returned here has nobody to go to.
                spawn(async move {
                    let _ = connection
                        .run(
                            Refuse,
                            std::future::pending(),
                            Owner::Handles,
                            DEFAULT_MAX_IN_FLIGHT,
                        )
                        .await;
                });
                client
            }
        }
    };
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
constructors!(AsyncRead + AsyncWrite + Send + 'static);
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
constructors!(AsyncRead + AsyncWrite + 'static);

impl Client {
    /// A client with nothing driving its connection yet, and what a
    /// [`Connection`] needs to drive it: the receiving end of its request
    /// queue, and the sender whose drop tells the client the connection
    /// has ended.
    pub(crate) fn detached() -> (Client, mpsc::Receiver<Outgoing>, watch::Sender<()>) {
        let (outgoing, incoming) = mpsc::channel(16);
        let (open, closed) = watch::channel(());
        let client = Client {
            outgoing,
            next_ask: Arc::new(AtomicU64::new(1)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            cause: Arc::new(Mutex::new(None)),
            closed,
            timeout: None,
        };
        (client, incoming, open)
    }

    /// Fails any call or notification that has not completed within `limit`
    /// with [`Error::Timeout`]. Without a limit, a call waits until it is
    /// answered or the connection ends.
    ///
    /// The limit belongs to this handle and to clones taken from it
    /// afterwards, so handles sharing one connection can have different
    /// limits:
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # use tower_amp::Client;
    /// # async fn example(client: Client) {
    /// let client = client.with_timeout(Duration::from_secs(5));
    /// let patient = client.clone().with_timeout(Duration::from_secs(60));
    /// # }
    /// ```
    ///
    /// A reply that arrives after its call has timed out is discarded, and a
    /// request whose caller gave up before it was written is not sent.
    pub fn with_timeout(mut self, limit: Duration) -> Client {
        self.timeout = Some(limit);
        self
    }

    /// Calls the command that `arguments` belongs to and waits for its
    /// results.
    ///
    /// An error response from the peer comes back as [`Error::Command`] with
    /// the peer's code and description. Results that do not match
    /// `C::Response` fail with [`Error::Deserialize`].
    pub async fn call<C>(&self, arguments: &C) -> Result<C::Response, Error>
    where
        C: Command + Serialize,
        C::Response: DeserializeOwned,
    {
        // Any `_ask` marks the request as wanting a reply; `send` assigns the
        // real one.
        let arguments = serde_amp::to_box(arguments).map_err(Error::Serialize)?;
        let request = Request::new(C::NAME, arguments).with_ask(Vec::new());
        let results = self.send(request).await?;
        serde_amp::from_box(&results).map_err(Error::Deserialize)
    }

    /// Sends the command that `arguments` belongs to without asking for a
    /// reply. Returns once the request has been written to the connection.
    pub async fn notify<C>(&self, arguments: &C) -> Result<(), Error>
    where
        C: Command + Serialize,
    {
        let arguments = serde_amp::to_box(arguments).map_err(Error::Serialize)?;
        let request = Request::new(C::NAME, arguments);
        self.send(request).await.map(drop)
    }

    /// Sends `request`, assigning its `_ask` if it has one, and waits for the
    /// answer, or for the request to be written if it has none, within the
    /// client's timeout if it has one.
    async fn send(&self, request: Request) -> Result<AmpBox, Error> {
        let exchange = self.exchange(request);
        match self.timeout {
            Some(limit) => timeout(limit, exchange)
                .await
                .unwrap_or(Err(Error::Timeout)),
            None => exchange.await,
        }
    }

    /// [`send`](Client::send) without the timeout.
    async fn exchange(&self, mut request: Request) -> Result<AmpBox, Error> {
        let (reply, response) = oneshot::channel();
        // A call waits in `pending` for its answer, and is removed from it
        // when this future completes or is dropped. A notification's reply
        // travels with the request, for the writer to answer.
        let mut written = Some(reply);
        let _waiting = request.ask.as_mut().map(|ask| {
            // The ask only has to be unique on this connection, and the
            // counter is the one place that can guarantee that, so whatever
            // the caller set is replaced. Only its presence matters.
            *ask = self
                .next_ask
                .fetch_add(1, Ordering::Relaxed)
                .to_string()
                .into();
            lock(&self.pending).insert(ask.clone(), written.take().expect("one reply sender"));
            Waiting {
                ask: ask.clone(),
                pending: &self.pending,
            }
        });
        // Either channel is gone only once the connection has ended, and
        // `run` records why before it drops either of them.
        if self
            .outgoing
            .send(Outgoing { request, written })
            .await
            .is_err()
        {
            return Err(self.connection_closed());
        }
        response
            .await
            .unwrap_or_else(|_| Err(self.connection_closed()))
    }

    /// Waits until the connection has ended, and returns what ended it:
    /// the same cause that [`Error::ConnectionClosed`] carries, or `None`
    /// for a connection the peer closed cleanly, or that ended because
    /// whoever drove it stopped. Returns at once if the connection has
    /// already ended.
    ///
    /// Every clone of a client shares the connection, so this resolves for
    /// all of them together. A client that reconnects waits here, then
    /// makes a new client over a new connection:
    ///
    /// ```no_run
    /// # use tower_amp::Client;
    /// # async fn connect() -> Client { unimplemented!() }
    /// # async fn example() {
    /// let mut client = connect().await;
    /// loop {
    ///     let cause = client.closed().await;
    ///     eprintln!("connection lost: {cause:?}; reconnecting");
    ///     client = connect().await;
    /// }
    /// # }
    /// ```
    pub async fn closed(&self) -> Option<Arc<Error>> {
        // `changed` only ever errs, and only once the sender is dropped,
        // since nothing is ever sent.
        let _ = self.closed.clone().changed().await;
        lock(&self.cause).clone()
    }

    /// Whether the connection has ended, in which case every call fails
    /// with [`Error::ConnectionClosed`].
    pub fn is_closed(&self) -> bool {
        self.closed.has_changed().is_err()
    }

    /// The error for a call the connection ended on.
    fn connection_closed(&self) -> Error {
        Error::closed(lock(&self.cause).clone())
    }
}

/// A call's place in [`Pending`], given up when the call is dropped so that a
/// caller that stops waiting, through a timeout or otherwise, leaves nothing
/// behind. Removing a call that has already been answered is a no-op.
struct Waiting<'a> {
    ask: Vec<u8>,
    pending: &'a Mutex<Pending>,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        lock(self.pending).remove(&self.ask);
    }
}

/// The untyped form of [`Client::call`] and [`Client::notify`].
///
/// A request with an `_ask` waits for its answer; one without is sent and
/// resolves at once with an empty box. The value of the `_ask` is ignored:
/// the client replaces it with one of its own, since an `_ask` only has to
/// be unique on this connection and only the client can guarantee that. A
/// request forwarded from another connection can therefore keep the `_ask`
/// it arrived with.
///
/// The client's [`timeout`](Client::with_timeout), if it has one, applies
/// here too.
impl Service<Request> for Client {
    type Response = AmpBox;
    type Error = Error;
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    type Future = Pin<Box<dyn Future<Output = Result<AmpBox, Error>> + Send>>;
    /// Not `Send`, since the host's timer that the timeout waits on is not.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    type Future = Pin<Box<dyn Future<Output = Result<AmpBox, Error>>>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let client = self.clone();
        Box::pin(async move { client.send(request).await })
    }
}

/// The service of a connection made by [`Client::new`]: refuses every
/// request the peer sends with the `UNHANDLED` error code.
#[derive(Clone)]
struct Refuse;

impl Service<Request> for Refuse {
    type Response = AmpBox;
    type Error = Error;
    type Future = Ready<Result<AmpBox, Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        ready(Err(Error::unhandled(&request.command)))
    }
}

/// Waits for `future` for at most `limit`; `None` if the limit was reached
/// first.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
async fn timeout<F: Future>(limit: Duration, future: F) -> Option<F::Output> {
    tokio::time::timeout(limit, future).await.ok()
}

/// Waits for `future` for at most `limit`; `None` if the limit was reached
/// first. The timer is the host's, since tokio's needs a runtime and the
/// clock behind it is not available in WebAssembly.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
async fn timeout<F: Future>(limit: Duration, future: F) -> Option<F::Output> {
    use futures_util::future::{Either, select};
    use gloo_timers::future::TimeoutFuture;

    let millis = u32::try_from(limit.as_millis()).unwrap_or(u32::MAX);
    match select(std::pin::pin!(future), TimeoutFuture::new(millis)).await {
        Either::Left((output, _)) => Some(output),
        Either::Right(((), _)) => None,
    }
}

/// Locks `mutex`. A poisoned lock only means a task panicked while holding
/// it, which nothing here can do, so the value is still sound to use.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::AsyncReadExt;

    /// A client whose peer never answers, and the peer's end of the pipe.
    fn silent_peer() -> (Client, tokio::io::DuplexStream) {
        let (client_io, peer) = tokio::io::duplex(4096);
        (Client::new(client_io), peer)
    }

    fn call() -> Request {
        Request::new("Sleep", AmpBox::new()).with_ask("x")
    }

    #[tokio::test]
    async fn a_timed_out_call_leaves_pending_empty() {
        let (client, _peer) = silent_peer();
        let client = client.with_timeout(Duration::from_millis(20));
        let failed = client.send(call()).await;
        assert!(matches!(failed, Err(Error::Timeout)), "{failed:?}");
        assert!(lock(&client.pending).is_empty());
    }

    #[tokio::test]
    async fn a_dropped_call_leaves_pending_empty() {
        let (client, _peer) = silent_peer();
        let elapsed = tokio::time::timeout(Duration::from_millis(20), client.send(call())).await;
        assert!(elapsed.is_err(), "the peer answered?");
        assert!(lock(&client.pending).is_empty());
    }

    #[tokio::test]
    async fn a_call_is_registered_until_answered() {
        let (client, mut peer) = silent_peer();
        let mut waiting = Box::pin(client.send(call()));
        // Drive the call until its request has been written.
        let mut buf = [0; 64];
        tokio::select! {
            _ = &mut waiting => panic!("answered by nobody"),
            n = peer.read(&mut buf) => assert!(n.unwrap() > 0),
        }
        assert_eq!(lock(&client.pending).len(), 1);
        drop(waiting);
        assert!(lock(&client.pending).is_empty());
    }
}
