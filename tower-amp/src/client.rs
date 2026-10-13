use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use amp_protocol::codec::AmpCodec;
use amp_protocol::{AmpBox, ErrorResponse, Message, Request};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use tower::Service;

use crate::{Command, Error};

/// Calls commands on a peer over one connection.
///
/// A client is cheap to clone, and every clone shares the connection. Calls
/// from any number of tasks may be in flight at once; the protocol matches
/// each answer to its request. The connection closes when the last clone is
/// dropped.
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
/// Requests the peer sends to this client are refused with the `UNHANDLED`
/// error code.
///
/// Once the connection has ended, every call fails with
/// [`Error::ConnectionClosed`], which carries what ended it: an I/O error,
/// or a box from the peer that broke the protocol. A connection the peer
/// closed cleanly has no cause.
#[derive(Clone)]
pub struct Client {
    outgoing: mpsc::Sender<Outgoing>,
    next_ask: Arc<AtomicU64>,
    pending: Arc<Mutex<Pending>>,
    cause: Arc<Cause>,
    timeout: Option<Duration>,
}

/// A request queued for the connection's writer.
struct Outgoing {
    request: Request,
    /// Answered with an empty box once the request has been written. Only a
    /// notification has one; a call's reply sender waits in [`Pending`] for
    /// the peer's answer instead.
    written: Option<oneshot::Sender<Result<AmpBox, Error>>>,
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
type Pending = HashMap<Vec<u8>, oneshot::Sender<Result<AmpBox, Error>>>;

/// What ended the connection, recorded by [`run`] before it lets go of
/// anything that fails a call. `None` while the connection is up, and once
/// it has ended cleanly.
type Cause = Mutex<Option<Arc<Error>>>;

impl Client {
    /// Starts a client over `io`, typically a connected `TcpStream`. The
    /// connection is driven by a task spawned on the current runtime.
    ///
    /// The connection uses a default [`AmpCodec`], which refuses a box
    /// longer than 16 MiB and closes the connection; [`with_codec`](Client::with_codec)
    /// takes another.
    pub fn new<T>(io: T) -> Client
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        Self::with_codec(io, AmpCodec::new())
    }

    /// [`new`](Client::new) with the codec the connection is framed by, for
    /// setting the largest box the peer may send with
    /// [`AmpCodec::with_max_frame_length`].
    pub fn with_codec<T>(io: T, codec: AmpCodec) -> Client
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (outgoing, incoming) = mpsc::channel(16);
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let cause = Arc::new(Mutex::new(None));
        tokio::spawn(run(io, codec, incoming, pending.clone(), cause.clone()));
        Client {
            outgoing,
            next_ask: Arc::new(AtomicU64::new(1)),
            pending,
            cause,
            timeout: None,
        }
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
            Some(limit) => tokio::time::timeout(limit, exchange)
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
            return Err(self.closed());
        }
        response.await.unwrap_or_else(|_| Err(self.closed()))
    }

    /// The error for a call the connection ended on.
    fn closed(&self) -> Error {
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
    type Future = Pin<Box<dyn Future<Output = Result<AmpBox, Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let client = self.clone();
        Box::pin(async move { client.send(request).await })
    }
}

/// Drives the connection: writes outgoing requests, matches incoming
/// responses to them, and refuses incoming requests.
///
/// Reading and writing are separate futures so that a write the peer is slow
/// to accept never stops this side from reading. If both sides stopped
/// reading while blocked on a write, neither would ever drain the other's
/// buffer. The connection ends when either future does: the peer closed it
/// or broke the protocol, a write failed, or every clone of the client is
/// gone.
///
/// A call learns that the connection ended when the sender of its reply is
/// dropped, and then reads `cause` for why, so every failure here is
/// recorded in `cause` before the future it happened in returns. Everything
/// holding a reply sender, the entries of `pending`, the other future, and
/// the queue behind `outgoing`, is dropped later than that.
async fn run<T>(
    io: T,
    codec: AmpCodec,
    mut outgoing: mpsc::Receiver<Outgoing>,
    pending: Arc<Mutex<Pending>>,
    cause: Arc<Cause>,
) where
    T: AsyncRead + AsyncWrite,
{
    let (mut sink, mut stream) = Framed::new(io, codec).split();
    let (refusals, mut refused) = mpsc::channel::<AmpBox>(16);
    let end = |err: Error| *lock(&cause) = Some(Arc::new(err));

    let read = async {
        loop {
            let amp_box = match stream.next().await {
                Some(Ok(amp_box)) => amp_box,
                // Reading failed, or the peer sent something that is not a
                // box.
                Some(Err(err)) => return end(err.into()),
                // The peer closed the connection.
                None => return,
            };
            match Message::from_box(amp_box) {
                // A reply nobody is waiting for any more is discarded.
                Ok(Message::Answer(answer)) => {
                    if let Some(reply) = lock(&pending).remove(&answer.answer) {
                        let _ = reply.send(Ok(answer.results));
                    }
                }
                Ok(Message::Error(error)) => {
                    if let Some(reply) = lock(&pending).remove(&error.error) {
                        let _ = reply.send(Err(Error::new(
                            String::from_utf8_lossy(&error.code),
                            String::from_utf8_lossy(&error.description),
                        )));
                    }
                }
                Ok(Message::Request(request)) => {
                    let Some(refusal) = request.error(
                        ErrorResponse::UNHANDLED,
                        "this client does not handle commands",
                    ) else {
                        continue;
                    };
                    let Ok(amp_box) = refusal.into_box() else {
                        continue;
                    };
                    if refusals.send(amp_box).await.is_err() {
                        // The writer is gone, and recorded why.
                        return;
                    }
                }
                // The peer sent a box that is neither a request nor a
                // response.
                Err(err) => return end(err.into()),
            }
        }
    };
    let write = async {
        loop {
            tokio::select! {
                next = outgoing.recv() => {
                    let Some(Outgoing { request, written }) = next else {
                        // Every clone of the client is gone.
                        return;
                    };
                    // A request whose caller stopped waiting while it was
                    // queued is not worth the peer's time.
                    let abandoned = match &request.ask {
                        Some(ask) => !lock(&pending).contains_key(ask),
                        None => written.as_ref().is_none_or(|reply| reply.is_closed()),
                    };
                    if abandoned {
                        continue;
                    }
                    let ask = request.ask.clone();
                    let amp_box = match request.into_box() {
                        Ok(amp_box) => amp_box,
                        Err(err) => {
                            let reply = match ask {
                                Some(ask) => lock(&pending).remove(&ask),
                                None => written,
                            };
                            if let Some(reply) = reply {
                                let _ = reply.send(Err(err.into()));
                            }
                            continue;
                        }
                    };
                    if let Err(err) = sink.send(amp_box).await {
                        return end(err.into());
                    }
                    if let Some(reply) = written {
                        let _ = reply.send(Ok(AmpBox::new()));
                    }
                }
                Some(refusal) = refused.recv() => {
                    if let Err(err) = sink.send(refusal).await {
                        return end(err.into());
                    }
                }
            }
        }
    };
    tokio::select! {
        () = read => {}
        () = write => {}
    }
    // Clearing `pending` and dropping `outgoing` fails every call still
    // waiting, and each finds `cause` already recorded. The client still
    // holds `pending`, so it has to be emptied rather than dropped.
    lock(&pending).clear();
}

/// Locks `mutex`. A poisoned lock only means a task panicked while holding
/// it, which nothing here can do, so the value is still sound to use.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
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
