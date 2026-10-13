use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use amp_protocol::codec::AmpCodec;
use amp_protocol::{AmpBox, ErrorResponse, Message, Request};
use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
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
    cause: Arc<Cause>,
}

struct Outgoing {
    request: Request,
    reply: oneshot::Sender<Result<AmpBox, Error>>,
}

/// Calls waiting for a reply, keyed by ask.
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
        let cause = Arc::new(Mutex::new(None));
        tokio::spawn(run(io, codec, incoming, cause.clone()));
        Client {
            outgoing,
            next_ask: Arc::new(AtomicU64::new(1)),
            cause,
        }
    }

    /// Calls the command that `arguments` belongs to and waits for its
    /// results.
    ///
    /// An error response from the peer comes back as [`Error::Command`] with
    /// the peer's code and description.
    pub async fn call<C>(&self, arguments: &C) -> Result<C::Response, Error>
    where
        C: Command + Serialize,
        C::Response: DeserializeOwned,
    {
        // Any `_ask` marks the request as wanting a reply; `send` assigns the
        // real one.
        let request = Request::new(C::NAME, serde_amp::to_box(arguments)?).with_ask(Vec::new());
        let results = self.send(request).await?;
        Ok(serde_amp::from_box(&results)?)
    }

    /// Sends the command that `arguments` belongs to without asking for a
    /// reply. Returns once the request has been written to the connection.
    pub async fn notify<C>(&self, arguments: &C) -> Result<(), Error>
    where
        C: Command + Serialize,
    {
        let request = Request::new(C::NAME, serde_amp::to_box(arguments)?);
        self.send(request).await.map(drop)
    }

    /// Sends `request`, assigning its `_ask` if it has one, and waits for the
    /// answer, or for the request to be written if it has none.
    async fn send(&self, mut request: Request) -> Result<AmpBox, Error> {
        // The ask only has to be unique on this connection, and the counter
        // is the one place that can guarantee that, so whatever the caller
        // set is replaced. Only its presence matters.
        if request.ask.is_some() {
            request.ask = Some(
                self.next_ask
                    .fetch_add(1, Ordering::Relaxed)
                    .to_string()
                    .into(),
            );
        }
        let (reply, response) = oneshot::channel();
        // Either channel is gone only once the connection has ended, and
        // `run` records why before it drops either of them.
        if self
            .outgoing
            .send(Outgoing { request, reply })
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

/// The untyped form of [`Client::call`] and [`Client::notify`].
///
/// A request with an `_ask` waits for its answer; one without is sent and
/// resolves at once with an empty box. The value of the `_ask` is ignored:
/// the client replaces it with one of its own, since an `_ask` only has to
/// be unique on this connection and only the client can guarantee that. A
/// request forwarded from another connection can therefore keep the `_ask`
/// it arrived with.
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
/// holding a reply sender, `pending`, the other future, and the queue behind
/// `outgoing`, is dropped later than that.
async fn run<T>(io: T, codec: AmpCodec, mut outgoing: mpsc::Receiver<Outgoing>, cause: Arc<Cause>)
where
    T: AsyncRead + AsyncWrite,
{
    let (mut sink, mut stream) = Framed::new(io, codec).split();
    // Calls waiting for a reply, keyed by ask. The writer registers a call
    // before it writes the request, so a reply can never arrive first.
    // NOTE: this is a `std::sync::Mutex` by design! We don't want the mutex
    // to be held across an await, and using the `std::sync` version, rather
    // than the `tokio::sync` version, ensures we can't mess that up.
    let pending: Mutex<Pending> = Mutex::new(HashMap::new());
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
                    let Some(Outgoing { request, reply }) = next else {
                        // Every clone of the client is gone.
                        return;
                    };
                    let ask = request.ask.clone();
                    let amp_box = match request.into_box() {
                        Ok(amp_box) => amp_box,
                        Err(err) => {
                            let _ = reply.send(Err(err.into()));
                            continue;
                        }
                    };
                    // A call waits in `pending` for its answer; a
                    // notification is answered as soon as it is written.
                    let notification = match ask {
                        Some(ask) => {
                            lock(&pending).insert(ask, reply);
                            None
                        }
                        None => Some(reply),
                    };
                    if let Err(err) = sink.send(amp_box).await {
                        return end(err.into());
                    }
                    if let Some(reply) = notification {
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
    // Dropping `pending` and `outgoing` fails every call still waiting, and
    // each finds `cause` already recorded.
}

/// Locks `mutex`. A poisoned lock only means a task panicked while holding
/// it, which nothing here can do, so the value is still sound to use.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
