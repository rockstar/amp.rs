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
/// with an empty box.
///
/// Requests the peer sends to this client are refused with the `UNHANDLED`
/// error code.
#[derive(Clone)]
pub struct Client {
    outgoing: mpsc::Sender<Outgoing>,
    next_ask: Arc<AtomicU64>,
}

struct Outgoing {
    request: Request,
    reply: oneshot::Sender<Result<AmpBox, Error>>,
}

/// Calls waiting for a reply, keyed by ask.
type Pending = HashMap<Vec<u8>, oneshot::Sender<Result<AmpBox, Error>>>;

impl Client {
    /// Starts a client over `io`, typically a connected `TcpStream`. The
    /// connection is driven by a task spawned on the current runtime.
    pub fn new<T>(io: T) -> Client
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (outgoing, incoming) = mpsc::channel(16);
        tokio::spawn(run(io, incoming));
        Client {
            outgoing,
            next_ask: Arc::new(AtomicU64::new(1)),
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
        let ask = self.next_ask.fetch_add(1, Ordering::Relaxed).to_string();
        let request = Request::new(C::NAME, serde_amp::to_box(arguments)?).with_ask(ask);
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

    async fn send(&self, request: Request) -> Result<AmpBox, Error> {
        let (reply, response) = oneshot::channel();
        self.outgoing
            .send(Outgoing { request, reply })
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        response.await.map_err(|_| Error::ConnectionClosed)?
    }
}

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
async fn run<T>(io: T, mut outgoing: mpsc::Receiver<Outgoing>)
where
    T: AsyncRead + AsyncWrite,
{
    let (mut sink, mut stream) = Framed::new(io, AmpCodec::new()).split();
    // Calls waiting for a reply, keyed by ask. The writer registers a call
    // before it writes the request, so a reply can never arrive first.
    // NOTE: this is a `std::sync::Mutex` by design! We don't want the mutex
    // to be held across an await, and using the `std::sync` version, rather
    // than the `tokio::sync` version, ensures we can't mess that up.
    let pending: Mutex<Pending> = Mutex::new(HashMap::new());
    let (refusals, mut refused) = mpsc::channel::<AmpBox>(16);

    let read = async {
        while let Some(Ok(amp_box)) = stream.next().await {
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
                        break;
                    }
                }
                // The peer sent a box that is not a message.
                Err(_) => break,
            }
        }
        // The peer closed the connection or sent something that is not a box.
    };
    let write = async {
        loop {
            tokio::select! {
                next = outgoing.recv() => {
                    let Some(Outgoing { request, reply }) = next else {
                        // Every clone of the client is gone.
                        break;
                    };
                    let ask = request.ask.clone();
                    let amp_box = match request.into_box() {
                        Ok(amp_box) => amp_box,
                        Err(err) => {
                            let _ = reply.send(Err(err.into()));
                            continue;
                        }
                    };
                    match ask {
                        Some(ask) => {
                            lock(&pending).insert(ask.clone(), reply);
                            if let Err(err) = sink.send(amp_box).await {
                                if let Some(reply) = lock(&pending).remove(&ask) {
                                    let _ = reply.send(Err(err.into()));
                                }
                                break;
                            }
                        }
                        None => {
                            if let Err(err) = sink.send(amp_box).await {
                                let _ = reply.send(Err(err.into()));
                                break;
                            }
                            let _ = reply.send(Ok(AmpBox::new()));
                        }
                    }
                }
                Some(refusal) = refused.recv() => {
                    if sink.send(refusal).await.is_err() {
                        break;
                    }
                }
            }
        }
    };
    tokio::select! {
        () = read => {}
        () = write => {}
    }
    // Dropping `pending` fails every call still waiting with
    // `Error::ConnectionClosed`.
}

/// Locks `pending`. A poisoned lock only means a task panicked while holding
/// it, which nothing here can do, so the map is still sound to use.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
