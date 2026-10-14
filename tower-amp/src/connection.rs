use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::pin;
use std::sync::Arc;

use amp_protocol::codec::AmpCodec;
use amp_protocol::{AmpBox, Answer, ErrorResponse, MAX_VALUE_LENGTH, Message, Request};
use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tower::{Service, ServiceExt};

use crate::client::{Outgoing, lock};
use crate::error::INTERNAL_ERROR;
use crate::{Client, Error, log};

/// How many requests one connection may have in flight: received and not
/// yet answered. Past this the connection stops reading until a reply has
/// been queued, so a peer that sends requests without reading the replies
/// is held to this much memory rather than as much as it cares to send.
const MAX_IN_FLIGHT: usize = 256;

/// One AMP connection, used in both directions: a [`Client`] handle calls
/// commands on the peer, and a service answers the commands the peer
/// calls.
///
/// AMP is symmetric, so either side of a connection may send a command.
/// [`Client::new`] and [`serve_connection`](crate::serve_connection) are the
/// one-way cases, a connection that only calls and one that only serves,
/// and both are built on this. Use it directly when one side does both:
/// a server whose handlers call commands on the peer that sent the
/// request, or a client that answers commands the server sends.
///
/// Take as many handles as you like with [`client`](Connection::client),
/// then drive the connection with [`serve`](Connection::serve). A handler
/// that needs the peer gets a handle through the service's state:
///
/// ```no_run
/// # use serde::{Deserialize, Serialize};
/// # use tower_amp::{command, Client, Connection, Error, HandleWith, Router};
/// # #[derive(Deserialize)]
/// # #[command(response = Total)]
/// # struct Sum { a: i64, b: i64 }
/// # #[derive(Serialize, Deserialize)]
/// # struct Total { total: i64 }
/// # #[derive(Serialize)]
/// # #[command(response = Total)]
/// # struct Relay { a: i64, b: i64 }
/// impl HandleWith<Client> for Sum {
///     async fn handle(self, peer: Client) -> Result<Total, Error> {
///         // Have the peer do the arithmetic, and answer with its answer.
///         peer.call(&Relay { a: self.a, b: self.b }).await
///     }
/// }
///
/// # async fn example(stream: tokio::net::TcpStream) -> Result<(), Error> {
/// let connection = Connection::new(stream);
/// let peer = connection.client();
/// let router = Router::with_state(peer).command::<Sum>();
/// connection.serve(router).await
/// # }
/// ```
///
/// Requests from the peer are handled as [`serve_connection`] describes:
/// concurrently, up to 256 in flight, each answered with its results or
/// the error's code and description. Calls through a handle work as
/// [`Client`] describes.
///
/// [`serve_connection`]: crate::serve_connection
pub struct Connection<T> {
    framed: Framed<T, AmpCodec>,
    client: Client,
    outgoing: mpsc::Receiver<Outgoing>,
}

impl<T> Connection<T>
where
    T: AsyncRead + AsyncWrite,
{
    /// A connection over `io`, typically a connected `TcpStream`, or in
    /// WebAssembly anything that reads and writes a byte stream. Nothing
    /// happens until it is [served](Connection::serve).
    ///
    /// Set `TCP_NODELAY` on a `TcpStream` before passing it, with
    /// `stream.set_nodelay(true)`, so that neither a request nor a reply
    /// waits for the acknowledgement of the one written before it.
    ///
    /// The connection uses a default [`AmpCodec`], which refuses a box
    /// longer than 16 MiB and closes the connection;
    /// [`with_codec`](Connection::with_codec) takes another.
    pub fn new(io: T) -> Self {
        Self::with_codec(io, AmpCodec::new())
    }

    /// [`new`](Connection::new) with the codec the connection is framed by,
    /// for setting the largest box the peer may send with
    /// [`AmpCodec::with_max_frame_length`].
    pub fn with_codec(io: T, codec: AmpCodec) -> Self {
        let (client, outgoing) = Client::detached();
        Connection {
            framed: Framed::new(io, codec),
            client,
            outgoing,
        }
    }

    /// A handle for calling commands on the peer. Every handle shares the
    /// connection, and calls from any number of them may be in flight at
    /// once.
    ///
    /// A call made before the connection is served waits until it is. Once
    /// the connection has ended, every call fails with
    /// [`Error::ConnectionClosed`], which carries a copy of the error that
    /// ended it, if one did.
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// Drives the connection until the peer closes it, answering the
    /// peer's requests with `service` and writing the requests made through
    /// its handles.
    ///
    /// Returns once the peer closes the connection and every request
    /// already received has been answered, or with the error that ended
    /// the connection: an I/O error, or a box from the peer that broke the
    /// protocol. Dropping the future closes the connection; dropping its
    /// handles does not.
    ///
    /// Requests from the peer are handled as [`serve_connection`] describes.
    /// Answers the peer sends to commands nobody is waiting on any more are
    /// discarded.
    ///
    /// [`serve_connection`]: crate::serve_connection
    pub async fn serve<S>(self, service: S) -> Result<(), Error>
    where
        S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
        S::Error: Into<Error>,
        S::Future: Send,
    {
        self.run(service, CancellationToken::new(), Owner::Driver)
            .await
    }

    /// Drives the connection: writes the requests its handles make, matches
    /// the peer's answers to them, and answers the peer's requests with
    /// `service`.
    ///
    /// Once `shutdown` is cancelled the connection reads no more, and
    /// returns once the requests it had already received are answered.
    /// `owner` says whether the handles keep the connection open.
    ///
    /// Reading and writing are separate futures so that a write the peer is
    /// slow to accept never stops this side from reading. If both sides
    /// stopped reading while blocked on a write, neither would ever drain
    /// the other's buffer.
    ///
    /// A call learns that the connection ended when the sender of its reply
    /// is dropped, and then reads the cause for why, so every failure here
    /// is recorded in the cause before anything that holds a reply sender is
    /// let go of: the entries of `pending`, the futures, and the queue
    /// behind the handles.
    pub(crate) async fn run<S>(
        self,
        service: S,
        shutdown: CancellationToken,
        owner: Owner,
    ) -> Result<(), Error>
    where
        S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
        S::Error: Into<Error>,
        S::Future: Send,
    {
        let Connection {
            framed,
            client,
            mut outgoing,
        } = self;
        // Dropping the connection's own handle here is what lets `outgoing`
        // close once every handle the caller took is gone.
        let pending = Arc::clone(&client.pending);
        let cause = Arc::clone(&client.cause);
        drop(client);
        let pending = &pending;
        let (mut sink, mut stream) = framed.split();
        let (responses, mut replies) = mpsc::channel::<AmpBox>(16);
        let in_flight = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
        // Cancelled when the read future ends, however it ends, so that
        // the writer stops taking requests that could never be answered.
        let done_reading = CancellationToken::new();

        let done = done_reading.clone();
        let read = async move {
            let _done = done.drop_guard();
            loop {
                // `biased` so that a shutdown is seen even while the peer
                // keeps the stream ready. A `next` that loses to it is
                // dropped with nothing read, since the frame stays in the
                // codec's buffer.
                let incoming = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return Ok(()),
                    next = stream.next() => match next {
                        Some(incoming) => incoming?,
                        // The peer closed the connection.
                        None => return Ok(()),
                    },
                };
                match Message::from_box(incoming)? {
                    Message::Request(request) => {
                        // Waiting here, rather than spawning without bound,
                        // is what holds the peer to `MAX_IN_FLIGHT`. Replies
                        // are written by `write` below, which does not
                        // depend on this loop, so waiting cannot deadlock.
                        // A shutdown ends the wait, and the request with it.
                        let permit = tokio::select! {
                            biased;
                            () = shutdown.cancelled() => return Ok(()),
                            permit = Arc::clone(&in_flight).acquire_owned() => {
                                permit.expect("the semaphore is never closed")
                            }
                        };
                        spawn(respond(service.clone(), request, responses.clone(), permit));
                    }
                    // A reply nobody is waiting for any more is discarded.
                    Message::Answer(answer) => {
                        if let Some(reply) = lock(pending).remove(&answer.answer) {
                            let _ = reply.send(Ok(answer.results));
                        }
                    }
                    Message::Error(error) => {
                        if let Some(reply) = lock(pending).remove(&error.error) {
                            let _ = reply.send(Err(Error::new(
                                String::from_utf8_lossy(&error.code),
                                String::from_utf8_lossy(&error.description),
                            )));
                        }
                    }
                }
            }
            // `responses` and `service` are dropped with this future, which
            // lets the writer finish once every request already received
            // has been answered.
        };
        let write = async {
            // Whether an answer can still arrive, and whether the handles
            // may still queue a request.
            let mut reading = true;
            let mut requests = true;
            // Whether a box has been fed to the sink since it was last
            // flushed, and the notifications among those, whose callers
            // are answered once it has been.
            let mut unflushed = false;
            let mut notified = Vec::new();
            loop {
                // A box is flushed only once nothing is queued behind it,
                // so that a burst of small boxes is one write rather than
                // one each; `Framed` flushes on its own once 8 KiB are
                // waiting, so a batch is bounded. Waiting here, rather than
                // feeding more while the write is slow, is what keeps a
                // request queued behind it from being written after its
                // caller has given up.
                if unflushed && outgoing.is_empty() && replies.is_empty() {
                    sink.flush().await?;
                    unflushed = false;
                    answer_written(&mut notified);
                }
                tokio::select! {
                    biased;
                    () = done_reading.cancelled(), if reading => {
                        reading = false;
                        // No answer can arrive any more, so a call made
                        // from now on fails at once, and one already queued
                        // or waiting is failed rather than written. A
                        // notification already queued is still written,
                        // since its caller is waiting for exactly that.
                        outgoing.close();
                        lock(pending).clear();
                    }
                    next = outgoing.recv(), if requests => match next {
                        // Every handle is gone.
                        None if reading && owner == Owner::Handles => break,
                        None => requests = false,
                        Some(Outgoing { request, written }) => {
                            // A request whose caller stopped waiting while
                            // it was queued is not worth the peer's time.
                            let abandoned = match &request.ask {
                                Some(ask) => !lock(pending).contains_key(ask),
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
                                        Some(ask) => lock(pending).remove(&ask),
                                        None => written,
                                    };
                                    if let Some(reply) = reply {
                                        let _ = reply.send(Err(err.into()));
                                    }
                                    continue;
                                }
                            };
                            sink.feed(amp_box).await?;
                            unflushed = true;
                            notified.extend(written);
                        }
                    },
                    reply = replies.recv() => match reply {
                        Some(reply) => {
                            sink.feed(reply).await?;
                            unflushed = true;
                        }
                        // The read future has ended and every request it
                        // took on has been answered.
                        None => break,
                    },
                }
            }
            sink.flush().await?;
            answer_written(&mut notified);
            Ok(())
        };

        let mut read = pin!(read);
        let mut write = pin!(write);
        let result = tokio::select! {
            result = &mut read => match result {
                // The peer is done sending, or this side is done reading:
                // answer what was received, then close.
                Ok(()) => write.await,
                Err(err) => Err(err),
            },
            result = &mut write => result,
        };
        let result = result.map_err(|err| match owner {
            // The handles get the error itself: nobody else is waiting for
            // it.
            Owner::Handles => {
                let err = Arc::new(err);
                *lock(&cause) = Some(Arc::clone(&err));
                Error::closed(Some(err))
            }
            // The driver gets the error itself, and the handles, which may
            // outlive it, a copy.
            Owner::Driver => {
                *lock(&cause) = Some(Arc::new(err.duplicate()));
                err
            }
        });
        // Clearing `pending` and dropping the futures, and `outgoing` with
        // them, fails every call still waiting, and each finds the cause
        // already recorded. The handles still hold `pending`, so it has to
        // be emptied rather than dropped.
        lock(pending).clear();
        result
    }
}

/// Tells the callers of the notifications in `notified` that they have been
/// written. One whose caller has stopped waiting is dropped unheard.
fn answer_written(notified: &mut Vec<oneshot::Sender<Result<AmpBox, Error>>>) {
    for reply in notified.drain(..) {
        let _ = reply.send(Ok(AmpBox::new()));
    }
}

/// Who keeps a connection open.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Owner {
    /// Its client handles: the connection closes when the last one is
    /// dropped, as a connection made by [`Client::new`] does.
    Handles,
    /// Whoever drives its future: the connection stays open until the peer
    /// closes it, however many handles there are, as a connection served
    /// by [`Connection::serve`] does.
    Driver,
}

/// Runs one request through the service and queues its response, if the
/// request wants one. The permit is the request's share of the connection's
/// in-flight limit, released when this returns: once the response is queued,
/// or at once for a request that wants none.
async fn respond<S>(
    service: S,
    request: Request,
    responses: mpsc::Sender<AmpBox>,
    _permit: OwnedSemaphorePermit,
) where
    S: Service<Request, Response = AmpBox> + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    let command = request.command.clone();
    let ask = request.ask.clone();
    let result = match call(service, request).await {
        Ok(Ok(results)) => Ok(results),
        Ok(Err(err)) => {
            log::command_failed(&command, ask.as_deref(), &err);
            Err(err)
        }
        Err(reason) => {
            // The panic's message is for the log only: it says what the
            // handler was doing, which is this side's business, not the
            // peer's.
            log::command_panicked(&command, ask.as_deref(), &reason);
            Err(Error::new(ErrorResponse::UNKNOWN, INTERNAL_ERROR))
        }
    };
    let Some(ask) = ask else {
        return;
    };
    let response = match result {
        Ok(results) => match Answer::new(ask.clone(), results).into_box() {
            Ok(response) => response,
            Err(err) => {
                let err = Error::Protocol(err);
                log::command_failed(&command, Some(&ask), &err);
                error_response(ask, &err)
            }
        },
        Err(err) => error_response(ask, &err),
    };
    let _ = responses.send(response).await;
}

/// Calls `service` with `request`, catching a panic in the handler so that
/// the caller survives to answer the request, which would otherwise wait on
/// the peer forever. The outer error is what the handler had to say for
/// itself when it panicked. On wasm32-unknown-unknown a panic aborts the
/// whole module before it can be caught, so there this is just the call.
async fn call<S>(mut service: S, request: Request) -> Result<Result<AmpBox, Error>, String>
where
    S: Service<Request, Response = AmpBox> + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    // The service is not touched again after a panic, so whatever state the
    // panic left it in is never seen.
    AssertUnwindSafe(async move {
        let service = service.ready().await.map_err(Into::into)?;
        service.call(request).await.map_err(Into::into)
    })
    .catch_unwind()
    .await
    .map_err(|payload| panic_message(&*payload))
}

/// The log line for a handler's panic: its message, which `panic!` leaves as
/// a `&str` or a `String` depending on whether it had arguments, or nothing
/// for a payload of another type.
fn panic_message(payload: &(dyn Any + Send)) -> String {
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
    match message {
        Some(message) => format!("command panicked: {message}"),
        None => "command panicked".to_string(),
    }
}

/// Builds the error box for `ask`. A code or description longer than a box
/// value may be is cut down to fit, since the handler's error must be
/// answered somehow and the alternative is a request the peer waits on
/// forever.
fn error_response(ask: Vec<u8>, err: &Error) -> AmpBox {
    let description = err.description();
    ErrorResponse::new(ask, fit(err.code()), fit(&description))
        .into_box()
        .expect("the ask came off the wire and the other fields were cut to fit")
}

/// Truncates `s` to the longest prefix that fits a box value without
/// splitting a character.
fn fit(s: &str) -> &str {
    if s.len() <= MAX_VALUE_LENGTH {
        return s;
    }
    let mut end = MAX_VALUE_LENGTH;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Spawns a task on the current tokio runtime.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) fn spawn(task: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(task);
}

/// Spawns a task on the host's event loop, since there is no tokio runtime
/// to spawn it on in WebAssembly.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) fn spawn(task: impl Future<Output = ()> + 'static) {
    wasm_bindgen_futures::spawn_local(task);
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn fit_keeps_what_fits() {
        let short = "x".repeat(MAX_VALUE_LENGTH);
        assert_eq!(fit(&short).len(), MAX_VALUE_LENGTH);
        let long = "x".repeat(MAX_VALUE_LENGTH + 1);
        assert_eq!(fit(&long).len(), MAX_VALUE_LENGTH);
    }

    #[test]
    fn fit_does_not_split_a_character() {
        // Two-byte characters, so the limit falls in the middle of one.
        let long = "é".repeat(MAX_VALUE_LENGTH);
        let fitted = fit(&long);
        assert_eq!(fitted.len(), MAX_VALUE_LENGTH - 1);
        assert!(fitted.chars().all(|c| c == 'é'));
    }

    #[test]
    fn over_long_fields_still_make_a_box() {
        let err = Error::new("X".repeat(70_000), "y".repeat(70_000));
        let amp_box = error_response(b"1".to_vec(), &err);
        let response = ErrorResponse::from_box(amp_box).unwrap();
        assert_eq!(response.error, b"1");
        assert_eq!(response.code.len(), MAX_VALUE_LENGTH);
        assert_eq!(response.description.len(), MAX_VALUE_LENGTH);
    }
}
