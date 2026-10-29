use std::any::Any;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amp_protocol::codec::AmpCodec;
use amp_protocol::{AmpBox, Answer, ErrorResponse, MAX_VALUE_LENGTH, Message, Request};
use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tower::{Service, ServiceExt};

use crate::client::{Outgoing, Pending, lock};
use crate::error::INTERNAL_ERROR;
use crate::{Client, Error, log};

/// How many requests one connection may have in flight unless told
/// otherwise: received and not yet answered. Past this the connection stops
/// reading until a reply has been queued, so a peer that sends requests
/// without reading the replies is held to this much memory rather than as
/// much as it cares to send. While a call of this side's own is waiting the
/// connection reads on, and refuses what it cannot hold; see
/// [`Connection::run`].
pub(crate) const DEFAULT_MAX_IN_FLIGHT: usize = 256;

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
    /// Dropped once the connection has ended, which is what the handles'
    /// [`Client::closed`] waits for.
    open: watch::Sender<()>,
    pub(crate) idle_timeout: Option<Duration>,
}

impl<T> fmt::Debug for Connection<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("client", &self.client)
            .field("idle_timeout", &self.idle_timeout)
            .finish_non_exhaustive()
    }
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
    /// longer than 1 MiB or with more than 1,024 pairs and closes the
    /// connection; [`with_codec`](Connection::with_codec) takes another.
    pub fn new(io: T) -> Self {
        Self::with_codec(io, AmpCodec::new())
    }

    /// [`new`](Connection::new) with the codec the connection is framed by,
    /// for setting the largest box the peer may send with
    /// [`AmpCodec::with_max_frame_length`], or that and the most pairs it may
    /// hold with [`AmpCodec::with_limits`].
    pub fn with_codec(io: T, codec: AmpCodec) -> Self {
        let (client, outgoing, open) = Client::detached();
        Connection {
            framed: Framed::new(io, codec),
            client,
            outgoing,
            open,
            idle_timeout: None,
        }
    }

    /// How long the connection may wait on its peer, in either direction,
    /// before it is closed. There is no limit unless one is set.
    ///
    /// Reading, the clock starts over with every box the peer sends, and
    /// runs only while none of the peer's requests is in flight: a
    /// connection waiting on a handler is busy, not idle. A call made
    /// through a handle does not stop the clock, so the limit must be
    /// longer than the longest answer the peer is given to send, and than
    /// the gaps between calls. Writing, each write is given the same limit
    /// to be taken by the peer. A peer that sends nothing for `limit`, or
    /// that stops reading so that the requests and replies written to it
    /// back up, has its connection closed with [`Error::Io`] of kind
    /// `TimedOut`, which [`serve`](Connection::serve) returns and every
    /// handle sees as the cause of [`Error::ConnectionClosed`].
    ///
    /// Without a limit, a peer that stops reading holds the connection
    /// for good: its writer blocks on the socket, and nothing ends it, not
    /// even dropping every handle of a client made by [`Client::spawn`].
    /// Set one on a connection to a peer that may do that. Like
    /// [`Server::idle_timeout`](crate::Server::idle_timeout), the limit
    /// also bounds how long the handler of a notification may run on
    /// after the peer hangs up.
    ///
    /// Not available on wasm32-unknown-unknown, where there is no tokio
    /// runtime to time a wait with.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    #[must_use]
    pub fn idle_timeout(mut self, limit: Duration) -> Self {
        self.idle_timeout = Some(limit);
        self
    }

    /// A handle for calling commands on the peer. Every handle shares the
    /// connection, and calls from any number of them may be in flight at
    /// once.
    ///
    /// A call made before the connection is served waits until it is. Once
    /// the connection has ended, every call fails with
    /// [`Error::ConnectionClosed`], which carries a copy of the error that
    /// ended it, if one did, and [`Client::closed`] resolves to the same.
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// Drives the connection until the peer closes it, answering the
    /// peer's requests with `service` and writing the requests made through
    /// its handles.
    ///
    /// Returns once the peer closes the connection, or with the error that
    /// ended it. Either way the handlers of the requests still in flight are
    /// cancelled, as [`serve_connection`] describes, except those of
    /// notifications, which finish first. Dropping the future closes the
    /// connection; dropping its handles does not.
    ///
    /// Requests from the peer are handled as [`serve_connection`] describes,
    /// with one difference at the in-flight limit. A handler that calls the
    /// peer that asked, as the example above does, cannot answer until the
    /// peer has, so a connection that stopped reading at the limit while
    /// such handlers filled it would never read their answers, and would
    /// hang for good. Instead, while a call made through one of its handles
    /// is waiting for an answer, the connection keeps reading past the
    /// limit: it holds one request for the next free slot, as a connection
    /// that only serves does, and answers each further one with the error
    /// code `TOO_MANY_REQUESTS` and the description `too many requests in
    /// flight`, or drops it if it wanted no reply. A peer that pipelines
    /// more than 257 requests to a command that calls it back should expect
    /// that error and retry. Answers the peer sends to commands nobody is
    /// waiting on any more are discarded. There is no idle timeout unless
    /// [`idle_timeout`](Connection::idle_timeout) set one, so without it a
    /// peer that stops reading holds the connection for good.
    ///
    /// # Errors
    ///
    /// Fails with the error that ended the connection: [`Error::Io`] for an
    /// I/O error, including one of kind `TimedOut` when the
    /// [`idle_timeout`](Connection::idle_timeout) runs out, or
    /// [`Error::Protocol`] for a box from the peer that broke the protocol.
    ///
    /// [`serve_connection`]: crate::serve_connection
    pub async fn serve<S>(self, service: S) -> Result<(), Error>
    where
        S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
        S::Error: Into<Error>,
        S::Future: Send,
    {
        let idle_timeout = self.idle_timeout;
        self.run(
            service,
            std::future::pending(),
            Owner::Driver,
            DEFAULT_MAX_IN_FLIGHT,
            idle_timeout,
        )
        .await
    }

    /// Drives the connection: writes the requests its handles make, matches
    /// the peer's answers to them, and answers the peer's requests with
    /// `service`.
    ///
    /// Once `shutdown` completes the connection reads no more, and returns
    /// once the requests it had already received are answered. `owner` says
    /// whether the handles keep the connection open, and `max_in_flight`,
    /// which must not be zero, how many of the peer's requests may be in
    /// flight at once. With an `idle_timeout`, the connection ends with an
    /// error once that long has passed with nothing in flight and no box
    /// from the peer, or once a write to the peer has waited that long for
    /// it to take what was written.
    ///
    /// When the peer closes the connection, or an error ends it, the
    /// handlers of the requests that asked for a reply are cancelled: their
    /// reply has nowhere to go, and a peer could otherwise fill the
    /// in-flight limit with slow requests, hang up, and leave the handlers
    /// running for as long as they take. A notification's handler is given
    /// the idle timeout to finish first, since the peer never waited for it
    /// and may well have hung up on purpose once it was sent; without one it
    /// finishes however long it takes, and the connection with it. A
    /// shutdown cancels nothing: the peer is still there to be answered.
    ///
    /// Reading and writing are separate futures so that a write the peer is
    /// slow to accept never stops this side from reading. If both sides
    /// stopped reading while blocked on a write, neither would ever drain
    /// the other's buffer.
    ///
    /// The in-flight permits say nothing about a peer that stops reading:
    /// its handlers finish, and the responders then wait to queue replies
    /// that the writer cannot get rid of, each holding its permit, so the
    /// idle clock never starts. That is why every step the writer takes on
    /// the socket is timed with the idle limit too, and ends the connection
    /// with the same error when it runs out.
    ///
    /// A call learns that the connection ended when the sender of its reply
    /// is dropped, and then reads the cause for why, so every failure here
    /// is recorded in the cause before anything that holds a reply sender is
    /// let go of: the entries of `pending`, the futures, and the queue
    /// behind the handles.
    // The reader and writer below share the connection's state and have to
    // be dropped in a set order on every way out, which keeps them in one
    // body.
    #[expect(clippy::too_many_lines)]
    pub(crate) async fn run<S, F>(
        self,
        service: S,
        shutdown: F,
        owner: Owner,
        max_in_flight: usize,
        idle_timeout: Option<Duration>,
    ) -> Result<(), Error>
    where
        S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
        S::Error: Into<Error>,
        S::Future: Send,
        F: Future<Output = ()>,
    {
        debug_assert!(max_in_flight > 0, "a connection must be able to read");
        // `open` outlives everything else here: a handle waiting on it
        // must find the cause recorded and `pending` cleared, whether this
        // returns or is dropped part way through.
        let Connection {
            framed,
            client,
            mut outgoing,
            open: _open,
            idle_timeout: _,
        } = self;
        // Dropping the connection's own handle here is what lets `outgoing`
        // close once every handle the caller took is gone.
        let pending = Arc::clone(&client.pending);
        let cause = Arc::clone(&client.cause);
        drop(client);
        let pending = &pending;
        // The handles still hold `pending`, so it has to be emptied rather
        // than dropped, and on every way out of here, including being
        // dropped part way through: a call waiting for an answer that never
        // learned the connection ended would wait forever. Declared after
        // `_open` so that it runs first.
        let _clear = ClearOnDrop(pending);
        let (mut sink, mut stream) = framed.split();
        let (responses, mut replies) = mpsc::channel::<AmpBox>(16);
        let in_flight = Arc::new(Semaphore::new(max_in_flight));
        // Cancelled when the read future ends, however it ends, so that
        // the writer stops taking requests that could never be answered.
        let done_reading = CancellationToken::new();
        // Cancels the handlers whose reply could no longer be delivered:
        // when the peer closes the connection, and when an error ends this
        // future or it is dropped, since the guard goes with it. A shutdown
        // leaves it alone.
        let handlers = CancellationToken::new();
        let _cancel_handlers = handlers.clone().drop_guard();
        // Told by the writer each time a request that wants an answer goes
        // out, for the reader when it has stopped reading at the in-flight
        // limit; see the permit wait below.
        let calling = Notify::new();
        let calling = &calling;

        let done = done_reading.clone();
        let handlers = &handlers;
        let read = async move {
            let _done = done.drop_guard();
            // Polled only until it completes: each branch that sees it
            // complete returns.
            let mut shutdown = pin!(shutdown);
            // A request read while the connection was at its in-flight
            // limit; see below. Handled before anything else is read.
            let mut held = None;
            loop {
                let message = if let Some(request) = held.take() {
                    Message::Request(request)
                } else {
                    // `biased` so that a shutdown is seen even while the
                    // peer keeps the stream ready. A `next` that loses to
                    // it is dropped with nothing read, since the frame
                    // stays in the codec's buffer. The idle timer starts
                    // over with each box.
                    let incoming = tokio::select! {
                        biased;
                        () = &mut shutdown => return Ok(()),
                        limit = idle(idle_timeout, &in_flight, max_in_flight) => {
                            return Err(timed_out(limit, "sent"));
                        }
                        next = stream.next() => if let Some(incoming) = next {
                            incoming?
                        } else {
                            // The peer closed the connection.
                            handlers.cancel();
                            return Ok(());
                        },
                    };
                    Message::from_box(incoming)?
                };
                match message {
                    Message::Request(request) => {
                        // Waiting here, rather than spawning without bound,
                        // is what holds the peer to `max_in_flight`. Replies
                        // are written by `write` below, which does not
                        // depend on this loop, so waiting cannot deadlock.
                        // A shutdown ends the wait, and the request with it.
                        //
                        // The stream is read while waiting, so that a peer
                        // that hangs up is noticed rather than its handlers
                        // left to run with nowhere to send their replies,
                        // and so that an answer to a call from this side is
                        // matched to it; neither needs a permit. A request
                        // that arrives instead is held, one at most, until
                        // the permit comes.
                        //
                        // What happens past the held request depends on
                        // whether a call from this side is waiting. With
                        // none, the stream is left alone: the peer waits on
                        // its writes, and if it hangs up then, the hang-up
                        // is noticed when the next reply is written to it.
                        // With one, the stream is read on, since the call's
                        // answer may be queued behind the peer's requests,
                        // and if the handlers in flight are the callers, as
                        // handlers that call the peer that asked are, no
                        // permit would free until it was read: the
                        // connection would hang for good. A further request
                        // is then refused rather than held, since holding
                        // any number would let the peer send without bound.
                        // `calling` wakes this loop when a call is made
                        // while the stream was being left alone, so that
                        // whether to read is decided again.
                        let permit = loop {
                            let calls_waiting = !lock(pending).is_empty();
                            let reading = held.is_none() || calls_waiting;
                            tokio::select! {
                                biased;
                                () = &mut shutdown => return Ok(()),
                                permit = Arc::clone(&in_flight).acquire_owned() => match permit {
                                    Ok(permit) => break permit,
                                    // The semaphore is never closed. Were it,
                                    // no request could ever be admitted
                                    // again, which is a shutdown.
                                    Err(_closed) => return Ok(()),
                                },
                                () = calling.notified(), if !reading => {}
                                next = stream.next(), if reading => if let Some(incoming) = next {
                                    match Message::from_box(incoming?)? {
                                        Message::Request(request) => match held {
                                            None => held = Some(request),
                                            Some(_) => refuse(request, &responses).await,
                                        },
                                        Message::Answer(answer) => answered(pending, answer),
                                        Message::Error(error) => failed(pending, &error),
                                    }
                                } else {
                                    handlers.cancel();
                                    return Ok(());
                                },
                            }
                        };
                        spawn(respond(
                            service.clone(),
                            request,
                            responses.clone(),
                            permit,
                            handlers.clone(),
                            idle_timeout,
                        ));
                    }
                    Message::Answer(answer) => answered(pending, answer),
                    Message::Error(error) => failed(pending, &error),
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
                    within(idle_timeout, sink.flush()).await?;
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
                                None => written.as_ref().is_none_or(oneshot::Sender::is_closed),
                            };
                            if abandoned {
                                continue;
                            }
                            if request.ask.is_some() {
                                // The reader may have stopped at the
                                // in-flight limit, and now has an answer to
                                // read for.
                                calling.notify_one();
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
                            within(idle_timeout, sink.feed(amp_box)).await?;
                            unflushed = true;
                            notified.extend(written);
                        }
                    },
                    reply = replies.recv() => match reply {
                        Some(reply) => {
                            within(idle_timeout, sink.feed(reply)).await?;
                            unflushed = true;
                        }
                        // The read future has ended and every request it
                        // took on has been answered.
                        None => break,
                    },
                }
            }
            within(idle_timeout, sink.flush()).await?;
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
        // Returning clears `pending` and drops the futures, and `outgoing`
        // with them, which fails every call still waiting, and each finds
        // the cause already recorded.
        result
    }
}

/// Empties the map of calls waiting for an answer when dropped, so that
/// their callers learn the connection ended however [`Connection::run`]
/// ends.
struct ClearOnDrop<'a>(&'a Mutex<Pending>);

impl Drop for ClearOnDrop<'_> {
    fn drop(&mut self) {
        lock(self.0).clear();
    }
}

/// Hands the peer's answer to the call waiting for it. A reply nobody is
/// waiting for any more is discarded.
fn answered(pending: &Mutex<Pending>, answer: Answer) {
    if let Some(reply) = lock(pending).remove(&answer.answer) {
        let _ = reply.send(Ok(answer.results));
    }
}

/// Answers `request`, which arrived at the in-flight limit with a request
/// already held, with `TOO_MANY_REQUESTS`, as [`Connection::run`] describes.
/// A notification is dropped, since it asked for no answer; either way the
/// refusal is logged as the command failing.
async fn refuse(request: Request, responses: &mpsc::Sender<AmpBox>) {
    let err = Error::too_many_requests();
    log::command_failed(&request.command, request.ask.as_deref(), &err);
    if let Some(response) = request.ask.and_then(|ask| error_response(ask, &err)) {
        let _ = responses.send(response).await;
    }
}

/// Hands the peer's error to the call waiting for it, as [`answered`] does.
fn failed(pending: &Mutex<Pending>, error: &ErrorResponse) {
    if let Some(reply) = lock(pending).remove(&error.error) {
        let _ = reply.send(Err(Error::new(
            String::from_utf8_lossy(&error.code),
            String::from_utf8_lossy(&error.description),
        )));
    }
}

/// Completes with `limit` once the connection has been idle for it: nothing
/// in flight, and nothing read. Never completes without a limit.
///
/// A connection with a request in flight is waiting on its own handler,
/// not on the peer, so the clock does not run until every permit of
/// `in_flight`, of which there are `max_in_flight`, has been given back. The
/// permits are taken only to learn that, and given back before the clock
/// starts. This is one branch of a `select!` whose other branches read, so
/// the semaphore is not contended within the connection: a request that
/// arrives drops this future, permits and all, before it waits for one.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
async fn idle(limit: Option<Duration>, in_flight: &Semaphore, max_in_flight: usize) -> Duration {
    let Some(limit) = limit else {
        return std::future::pending().await;
    };
    // `Server::max_in_flight` keeps the limit within `u32`, so this is
    // only defensive.
    let all = u32::try_from(max_in_flight).unwrap_or(u32::MAX);
    // The semaphore is never closed. Were it, the connection could never
    // be found quiet, so the timer would never run.
    let Ok(quiet) = in_flight.acquire_many(all).await else {
        return std::future::pending().await;
    };
    drop(quiet);
    tokio::time::sleep(limit).await;
    limit
}

/// Never completes: nothing on this target sets an idle timeout, since the
/// server does not build here and [`Connection::serve`] has none, and
/// tokio's timer needs a runtime there is none of.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
async fn idle(limit: Option<Duration>, _in_flight: &Semaphore, _max_in_flight: usize) -> Duration {
    debug_assert!(limit.is_none(), "no idle timeout can be set on this target");
    std::future::pending().await
}

/// The error that ends a connection whose peer `did` nothing for `limit`:
/// "sent" nothing, or "read" nothing.
fn timed_out(limit: Duration, did: &str) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("the peer {did} nothing for {limit:?}"),
    ))
}

/// Runs `write`, one step of the writer's work on the socket, and fails
/// with [`Error::Io`] of kind `TimedOut` if it has not finished within
/// `limit`. Without a limit it just runs `write`.
///
/// A step is a `feed`, which writes only once 8 KiB are buffered, or a
/// `flush`, which writes what is buffered: at most that much and the last
/// box fed. A peer that reads at all takes that within any sensible limit;
/// one that has stopped reading never does, and this is how it is noticed.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
async fn within<F, E>(limit: Option<Duration>, write: F) -> Result<(), Error>
where
    F: Future<Output = Result<(), E>>,
    Error: From<E>,
{
    match limit {
        Some(limit) => match tokio::time::timeout(limit, write).await {
            Ok(result) => Ok(result?),
            Err(_) => Err(timed_out(limit, "read")),
        },
        None => Ok(write.await?),
    }
}

/// Just runs `write`: nothing on this target sets an idle timeout, as
/// [`idle`] says.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
async fn within<F, E>(limit: Option<Duration>, write: F) -> Result<(), Error>
where
    F: Future<Output = Result<(), E>>,
    Error: From<E>,
{
    debug_assert!(limit.is_none(), "no idle timeout can be set on this target");
    Ok(write.await?)
}

/// Completes once `grace` has passed, which is how long a notification's
/// handler has after its peer hangs up. Never completes without a grace
/// period.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
async fn after(grace: Option<Duration>) {
    match grace {
        Some(grace) => tokio::time::sleep(grace).await,
        None => std::future::pending().await,
    }
}

/// Never completes: nothing on this target sets an idle timeout, as
/// [`idle`] says.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
async fn after(grace: Option<Duration>) {
    debug_assert!(grace.is_none(), "no idle timeout can be set on this target");
    std::future::pending::<()>().await;
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
///
/// `cancelled` is cancelled once no response can reach the peer. A request
/// that wants one is then dropped wherever its handler is, and nothing is
/// logged: the peer left, and the handler did nothing wrong. A request that
/// wants none is given `grace` to finish first, since the peer never waited
/// for it, and is handled regardless without a grace period.
async fn respond<S>(
    service: S,
    request: Request,
    responses: mpsc::Sender<AmpBox>,
    _permit: OwnedSemaphorePermit,
    cancelled: CancellationToken,
    grace: Option<Duration>,
) where
    S: Service<Request, Response = AmpBox> + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    let command = request.command.clone();
    let ask = request.ask.clone();
    let gone = async {
        cancelled.cancelled().await;
        if ask.is_none() {
            after(grace).await;
        }
    };
    // `biased` so that a request whose peer is already gone is not started.
    // A notification whose peer is gone still is: its grace period has only
    // just begun.
    let outcome = tokio::select! {
        biased;
        () = gone => return,
        outcome = call(service, request) => outcome,
    };
    let result = match outcome {
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
            Ok(response) => Some(response),
            Err(err) => {
                let err = Error::Protocol(err);
                log::command_failed(&command, Some(&ask), &err);
                error_response(ask, &err)
            }
        },
        Err(err) => error_response(ask, &err),
    };
    if let Some(response) = response {
        let _ = responses.send(response).await;
    }
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
/// forever. `None` only if a box still cannot be built, which cannot happen:
/// the ask came off the wire, and the other fields were cut to fit.
fn error_response(ask: Vec<u8>, err: &Error) -> Option<AmpBox> {
    let description = err.description();
    ErrorResponse::new(ask, fit(err.code()), fit(&description))
        .into_box()
        .ok()
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
    drop(tokio::spawn(task));
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
        let amp_box = error_response(b"1".to_vec(), &err).unwrap();
        let response = ErrorResponse::from_box(amp_box).unwrap();
        assert_eq!(response.error, b"1");
        assert_eq!(response.code.len(), MAX_VALUE_LENGTH);
        assert_eq!(response.description.len(), MAX_VALUE_LENGTH);
    }
}
