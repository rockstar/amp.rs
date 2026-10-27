use std::future::Pending;
use std::io;
use std::pin::pin;
use std::time::Duration;

use std::sync::Arc;

use amp_protocol::codec::AmpCodec;
use amp_protocol::{AmpBox, Request};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower::Service;

use crate::connection::{DEFAULT_MAX_IN_FLIGHT, Owner};
use crate::{Connection, Error, log};

/// How long the accept loop waits after `accept` fails for want of a file
/// descriptor, so that it neither spins nor gives up while the process is
/// briefly at its limit.
const OUT_OF_DESCRIPTORS_BACKOFF: Duration = Duration::from_millis(100);

/// How long a connection may sit idle unless told otherwise: long enough
/// that a client which keeps a connection open between calls is rarely cut
/// off between them, and short enough that a peer which connects and sends
/// nothing does not hold a connection for long.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// A server with its options: the codec its connections are framed by, how
/// many requests each may have in flight, how long one may sit idle, how
/// many there may be at once, and a signal that stops it.
///
/// [`serve`] and [`serve_connection`] are the defaults; this is for a server
/// that needs something else. Set what differs, then run it on a listener
/// with [`serve`](Server::serve), or on one connection from an accept loop
/// of your own with [`serve_connection`](Server::serve_connection):
///
/// ```no_run
/// # use std::time::Duration;
/// # use tower_amp::{AmpCodec, Router, Server};
/// # #[tokio::main]
/// # async fn main() -> Result<(), tower_amp::Error> {
/// # let router = Router::new();
/// # let listener = tokio::net::TcpListener::bind("127.0.0.1:1234").await.unwrap();
/// Server::new(router)
///     .codec(AmpCodec::with_max_frame_length(64 * 1024))
///     .max_in_flight(32)
///     .idle_timeout(Duration::from_secs(60))
///     .max_connections(1_000)
///     .shutdown(async {
///         let _ = tokio::signal::ctrl_c().await;
///     })
///     .serve(listener)
///     .await
/// # }
/// ```
///
/// The defaults bound what one connection can cost, not what a peer can
/// cost by opening connections. Under them a connection holds at most:
///
/// | What a connection holds | At most |
/// |---|---|
/// | The read buffer, for a box still arriving | The frame limit, 1 MiB, and up to twice that as the buffer grows |
/// | The arguments of the requests in flight, while their handlers run | The in-flight limit times the frame limit: 256 × 1 MiB |
///
/// A handler owns its arguments for as long as it runs, so a peer reaches
/// the second with requests to a slow command that each carry a full box.
/// Lower [`max_in_flight`](Server::max_in_flight), or the frame limit with
/// [`codec`](Server::codec), for a server whose peers need less.
///
/// An idle connection is closed after the
/// [`idle_timeout`](Server::idle_timeout), five minutes by default, so a
/// peer holds that for at most that long without sending anything. Nothing
/// bounds how many connections there are at once unless
/// [`max_connections`](Server::max_connections) is set: the cost above is
/// multiplied by as many connections as the peers care to open. A server
/// open to peers it does not trust should set it, and bound how long a
/// handler may run with tower's `Timeout` on the service.
///
/// A server without a shutdown signal is `Clone`, so one configured once
/// can serve each connection an accept loop takes.
#[derive(Clone)]
pub struct Server<S, F = Pending<()>> {
    service: S,
    codec: AmpCodec,
    max_in_flight: usize,
    idle_timeout: Option<Duration>,
    max_connections: Option<usize>,
    shutdown: F,
}

impl<S> Server<S>
where
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    /// A server that answers every connection's requests with a clone of
    /// `service`, with the defaults [`serve`] and [`serve_connection`] use:
    /// a default [`AmpCodec`], 256 requests in flight per connection, an
    /// idle timeout of five minutes, no limit on connections, and no
    /// shutdown signal.
    pub fn new(service: S) -> Self {
        Server {
            service,
            codec: AmpCodec::new(),
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
            max_connections: None,
            shutdown: std::future::pending(),
        }
    }
}

impl<S, F> Server<S, F>
where
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
    F: Future<Output = ()>,
{
    /// The codec every connection is framed by, for setting the largest box
    /// a peer may send with [`AmpCodec::with_max_frame_length`] or
    /// [`AmpCodec::with_limits`]. The default refuses a box longer than
    /// 1 MiB or with more than 1,024 pairs.
    pub fn codec(mut self, codec: AmpCodec) -> Self {
        self.codec = codec;
        self
    }

    /// How many requests one connection may have in flight at once:
    /// received and not yet answered. Past this the connection stops
    /// reading until one has been answered, which a peer sees as ordinary
    /// backpressure on its writes. The default is 256.
    ///
    /// Fewer holds a peer to less memory, which matters when a handler
    /// holds its request body or a database connection while it runs. More
    /// lets a trusted peer pipeline deeper. This bounds what a connection
    /// takes on, not what runs: wrap the service in tower's
    /// `ConcurrencyLimit` to bound the handlers running across every
    /// connection, and in its `Timeout` to bound how long each runs.
    ///
    /// A peer that hangs up releases what it took on: the handlers of its
    /// requests still in flight are cancelled, as [`serve_connection`]
    /// describes. One that first sends more requests than the limit, so
    /// that the connection has stopped reading, is not noticed until a
    /// reply is next written to it; until then its connection counts
    /// against [`max_connections`](Server::max_connections) like any other.
    ///
    /// # Panics
    ///
    /// If `limit` is zero, since a connection that may never read a request
    /// would hang its peer.
    pub fn max_in_flight(mut self, limit: usize) -> Self {
        assert!(
            limit > 0,
            "a connection must be allowed a request in flight"
        );
        self.max_in_flight = limit;
        self
    }

    /// How long a connection may wait on its peer, in either direction,
    /// before it is closed. The default is five minutes;
    /// [`no_idle_timeout`](Server::no_idle_timeout) removes the limit.
    ///
    /// Reading, the clock starts over with every box the peer sends, and
    /// runs only while no request is in flight: a connection waiting on a
    /// handler is busy, not idle, however long the handler takes. Writing,
    /// each write of replies is given the same limit to be taken by the
    /// peer; one write is at most 8 KiB of replies and the one that pushed
    /// it past that, so a peer that reads at all takes it in time. A peer
    /// that sends nothing, or only part of a box, for `limit`, or that stops
    /// reading so that its replies back up, has its connection closed with
    /// [`Error::Io`] of kind `TimedOut`, which [`serve`](Server::serve)
    /// logs with the `tracing` feature and
    /// [`serve_connection`](Server::serve_connection) returns.
    ///
    /// The limit is what keeps a peer that connects and sends nothing,
    /// sends a byte every so often, or sends requests and never reads the
    /// replies from holding a connection and its buffers for good. The
    /// last would otherwise hold it past every other limit: its handlers
    /// finish, so a `Timeout` on the service has nothing to cut short, and
    /// its replies then wait on it with the in-flight permits held, so the
    /// connection never looks idle. Shorten the limit on a server open to
    /// peers it does not trust. A client that keeps a connection open
    /// between calls needs it to be longer than the gaps between them, or
    /// to reconnect when a call fails with [`Error::ConnectionClosed`].
    ///
    /// The limit also bounds what a peer leaves behind. A peer that hangs
    /// up has the handlers of its requests cancelled, as
    /// [`serve_connection`] describes, except those of its notifications,
    /// which it never waited for and which otherwise finish however long
    /// they take, with the connection open until they do. With a limit they
    /// are given that long after the hang-up and then cancelled, so a peer
    /// that fills the in-flight limit with notifications to a slow command,
    /// hangs up, and reconnects holds no more than the limit allows.
    pub fn idle_timeout(mut self, limit: Duration) -> Self {
        self.idle_timeout = Some(limit);
        self
    }

    /// Removes the [`idle_timeout`](Server::idle_timeout), so that a
    /// connection may wait on its peer for good, in either direction, and
    /// a peer that hangs up may leave the handlers of its notifications
    /// running until they finish. For a server whose peers are trusted and
    /// keep a connection open between calls any distance apart.
    pub fn no_idle_timeout(mut self) -> Self {
        self.idle_timeout = None;
        self
    }

    /// How many connections [`serve`](Server::serve) has open at once.
    /// There is no limit unless one is set, and running out of file
    /// descriptors is the only bound.
    ///
    /// At the limit the server stops accepting until a connection closes,
    /// so a peer that connects then waits in the listener's backlog, and
    /// once that is full the operating system refuses it. A connection
    /// counts until its task ends, which for a peer that hung up with
    /// handlers still running is when those are cancelled or finish. With
    /// the [`idle_timeout`](Server::idle_timeout) and a `Timeout` on the
    /// service, what one peer can hold is then bounded in every direction:
    /// connections, requests on each, how long each runs, and how long its
    /// replies may wait to be read.
    ///
    /// Only `serve` applies the limit, since
    /// [`serve_connection`](Server::serve_connection) is given its
    /// connections by an accept loop of your own.
    ///
    /// # Panics
    ///
    /// If `limit` is zero, since a server that may never accept a
    /// connection would serve nobody.
    pub fn max_connections(mut self, limit: usize) -> Self {
        assert!(limit > 0, "a server must be allowed a connection");
        self.max_connections = Some(limit);
        self
    }

    /// A future that stops the server once it completes, answering the
    /// requests in flight before [`serve`](Server::serve) returns.
    ///
    /// Once `signal` completes the listener is closed, so a peer that
    /// connects from then on is refused, and every connection stops reading
    /// requests. A request already received is still handled and its reply
    /// written; each connection closes once the last of its requests is
    /// answered, and `serve` returns `Ok(())` once every connection has
    /// closed. A request the peer had sent but the connection had not yet
    /// read is lost with the connection, which the peer sees as a call
    /// failed by the connection closing; the same goes for one that arrives
    /// afterwards.
    ///
    /// A handler that never finishes holds shutdown open: nothing here
    /// gives up on a request, and a peer that hangs up during shutdown is
    /// not noticed, since the connection is no longer reading. A peer that
    /// stops reading its replies holds it open too, for as long as the
    /// [`idle_timeout`](Server::idle_timeout) allows a write to wait, or
    /// for good without one. Wrap the call in [`tokio::time::timeout`] if
    /// that matters. Dropping the future, which the timeout does, still
    /// tells the connections to stop reading and answer what they have,
    /// but no longer waits for them.
    ///
    /// For [`serve_connection`](Server::serve_connection) the signal stops
    /// that one connection the same way: it reads no more requests and
    /// closes once those it had received are answered.
    pub fn shutdown<G>(self, signal: G) -> Server<S, G>
    where
        G: Future<Output = ()>,
    {
        Server {
            service: self.service,
            codec: self.codec,
            max_in_flight: self.max_in_flight,
            idle_timeout: self.idle_timeout,
            max_connections: self.max_connections,
            shutdown: signal,
        }
    }

    /// Accepts connections on `listener` and serves each one on its own
    /// task, as [`serve`] does, with this server's options.
    ///
    /// Returns when accepting a connection fails for good, as `serve`
    /// does, or with `Ok(())` once the [shutdown](Server::shutdown) signal
    /// has completed and every connection has closed. An accept error that
    /// ends `serve` ends this the same way: the connections finish what
    /// they have received and the error is returned.
    pub async fn serve(self, listener: TcpListener) -> Result<(), Error> {
        let Server {
            service,
            codec,
            max_in_flight,
            idle_timeout,
            max_connections,
            shutdown: signal,
        } = self;
        let shutdown = CancellationToken::new();
        // If this future is dropped instead of returning, say by a timeout
        // on it, the connections are still told to stop reading and finish
        // up.
        let _cancel_on_drop = shutdown.clone().drop_guard();
        let connections = TaskTracker::new();
        // One permit per connection allowed, held by its task until it
        // ends. A semaphore cannot hold more than `MAX_PERMITS`, which is
        // more connections than any process will have.
        let open = max_connections
            .map(|limit| Arc::new(Semaphore::new(limit.min(Semaphore::MAX_PERMITS))));
        let mut signal = pin!(signal);
        let result = loop {
            // `biased` so that the signal is seen even while peers keep the
            // listener ready, or while connections keep closing. Neither
            // branch is polled again after it completes.
            let permit = match &open {
                Some(open) => tokio::select! {
                    biased;
                    () = &mut signal => break Ok(()),
                    permit = Arc::clone(open).acquire_owned() => {
                        Some(permit.expect("the semaphore is never closed"))
                    }
                },
                None => None,
            };
            let accepted = tokio::select! {
                biased;
                () = &mut signal => break Ok(()),
                accepted = listener.accept() => accepted,
            };
            match accepted {
                Ok((stream, _)) => {
                    // Replies are small and written one at a time, and a
                    // peer that pipelines calls gets several back to back.
                    // With Nagle's algorithm on, each would wait for the
                    // ACK of the one before it, up to the peer's
                    // delayed-ACK timer. This only fails on a socket
                    // already dead, which the connection task finds out
                    // for itself.
                    let _ = stream.set_nodelay(true);
                    let connection = Server {
                        service: service.clone(),
                        codec: codec.clone(),
                        max_in_flight,
                        idle_timeout,
                        max_connections,
                        shutdown: shutdown.clone().cancelled_owned(),
                    };
                    connections.spawn(async move {
                        // The connection's share of `max_connections`, given
                        // back when the connection has ended.
                        let _permit = permit;
                        if let Err(err) = connection.serve_connection(stream).await {
                            log::connection_failed(&err);
                        }
                    });
                }
                Err(err) => match retry_after(&err) {
                    Some(delay) => {
                        log::accept_skipped(&err);
                        tokio::select! {
                            biased;
                            () = &mut signal => break Ok(()),
                            () = tokio::time::sleep(delay) => {}
                        }
                    }
                    None => break Err(err.into()),
                },
            }
        };
        // The listener goes first so that a peer connecting from now on is
        // refused rather than left in the backlog until the last connection
        // is done, and the connections are told to stop before this waits
        // on them.
        drop(listener);
        shutdown.cancel();
        connections.close();
        connections.wait().await;
        result
    }

    /// Serves one connection until the peer closes it, as
    /// [`serve_connection`] does, with this server's options.
    ///
    /// Returns once the peer closes the connection, or with the error that
    /// ended the connection, as `serve_connection` does, which includes the
    /// [idle timeout](Server::idle_timeout) running out. With a
    /// [shutdown](Server::shutdown) signal, it also returns `Ok(())` once
    /// the signal has completed and the requests received before it were
    /// answered. The [connection limit](Server::max_connections) does not
    /// apply, since the connection was accepted elsewhere.
    ///
    /// `io` need not be a socket, so nothing is done to its options. An
    /// accept loop of your own should set `TCP_NODELAY` on each
    /// `TcpStream`, as [`serve`] does.
    pub async fn serve_connection<T>(self, io: T) -> Result<(), Error>
    where
        T: AsyncRead + AsyncWrite,
    {
        Connection::with_codec(io, self.codec)
            .run(
                self.service,
                self.shutdown,
                Owner::Driver,
                self.max_in_flight,
                self.idle_timeout,
            )
            .await
    }
}

/// Accepts connections on `listener` forever, serving each one with a clone
/// of `service` on its own task.
///
/// Returns only when accepting a connection fails for good. A peer that
/// drops its connection before it is accepted, or an accept that fails
/// because the process is briefly out of file descriptors, is skipped and
/// the loop carries on; anything else, such as the listener being closed,
/// ends `serve` with that error, once the connections already accepted
/// have finished the requests they had received. An error on one
/// connection ends that connection. With the `tracing` feature, a skipped
/// accept error and a connection's error are logged; without it they are
/// discarded, and calling [`serve_connection`] from your own accept loop is
/// the way to observe the latter.
///
/// `TCP_NODELAY` is set on every connection accepted, so that the replies
/// to pipelined calls go out as they are ready rather than each waiting on
/// the acknowledgement of the one before it.
///
/// Each connection uses a default [`AmpCodec`], which refuses a box longer
/// than 1 MiB or with more than 1,024 pairs, may have 256 requests in
/// flight at once, and is closed once it has sat idle for five minutes.
/// There is no limit on the number of connections; [`Server`] serves with
/// one, with another codec, in-flight limit, or idle timeout, and with a
/// way to stop.
pub async fn serve<S>(listener: TcpListener, service: S) -> Result<(), Error>
where
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    Server::new(service).serve(listener).await
}

/// [`serve`] with the codec every connection is framed by.
#[deprecated(
    since = "0.3.0",
    note = "use `Server::new(service).codec(codec).serve(listener)`"
)]
pub async fn serve_with_codec<S>(
    listener: TcpListener,
    service: S,
    codec: AmpCodec,
) -> Result<(), Error>
where
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    Server::new(service).codec(codec).serve(listener).await
}

/// [`serve`] until `signal` completes, answering the requests in flight
/// before returning, as [`Server::shutdown`] describes.
#[deprecated(
    since = "0.3.0",
    note = "use `Server::new(service).shutdown(signal).serve(listener)`"
)]
pub async fn serve_with_shutdown<S, F>(
    listener: TcpListener,
    service: S,
    signal: F,
) -> Result<(), Error>
where
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
    F: Future<Output = ()>,
{
    Server::new(service).shutdown(signal).serve(listener).await
}

/// [`serve`] with the codec every connection is framed by, until `signal`
/// completes, as [`Server::shutdown`] describes.
#[deprecated(
    since = "0.3.0",
    note = "use `Server::new(service).codec(codec).shutdown(signal).serve(listener)`"
)]
pub async fn serve_with_codec_and_shutdown<S, F>(
    listener: TcpListener,
    service: S,
    codec: AmpCodec,
    signal: F,
) -> Result<(), Error>
where
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
    F: Future<Output = ()>,
{
    Server::new(service)
        .codec(codec)
        .shutdown(signal)
        .serve(listener)
        .await
}

/// How the accept loop treats an error from `accept`: `Some(delay)` means
/// wait that long and try again, `None` means the error ends `serve`.
///
/// A peer that resets or hangs up between connecting and being accepted
/// surfaces as a connection error on `accept`, and says nothing about the
/// listener. Running out of file descriptors (`EMFILE`, `ENFILE`) clears
/// once a connection closes, but retrying at once would spin until then.
/// Both are routine under load or a port scan, and neither is a reason to
/// stop serving. This matches what hyper's `AddrIncoming` and axum's
/// `serve` do.
fn retry_after(err: &io::Error) -> Option<Duration> {
    match err.kind() {
        io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionRefused
        | io::ErrorKind::Interrupted => Some(Duration::ZERO),
        _ if out_of_descriptors(err) => Some(OUT_OF_DESCRIPTORS_BACKOFF),
        _ => None,
    }
}

/// Whether `err` is `EMFILE` or `ENFILE`: the process or the system has no
/// file descriptor left for the connection. `io::ErrorKind` has no variant
/// for either, so this goes by the OS error code.
fn out_of_descriptors(err: &io::Error) -> bool {
    const OUT_OF_DESCRIPTORS: &[i32] = if cfg!(windows) {
        // WSAEMFILE; Windows has no per-system equivalent of ENFILE.
        &[10024]
    } else {
        // ENFILE, EMFILE: the same on Linux, macOS and the BSDs.
        &[23, 24]
    };
    err.raw_os_error()
        .is_some_and(|code| OUT_OF_DESCRIPTORS.contains(&code))
}

/// Serves one connection until the peer closes it.
///
/// Every request is handed to a clone of `service` on its own task, so the
/// requests on a connection run concurrently and are answered in whatever
/// order they finish, as the protocol allows. At most 256 requests are in
/// flight on a connection at once; past that it stops reading until one has
/// been answered, which a peer sees as ordinary backpressure on its writes.
/// A request without `_ask` gets no reply. A failed request is answered with
/// the error's code and description, or with `UNKNOWN` for an error that has
/// no code; see [`Error`] for which of those carry their message to the peer.
/// A request whose handler panics is answered with `UNKNOWN` and the
/// description `internal error`, and the connection carries on. With the
/// `tracing` feature, every failure is logged with the command's name, the
/// error or the panic's message; without it, a failure the peer did not ask
/// to hear about is discarded. Answers and errors the peer sends, which would
/// belong to commands this side had called, are ignored: this side calls
/// none. A [`Connection`] serves the peer and calls commands on it.
///
/// `io` need not be a socket, so nothing is done to its options. An accept
/// loop of your own should set `TCP_NODELAY` on each `TcpStream`, as
/// [`serve`] does, so that replies written back to back are not each held
/// for the peer's acknowledgement of the one before.
///
/// Returns once the peer closes the connection, or with the error that ended
/// the connection. A box longer than the codec allows is such an error,
/// reported as [`Error::Protocol`] wrapping
/// [`amp_protocol::Error::FrameTooLong`], and so is one with more pairs,
/// wrapping [`amp_protocol::Error::TooManyPairs`]. The default [`AmpCodec`]
/// allows 1 MiB and 1,024 pairs, and the connection is closed once it has
/// sat idle for five minutes. [`Server::serve_connection`] serves with
/// another codec, in-flight limit, or idle timeout, and with a way to stop.
///
/// Either way, the handlers of the requests still in flight are cancelled:
/// their reply has nowhere to go, and a peer could otherwise fill the
/// in-flight limit with slow requests, hang up, and leave the handlers
/// running for as long as they take. A cancelled handler is dropped
/// wherever it is, and nothing is logged. A handler whose work must finish
/// once begun should spawn that work on its own task rather than await it.
/// The handler of a request without `_ask` is not cancelled, since the
/// peer never waited for it and may have hung up on purpose once it was
/// sent. The [idle timeout](Server::idle_timeout) gives such a handler that
/// long after the hang-up, then cancels it too, and `serve_connection`
/// returns once those have finished or been cancelled.
pub async fn serve_connection<T, S>(io: T, service: S) -> Result<(), Error>
where
    T: AsyncRead + AsyncWrite,
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    Server::new(service).serve_connection(io).await
}

/// [`serve_connection`] with the codec the connection is framed by.
#[deprecated(
    since = "0.3.0",
    note = "use `Server::new(service).codec(codec).serve_connection(io)`"
)]
pub async fn serve_connection_with_codec<T, S>(
    io: T,
    service: S,
    codec: AmpCodec,
) -> Result<(), Error>
where
    T: AsyncRead + AsyncWrite,
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    Server::new(service).codec(codec).serve_connection(io).await
}

/// What the `tracing` feature logs. This lives with the unit tests rather
/// than in `tests/`, since it needs a process of its own: tracing caches
/// each callsite's interest the first time it is hit, from whichever
/// thread hits it, and a test in another thread with no subscriber would
/// cache "never" for the callsites this test wants to see.
#[cfg(all(test, feature = "tracing"))]
mod log_test {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use tower::service_fn;
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::util::SubscriberInitExt;

    use super::*;
    use crate::{Client, Router};

    /// Collects what the subscriber writes.
    #[derive(Clone, Default)]
    struct Log(Arc<Mutex<Vec<u8>>>);

    impl Log {
        fn lines(&self) -> Vec<String> {
            let bytes = self.0.lock().unwrap();
            String::from_utf8_lossy(&bytes)
                .lines()
                .map(String::from)
                .collect()
        }
    }

    impl Write for Log {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Log {
        type Writer = Log;

        fn make_writer(&'a self) -> Log {
            self.clone()
        }
    }

    /// A service whose every command fails in its own way: `Boom` panics,
    /// `Flaky` fails with an error about this side, and anything else fails
    /// with the handler's own error.
    fn service() -> impl Service<
        Request,
        Response = AmpBox,
        Error = Error,
        Future = impl Future<Output = Result<AmpBox, Error>> + Send,
    > + Clone
    + Send
    + 'static {
        service_fn(|request: Request| async move {
            match request.command.as_slice() {
                b"Boom" => panic!("handler bug"),
                b"Flaky" => Err(Error::Io(io::Error::other(
                    "db at 10.0.0.7:5432 refused the connection",
                ))),
                _ => Err(Error::new("ZERO_DIVISION", "cannot divide by zero")),
            }
        })
    }

    /// The test runs on a current-thread runtime, so the subscriber set for
    /// its thread also sees the events from the tasks the server spawns.
    #[tokio::test]
    async fn failures_are_logged() {
        let log = Log::default();
        let _guard = tracing_subscriber::fmt()
            .with_writer(log.clone())
            .with_ansi(false)
            .with_max_level(LevelFilter::DEBUG)
            .finish()
            .set_default();

        let (client_io, server_io) = tokio::io::duplex(4096);
        tokio::spawn(serve_connection(server_io, service()));
        let mut client = Client::new(client_io);
        for command in ["Boom", "Flaky", "Divide"] {
            let request = Request::new(command, AmpBox::new()).with_ask("x");
            Service::call(&mut client, request).await.unwrap_err();
        }

        let lines = log.lines();
        assert_eq!(lines.len(), 3, "{lines:#?}");
        assert!(lines[0].contains("ERROR"), "{lines:#?}");
        assert!(lines[0].contains(r#"command="Boom""#), "{lines:#?}");
        assert!(
            lines[0].contains("command panicked: handler bug"),
            "{lines:#?}"
        );
        assert!(lines[1].contains("WARN"), "{lines:#?}");
        assert!(lines[1].contains(r#"command="Flaky""#), "{lines:#?}");
        assert!(lines[1].contains("10.0.0.7:5432"), "{lines:#?}");
        // A handler's own error is the peer's business, so it is only of
        // interest when debugging.
        assert!(lines[2].contains("DEBUG"), "{lines:#?}");
        assert!(lines[2].contains(r#"command="Divide""#), "{lines:#?}");
        assert!(lines[2].contains("ZERO_DIVISION"), "{lines:#?}");
    }

    /// A middleware error's text goes to the log, which is the only place
    /// it goes: the peer is told `internal error`.
    #[tokio::test]
    async fn middleware_errors_are_logged() {
        let log = Log::default();
        let _guard = tracing_subscriber::fmt()
            .with_writer(log.clone())
            .with_ansi(false)
            .finish()
            .set_default();

        let service = service_fn(|_: Request| async {
            Err::<AmpBox, tower::BoxError>("db at 10.0.0.7:5432 refused the connection".into())
        });
        let (client_io, server_io) = tokio::io::duplex(4096);
        tokio::spawn(serve_connection(server_io, service));
        let mut client = Client::new(client_io);
        let request = Request::new("Lookup", AmpBox::new()).with_ask("x");
        let failed = Service::call(&mut client, request).await.unwrap_err();
        assert!(
            matches!(
                failed,
                Error::Command { ref code, ref description }
                    if code == "UNKNOWN" && description == "internal error"
            ),
            "{failed:?}"
        );

        let lines = log.lines();
        assert_eq!(lines.len(), 1, "{lines:#?}");
        assert!(lines[0].contains("WARN"), "{lines:#?}");
        assert!(lines[0].contains(r#"command="Lookup""#), "{lines:#?}");
        assert!(lines[0].contains("10.0.0.7:5432"), "{lines:#?}");
    }

    /// A request that wants no reply is still logged when it fails, since
    /// the log is the only place its failure can go.
    #[tokio::test]
    async fn failed_notifications_are_logged() {
        let log = Log::default();
        let _guard = tracing_subscriber::fmt()
            .with_writer(log.clone())
            .with_ansi(false)
            .finish()
            .set_default();

        let (client_io, server_io) = tokio::io::duplex(4096);
        tokio::spawn(serve_connection(server_io, service()));
        let mut client = Client::new(client_io);
        let request = Request::new("Flaky", AmpBox::new());
        Service::call(&mut client, request).await.unwrap();
        // A notification is answered before it is handled, so give the
        // server a moment. The subscriber is on this thread, so there is
        // no other thread to wait on.
        tokio::task::yield_now().await;

        let lines = log.lines();
        assert_eq!(lines.len(), 1, "{lines:#?}");
        assert!(lines[0].contains("WARN"), "{lines:#?}");
        assert!(lines[0].contains(r#"command="Flaky""#), "{lines:#?}");
        assert!(lines[0].contains(r#"ask="-""#), "{lines:#?}");
    }

    /// The command is the peer's bytes, and the error text repeats it. A
    /// newline in it must not become a second log line, and an escape byte
    /// must not reach the terminal as is.
    #[tokio::test]
    async fn peer_bytes_are_escaped() {
        let log = Log::default();
        let _guard = tracing_subscriber::fmt()
            .with_writer(log.clone())
            .with_ansi(false)
            .with_max_level(LevelFilter::DEBUG)
            .finish()
            .set_default();

        let (client_io, server_io) = tokio::io::duplex(4096);
        tokio::spawn(serve_connection(server_io, Router::new()));
        let mut client = Client::new(client_io);
        let command = "Nope\n2026-10-14T00:00:00Z ERROR tower_amp: admin login \x1b[31msucceeded";
        let request = Request::new(command, AmpBox::new()).with_ask("x");
        Service::call(&mut client, request).await.unwrap_err();

        let lines = log.lines();
        assert_eq!(lines.len(), 1, "{lines:#?}");
        assert!(!lines[0].contains('\x1b'), "{lines:#?}");
        let escaped =
            r#""Nope\n2026-10-14T00:00:00Z ERROR tower_amp: admin login \u{1b}[31msucceeded""#;
        assert!(
            lines[0].contains(&format!("command={escaped}")),
            "{lines:#?}"
        );
        assert!(
            lines[0].contains(&format!(
                "error=\"UNHANDLED: unhandled command: {}",
                &escaped[1..]
            )),
            "{lines:#?}"
        );
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::Router;

    #[test]
    fn idle_timeout_is_on_by_default_and_can_be_removed() {
        let server = Server::new(Router::new());
        assert_eq!(server.idle_timeout, Some(DEFAULT_IDLE_TIMEOUT));
        let server = server.no_idle_timeout();
        assert_eq!(server.idle_timeout, None);
        let server = server.idle_timeout(Duration::from_secs(1));
        assert_eq!(server.idle_timeout, Some(Duration::from_secs(1)));
    }

    #[test]
    fn a_peer_that_hangs_up_before_accept_is_skipped() {
        for kind in [
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::Interrupted,
        ] {
            assert_eq!(retry_after(&io::Error::from(kind)), Some(Duration::ZERO));
        }
    }

    #[test]
    fn running_out_of_descriptors_backs_off() {
        let codes: &[i32] = if cfg!(windows) { &[10024] } else { &[23, 24] };
        for &code in codes {
            let err = io::Error::from_raw_os_error(code);
            assert_eq!(retry_after(&err), Some(OUT_OF_DESCRIPTORS_BACKOFF), "{err}");
        }
    }

    #[test]
    fn other_accept_errors_end_serve() {
        for err in [
            io::Error::from(io::ErrorKind::NotConnected),
            io::Error::from(io::ErrorKind::Other),
            // EBADF: the listener's descriptor is gone.
            io::Error::from_raw_os_error(9),
        ] {
            assert_eq!(retry_after(&err), None, "{err}");
        }
    }
}
