use std::io;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use amp_protocol::codec::AmpCodec;
use amp_protocol::{AmpBox, Answer, ErrorResponse, Message, Request};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinError;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower::{Service, ServiceExt};

use crate::Error;
use crate::error::INTERNAL_ERROR;

/// How many requests one connection may have in flight: received and not
/// yet answered. Past this the connection stops reading until a reply has
/// been queued, so a peer that sends requests without reading the replies
/// is held to this much memory rather than as much as it cares to send.
const MAX_IN_FLIGHT: usize = 256;

/// How long the accept loop waits after `accept` fails for want of a file
/// descriptor, so that it neither spins nor gives up while the process is
/// briefly at its limit.
const OUT_OF_DESCRIPTORS_BACKOFF: Duration = Duration::from_millis(100);

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
/// Each connection uses a default [`AmpCodec`], which refuses a box longer
/// than 16 MiB; [`serve_with_codec`] takes another. [`serve_with_shutdown`]
/// is the same server with a way to stop it.
pub async fn serve<S>(listener: TcpListener, service: S) -> Result<(), Error>
where
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    serve_with_codec(listener, service, AmpCodec::new()).await
}

/// [`serve`] with the codec every connection is framed by, for setting the
/// largest box a peer may send with
/// [`AmpCodec::with_max_frame_length`].
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
    serve_with_codec_and_shutdown(listener, service, codec, std::future::pending()).await
}

/// [`serve`] until `signal` completes, answering the requests in flight
/// before returning.
///
/// Once `signal` completes the listener is closed, so a peer that connects
/// from then on is refused, and every connection stops reading requests.
/// A request already received is still handled and its reply written; each
/// connection closes once the last of its requests is answered, and
/// `serve_with_shutdown` returns `Ok(())` once every connection has closed.
/// A request the peer had sent but the connection had not yet read is lost
/// with the connection, which the peer sees as a call failed by the
/// connection closing; the same goes for one that arrives afterwards.
///
/// A handler that never finishes holds shutdown open: nothing here gives up
/// on a request. Wrap the call in [`tokio::time::timeout`] if that matters.
/// Dropping the future, which the timeout does, still tells the connections
/// to stop reading and answer what they have, but no longer waits for them.
///
/// An accept error that ends [`serve`] ends this the same way: the
/// connections finish what they have received and the error is returned.
///
/// Each connection uses a default [`AmpCodec`];
/// [`serve_with_codec_and_shutdown`] takes another.
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
    serve_with_codec_and_shutdown(listener, service, AmpCodec::new(), signal).await
}

/// [`serve_with_shutdown`] with the codec every connection is framed by,
/// for setting the largest box a peer may send with
/// [`AmpCodec::with_max_frame_length`].
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
    let shutdown = CancellationToken::new();
    // If this future is dropped instead of returning, say by a timeout on
    // it, the connections are still told to stop reading and finish up.
    let _cancel_on_drop = shutdown.clone().drop_guard();
    let connections = TaskTracker::new();
    let mut signal = pin!(signal);
    let result = loop {
        // `biased` so that the signal is seen even while peers keep the
        // listener ready. Neither branch is polled again after it completes.
        let accepted = tokio::select! {
            biased;
            () = &mut signal => break Ok(()),
            accepted = listener.accept() => accepted,
        };
        match accepted {
            Ok((stream, _)) => {
                let service = service.clone();
                let shutdown = shutdown.clone();
                connections.spawn(async move {
                    if let Err(err) = serve_connection_until(stream, service, codec, shutdown).await
                    {
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
    // refused rather than left in the backlog until the last connection is
    // done, and the connections are told to stop before this waits on them.
    drop(listener);
    shutdown.cancel();
    connections.close();
    connections.wait().await;
    result
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
/// belong to commands this side had called, are ignored.
///
/// Returns once the peer closes the connection and every request already
/// received has been answered, or with the error that ended the connection.
/// A box longer than the codec allows is such an error, reported as
/// [`Error::Protocol`] wrapping [`amp_protocol::Error::FrameTooLong`]. The
/// default [`AmpCodec`] allows 16 MiB; [`serve_connection_with_codec`] takes
/// another.
pub async fn serve_connection<T, S>(io: T, service: S) -> Result<(), Error>
where
    T: AsyncRead + AsyncWrite,
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    serve_connection_with_codec(io, service, AmpCodec::new()).await
}

/// [`serve_connection`] with the codec the connection is framed by, for
/// setting the largest box the peer may send with
/// [`AmpCodec::with_max_frame_length`].
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
    serve_connection_until(io, service, codec, CancellationToken::new()).await
}

/// [`serve_connection_with_codec`] until `shutdown` is cancelled, after which
/// the connection reads no more requests, and returns once those it had
/// already received are answered. A request the peer sent and this side had
/// not read by then is lost with the connection.
async fn serve_connection_until<T, S>(
    io: T,
    service: S,
    codec: AmpCodec,
    shutdown: CancellationToken,
) -> Result<(), Error>
where
    T: AsyncRead + AsyncWrite,
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    let (mut sink, mut stream) = Framed::new(io, codec).split();
    let (responses, mut pending) = mpsc::channel::<AmpBox>(16);
    let in_flight = Arc::new(Semaphore::new(MAX_IN_FLIGHT));

    // Reading and writing are separate futures so that a write the peer is
    // slow to accept never stops this side from reading. If both sides
    // stopped reading while blocked on a write, neither would ever drain the
    // other's buffer.
    let read = async move {
        loop {
            // `biased` so that a shutdown is seen even while the peer keeps
            // the stream ready. A `next` that loses to it is dropped with
            // nothing read, since the frame stays in the codec's buffer.
            let incoming = tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                next = stream.next() => match next {
                    Some(incoming) => incoming?,
                    None => break,
                },
            };
            match Message::from_box(incoming)? {
                Message::Request(request) => {
                    // Waiting here, rather than spawning without bound, is
                    // what holds the peer to `MAX_IN_FLIGHT`. Replies are
                    // written by `write` below, which does not depend on
                    // this loop, so waiting cannot deadlock. A shutdown
                    // ends the wait, and the request with it.
                    let permit = tokio::select! {
                        biased;
                        () = shutdown.cancelled() => break,
                        permit = Arc::clone(&in_flight).acquire_owned() => {
                            permit.expect("the semaphore is never closed")
                        }
                    };
                    tokio::spawn(respond(service.clone(), request, responses.clone(), permit));
                }
                Message::Answer(_) | Message::Error(_) => {}
            }
        }
        // The peer is done sending, or this side is done reading. Dropping
        // this sender lets the writer finish once every request already
        // received has been answered.
        drop(responses);
        Ok::<_, Error>(())
    };
    let write = async move {
        while let Some(response) = pending.recv().await {
            sink.send(response).await?;
        }
        Ok::<_, Error>(())
    };
    tokio::try_join!(read, write)?;
    Ok(())
}

/// Runs one request through the service and queues its response, if the
/// request wants one. The permit is the request's share of the connection's
/// in-flight limit, released when this returns: once the response is queued,
/// or at once for a request that wants none.
async fn respond<S>(
    mut service: S,
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
    // The service runs on a task of its own so that a handler that panics
    // takes down only that task. This one survives to answer the request,
    // which would otherwise wait on the peer forever.
    let call = tokio::spawn(async move {
        let service = service.ready().await.map_err(Into::into)?;
        service.call(request).await.map_err(Into::into)
    });
    let result = match call.await {
        Ok(Ok(results)) => Ok(results),
        Ok(Err(err)) => {
            log::command_failed(&command, ask.as_deref(), &err);
            Err(err)
        }
        Err(err) => {
            // The panic's message is for the log only: it says what the
            // handler was doing, which is this side's business, not the
            // peer's.
            log::command_panicked(&command, ask.as_deref(), &panicked(err));
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

/// What a handler that did not finish has to say for itself: the panic's
/// message, when it has one.
fn panicked(err: JoinError) -> String {
    match err.try_into_panic() {
        Ok(payload) => match payload.downcast_ref::<&str>() {
            Some(message) => format!("command panicked: {message}"),
            None => match payload.downcast_ref::<String>() {
                Some(message) => format!("command panicked: {message}"),
                None => "command panicked".to_string(),
            },
        },
        Err(_) => "command was cancelled".to_string(),
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

/// The longest value a box may carry, as `amp_protocol` enforces it. The
/// crate does not export the limit; `limit_matches_amp_protocol` below pins
/// this copy to it.
const MAX_VALUE_LENGTH: usize = 0xFFFF;

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

/// What a server has to say about failures the peer does not hear about in
/// full. Each function is an event with the `tracing` feature, and nothing
/// without it, so the callers need no `cfg`.
#[cfg(feature = "tracing")]
mod log {
    use std::borrow::Cow;
    use std::io;

    use crate::Error;

    /// The ask as a log field: the peer's own text, or `-` for a request
    /// that wanted no reply.
    fn ask(ask: Option<&[u8]>) -> Cow<'_, str> {
        ask.map_or(Cow::Borrowed("-"), String::from_utf8_lossy)
    }

    /// A command whose service returned `err`. A command error is the
    /// handler's own answer to the peer, so it is only of interest when
    /// debugging; anything else is a failure the peer was told nothing
    /// about, or told only that its request was malformed.
    pub(super) fn command_failed(command: &[u8], ask: Option<&[u8]>, err: &Error) {
        let command = String::from_utf8_lossy(command);
        let ask = self::ask(ask);
        match err {
            Error::Command { .. } => {
                tracing::debug!(%command, %ask, error = %err, "command failed")
            }
            _ => tracing::warn!(%command, %ask, error = %err, "command failed"),
        }
    }

    /// A command whose handler panicked, or was cancelled, with `reason`.
    pub(super) fn command_panicked(command: &[u8], ask: Option<&[u8]>, reason: &str) {
        let command = String::from_utf8_lossy(command);
        let ask = self::ask(ask);
        tracing::error!(%command, %ask, "{reason}");
    }

    /// An error from `accept` that the accept loop carries on past.
    pub(super) fn accept_skipped(err: &io::Error) {
        tracing::warn!(error = %err, "could not accept a connection");
    }

    /// An error that ended one of `serve`'s connections.
    pub(super) fn connection_failed(err: &Error) {
        tracing::warn!(error = %err, "connection failed");
    }
}

#[cfg(not(feature = "tracing"))]
mod log {
    use std::io;

    use crate::Error;

    pub(super) fn command_failed(_: &[u8], _: Option<&[u8]>, _: &Error) {}
    pub(super) fn command_panicked(_: &[u8], _: Option<&[u8]>, _: &str) {}
    pub(super) fn accept_skipped(_: &io::Error) {}
    pub(super) fn connection_failed(_: &Error) {}
}

/// What the `tracing` feature logs. This lives with the unit tests rather
/// than in `tests/`, since it needs a process of its own: tracing caches
/// each callsite's interest the first time it is hit, from whichever
/// thread hits it, and a test in another thread with no subscriber would
/// cache "never" for the callsites this test wants to see.
#[cfg(all(test, feature = "tracing"))]
mod log_test {
    use std::io::Write;
    use std::sync::Mutex;

    use tower::service_fn;
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::util::SubscriberInitExt;

    use super::*;
    use crate::Client;

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
        assert!(lines[0].contains("command=Boom"), "{lines:#?}");
        assert!(
            lines[0].contains("command panicked: handler bug"),
            "{lines:#?}"
        );
        assert!(lines[1].contains("WARN"), "{lines:#?}");
        assert!(lines[1].contains("command=Flaky"), "{lines:#?}");
        assert!(lines[1].contains("10.0.0.7:5432"), "{lines:#?}");
        // A handler's own error is the peer's business, so it is only of
        // interest when debugging.
        assert!(lines[2].contains("DEBUG"), "{lines:#?}");
        assert!(lines[2].contains("command=Divide"), "{lines:#?}");
        assert!(lines[2].contains("ZERO_DIVISION"), "{lines:#?}");
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
        assert!(lines[0].contains("command=Flaky"), "{lines:#?}");
        assert!(lines[0].contains("ask=-"), "{lines:#?}");
    }
}

#[cfg(test)]
mod test {
    use super::*;

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

    #[test]
    fn limit_matches_amp_protocol() {
        let mut amp_box = AmpBox::new();
        amp_box.insert("k", vec![0; MAX_VALUE_LENGTH]).unwrap();
        assert!(amp_box.insert("k", vec![0; MAX_VALUE_LENGTH + 1]).is_err());
    }

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
