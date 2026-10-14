use std::io;
use std::pin::pin;
use std::time::Duration;

use amp_protocol::codec::AmpCodec;
use amp_protocol::{AmpBox, Request};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower::Service;

use crate::connection::Owner;
use crate::{Connection, Error, log};

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
/// `TCP_NODELAY` is set on every connection accepted, so that the replies
/// to pipelined calls go out as they are ready rather than each waiting on
/// the acknowledgement of the one before it.
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
                // Replies are small and written one at a time, and a peer
                // that pipelines calls gets several back to back. With
                // Nagle's algorithm on, each would wait for the ACK of the
                // one before it, up to the peer's delayed-ACK timer. This
                // only fails on a socket already dead, which the connection
                // task finds out for itself.
                let _ = stream.set_nodelay(true);
                let service = service.clone();
                let codec = codec.clone();
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
/// belong to commands this side had called, are ignored: this side calls
/// none. A [`Connection`] serves the peer and calls commands on it.
///
/// `io` need not be a socket, so nothing is done to its options. An accept
/// loop of your own should set `TCP_NODELAY` on each `TcpStream`, as
/// [`serve`] does, so that replies written back to back are not each held
/// for the peer's acknowledgement of the one before.
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
    Connection::with_codec(io, codec)
        .run(service, shutdown, Owner::Driver)
        .await
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
}
