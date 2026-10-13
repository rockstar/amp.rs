use std::io;
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
use tower::{Service, ServiceExt};

use crate::Error;

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
/// ends `serve` with that error. An error on one connection ends that
/// connection and is discarded; call [`serve_connection`] from your own
/// accept loop to observe those errors.
///
/// Each connection uses a default [`AmpCodec`], which refuses a box longer
/// than 16 MiB; [`serve_with_codec`] takes another.
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
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => match retry_after(&err) {
                Some(delay) => {
                    tokio::time::sleep(delay).await;
                    continue;
                }
                None => return Err(err.into()),
            },
        };
        let service = service.clone();
        tokio::spawn(async move {
            let _ = serve_connection_with_codec(stream, service, codec).await;
        });
    }
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
/// A request without `_ask` gets no reply. A failed request is answered with the error's code and
/// description, or with `UNKNOWN` for an error that has no code. A request
/// whose handler panics is answered with `UNKNOWN` and the panic's message,
/// and the connection carries on. Answers and errors the peer sends, which
/// would belong to commands this side had called, are ignored.
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
    let (mut sink, mut stream) = Framed::new(io, codec).split();
    let (responses, mut pending) = mpsc::channel::<AmpBox>(16);
    let in_flight = Arc::new(Semaphore::new(MAX_IN_FLIGHT));

    // Reading and writing are separate futures so that a write the peer is
    // slow to accept never stops this side from reading. If both sides
    // stopped reading while blocked on a write, neither would ever drain the
    // other's buffer.
    let read = async move {
        while let Some(incoming) = stream.next().await {
            match Message::from_box(incoming?)? {
                Message::Request(request) => {
                    // Waiting here, rather than spawning without bound, is
                    // what holds the peer to `MAX_IN_FLIGHT`. Replies are
                    // written by `write` below, which does not depend on
                    // this loop, so waiting cannot deadlock.
                    let permit = Arc::clone(&in_flight)
                        .acquire_owned()
                        .await
                        .expect("the semaphore is never closed");
                    tokio::spawn(respond(service.clone(), request, responses.clone(), permit));
                }
                Message::Answer(_) | Message::Error(_) => {}
            }
        }
        // The peer is done sending. Dropping this sender lets the writer
        // finish once every request already received has been answered.
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
    let ask = request.ask.clone();
    // The service runs on a task of its own so that a handler that panics
    // takes down only that task. This one survives to answer the request,
    // which would otherwise wait on the peer forever.
    let call = tokio::spawn(async move {
        let service = service.ready().await.map_err(Into::into)?;
        service.call(request).await.map_err(Into::into)
    });
    let result = match call.await {
        Ok(result) => result,
        Err(err) => Err(panicked(err)),
    };
    let Some(ask) = ask else {
        return;
    };
    let response = match result {
        Ok(results) => match Answer::new(ask.clone(), results).into_box() {
            Ok(response) => response,
            Err(err) => error_response(ask, &Error::Protocol(err)),
        },
        Err(err) => error_response(ask, &err),
    };
    let _ = responses.send(response).await;
}

/// The error a handler that did not finish is answered with. A panic's
/// message, when it has one, goes in the description, since the peer has no
/// other way to learn what went wrong.
fn panicked(err: JoinError) -> Error {
    let description = match err.try_into_panic() {
        Ok(payload) => match payload.downcast_ref::<&str>() {
            Some(message) => format!("command panicked: {message}"),
            None => match payload.downcast_ref::<String>() {
                Some(message) => format!("command panicked: {message}"),
                None => "command panicked".to_string(),
            },
        },
        Err(_) => "command was cancelled".to_string(),
    };
    Error::new(ErrorResponse::UNKNOWN, description)
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
