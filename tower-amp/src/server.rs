use amp_protocol::codec::AmpCodec;
use amp_protocol::{AmpBox, Answer, ErrorResponse, Message, Request};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tower::{Service, ServiceExt};

use crate::Error;

/// Accepts connections on `listener` forever, serving each one with a clone
/// of `service` on its own task.
///
/// Returns only when accepting a connection fails. An error on one
/// connection ends that connection and is discarded; call
/// [`serve_connection`] from your own accept loop to observe those errors.
pub async fn serve<S>(listener: TcpListener, service: S) -> Result<(), Error>
where
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    loop {
        let (stream, _) = listener.accept().await?;
        let service = service.clone();
        tokio::spawn(async move {
            let _ = serve_connection(stream, service).await;
        });
    }
}

/// Serves one connection until the peer closes it.
///
/// Every request is handed to a clone of `service` on its own task, so the
/// requests on a connection run concurrently and are answered in whatever
/// order they finish, as the protocol allows. A request without `_ask` gets
/// no reply. A failed request is answered with the error's code and
/// description, or with `UNKNOWN` for an error that has no code. Answers and
/// errors the peer sends, which would belong to commands this side had
/// called, are ignored.
///
/// Returns once the peer closes the connection and every request already
/// received has been answered, or with the error that ended the connection.
pub async fn serve_connection<T, S>(io: T, service: S) -> Result<(), Error>
where
    T: AsyncRead + AsyncWrite,
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    let (mut sink, mut stream) = Framed::new(io, AmpCodec::new()).split();
    let (responses, mut pending) = mpsc::channel::<AmpBox>(16);

    // Reading and writing are separate futures so that a write the peer is
    // slow to accept never stops this side from reading. If both sides
    // stopped reading while blocked on a write, neither would ever drain the
    // other's buffer.
    let read = async move {
        while let Some(incoming) = stream.next().await {
            match Message::from_box(incoming?)? {
                Message::Request(request) => {
                    tokio::spawn(respond(service.clone(), request, responses.clone()));
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
/// request wants one.
async fn respond<S>(mut service: S, request: Request, responses: mpsc::Sender<AmpBox>)
where
    S: Service<Request, Response = AmpBox>,
    S::Error: Into<Error>,
{
    let ask = request.ask.clone();
    let result = async {
        let service = service.ready().await.map_err(Into::into)?;
        service.call(request).await.map_err(Into::into)
    }
    .await;
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

/// Builds the error box for `ask`. Falls back to an empty description if the
/// error's description is too long for the wire.
fn error_response(ask: Vec<u8>, err: &Error) -> AmpBox {
    ErrorResponse::new(ask.clone(), err.code(), err.description())
        .into_box()
        .or_else(|_| ErrorResponse::new(ask, err.code(), "").into_box())
        .expect("an ask that came off the wire fits back on it")
}
