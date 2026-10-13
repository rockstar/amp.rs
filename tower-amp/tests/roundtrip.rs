//! A client and a served connection joined by an in-memory pipe.

// There is no server on WebAssembly; `client.rs` covers the client there.
#![cfg(not(target_family = "wasm"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use amp_protocol::{AmpBox, Request};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tower::{Service, ServiceBuilder};
use tower_amp::{AmpCodec, Client, Error, Handle, HandleWith, Router, command};

#[derive(Serialize, Deserialize)]
#[command(response = Total)]
struct Sum {
    a: i64,
    b: i64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Total {
    total: i64,
}

impl Handle for Sum {
    async fn handle(self) -> Result<Total, Error> {
        Ok(Total {
            total: self.a + self.b,
        })
    }
}

// The attribute works on either side of the derive.
#[command(response = Quotient)]
#[derive(Serialize, Deserialize)]
struct Divide {
    numerator: i64,
    denominator: i64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Quotient {
    result: f64,
}

impl Handle for Divide {
    async fn handle(self) -> Result<Quotient, Error> {
        if self.denominator == 0 {
            return Err(Error::new("ZERO_DIVISION", "cannot divide by zero"));
        }
        Ok(Quotient {
            result: self.numerator as f64 / self.denominator as f64,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[command(response = Slept)]
struct Sleep {
    millis: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Slept {
    millis: u64,
}

impl Handle for Sleep {
    async fn handle(self) -> Result<Slept, Error> {
        tokio::time::sleep(Duration::from_millis(self.millis)).await;
        Ok(Slept {
            millis: self.millis,
        })
    }
}

#[derive(Clone, Default)]
struct Counter(Arc<AtomicU64>);

/// No response, so the empty box.
#[derive(Serialize, Deserialize)]
#[command]
struct Bump {}

impl HandleWith<Counter> for Bump {
    async fn handle(self, counter: Counter) -> Result<(), Error> {
        counter.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[command(response = Count)]
struct Read {}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Count {
    count: u64,
}

impl HandleWith<Counter> for Read {
    async fn handle(self, counter: Counter) -> Result<Count, Error> {
        Ok(Count {
            count: counter.0.load(Ordering::SeqCst),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[command(response = Echoed)]
struct Echo {
    payload: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Echoed {
    payload: String,
}

impl Handle for Echo {
    async fn handle(self) -> Result<Echoed, Error> {
        Ok(Echoed {
            payload: self.payload,
        })
    }
}

/// `Sum` as a client that expects a key the server never sends.
#[derive(Serialize)]
#[command(name = "Sum", response = Misread)]
struct MisreadSum {
    a: i64,
    b: i64,
}

#[derive(Deserialize, Debug)]
struct Misread {
    /// Never read, since the results never parse.
    #[allow(dead_code)]
    sum: i64,
}

/// Arguments with no AMP representation.
#[derive(Serialize)]
#[command(response = Total)]
struct NestedSum {
    inner: Total,
}

/// A command nothing handles.
#[derive(Serialize, Deserialize)]
#[command(response = Total)]
struct Nope {}

/// Claims to be `Sum` on the wire but carries the wrong arguments.
#[derive(Serialize, Deserialize)]
#[command(name = "Sum", response = Total)]
struct BadSum {}

/// Fails with an error code longer than a box value may be.
#[derive(Serialize, Deserialize)]
#[command]
struct LongCode {}

impl Handle for LongCode {
    async fn handle(self) -> Result<(), Error> {
        Err(Error::new("X".repeat(70_000), "too long to send"))
    }
}

/// Fails with an error description longer than a box value may be.
#[derive(Serialize, Deserialize)]
#[command]
struct LongDescription {}

impl Handle for LongDescription {
    async fn handle(self) -> Result<(), Error> {
        Err(Error::new("LONG", "y".repeat(70_000)))
    }
}

/// Panics instead of answering.
#[derive(Serialize, Deserialize)]
#[command]
struct Boom {}

impl Handle for Boom {
    async fn handle(self) -> Result<(), Error> {
        panic!("handler bug")
    }
}

fn router() -> Router {
    Router::new()
        .command::<Sum>()
        .command::<Divide>()
        .command::<Sleep>()
        .command::<Echo>()
}

/// Serves `service` on one end of a pipe and returns a client on the other.
fn connect<S>(service: S) -> Client
where
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    let (client_io, server_io) = tokio::io::duplex(4096);
    tokio::spawn(tower_amp::serve_connection(server_io, service));
    Client::new(client_io)
}

/// Whether `result` failed because the peer closed the connection: cleanly,
/// or with an I/O error from writing to it after it closed, whichever the
/// client noticed first.
fn peer_closed<T>(result: &Result<T, Error>) -> bool {
    matches!(
        result,
        Err(Error::ConnectionClosed { cause })
            if cause.as_deref().is_none_or(|cause| matches!(cause, Error::Io(_)))
    )
}

#[tokio::test]
async fn calls_a_command() {
    let client = connect(router());
    let total = client.call(&Sum { a: 13, b: 81 }).await.unwrap();
    assert_eq!(total, Total { total: 94 });
}

#[tokio::test]
async fn receives_the_handlers_error() {
    let client = connect(router());
    let failed = client
        .call(&Divide {
            numerator: 1,
            denominator: 0,
        })
        .await;
    assert!(matches!(
        failed,
        Err(Error::Command { code, description })
            if code == "ZERO_DIVISION" && description == "cannot divide by zero"
    ));
}

#[tokio::test]
async fn unknown_commands_are_unhandled() {
    let client = connect(router());
    let failed = client.call(&Nope {}).await;
    assert!(matches!(
        failed,
        Err(Error::Command { code, description })
            if code == "UNHANDLED" && description == "unhandled command: Nope"
    ));
}

#[tokio::test]
async fn bad_arguments_are_unknown_errors() {
    let client = connect(router());
    let failed = client.call(&BadSum {}).await;
    assert!(matches!(
        failed,
        Err(Error::Command { code, description })
            if code == "UNKNOWN"
                && description.starts_with("could not deserialize: ")
                && description.contains("missing field")
    ));
}

#[tokio::test]
async fn requests_on_one_connection_run_concurrently() {
    let client = connect(router());
    let started = std::time::Instant::now();
    let (slow, fast) = tokio::join!(
        client.call(&Sleep { millis: 200 }),
        client.call(&Sleep { millis: 10 }),
    );
    assert_eq!(slow.unwrap().millis, 200);
    assert_eq!(fast.unwrap().millis, 10);
    assert!(started.elapsed() < Duration::from_millis(400));
}

#[tokio::test]
async fn commands_share_state_and_stateless_commands_join_them() {
    let router = Router::with_state(Counter::default())
        .command::<Bump>()
        .command::<Read>()
        .command::<Sum>();
    let client = connect(router);
    client.notify(&Bump {}).await.unwrap();
    client.notify(&Bump {}).await.unwrap();
    client.call(&Bump {}).await.unwrap();
    let count = client.call(&Read {}).await.unwrap();
    assert_eq!(count, Count { count: 3 });
    let total = client.call(&Sum { a: 2, b: 2 }).await.unwrap();
    assert_eq!(total.total, 4);
}

#[tokio::test]
async fn middleware_errors_reach_the_peer_as_unknown() {
    let service = ServiceBuilder::new()
        .timeout(Duration::from_millis(20))
        .service(router());
    let client = connect(service);
    let failed = client.call(&Sleep { millis: 500 }).await;
    assert!(matches!(
        failed,
        Err(Error::Command { code, .. }) if code == "UNKNOWN"
    ));
    let total = client.call(&Sum { a: 1, b: 2 }).await.unwrap();
    assert_eq!(total.total, 3);
}

#[tokio::test]
async fn client_is_a_service() {
    let mut client = connect(router());
    let mut arguments = AmpBox::new();
    arguments.insert("a", "1").unwrap();
    arguments.insert("b", "2").unwrap();
    // The typed `Client::call` shadows `Service::call`, so the untyped form
    // is reached through the trait.
    let request = Request::new("Sum", arguments).with_ask("x");
    let results = Service::call(&mut client, request).await.unwrap();
    assert_eq!(results.get("total"), Some(&b"3"[..]));

    let request = Request::new("Sum", AmpBox::new());
    let results = Service::call(&mut client, request).await.unwrap();
    assert!(
        results.is_empty(),
        "a request without _ask resolves at once"
    );
}

#[tokio::test]
async fn the_client_assigns_every_ask() {
    let client = connect(router());
    // A fresh client gives its first typed call the ask "1". A request sent
    // through the service with that same ask must not evict it.
    let slow = client.call(&Sleep { millis: 50 });
    let mut arguments = AmpBox::new();
    arguments.insert("a", "1").unwrap();
    arguments.insert("b", "2").unwrap();
    let request = Request::new("Sum", arguments).with_ask("1");
    let fast = Service::call(&mut client.clone(), request);
    let (slow, fast) = tokio::join!(slow, fast);
    slow.unwrap();
    assert_eq!(fast.unwrap().get("total"), Some(&b"3"[..]));
}

#[tokio::test]
async fn a_closed_connection_fails_calls() {
    let (client_io, server_io) = tokio::io::duplex(64);
    drop(server_io);
    let client = Client::new(client_io);
    let failed = client.call(&Sum { a: 1, b: 2 }).await;
    assert!(peer_closed(&failed), "{failed:?}");
}

#[tokio::test]
async fn server_answers_in_flight_requests_after_the_client_stops_sending() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let server = tokio::spawn(tower_amp::serve_connection(server_io, router()));
    let client = Client::new(client_io);
    let slept = client.call(&Sleep { millis: 50 }).await.unwrap();
    assert_eq!(slept.millis, 50);
    drop(client);
    server.await.unwrap().unwrap();
}

/// Both sides must keep reading while they are blocked on a write. Thirty-two
/// 8 KB calls in flight at once saturate the 4 KB pipe in both directions, so
/// a connection loop that stops reading while it writes hangs here.
#[tokio::test]
async fn pipelined_large_calls_do_not_deadlock() {
    let client = connect(router());
    let payload = "x".repeat(8_000);
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let client = client.clone();
        let payload = payload.clone();
        set.spawn(async move { client.call(&Echo { payload }).await.unwrap() });
    }
    let all = async {
        while let Some(echoed) = set.join_next().await {
            assert_eq!(echoed.unwrap().payload.len(), 8_000);
        }
    };
    tokio::time::timeout(Duration::from_secs(5), all)
        .await
        .expect("deadlocked: neither side is reading while blocked on write");
}

/// An error code too long for the wire must still be answered, cut down to
/// what fits, rather than panicking the task that would have answered it.
#[tokio::test]
async fn over_long_error_code_still_gets_an_answer() {
    let client = connect(Router::new().command::<LongCode>().command::<Sum>());
    let failed = tokio::time::timeout(Duration::from_secs(2), client.call(&LongCode {}))
        .await
        .expect("no answer: the respond task panicked");
    assert!(matches!(
        failed,
        Err(Error::Command { code, description })
            if code.len() == 65_535
                && code.bytes().all(|b| b == b'X')
                && description == "too long to send"
    ));
    // The connection must survive.
    let total = client.call(&Sum { a: 1, b: 2 }).await.unwrap();
    assert_eq!(total.total, 3);
}

/// An over-long description keeps as much as fits instead of being dropped.
#[tokio::test]
async fn over_long_error_description_is_truncated() {
    let client = connect(Router::new().command::<LongDescription>());
    let failed = client.call(&LongDescription {}).await;
    assert!(matches!(
        failed,
        Err(Error::Command { code, description })
            if code == "LONG"
                && description.len() == 65_535
                && description.bytes().all(|b| b == b'y')
    ));
}

/// A handler that panics must still be answered, as `UNKNOWN`, and must not
/// take the connection down with it.
#[tokio::test]
async fn a_panicking_handler_is_answered_with_unknown() {
    let client = connect(Router::new().command::<Boom>().command::<Sum>());
    let failed = tokio::time::timeout(Duration::from_secs(2), client.call(&Boom {}))
        .await
        .expect("no answer: the panic was swallowed");
    assert!(matches!(
        failed,
        Err(Error::Command { code, description })
            if code == "UNKNOWN" && description.contains("handler bug")
    ));
    // The connection must survive.
    let total = client.call(&Sum { a: 1, b: 2 }).await.unwrap();
    assert_eq!(total.total, 3);
}

/// A peer that sends a box longer than the server allows gets its connection
/// closed with the reason, rather than the server buffering it forever.
#[tokio::test]
async fn server_rejects_a_box_over_its_frame_limit() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let codec = AmpCodec::with_max_frame_length(1024);
    let server = tokio::spawn(tower_amp::serve_connection_with_codec(
        server_io,
        router(),
        codec,
    ));
    let client = Client::new(client_io);

    let small = client
        .call(&Echo {
            payload: "ok".into(),
        })
        .await
        .unwrap();
    assert_eq!(small.payload, "ok");

    let payload = "x".repeat(8_000);
    let failed = tokio::time::timeout(Duration::from_secs(2), client.call(&Echo { payload }))
        .await
        .expect("the server buffered the oversized box instead of rejecting it");
    assert!(peer_closed(&failed), "{failed:?}");
    assert!(matches!(
        server.await.unwrap(),
        Err(Error::Protocol(amp_protocol::Error::FrameTooLong {
            limit: 1024
        }))
    ));
}

/// The client holds the peer to its limit too: an answer that is too long
/// ends the connection and fails the call instead of waiting on it.
#[tokio::test]
async fn client_rejects_an_answer_over_its_frame_limit() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    tokio::spawn(tower_amp::serve_connection(server_io, router()));
    let client = Client::with_codec(client_io, AmpCodec::with_max_frame_length(1024));

    let payload = "x".repeat(8_000);
    let failed = tokio::time::timeout(Duration::from_secs(2), client.call(&Echo { payload }))
        .await
        .expect("the client buffered the oversized answer instead of rejecting it");
    assert!(
        matches!(
            &failed,
            Err(Error::ConnectionClosed { cause: Some(cause) })
                if matches!(
                    **cause,
                    Error::Protocol(amp_protocol::Error::FrameTooLong { limit: 1024 })
                )
        ),
        "{failed:?}"
    );
}

/// A peer that breaks the protocol ends the connection, and every call on
/// it, whether waiting at the time or made afterwards, reports what the peer
/// did rather than only that the connection closed.
#[tokio::test]
async fn a_malformed_box_fails_calls_with_the_cause() {
    let (client_io, mut peer) = tokio::io::duplex(4096);
    let client = Client::new(client_io);
    // A key length of 256, longer than a key may be.
    peer.write_all(b"\x01\x00").await.unwrap();

    let failed = client.call(&Sum { a: 1, b: 2 }).await;
    assert_key_too_long(&failed);
    let later = client.call(&Sum { a: 3, b: 4 }).await;
    assert_key_too_long(&later);
    let err = later.unwrap_err();
    assert_eq!(
        err.to_string(),
        "connection closed: protocol error: key is 256 bytes, longer than the limit of 255"
    );
    assert!(std::error::Error::source(&err).is_some());
}

fn assert_key_too_long<T: std::fmt::Debug>(result: &Result<T, Error>) {
    assert!(
        matches!(
            result,
            Err(Error::ConnectionClosed { cause: Some(cause) })
                if matches!(
                    **cause,
                    Error::Protocol(amp_protocol::Error::KeyTooLong { len: 256 })
                )
        ),
        "{result:?}"
    );
}

/// A call the peer is too slow to answer fails with `Timeout`, and the
/// connection carries on: the answer that arrives later is discarded rather
/// than confusing a later call.
#[tokio::test]
async fn a_slow_answer_times_out_and_is_later_discarded() {
    let client = connect(router()).with_timeout(Duration::from_millis(20));
    let failed = client.call(&Sleep { millis: 100 }).await;
    assert!(matches!(failed, Err(Error::Timeout)), "{failed:?}");
    assert_eq!(failed.unwrap_err().to_string(), "timed out");

    let total = client.call(&Sum { a: 1, b: 2 }).await.unwrap();
    assert_eq!(total.total, 3);
    // Let the late answer arrive, then make sure nothing is the worse for it.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let total = client.call(&Sum { a: 3, b: 4 }).await.unwrap();
    assert_eq!(total.total, 7);
}

/// The limit belongs to the handle, so clones of one connection can have
/// different limits, including none.
#[tokio::test]
async fn clones_take_their_own_timeout() {
    let patient = connect(router());
    let hasty = patient.clone().with_timeout(Duration::from_millis(20));
    let (slow, failed) = tokio::join!(
        patient.call(&Sleep { millis: 100 }),
        hasty.call(&Sleep { millis: 100 }),
    );
    assert_eq!(slow.unwrap().millis, 100);
    assert!(matches!(failed, Err(Error::Timeout)), "{failed:?}");
}

#[tokio::test]
async fn the_service_form_honors_the_timeout() {
    let mut client = connect(router()).with_timeout(Duration::from_millis(20));
    let mut arguments = AmpBox::new();
    arguments.insert("millis", "100").unwrap();
    let request = Request::new("Sleep", arguments).with_ask("x");
    let failed = Service::call(&mut client, request).await;
    assert!(matches!(failed, Err(Error::Timeout)), "{failed:?}");
}

/// A request whose caller gave up while it was still queued behind a slow
/// write is never sent. The pipe holds less than one request and the peer
/// does not read until both calls have timed out, so the first request is
/// stuck mid-write and the second is still queued when they time out.
#[tokio::test]
async fn an_abandoned_request_is_not_written() {
    let (client_io, peer) = tokio::io::duplex(16);
    let client = Client::new(client_io).with_timeout(Duration::from_millis(20));
    let first = Echo {
        payload: "first".into(),
    };
    let second = Echo {
        payload: "second".into(),
    };
    let (first, second) = tokio::join!(client.call(&first), client.call(&second));
    assert!(matches!(first, Err(Error::Timeout)), "{first:?}");
    assert!(matches!(second, Err(Error::Timeout)), "{second:?}");
    // Closing the client ends the connection once the writer is done.
    drop(client);

    let mut peer = tokio_util::codec::Framed::new(peer, AmpCodec::new());
    let mut written = Vec::new();
    while let Some(amp_box) = peer.next().await {
        written.push(amp_box.unwrap());
    }
    assert_eq!(written.len(), 1, "{written:?}");
    assert_eq!(written[0].get("payload"), Some(&b"first"[..]));
}

#[tokio::test]
async fn results_that_do_not_match_fail_the_client_with_deserialize() {
    let client = connect(router());
    let failed = client.call(&MisreadSum { a: 1, b: 2 }).await;
    assert!(matches!(failed, Err(Error::Deserialize(_))), "{failed:?}");
    let err = failed.unwrap_err();
    assert!(std::error::Error::source(&err).is_some());
    assert!(
        err.to_string().starts_with("could not deserialize: "),
        "{err}"
    );
}

#[tokio::test]
async fn arguments_that_cannot_become_a_box_fail_the_client_with_serialize() {
    // No peer at all: the failure is local and nothing is sent.
    let (client_io, _peer) = tokio::io::duplex(64);
    let client = Client::new(client_io);
    let failed = client
        .call(&NestedSum {
            inner: Total { total: 1 },
        })
        .await;
    assert!(matches!(failed, Err(Error::Serialize(_))), "{failed:?}");
    let failed = client
        .notify(&NestedSum {
            inner: Total { total: 1 },
        })
        .await;
    assert!(matches!(failed, Err(Error::Serialize(_))), "{failed:?}");
}
