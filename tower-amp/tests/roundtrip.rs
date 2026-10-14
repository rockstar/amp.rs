//! A client and a served connection joined by an in-memory pipe.

// There is no server on WebAssembly; `client.rs` covers the client there.
#![cfg(not(target_family = "wasm"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use amp_protocol::{AmpBox, Request};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tower::{Service, ServiceBuilder};
use tower_amp::{AmpCodec, Client, Connection, Error, Handle, HandleWith, Router, Server, command};

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

/// Counts itself in and never answers.
#[derive(Serialize, Deserialize)]
#[command]
struct Hang {}

impl HandleWith<Counter> for Hang {
    async fn handle(self, counter: Counter) -> Result<(), Error> {
        counter.0.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
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

/// Fails with an error about this side's own trouble, of the kind that
/// names things a peer has no business learning.
#[derive(Serialize, Deserialize)]
#[command]
struct Flaky {}

impl Handle for Flaky {
    async fn handle(self) -> Result<(), Error> {
        Err(Error::Io(std::io::Error::other(
            "db at 10.0.0.7:5432 refused the connection",
        )))
    }
}

/// Answers with a result whose key is reserved for the protocol, which no
/// handler should do, but which the server must survive.
#[derive(Serialize, Deserialize)]
#[command(response = Reserved)]
struct Hijack {}

#[derive(Serialize, Deserialize, Debug)]
struct Reserved {
    #[serde(rename = "_ask")]
    ask: String,
}

impl Handle for Hijack {
    async fn handle(self) -> Result<Reserved, Error> {
        Ok(Reserved {
            ask: "stolen".into(),
        })
    }
}

/// Another command that claims the name `Sum` on the wire.
#[derive(Serialize, Deserialize)]
#[command(name = "Sum", response = Total)]
struct SecondSum {}

impl Handle for SecondSum {
    async fn handle(self) -> Result<Total, Error> {
        Ok(Total { total: 0 })
    }
}

/// A command whose handler reports that it has started and then waits to
/// be let go, so a test can hold a request in flight for as long as it
/// needs without guessing at timing.
#[derive(Serialize, Deserialize)]
#[command]
struct Hold {}

/// Both sides of `Hold`. A `Notify` keeps a permit when nobody is waiting,
/// so neither side has to get there first.
#[derive(Clone, Default)]
struct Gate {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl HandleWith<Gate> for Hold {
    async fn handle(self, gate: Gate) -> Result<(), Error> {
        gate.started.notify_one();
        gate.release.notified().await;
        Ok(())
    }
}

/// Answered by asking the peer that sent it to do the sum: a command that
/// goes the other way on the same connection, inside a handler.
#[derive(Serialize, Deserialize)]
#[command(response = Total)]
struct Relay {
    a: i64,
    b: i64,
}

impl HandleWith<Client> for Relay {
    async fn handle(self, peer: Client) -> Result<Total, Error> {
        peer.call(&Sum {
            a: self.a,
            b: self.b,
        })
        .await
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

/// The served side has no `Sum` of its own: the only way to answer `Relay`
/// is to call `Sum` on the peer that asked, which serves it, while that
/// peer's own call is still waiting.
#[tokio::test]
async fn a_handler_calls_a_command_on_the_peer_that_asked() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let served = Connection::new(server_io);
    let peer = served.client();
    tokio::spawn(served.serve(Router::with_state(peer).command::<Relay>()));
    let calling = Connection::new(client_io);
    let client = calling.client();
    tokio::spawn(calling.serve(Router::new().command::<Sum>()));

    let total = client.call(&Relay { a: 13, b: 81 }).await.unwrap();
    assert_eq!(total, Total { total: 94 });
}

/// A served connection is kept open by whoever drives it, not by its
/// handles: it goes on serving after the last one is dropped, and ends
/// when the peer closes it.
#[tokio::test]
async fn a_served_connection_outlives_its_handles() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let connection = Connection::new(server_io);
    drop(connection.client());
    let server = tokio::spawn(connection.serve(router()));

    let client = Client::new(client_io);
    let total = client.call(&Sum { a: 1, b: 2 }).await.unwrap();
    assert_eq!(total.total, 3);
    drop(client);
    server.await.unwrap().unwrap();
}

/// Whoever drives a served connection gets the error that ended it, and a
/// handle that outlives it gets the same error as the cause, both for a
/// call that was waiting and for one made afterwards.
#[tokio::test]
async fn a_handle_outlives_the_connection_with_its_cause() {
    let (mut client_io, server_io) = tokio::io::duplex(4096);
    let connection = Connection::new(server_io);
    let peer = connection.client();
    let server = tokio::spawn(connection.serve(router()));
    let waiting = tokio::spawn({
        let peer = peer.clone();
        async move { peer.call(&Sum { a: 1, b: 2 }).await }
    });

    // A key length of 256, longer than a key may be.
    client_io.write_all(b"\x01\x00").await.unwrap();
    let failed = server.await.unwrap();
    assert!(
        matches!(
            failed,
            Err(Error::Protocol(amp_protocol::Error::KeyTooLong {
                len: 256
            }))
        ),
        "{failed:?}"
    );
    assert_key_too_long(&waiting.await.unwrap());
    assert_key_too_long(&peer.call(&Sum { a: 3, b: 4 }).await);
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

#[tokio::test(start_paused = true)]
async fn requests_on_one_connection_run_concurrently() {
    let client = connect(router());
    let started = tokio::time::Instant::now();
    let (slow, fast) = tokio::join!(
        client.call(&Sleep { millis: 200 }),
        client.call(&Sleep { millis: 10 }),
    );
    assert_eq!(slow.unwrap().millis, 200);
    assert_eq!(fast.unwrap().millis, 10);
    // The clock is paused, so this is exactly the longer sleep rather than
    // the sum of both: the two requests overlapped.
    assert_eq!(started.elapsed(), Duration::from_millis(200));
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

#[tokio::test(start_paused = true)]
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

/// A handler's own error must come back out of the boxed error type that
/// middleware wraps it in, with its code intact, rather than being read as
/// some other error and answered as `UNKNOWN`.
#[tokio::test]
async fn handler_errors_pass_through_middleware_unchanged() {
    let service = ServiceBuilder::new()
        .timeout(Duration::from_secs(5))
        .service(router());
    let client = connect(service);
    let failed = client
        .call(&Divide {
            numerator: 1,
            denominator: 0,
        })
        .await;
    assert!(
        matches!(
            failed,
            Err(Error::Command { ref code, ref description })
                if code == "ZERO_DIVISION" && description == "cannot divide by zero"
        ),
        "{failed:?}"
    );
}

/// Results that use a reserved key cannot go on the wire as an answer. The
/// peer still gets a reply, as `UNKNOWN`, and the connection carries on.
#[tokio::test]
async fn results_with_a_reserved_key_are_answered_with_unknown() {
    let client = connect(Router::new().command::<Hijack>().command::<Sum>());
    let failed = tokio::time::timeout(Duration::from_secs(2), client.call(&Hijack {}))
        .await
        .expect("no answer: the unsendable results were dropped");
    assert!(
        matches!(
            failed,
            Err(Error::Command { ref code, ref description })
                if code == "UNKNOWN" && description == "internal error"
        ),
        "{failed:?}"
    );
    let total = client.call(&Sum { a: 1, b: 2 }).await.unwrap();
    assert_eq!(total.total, 3);
}

#[test]
#[should_panic(expected = "command \"Sum\" registered twice")]
fn registering_a_command_name_twice_panics() {
    Router::new().command::<Sum>().command::<SecondSum>();
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

/// How many requests a connection served by `server` takes on from a peer
/// that sends far more than any limit and never reads. Each `Hang` handler
/// counts itself in and never finishes, so its request stays in flight for
/// good and the count is exactly how many requests the server took on.
async fn flood<F>(server: Server<Router<Counter>, F>, counter: Counter) -> u64
where
    F: Future<Output = ()> + Send + 'static,
{
    let (peer, server_io) = tokio::io::duplex(4096);
    tokio::spawn(server.serve_connection(server_io));
    // The peer's writes block once the server stops reading, so it runs on
    // its own task and keeps the pipe open by never finishing.
    tokio::spawn(async move {
        let mut peer = tokio_util::codec::Framed::new(peer, AmpCodec::new());
        for i in 0..2_000u32 {
            let request = Request::new("Hang", AmpBox::new()).with_ask(i.to_string());
            peer.send(request.into_box().unwrap()).await.unwrap();
        }
        std::future::pending::<()>().await;
    });

    // Give the server far longer than it needs to take on everything it is
    // going to, then check where it stopped.
    tokio::time::sleep(Duration::from_millis(500)).await;
    counter.0.load(Ordering::SeqCst)
}

/// A peer that sends requests faster than they are answered must not make
/// the server take on every request it sends.
#[tokio::test]
async fn a_flooding_peer_is_held_to_the_in_flight_limit() {
    let counter = Counter::default();
    let router = Router::with_state(counter.clone()).command::<Hang>();
    let handled = flood(Server::new(router), counter).await;
    assert_eq!(handled, 256, "the server took on {handled} requests");
}

/// The limit is whatever the server was given, below and above the default.
#[tokio::test]
async fn the_in_flight_limit_is_configurable() {
    for limit in [4, 1_024] {
        let counter = Counter::default();
        let router = Router::with_state(counter.clone()).command::<Hang>();
        let server = Server::new(router).max_in_flight(limit);
        let handled = flood(server, counter).await;
        assert_eq!(
            handled, limit as u64,
            "the server took on {handled} requests"
        );
    }
}

/// A connection that may never read a request would hang its peer, so a
/// limit of zero is refused where it is set rather than at the first call.
#[test]
#[should_panic(expected = "a connection must be allowed a request in flight")]
fn a_zero_in_flight_limit_is_refused() {
    let _ = Server::new(router()).max_in_flight(0);
}

/// The shutdown signal works on one connection as it does on a listener:
/// the connection stops reading, answers the request it has, and closes.
#[tokio::test]
async fn shutdown_stops_one_connection_once_its_requests_are_answered() {
    let gate = Gate::default();
    let router = Router::with_state(gate.clone())
        .command::<Hold>()
        .command::<Sum>();
    let (client_io, server_io) = tokio::io::duplex(4096);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = Server::new(router).shutdown(async {
        let _ = stopped.await;
    });
    let server = tokio::spawn(server.serve_connection(server_io));
    let client = Client::new(client_io);

    let held = tokio::spawn({
        let client = client.clone();
        async move { client.call(&Hold {}).await }
    });
    gate.started.notified().await;
    stop.send(()).unwrap();
    // The handler is still running, so the connection cannot have closed.
    tokio::task::yield_now().await;
    assert!(!server.is_finished());

    gate.release.notify_one();
    held.await.unwrap().unwrap();
    server.await.unwrap().unwrap();
    let failed = client.call(&Sum { a: 1, b: 2 }).await;
    assert!(peer_closed(&failed), "{failed:?}");
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
/// take the connection down with it. The panic's message says what the
/// handler was doing, which stays on this side.
#[tokio::test]
async fn a_panicking_handler_is_answered_with_unknown() {
    let client = connect(Router::new().command::<Boom>().command::<Sum>());
    let failed = tokio::time::timeout(Duration::from_secs(2), client.call(&Boom {}))
        .await
        .expect("no answer: the panic was swallowed");
    assert!(matches!(
        failed,
        Err(Error::Command { code, description })
            if code == "UNKNOWN" && description == "internal error"
    ));
    // The connection must survive.
    let total = client.call(&Sum { a: 1, b: 2 }).await.unwrap();
    assert_eq!(total.total, 3);
}

/// An error that is about this side rather than the request, such as an
/// I/O error, is answered as `UNKNOWN` without its message, which may name
/// hosts and other internals.
#[tokio::test]
async fn internal_errors_do_not_reach_the_peer() {
    let client = connect(Router::new().command::<Flaky>());
    let failed = client.call(&Flaky {}).await;
    assert!(matches!(
        failed,
        Err(Error::Command { code, description })
            if code == "UNKNOWN" && description == "internal error"
    ));
}

/// A peer that sends a box longer than the server allows gets its connection
/// closed with the reason, rather than the server buffering it forever.
#[tokio::test]
async fn server_rejects_a_box_over_its_frame_limit() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let codec = AmpCodec::with_max_frame_length(1024);
    let server = tokio::spawn(
        Server::new(router())
            .codec(codec)
            .serve_connection(server_io),
    );
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
#[tokio::test(start_paused = true)]
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
#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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
/// stuck mid-write when the second is made, and the second is still queued
/// when they time out. The second is made only once the writer has taken
/// the first: made together, the two would be written as one batch.
#[tokio::test(start_paused = true)]
async fn an_abandoned_request_is_not_written() {
    let (client_io, peer) = tokio::io::duplex(16);
    let client = Client::new(client_io).with_timeout(Duration::from_millis(20));
    let first = Echo {
        payload: "first".into(),
    };
    let second = Echo {
        payload: "second".into(),
    };
    let second = async {
        tokio::task::yield_now().await;
        client.call(&second).await
    };
    let (first, second) = tokio::join!(client.call(&first), second);
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

/// Binds a listener on a free port and serves `service` on it with `serve`,
/// so a test can connect to it over real TCP.
async fn listen<S>(service: S) -> std::net::SocketAddr
where
    S: Service<Request, Response = AmpBox> + Clone + Send + 'static,
    S::Error: Into<Error>,
    S::Future: Send,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(tower_amp::serve(listener, service));
    addr
}

/// A client connected over TCP to the server at `addr`.
async fn connect_tcp(addr: std::net::SocketAddr) -> Client {
    Client::new(tokio::net::TcpStream::connect(addr).await.unwrap())
}

/// `serve` accepts connections over real TCP, serves each on its own, and
/// goes on accepting after a peer connects and hangs up without a word.
#[tokio::test]
async fn serve_accepts_connections_over_tcp() {
    let addr = listen(router()).await;

    let first = connect_tcp(addr).await;
    let second = connect_tcp(addr).await;
    let total = first.call(&Sum { a: 1, b: 2 }).await.unwrap();
    assert_eq!(total.total, 3);
    let total = second.call(&Sum { a: 3, b: 4 }).await.unwrap();
    assert_eq!(total.total, 7);

    // A peer that connects and leaves at once ends its own connection, not
    // the server.
    drop(tokio::net::TcpStream::connect(addr).await.unwrap());
    let third = connect_tcp(addr).await;
    let total = third.call(&Sum { a: 5, b: 6 }).await.unwrap();
    assert_eq!(total.total, 11);
}

/// Connects to `addr` until the connection is refused, which it is once the
/// listener is closed, and fails the test if that takes longer than a few
/// seconds. A connection that is accepted in the meantime is dropped.
async fn refused_eventually(addr: std::net::SocketAddr) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            // Lets the server act on a signal just sent before the first
            // attempt. A SYN that reaches the listener as it closes gets no
            // answer, and the retry a second later would be the test's time.
            tokio::task::yield_now().await;
            if tokio::net::TcpStream::connect(addr).await.is_err() {
                break;
            }
        }
    })
    .await
    .expect("the listener was not closed");
}

/// A server with a shutdown signal stops accepting as soon as it fires,
/// still answers the request in flight, and returns once that connection
/// closes.
#[tokio::test]
async fn shutdown_answers_the_requests_in_flight() {
    let gate = Gate::default();
    let router = Router::with_state(gate.clone())
        .command::<Hold>()
        .command::<Sum>();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = Server::new(router).shutdown(async {
        let _ = stopped.await;
    });
    let server = tokio::spawn(server.serve(listener));

    let client = connect_tcp(addr).await;
    let held = tokio::spawn({
        let client = client.clone();
        async move { client.call(&Hold {}).await }
    });
    gate.started.notified().await;

    stop.send(()).unwrap();
    refused_eventually(addr).await;
    // The handler is still running, so the server cannot have returned.
    assert!(!server.is_finished());

    gate.release.notify_one();
    held.await.unwrap().unwrap();
    server.await.unwrap().unwrap();
    // The connection closed with the server, so it takes no more requests.
    let failed = client.call(&Sum { a: 1, b: 2 }).await;
    assert!(peer_closed(&failed), "{failed:?}");
}

/// A server with a codec and a shutdown signal frames its connections with
/// the codec and still stops on the signal, with nothing in flight to wait
/// for.
#[tokio::test]
async fn shutdown_with_codec_keeps_the_frame_limit() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = Server::new(router())
        .codec(AmpCodec::with_max_frame_length(1024))
        .shutdown(async {
            let _ = stopped.await;
        });
    let server = tokio::spawn(server.serve(listener));

    let client = connect_tcp(addr).await;
    let payload = "x".repeat(8_000);
    let failed = client.call(&Echo { payload }).await;
    assert!(peer_closed(&failed), "{failed:?}");

    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
    refused_eventually(addr).await;
}

/// A server with a codec frames every connection it accepts with it, and a
/// connection that breaks the limit ends only itself.
#[tokio::test]
async fn a_server_frames_every_connection_with_its_codec() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = Server::new(router()).codec(AmpCodec::with_max_frame_length(1024));
    tokio::spawn(server.serve(listener));

    let client = connect_tcp(addr).await;
    let payload = "x".repeat(8_000);
    let failed = tokio::time::timeout(Duration::from_secs(2), client.call(&Echo { payload }))
        .await
        .expect("the server buffered the oversized box instead of rejecting it");
    assert!(peer_closed(&failed), "{failed:?}");

    // The server is still accepting, and still enforcing the limit.
    let client = connect_tcp(addr).await;
    let small = client
        .call(&Echo {
            payload: "ok".into(),
        })
        .await
        .unwrap();
    assert_eq!(small.payload, "ok");
}
