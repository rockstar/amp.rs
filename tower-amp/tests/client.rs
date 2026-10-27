//! A client and a hand-rolled peer joined by an in-memory pipe, so that the
//! client is tested without the server. This is the only test of a
//! connection that runs on WebAssembly, where there is no server. On
//! wasm32-unknown-unknown there is no tokio runtime either, so there the
//! tests run under wasm-bindgen-test and wait on the host's timer.

use std::time::Duration;

use amp_protocol::{AmpBox, Message, Request};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::DuplexStream;
use tokio_util::codec::Framed;
use tower::Service;
use tower_amp::{AmpCodec, Client, Connection, Error, Handle, Router, command};

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

/// Served when the peer calls `Sum` on a [`Connection`].
impl Handle for Sum {
    async fn handle(self) -> Result<Total, Error> {
        Ok(Total {
            total: self.a + self.b,
        })
    }
}

#[derive(Serialize)]
#[command(response = Total)]
struct Fail {}

/// Never answered.
#[derive(Serialize)]
#[command(response = Total)]
struct Silence {}

/// What the peer does with a request.
fn respond(request: &Request) -> Option<AmpBox> {
    let argument = |name: &str| -> i64 {
        String::from_utf8_lossy(request.arguments.get(name).unwrap())
            .parse()
            .unwrap()
    };
    match request.command.as_slice() {
        b"Sum" => {
            let mut results = AmpBox::new();
            results
                .insert("total", (argument("a") + argument("b")).to_string())
                .unwrap();
            request.answer(results).map(|a| a.into_box().unwrap())
        }
        b"Fail" => request
            .error("NOPE", "as requested")
            .map(|e| e.into_box().unwrap()),
        _ => None,
    }
}

/// Answers requests on `io` until the client closes the connection.
async fn peer(io: DuplexStream) {
    let mut framed = Framed::new(io, AmpCodec::new());
    while let Some(Ok(amp_box)) = framed.next().await {
        let Ok(Message::Request(request)) = Message::from_box(amp_box) else {
            continue;
        };
        if let Some(reply) = respond(&request) {
            framed.send(reply).await.unwrap();
        }
    }
}

/// Runs `test` against a client whose peer is [`peer`]. The peer runs on the
/// test's own task, since WebAssembly may have no runtime to spawn it on.
async fn with_peer<F, T>(test: impl FnOnce(Client) -> F) -> T
where
    F: Future<Output = T>,
{
    let (client_io, peer_io) = tokio::io::duplex(4096);
    let client = Client::new(client_io).with_timeout(Duration::from_millis(200));
    tokio::select! {
        result = test(client) => result,
        () = peer(peer_io) => unreachable!("the peer outlives the test"),
    }
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
async fn calls_a_command() {
    with_peer(|client| async move {
        let total = client.call(&Sum { a: 40, b: 2 }).await.unwrap();
        assert_eq!(total, Total { total: 42 });
    })
    .await;
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
async fn receives_the_peers_error() {
    with_peer(|client| async move {
        match client.call(&Fail {}).await {
            Err(Error::Command { code, description }) => {
                assert_eq!(code, "NOPE");
                assert_eq!(description, "as requested");
            }
            other => panic!("{other:?}"),
        }
    })
    .await;
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
async fn an_unanswered_call_times_out() {
    with_peer(|client| async move {
        let failed = client.call(&Silence {}).await;
        assert!(matches!(failed, Err(Error::Timeout)), "{failed:?}");
        // The connection is still good.
        let total = client.call(&Sum { a: 1, b: 2 }).await.unwrap();
        assert_eq!(total, Total { total: 3 });
    })
    .await;
}

/// A client whose peer is the test itself, framed, so that a test can read
/// exactly what the client wrote and write anything back.
fn with_framed_peer() -> (Client, Framed<DuplexStream, AmpCodec>) {
    let (client_io, peer_io) = tokio::io::duplex(4096);
    let client = Client::new(client_io).with_timeout(Duration::from_millis(200));
    (client, Framed::new(peer_io, AmpCodec::new()))
}

/// Waits for `future` for at most `limit`; `None` if the limit was reached
/// first. The timer is tokio's, or on wasm32-unknown-unknown the host's,
/// since there is no tokio runtime to drive one there.
async fn timeout<F: Future>(limit: Duration, future: F) -> Option<F::Output> {
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    let sleep = tokio::time::sleep(limit);
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    let sleep = gloo_timers::future::sleep(limit);
    tokio::select! {
        output = future => Some(output),
        () = sleep => None,
    }
}

/// The next box the client writes. Fails rather than waits forever if the
/// client writes nothing, so a broken branch fails the test instead of
/// hanging it.
async fn next_box(peer: &mut Framed<DuplexStream, AmpCodec>) -> AmpBox {
    timeout(Duration::from_secs(2), peer.next())
        .await
        .expect("the client wrote nothing")
        .expect("the client closed the connection")
        .unwrap()
}

/// Everything the client writes before it closes the connection.
async fn drain(peer: &mut Framed<DuplexStream, AmpCodec>) -> Vec<AmpBox> {
    let mut written = Vec::new();
    let all = async {
        while let Some(amp_box) = peer.next().await {
            written.push(amp_box.unwrap());
        }
    };
    timeout(Duration::from_secs(2), all)
        .await
        .expect("the client did not close the connection");
    written
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
async fn a_notification_is_written_and_not_answered() {
    let (client, mut peer) = with_framed_peer();
    client.notify(&Sum { a: 1, b: 1 }).await.unwrap();
    // Closing the client ends the connection once the writer is done, so
    // the peer reads everything the client wrote and then end of stream.
    drop(client);
    let written = drain(&mut peer).await;
    assert_eq!(written.len(), 1, "{written:?}");
    assert_eq!(written[0].get("_command"), Some(&b"Sum"[..]));
    assert_eq!(written[0].get("a"), Some(&b"1"[..]));
    assert!(!written[0].contains_key("_ask"), "{written:?}");
}

/// A request from the peer that wants an answer is refused as `UNHANDLED`.
#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
async fn a_request_from_the_peer_is_refused() {
    let (client, mut peer) = with_framed_peer();
    let request = Request::new("Sum", AmpBox::new()).with_ask("7");
    peer.send(request.into_box().unwrap()).await.unwrap();
    let refusal = next_box(&mut peer).await;
    assert_eq!(refusal.get("_error"), Some(&b"7"[..]));
    assert_eq!(refusal.get("_error_code"), Some(&b"UNHANDLED"[..]));
    assert!(refusal.contains_key("_error_description"), "{refusal:?}");
    // The connection is still good.
    drop(peer);
    let failed = client.call(&Sum { a: 1, b: 2 }).await;
    assert!(
        matches!(failed, Err(Error::ConnectionClosed { .. })),
        "{failed:?}"
    );
}

/// A request from the peer that wants no answer gets none: the refusal of
/// the request with an `_ask` sent after it is the first thing written.
#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
async fn a_notification_from_the_peer_is_ignored() {
    let (client, mut peer) = with_framed_peer();
    let silent = Request::new("Sum", AmpBox::new());
    peer.send(silent.into_box().unwrap()).await.unwrap();
    let asked = Request::new("Sum", AmpBox::new()).with_ask("8");
    peer.send(asked.into_box().unwrap()).await.unwrap();
    let refusal = next_box(&mut peer).await;
    assert_eq!(refusal.get("_error"), Some(&b"8"[..]));
    // Nothing else was written on the notification's account.
    drop(client);
    assert!(drain(&mut peer).await.is_empty());
}

/// A request handed to the client as a service, whose arguments use a key
/// the protocol reserves, fails without reaching the wire and without
/// harming the connection.
#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
async fn a_request_with_a_reserved_argument_fails_locally() {
    with_peer(|mut client| async move {
        let mut arguments = AmpBox::new();
        arguments.insert("_answer", "no").unwrap();
        let request = Request::new("Sum", arguments.clone()).with_ask("x");
        let failed = Service::call(&mut client, request).await;
        assert_reserved(&failed, b"_answer");
        // The same for a request that wants no answer.
        let request = Request::new("Sum", arguments);
        let failed = Service::call(&mut client, request).await;
        assert_reserved(&failed, b"_answer");
        // The connection is still good.
        let total = client.call(&Sum { a: 1, b: 2 }).await.unwrap();
        assert_eq!(total, Total { total: 3 });
    })
    .await;
}

fn assert_reserved<T: std::fmt::Debug>(result: &Result<T, Error>, expected: &[u8]) {
    assert!(
        matches!(
            result,
            Err(Error::Protocol(amp_protocol::Error::ReservedKey(key))) if key == expected
        ),
        "{result:?}"
    );
}

/// A connection with a service answers the peer's requests and still makes
/// calls of its own, both on the same connection. The connection is driven
/// on the test's own task, since WebAssembly may have no runtime to spawn
/// it on.
#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
async fn a_connection_answers_the_peer_and_calls_it() {
    let (io, peer_io) = tokio::io::duplex(4096);
    let connection = Connection::new(io);
    let client = connection.client().with_timeout(Duration::from_millis(200));
    let mut peer = Framed::new(peer_io, AmpCodec::new());
    let test = async move {
        // The peer asks, and the connection's router answers.
        let mut arguments = AmpBox::new();
        arguments.insert("a", "1").unwrap();
        arguments.insert("b", "2").unwrap();
        let request = Request::new("Sum", arguments).with_ask("7");
        peer.send(request.into_box().unwrap()).await.unwrap();
        let answer = next_box(&mut peer).await;
        assert_eq!(answer.get("_answer"), Some(&b"7"[..]));
        assert_eq!(answer.get("total"), Some(&b"3"[..]));

        // The connection asks, and the peer answers.
        let answering = async {
            let amp_box = next_box(&mut peer).await;
            let Ok(Message::Request(request)) = Message::from_box(amp_box) else {
                panic!("the connection wrote something other than a request");
            };
            peer.send(respond(&request).unwrap()).await.unwrap();
        };
        let (total, ()) = tokio::join!(client.call(&Sum { a: 40, b: 2 }), answering);
        assert_eq!(total.unwrap(), Total { total: 42 });
    };
    tokio::select! {
        () = test => {}
        ended = connection.serve(Router::new().command::<Sum>()) => {
            panic!("the connection ended before the test: {ended:?}")
        }
    }
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
async fn a_closed_connection_fails_calls() {
    let (client_io, peer_io) = tokio::io::duplex(4096);
    let client = Client::new(client_io);
    drop(peer_io);
    // Whether the connection task sees the read half's end of stream or the
    // write half's broken pipe first depends on scheduling, so the cause is
    // not checked.
    let failed = client.call(&Sum { a: 1, b: 1 }).await;
    assert!(
        matches!(failed, Err(Error::ConnectionClosed { .. })),
        "{failed:?}"
    );
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
async fn a_client_learns_its_connection_closed_without_a_call() {
    let (client_io, peer_io) = tokio::io::duplex(4096);
    let client = Client::new(client_io);
    let clone = client.clone();
    assert!(!client.is_closed());
    drop(peer_io);
    // Whether the connection task sees the read half's end of stream or the
    // write half's broken pipe first depends on scheduling, so the cause is
    // not checked.
    timeout(Duration::from_secs(2), client.closed())
        .await
        .expect("the client never noticed");
    assert!(client.is_closed());
    assert!(clone.is_closed(), "every clone shares the connection");
    // Already closed, so this returns at once.
    timeout(Duration::from_secs(2), clone.closed())
        .await
        .expect("closed did not return at once");
    let failed = client.call(&Sum { a: 1, b: 1 }).await;
    assert!(
        matches!(failed, Err(Error::ConnectionClosed { .. })),
        "{failed:?}"
    );
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
async fn dropping_the_serve_future_closes_its_clients() {
    let (client_io, _peer_io) = tokio::io::duplex(4096);
    let connection = Connection::new(client_io);
    let client = connection.client();
    let serve = Box::pin(connection.serve(Router::new()));
    assert!(!client.is_closed(), "not served yet, but not closed either");
    drop(serve);
    // Nothing went wrong on the connection: it was simply let go of.
    let cause = timeout(Duration::from_secs(2), client.closed())
        .await
        .expect("the client never noticed");
    assert!(cause.is_none(), "{cause:?}");
    assert!(client.is_closed());
}

/// A call still waiting for its answer when the `serve` future is dropped
/// fails rather than waiting forever: the map of waiting calls outlives the
/// future, in the handles, so it has to be emptied on the way out.
#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
async fn dropping_the_serve_future_fails_the_calls_in_flight() {
    let (client_io, peer_io) = tokio::io::duplex(4096);
    let mut peer = Framed::new(peer_io, AmpCodec::new());
    let connection = Connection::new(client_io);
    let client = connection.client();
    let mut serve = Box::pin(connection.serve(Router::new()));
    let mut call = std::pin::pin!(client.call(&Silence {}));
    // Drive both until the request reaches the peer, who never answers.
    let written = tokio::select! {
        result = &mut serve => unreachable!("the connection ended: {result:?}"),
        failed = &mut call => unreachable!("the call ended: {failed:?}"),
        written = peer.next() => written,
    };
    assert!(written.is_some(), "the request was never written");
    drop(serve);
    let failed = timeout(Duration::from_secs(2), call)
        .await
        .expect("the call never noticed");
    assert!(
        matches!(failed, Err(Error::ConnectionClosed { cause: None })),
        "{failed:?}"
    );
}

/// The cause an idle timeout ends a connection with.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn assert_timed_out(cause: Option<&Error>) {
    match cause {
        Some(Error::Io(err)) if err.kind() == std::io::ErrorKind::TimedOut => {}
        other => panic!("not an idle timeout: {other:?}"),
    }
}

/// Writes notifications until the peer stops taking them, then waits for
/// good: once the writer is stuck, what ends the connection is the test.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
async fn fill(client: &Client) {
    loop {
        if client.notify(&Sum { a: 1, b: 1 }).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// Without an idle timeout the writer blocks on a peer that stops reading
/// and the connection task is never reclaimed, not even once every handle
/// is dropped; with one, the write times out and the connection ends.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[tokio::test]
async fn a_client_with_an_idle_timeout_gives_up_on_a_peer_that_stops_reading() {
    // Small enough that a couple of boxes fill it.
    let (client_io, _peer_io) = tokio::io::duplex(64);
    let connection = Connection::new(client_io).idle_timeout(Duration::from_millis(100));
    let client = Client::spawn(connection);
    let ended = async {
        tokio::select! {
            cause = client.closed() => cause,
            () = fill(&client) => unreachable!(),
        }
    };
    // `closed` resolves only once the connection task has ended, so this
    // is also the check that the task was reclaimed.
    let cause = timeout(Duration::from_secs(2), ended)
        .await
        .expect("the connection never ended");
    assert_timed_out(cause.as_deref());
    let failed = client.notify(&Sum { a: 1, b: 1 }).await;
    assert!(
        matches!(failed, Err(Error::ConnectionClosed { .. })),
        "{failed:?}"
    );
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[tokio::test]
async fn a_served_connection_with_an_idle_timeout_gives_up_on_a_peer_that_stops_reading() {
    let (client_io, _peer_io) = tokio::io::duplex(64);
    let connection = Connection::new(client_io).idle_timeout(Duration::from_millis(100));
    let client = connection.client();
    let served = async {
        tokio::select! {
            result = connection.serve(Router::new()) => result,
            () = fill(&client) => unreachable!(),
        }
    };
    let result = timeout(Duration::from_secs(2), served)
        .await
        .expect("the connection never ended");
    assert_timed_out(result.as_ref().err());
    assert_timed_out(client.closed().await.as_deref());
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[tokio::test]
async fn a_served_connection_with_an_idle_timeout_gives_up_on_a_peer_that_sends_nothing() {
    let (client_io, _peer_io) = tokio::io::duplex(4096);
    let connection = Connection::new(client_io).idle_timeout(Duration::from_millis(100));
    let result = timeout(Duration::from_secs(2), connection.serve(Router::new()))
        .await
        .expect("the connection never ended");
    assert_timed_out(result.as_ref().err());
}
