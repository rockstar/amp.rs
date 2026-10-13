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
use tower_amp::{AmpCodec, Client, Error, command};

#[derive(Serialize)]
#[command(response = Total)]
struct Sum {
    a: i64,
    b: i64,
}

#[derive(Deserialize, Debug, PartialEq)]
struct Total {
    total: i64,
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
