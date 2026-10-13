//! A client and a hand-rolled peer joined by an in-memory pipe, so that the
//! client is tested without the server. This is the only test of a
//! connection that runs on WebAssembly, where there is no server.

use std::time::Duration;

use amp_protocol::{AmpBox, Message, Request};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::DuplexStream;
use tokio_util::codec::Framed;
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

#[tokio::test]
async fn calls_a_command() {
    with_peer(|client| async move {
        let total = client.call(&Sum { a: 40, b: 2 }).await.unwrap();
        assert_eq!(total, Total { total: 42 });
    })
    .await;
}

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
async fn a_notification_is_written_and_not_answered() {
    with_peer(|client| async move {
        client.notify(&Sum { a: 1, b: 1 }).await.unwrap();
    })
    .await;
}

#[tokio::test]
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
