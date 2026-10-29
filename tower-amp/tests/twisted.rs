//! Interoperability with Twisted, the reference implementation of AMP: this
//! crate's server against Twisted's client, and this crate's client against
//! Twisted's server, with each side calling a command on the other inside
//! a handler, and with one command that carries an argument of every AMP
//! type. The Python side is `tests/twisted/peer.py`.
//!
//! These tests need a Python with Twisted installed, so they are ignored by
//! default and run only in CI, or by hand with
//! `cargo test -p tower-amp --test twisted -- --ignored`. `PYTHON` names the
//! interpreter; it defaults to `python3`.

// The tests shell out to Python and serve over TCP, neither of which
// WebAssembly has.
#![cfg(not(target_family = "wasm"))]

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tower_amp::{Client, Connection, Error, Handle, HandleWith, Router, command};

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

#[derive(Serialize, Deserialize)]
#[command(response = Quotient)]
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

/// Answered by asking the peer that sent it to do the sum, which is how
/// Twisted's responder for it works too.
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

/// One argument per row of serde_amp's type-mapping table, answered by
/// checking every argument against [`kitchen_sink`] and echoing it back.
/// Twisted does the same with its `KITCHEN_SINK`, so a call in either
/// direction proves that the encoding of each type is what Twisted reads,
/// and that Twisted's encoding is what this crate reads.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[command(response = KitchenSink)]
struct KitchenSink {
    count: i64,
    text: String,
    bytes: Bytes,
    yes: bool,
    no: bool,
    floats: Vec<f64>,
    price: String,
    when: String,
    ints: Vec<i64>,
    items: Vec<Item>,
    maybe: Option<i64>,
    never: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Item {
    name: String,
    size: i64,
}

/// The values `peer.py`'s `KITCHEN_SINK` holds, and the comment there says
/// why these.
fn kitchen_sink() -> KitchenSink {
    KitchenSink {
        count: i64::MIN,
        text: "h\u{e9}llo w\u{f6}rld \u{2713}".into(),
        bytes: Bytes::from_static(b"\x00\xff\x01"),
        yes: true,
        no: false,
        floats: vec![246.8, 1e300, f64::INFINITY, f64::NEG_INFINITY],
        price: "19.99".into(),
        when: "2026-10-14T09:30:15.250000+02:00".into(),
        ints: vec![-1, 0, i64::MAX],
        items: vec![
            Item {
                name: "a".into(),
                size: 1,
            },
            Item {
                name: "b".into(),
                size: 2,
            },
        ],
        maybe: Some(7),
        never: None,
    }
}

impl Handle for KitchenSink {
    async fn handle(self) -> Result<KitchenSink, Error> {
        if self != kitchen_sink() {
            return Err(Error::new("MISMATCH", format!("{self:?}")));
        }
        Ok(self)
    }
}

/// `peer.py` run with `args`, with its output captured for the test's
/// report and the process killed when the test is done with it.
fn peer(args: &[&str]) -> Command {
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/twisted/peer.py");
    let python = std::env::var("PYTHON").unwrap_or_else(|_| "python3".into());
    let mut command = Command::new(python);
    let _ = command
        .arg(script)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

/// Twisted's client calls this server's commands, among them `Relay`,
/// which this server answers by calling `Sum` on Twisted's client, and
/// checks every answer itself; the exit status says whether it was
/// satisfied.
#[tokio::test]
#[ignore = "needs a Python with Twisted; run with --ignored"]
async fn twisted_client_calls_this_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    // Each connection's router holds a handle to its own peer, for `Relay`.
    drop(tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let connection = Connection::new(stream);
            let router = Router::with_state(connection.client())
                .command::<Sum>()
                .command::<Divide>()
                .command::<Relay>()
                .command::<KitchenSink>();
            drop(tokio::spawn(connection.serve(router)));
        }
    }));
    let client = peer(&["client", &port.to_string()]).spawn().unwrap();
    let output = tokio::time::timeout(Duration::from_secs(30), client.wait_with_output())
        .await
        .expect("Twisted's client did not finish")
        .unwrap();
    assert!(
        output.status.success(),
        "Twisted's client failed ({}):\n{}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// This client calls the commands of a Twisted server and checks each
/// answer against what Twisted's own responders return. For `Relay`, the
/// Twisted server calls `Sum` back on this client, which serves it.
#[tokio::test]
#[ignore = "needs a Python with Twisted; run with --ignored"]
async fn this_client_calls_a_twisted_server() {
    let mut server = peer(&["server"]).spawn().unwrap();
    let mut stdout = BufReader::new(server.stdout.take().unwrap());
    let mut line = String::new();
    let _ = tokio::time::timeout(Duration::from_secs(30), stdout.read_line(&mut line))
        .await
        .expect("Twisted's server did not report its port")
        .unwrap();
    let port: u16 = line
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("not a port: {line:?}"));

    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let connection = Connection::new(stream);
    let client = connection.client().with_timeout(Duration::from_secs(30));
    drop(tokio::spawn(
        connection.serve(Router::new().command::<Sum>()),
    ));

    let total = client.call(&Sum { a: 13, b: 81 }).await.unwrap();
    assert_eq!(total, Total { total: 94 });
    let quotient = client
        .call(&Divide {
            numerator: 1234,
            denominator: 5,
        })
        .await
        .unwrap();
    assert_eq!(quotient, Quotient { result: 246.8 });
    let failed = client
        .call(&Divide {
            numerator: 1,
            denominator: 0,
        })
        .await;
    assert!(
        matches!(failed, Err(Error::Command { ref code, .. }) if code == "ZERO_DIVISION"),
        "{failed:?}"
    );
    let total = client.call(&Relay { a: 13, b: 81 }).await.unwrap();
    assert_eq!(total, Total { total: 94 });
    let echo = client.call(&kitchen_sink()).await.unwrap();
    assert_eq!(echo, kitchen_sink());
}
