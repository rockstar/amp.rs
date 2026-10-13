//! Interoperability with Twisted, the reference implementation of AMP: this
//! crate's server against Twisted's client, and this crate's client against
//! Twisted's server. The Python side is `tests/twisted/peer.py`.
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

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tower_amp::{Client, Error, Handle, Router, command};

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

/// `peer.py` run with `args`, with its output captured for the test's
/// report and the process killed when the test is done with it.
fn peer(args: &[&str]) -> Command {
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/twisted/peer.py");
    let python = std::env::var("PYTHON").unwrap_or_else(|_| "python3".into());
    let mut command = Command::new(python);
    command
        .arg(script)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

/// Twisted's client calls this server's commands and checks every answer
/// itself; the exit status says whether it was satisfied.
#[tokio::test]
#[ignore = "needs a Python with Twisted; run with --ignored"]
async fn twisted_client_calls_this_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let router = Router::new().command::<Sum>().command::<Divide>();
    tokio::spawn(tower_amp::serve(listener, router));

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
/// answer against what Twisted's own responders return.
#[tokio::test]
#[ignore = "needs a Python with Twisted; run with --ignored"]
async fn this_client_calls_a_twisted_server() {
    let mut server = peer(&["server"]).spawn().unwrap();
    let mut stdout = BufReader::new(server.stdout.take().unwrap());
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(30), stdout.read_line(&mut line))
        .await
        .expect("Twisted's server did not report its port")
        .unwrap();
    let port: u16 = line
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("not a port: {line:?}"));

    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let client = Client::new(stream).with_timeout(Duration::from_secs(30));

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
}
