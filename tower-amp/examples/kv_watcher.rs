//! Watches the key/value store in `kv_server.rs`: subscribes, sets a key,
//! and prints every change the server pushes. The pushes arrive as
//! `Changed` commands on the same connection this program calls the server
//! on, so it is both a client and a server of that connection.
//!
//! Start `kv_server` first, then run with `cargo run --example kv_watcher`,
//! in as many terminals as you like; each one sees every other's change.
//! Stop it with Ctrl-C.

use std::process;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tower_amp::{Connection, Error, Handle, Router, command};

/// Pushed by the server when a key is set. This side only receives it, so
/// it needs `Deserialize` and a `Handle`.
#[derive(Deserialize)]
#[command]
struct Changed {
    key: String,
    value: String,
}

impl Handle for Changed {
    async fn handle(self) -> Result<(), Error> {
        println!("{} is now {:?}", self.key, self.value);
        Ok(())
    }
}

#[derive(Serialize)]
#[command]
struct Subscribe {}

#[derive(Serialize)]
#[command(response = Previous)]
struct Set {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct Previous {
    previous: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect("127.0.0.1:1234").await?;
    stream.set_nodelay(true)?;

    // A handle to call the server with, taken before the connection is
    // served. Serving it is what reads the server's answers to those calls
    // as well as its `Changed` commands, so it runs on a task of its own
    // for as long as the connection lasts.
    let connection = Connection::new(stream);
    let client = connection.client().with_timeout(Duration::from_secs(5));
    let served = tokio::spawn(connection.serve(Router::new().command::<Changed>()));

    client.call(&Subscribe {}).await?;
    let previous = client
        .call(&Set {
            key: "watcher".into(),
            value: format!("pid {}", process::id()),
        })
        .await?;
    match previous.previous {
        Some(previous) => println!("replaced {previous:?}; watching for changes"),
        None => println!("set a new key; watching for changes"),
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        ended = served => match ended? {
            Ok(()) => println!("the server closed the connection"),
            Err(err) => eprintln!("the connection failed: {err}"),
        },
    }
    Ok(())
}
