#![expect(unused_crate_dependencies)]
//! Calls the `Sum` and `Divide` commands of the server in `sum_server.rs`.
//!
//! Start that server first, then run with `cargo run --example client`.
//! In a real program the command structs would live in a module shared with
//! the server rather than being repeated here; a client needs only the
//! `#[command]` schema, not the `Handle` implementation.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tower_amp::{Client, Error, command};

#[derive(Serialize)]
#[command(response = Total)]
struct Sum {
    a: i64,
    b: i64,
}

#[derive(Deserialize)]
struct Total {
    total: i64,
}

#[derive(Serialize)]
#[command(response = Quotient)]
struct Divide {
    numerator: i64,
    denominator: i64,
}

#[derive(Deserialize)]
struct Quotient {
    result: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect("127.0.0.1:1234").await?;
    // Without this, a request made while the one before it is
    // unacknowledged waits for that acknowledgement (Nagle's algorithm).
    stream.set_nodelay(true)?;
    // A call that is not answered within five seconds fails with
    // `Error::Timeout` instead of waiting on the server forever.
    let client = Client::new(stream).with_timeout(Duration::from_secs(5));

    let total = client.call(&Sum { a: 13, b: 81 }).await?;
    println!("13 + 81 = {}", total.total);

    let quotient = client
        .call(&Divide {
            numerator: 1234,
            denominator: 5,
        })
        .await?;
    println!("1234 / 5 = {}", quotient.result);

    let failed = client
        .call(&Divide {
            numerator: 1,
            denominator: 0,
        })
        .await;
    match failed {
        Err(Error::Command { code, description }) => {
            println!("1 / 0 failed as expected: {code} ({description})")
        }
        Ok(quotient) => println!("1 / 0 unexpectedly gave {}", quotient.result),
        Err(err) => return Err(err.into()),
    }
    Ok(())
}
