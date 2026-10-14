//! The canonical AMP example: a server with `Sum` and `Divide` commands,
//! compatible with the client in Twisted's AMP documentation.
//!
//! Run with `cargo run --example sum_server`, then call it with
//! `cargo run --example client`.

use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_amp::{Error, Handle, Router, Server, command};

#[derive(Deserialize)]
#[command(response = Total)]
struct Sum {
    a: i64,
    b: i64,
}

#[derive(Serialize)]
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

#[derive(Deserialize)]
#[command(response = Quotient)]
struct Divide {
    numerator: i64,
    denominator: i64,
}

#[derive(Serialize)]
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let router = Router::new().command::<Sum>().command::<Divide>();

    let listener = TcpListener::bind("127.0.0.1:1234").await?;
    println!("listening on {}", listener.local_addr()?);
    // Ctrl-C stops the server once the requests in flight are answered.
    Server::new(router)
        .shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .serve(listener)
        .await?;
    Ok(())
}
