//! Tower middleware around an AMP server. The router is an ordinary tower
//! service, so anything from the tower ecosystem wraps it, and so does a
//! layer of your own. This server stacks four:
//!
//! * `Timing`, written here, prints each command's name, outcome, and how
//!   long it took;
//! * tower's `MapErr` decides what the peer is told when a layer fails;
//! * tower's `Timeout` fails a command that takes too long;
//! * tower's `ConcurrencyLimit` bounds how many run at once.
//!
//! It then calls itself to show each layer at work. Run with
//! `cargo run --example middleware`.
//!
//! An error from a layer, rather than from a handler, was not written with
//! the peer in mind, so tower-amp answers it with `UNKNOWN` and the fixed
//! description `internal error`, and logs the error itself (with the
//! `tracing` feature). To tell the peer something specific, a layer maps
//! its error to `Error::new`, as `peer_facing` below does for a timeout.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tower::timeout::error::Elapsed;
use tower::{BoxError, Layer, Service, ServiceBuilder};
use tower_amp::{AmpBox, Client, Error, Handle, Request, Router, Server, command};

#[derive(Serialize, Deserialize)]
#[command(response = Total)]
struct Sum {
    a: i64,
    b: i64,
}

#[derive(Serialize, Deserialize)]
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

/// Takes as long as it is told to, so that the timeout layer has something
/// to cut short.
#[derive(Serialize, Deserialize)]
#[command(response = Slept)]
struct Sleep {
    millis: u64,
}

#[derive(Serialize, Deserialize)]
struct Slept {}

impl Handle for Sleep {
    async fn handle(self) -> Result<Slept, Error> {
        tokio::time::sleep(Duration::from_millis(self.millis)).await;
        Ok(Slept {})
    }
}

/// Looks a key up in a store that is down. The store's error names its
/// host, which is for this side's log, not for whoever is calling.
#[derive(Serialize, Deserialize)]
#[command(response = Found)]
struct Lookup {
    key: String,
}

#[derive(Serialize, Deserialize)]
struct Found {
    value: String,
}

impl Handle for Lookup {
    async fn handle(self) -> Result<Found, Error> {
        let value = query(&self.key).map_err(Error::Internal)?;
        Ok(Found { value })
    }
}

/// Stands in for a database client, with the error type one would have.
fn query(_key: &str) -> Result<String, BoxError> {
    Err("db at 10.0.0.7:5432 refused the connection".into())
}

/// What the peer is told when a layer fails. A timeout gets a code the peer
/// can act on. Anything else is left to tower-amp, which answers `internal
/// error` and keeps the message for the log, since the layer that wrote it
/// was not addressing the peer.
fn peer_facing(err: BoxError) -> Error {
    match err.downcast::<Elapsed>() {
        Ok(_) => Error::new("TIMEOUT", "the command took too long"),
        Err(err) => Error::from(err),
    }
}

/// A layer of this program's own. Its service wraps another and prints
/// each command's name, outcome, and duration once the inner service has
/// answered.
#[derive(Clone)]
struct TimingLayer;

impl<S> Layer<S> for TimingLayer {
    type Service = Timing<S>;

    fn layer(&self, inner: S) -> Timing<S> {
        Timing { inner }
    }
}

#[derive(Clone)]
struct Timing<S> {
    inner: S,
}

impl<S> Service<Request> for Timing<S>
where
    S: Service<Request, Response = AmpBox>,
    S::Error: std::fmt::Display + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = AmpBox;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<AmpBox, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        // The command's name is the peer's bytes, so it is escaped before
        // it reaches the terminal.
        let command = String::from_utf8_lossy(&request.command)
            .escape_debug()
            .to_string();
        let started = Instant::now();
        let answered = self.inner.call(request);
        Box::pin(async move {
            let result = answered.await;
            let outcome = match &result {
                Ok(_) => "ok".to_string(),
                Err(err) => format!("failed: {err}"),
            };
            println!("server: {command} {outcome} in {:?}", started.elapsed());
            result
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let router = Router::new()
        .command::<Sum>()
        .command::<Lookup>()
        .command::<Sleep>();

    // The first layer is the outermost: a request passes through `Timing`,
    // then `MapErr`, then `Timeout`, then `ConcurrencyLimit`, and reaches
    // the router; its result comes back out the same way. `Timeout` and
    // `ConcurrencyLimit` make the stack's error type `BoxError`, which
    // `peer_facing` turns back into `tower_amp::Error` so that `Timing`
    // and `serve` see one.
    let service = ServiceBuilder::new()
        .layer(TimingLayer)
        .map_err(peer_facing)
        .timeout(Duration::from_millis(200))
        .concurrency_limit(64)
        .service(router);

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(Server::new(service).serve(listener));

    let stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;
    let client = Client::new(stream);

    let total = client.call(&Sum { a: 13, b: 81 }).await?;
    println!("client: 13 + 81 = {}", total.total);

    client.call(&Sleep { millis: 50 }).await?;
    println!("client: slept for 50 ms");

    // The timeout layer cuts this short; `peer_facing` gives it a code.
    report(client.call(&Sleep { millis: 5_000 }).await);

    // The store's error, with its host, stays on the server's side.
    report(client.call(&Lookup { key: "k".into() }).await);
    Ok(())
}

/// Prints how a call failed, as the peer sees it.
fn report<T>(result: Result<T, Error>) {
    match result {
        Ok(_) => println!("client: unexpectedly succeeded"),
        Err(Error::Command { code, description }) => println!("client: {code}: {description}"),
        Err(err) => println!("client: {err}"),
    }
}
