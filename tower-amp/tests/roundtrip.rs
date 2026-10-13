//! A client and a served connection joined by an in-memory pipe.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use amp_protocol::{AmpBox, Request};
use serde::{Deserialize, Serialize};
use tower::{Service, ServiceBuilder};
use tower_amp::{command, Client, Error, Handle, HandleWith, Router};

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
            if code == "UNKNOWN" && description.contains("missing field")
    ));
}

#[tokio::test]
async fn requests_on_one_connection_run_concurrently() {
    let client = connect(router());
    let started = std::time::Instant::now();
    let (slow, fast) = tokio::join!(
        client.call(&Sleep { millis: 200 }),
        client.call(&Sleep { millis: 10 }),
    );
    assert_eq!(slow.unwrap().millis, 200);
    assert_eq!(fast.unwrap().millis, 10);
    assert!(started.elapsed() < Duration::from_millis(400));
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

#[tokio::test]
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
async fn a_closed_connection_fails_calls() {
    let (client_io, server_io) = tokio::io::duplex(64);
    drop(server_io);
    let client = Client::new(client_io);
    let failed = client.call(&Sum { a: 1, b: 2 }).await;
    assert!(matches!(
        failed,
        Err(Error::ConnectionClosed) | Err(Error::Io(_))
    ));
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
