#![expect(unused_crate_dependencies)]
//! Calls through a served connection, one at a time and pipelined.
//!
//! Run with `cargo bench -p tower-amp`. A client and a server run on a
//! current-thread runtime, joined by an in-memory pipe or by loopback TCP.
//! Over the pipe, what is timed is the work the two sides do for each call
//! and nothing else: framing it, routing it, the task per request, framing
//! the reply, and matching it to the call. Over TCP, the writes and reads
//! are system calls, so the number of each per call shows up in the time.
//! The sequential case times one round trip at a time, where every step is
//! on the critical path. The pipelined case has every call in flight at
//! once, up to the server's limit of 256, so a step that overlaps with the
//! others, such as the spawn of the request's task, costs less there, and
//! a side that writes several frames in one go pays for one write rather
//! than one per frame. Watch all four when touching how a connection reads,
//! writes, dispatches or answers a request. divan reports throughput in
//! calls per second alongside the time.
#![expect(
    clippy::unwrap_used,
    reason = "a bench that cannot set up should fail loudly"
)]

use divan::counter::ItemsCount;
use divan::{Bencher, black_box};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tower_amp::{Client, Error, Handle, Router, command};

fn main() {
    divan::main();
}

/// Calls per iteration. Enough that the per-call cost dominates the
/// iteration's fixed cost of entering the runtime.
const CALLS: usize = 1_000;

/// A typical small command: two fields in, one out.
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

/// What joins the client to the server.
#[derive(Debug, Clone, Copy)]
enum Transport {
    /// An in-memory pipe, so no system calls.
    Pipe,
    /// Loopback TCP, with Nagle's algorithm off as `serve` sets it.
    Tcp,
}

/// A client joined to a served `Sum` by `transport`, and the runtime both
/// run on. Current-thread, so that the timings are not at the mercy of the
/// scheduler moving tasks between workers.
fn connect(transport: Transport) -> (Runtime, Client) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let client = runtime.block_on(async {
        let router = Router::new().command::<Sum>();
        match transport {
            Transport::Pipe => {
                let (client_io, server_io) = tokio::io::duplex(64 * 1024);
                drop(tokio::spawn(tower_amp::serve_connection(server_io, router)));
                Client::new(client_io)
            }
            Transport::Tcp => {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let client_io = TcpStream::connect(listener.local_addr().unwrap())
                    .await
                    .unwrap();
                let (server_io, _) = listener.accept().await.unwrap();
                client_io.set_nodelay(true).unwrap();
                server_io.set_nodelay(true).unwrap();
                drop(tokio::spawn(tower_amp::serve_connection(server_io, router)));
                Client::new(client_io)
            }
        }
    });
    (runtime, client)
}

#[divan::bench(args = [Transport::Pipe, Transport::Tcp])]
fn sequential(bencher: Bencher, transport: Transport) {
    let (runtime, client) = connect(transport);
    let sum = Sum { a: 13, b: 81 };
    bencher.counter(ItemsCount::new(CALLS)).bench(|| {
        runtime.block_on(async {
            for _ in 0..CALLS {
                let _ = black_box(client.call(black_box(&sum)).await.unwrap());
            }
        })
    });
}

#[divan::bench(args = [Transport::Pipe, Transport::Tcp])]
fn pipelined(bencher: Bencher, transport: Transport) {
    let (runtime, client) = connect(transport);
    let sum = Sum { a: 13, b: 81 };
    bencher.counter(ItemsCount::new(CALLS)).bench(|| {
        runtime.block_on(async {
            let calls = (0..CALLS).map(|_| client.call(black_box(&sum)));
            for total in futures_util::future::join_all(calls).await {
                let _ = black_box(total.unwrap());
            }
        })
    });
}
