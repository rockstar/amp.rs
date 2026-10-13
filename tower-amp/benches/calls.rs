//! Calls through a served connection, one at a time and pipelined.
//!
//! Run with `cargo bench -p tower-amp`. A client and a server share an
//! in-memory pipe on a current-thread runtime, so what is timed is the work
//! the two sides do for each call and nothing else: framing it, routing it,
//! the task per request, framing the reply, and matching it to the call.
//! The sequential case times one round trip at a time, where every step is
//! on the critical path. The pipelined case has every call in flight at
//! once, up to the server's limit of 256, so a step that overlaps with the
//! others, such as the spawn of the request's task, costs less there. Watch
//! both when touching how a connection reads, dispatches or answers a
//! request. divan reports throughput in calls per second alongside the
//! time.

use divan::counter::ItemsCount;
use divan::{Bencher, black_box};
use serde::{Deserialize, Serialize};
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

/// A client joined to a served `Sum` by an in-memory pipe, and the runtime
/// both run on. Current-thread, so that the timings are not at the mercy of
/// the scheduler moving tasks between workers.
fn connect() -> (Runtime, Client) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let client = runtime.block_on(async {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        tokio::spawn(tower_amp::serve_connection(
            server_io,
            Router::new().command::<Sum>(),
        ));
        Client::new(client_io)
    });
    (runtime, client)
}

#[divan::bench]
fn sequential(bencher: Bencher) {
    let (runtime, client) = connect();
    let sum = Sum { a: 13, b: 81 };
    bencher.counter(ItemsCount::new(CALLS)).bench(|| {
        runtime.block_on(async {
            for _ in 0..CALLS {
                black_box(client.call(black_box(&sum)).await.unwrap());
            }
        })
    });
}

#[divan::bench]
fn pipelined(bencher: Bencher) {
    let (runtime, client) = connect();
    let sum = Sum { a: 13, b: 81 };
    bencher.counter(ItemsCount::new(CALLS)).bench(|| {
        runtime.block_on(async {
            let calls = (0..CALLS).map(|_| client.call(black_box(&sum)));
            for total in futures_util::future::join_all(calls).await {
                black_box(total.unwrap());
            }
        })
    });
}
