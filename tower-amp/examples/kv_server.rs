//! A key/value store: commands that share state, optional arguments, list
//! results, a command with no results, tower middleware, and a hand-written
//! accept loop that reports connection errors.
//!
//! Run with `cargo run --example kv_server`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_amp::{Error, HandleWith, Router, command};

/// The state every command receives. It is cloned per request, so it must
/// be cheap to clone and must own its data through a shared handle.
#[derive(Clone, Default)]
struct Store {
    entries: Arc<Mutex<BTreeMap<String, String>>>,
}

#[derive(Deserialize)]
#[command(response = Previous)]
struct Set {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct Previous {
    /// Absent on the wire when the key was new.
    previous: Option<String>,
}

impl HandleWith<Store> for Set {
    async fn handle(self, store: Store) -> Result<Previous, Error> {
        let previous = store.entries.lock().unwrap().insert(self.key, self.value);
        Ok(Previous { previous })
    }
}

#[derive(Deserialize)]
#[command(response = Value)]
struct Get {
    key: String,
}

#[derive(Serialize)]
struct Value {
    value: String,
}

impl HandleWith<Store> for Get {
    async fn handle(self, store: Store) -> Result<Value, Error> {
        match store.entries.lock().unwrap().get(&self.key) {
            Some(value) => Ok(Value {
                value: value.clone(),
            }),
            None => Err(Error::new("NOT_FOUND", format!("no key {:?}", self.key))),
        }
    }
}

#[derive(Deserialize)]
#[command(response = KeyList)]
struct Keys {
    /// Absent on the wire means every key.
    prefix: Option<String>,
}

#[derive(Serialize)]
struct KeyList {
    keys: Vec<String>,
}

impl HandleWith<Store> for Keys {
    async fn handle(self, store: Store) -> Result<KeyList, Error> {
        let prefix = self.prefix.unwrap_or_default();
        let keys = store
            .entries
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();
        Ok(KeyList { keys })
    }
}

/// `Clear` has nothing to report, so its response defaults to `()`, the
/// empty box.
#[derive(Deserialize)]
#[command]
struct Clear {}

impl HandleWith<Store> for Clear {
    async fn handle(self, store: Store) -> Result<(), Error> {
        store.entries.lock().unwrap().clear();
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let router = Router::with_state(Store::default())
        .command::<Set>()
        .command::<Get>()
        .command::<Keys>()
        .command::<Clear>();

    // The router is a tower service, so ordinary tower middleware applies.
    // A timeout or a shed request comes back to the peer as an `UNKNOWN`
    // error carrying the middleware's message.
    let service = ServiceBuilder::new()
        .concurrency_limit(64)
        .timeout(Duration::from_secs(5))
        .service(router);

    let listener = TcpListener::bind("127.0.0.1:1234").await?;
    println!("listening on {}", listener.local_addr()?);
    loop {
        let (stream, peer) = listener.accept().await?;
        let service = service.clone();
        tokio::spawn(async move {
            if let Err(err) = tower_amp::serve_connection(stream, service).await {
                eprintln!("connection from {peer} failed: {err}");
            }
        });
    }
}
