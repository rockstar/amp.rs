//! A key/value store: commands that share state, an error type of the
//! server's own, optional arguments, list results, a command with no
//! results, tower middleware, a hand-written accept loop that reports
//! connection errors, and commands going both ways on one connection: a
//! client that calls `Subscribe` is sent a `Changed` command whenever a key
//! is set, on the connection it subscribed from.
//!
//! Run with `cargo run --example kv_server`, then run one or more
//! `cargo run --example kv_watcher` against it.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_amp::{Client, Connection, Error, HandleWith, Router, command};

/// What every connection shares. It is cloned per request, so it must be
/// cheap to clone and must own its data through shared handles.
#[derive(Clone, Default)]
struct Store {
    entries: Arc<Mutex<BTreeMap<String, String>>>,
    /// A client handle for every connection that subscribed, by the
    /// connection's number, so that one can be dropped when its connection
    /// is gone.
    watchers: Arc<Mutex<HashMap<u64, Client>>>,
}

/// The state every command receives: the store, and the connection the
/// request came in on, which is what `Subscribe` registers and `Set`
/// pushes to.
#[derive(Clone)]
struct Session {
    store: Store,
    /// This connection's number in `watchers`.
    id: u64,
    /// Calls commands on the peer at the other end of this connection.
    peer: Client,
}

/// Every way a command on this server can fail. The commands that can fail
/// declare it with `error = KvError` and return it directly; the ones that
/// cannot keep the default, `tower_amp::Error`. The code each variant goes
/// on the wire with is decided once, here, rather than at every `Err`.
#[derive(Debug)]
enum KvError {
    NotFound(String),
    EmptyKey,
}

impl KvError {
    /// The AMP error code, by convention all-caps `SNAKE_CASE`.
    fn code(&self) -> &'static str {
        match self {
            KvError::NotFound(_) => "NOT_FOUND",
            KvError::EmptyKey => "EMPTY_KEY",
        }
    }
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvError::NotFound(key) => write!(f, "no key {key:?}"),
            KvError::EmptyKey => f.write_str("a key cannot be empty"),
        }
    }
}

impl std::error::Error for KvError {}

/// What the peer sees: the variant's code, and its message as the
/// description.
impl From<KvError> for Error {
    fn from(err: KvError) -> Self {
        Error::new(err.code(), err.to_string())
    }
}

#[derive(Deserialize)]
#[command(response = Previous, error = KvError)]
struct Set {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct Previous {
    /// Absent on the wire when the key was new.
    previous: Option<String>,
}

impl HandleWith<Session> for Set {
    async fn handle(self, session: Session) -> Result<Previous, KvError> {
        if self.key.is_empty() {
            return Err(KvError::EmptyKey);
        }
        let store = &session.store;
        let previous = store
            .entries
            .lock()
            .unwrap()
            .insert(self.key.clone(), self.value.clone());
        // The lock is not held while the watchers are written to. The
        // setter is a watcher like any other, if it subscribed, so it sees
        // its own change come back.
        let watchers: Vec<(u64, Client)> = store
            .watchers
            .lock()
            .unwrap()
            .iter()
            .map(|(id, client)| (*id, client.clone()))
            .collect();
        for (id, watcher) in watchers {
            let changed = Changed {
                key: self.key.clone(),
                value: self.value.clone(),
            };
            // A watcher is told, not asked, so a slow one does not hold up
            // this reply. A watcher whose connection has ended is dropped
            // here if the failure is already known, and otherwise by the
            // accept loop below once its connection returns.
            if watcher.notify(&changed).await.is_err() {
                store.watchers.lock().unwrap().remove(&id);
            }
        }
        Ok(Previous { previous })
    }
}

/// Sent to every subscriber when a key is set. The server only sends it, so
/// it only needs `Serialize` here; `kv_watcher.rs` handles it.
#[derive(Serialize)]
#[command]
struct Changed {
    key: String,
    value: String,
}

/// Asks to be sent `Changed` on this connection from now on.
#[derive(Deserialize)]
#[command]
struct Subscribe {}

impl HandleWith<Session> for Subscribe {
    async fn handle(self, session: Session) -> Result<(), Error> {
        session
            .store
            .watchers
            .lock()
            .unwrap()
            .insert(session.id, session.peer);
        Ok(())
    }
}

#[derive(Deserialize)]
#[command(response = Value, error = KvError)]
struct Get {
    key: String,
}

#[derive(Serialize)]
struct Value {
    value: String,
}

impl HandleWith<Session> for Get {
    async fn handle(self, session: Session) -> Result<Value, KvError> {
        match session.store.entries.lock().unwrap().get(&self.key) {
            Some(value) => Ok(Value {
                value: value.clone(),
            }),
            None => Err(KvError::NotFound(self.key)),
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

impl HandleWith<Session> for Keys {
    async fn handle(self, session: Session) -> Result<KeyList, Error> {
        let prefix = self.prefix.unwrap_or_default();
        let keys = session
            .store
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

impl HandleWith<Session> for Clear {
    async fn handle(self, session: Session) -> Result<(), Error> {
        session.store.entries.lock().unwrap().clear();
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::default();
    let listener = TcpListener::bind("127.0.0.1:1234").await?;
    println!("listening on {}", listener.local_addr()?);
    for id in 0u64.. {
        let (stream, peer) = listener.accept().await?;
        // As `serve` does, so that replies written back to back are not
        // each held for the acknowledgement of the one before.
        let _ = stream.set_nodelay(true);

        // The connection is built here rather than handed to
        // `serve_connection`, so that this connection's handlers get a
        // handle to call commands on its peer. The router is therefore
        // built per connection, with that handle in its state.
        let connection = Connection::new(stream);
        let session = Session {
            store: store.clone(),
            id,
            peer: connection.client(),
        };
        let router = Router::with_state(session)
            .command::<Set>()
            .command::<Get>()
            .command::<Keys>()
            .command::<Clear>()
            .command::<Subscribe>();

        // The router is a tower service, so ordinary tower middleware
        // applies. A timeout or a shed request comes back to the peer as
        // an `UNKNOWN` error described as `internal error`; the
        // `middleware` example gives such failures a code of their own.
        let service = ServiceBuilder::new()
            .concurrency_limit(64)
            .timeout(Duration::from_secs(5))
            .service(router);

        let store = store.clone();
        tokio::spawn(async move {
            if let Err(err) = connection.serve(service).await {
                eprintln!("connection from {peer} failed: {err}");
            }
            // Whether it subscribed or not; removing nothing is fine.
            store.watchers.lock().unwrap().remove(&id);
        });
    }
    Ok(())
}
