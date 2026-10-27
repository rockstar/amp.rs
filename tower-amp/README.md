tower-amp
==

![build-and-check](https://github.com/rockstar/amp.rs/actions/workflows/build-and-check.yml/badge.svg) ![crates.io](https://img.shields.io/crates/v/tower-amp.svg) ![docs.rs](https://docs.rs/tower-amp/badge.svg)

Servers and clients for [Asynchronous Messaging Protocol](https://amp-protocol.net/)
(AMP), the key/value remoting protocol from Twisted, built on
[tower](https://github.com/tower-rs/tower) and [tokio](https://tokio.rs).

A command is a struct holding its arguments, marked with `#[command]` to
give it a name on the wire and a response type, and implementing `Handle`
to say what it does. A `Router` serves commands and is a tower service, so
tower middleware applies to it, and `serve` runs it on a listener.

A server
--

```rust,no_run
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_amp::{command, Error, Handle, Router};

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
    let router = Router::new().command::<Divide>();
    let listener = TcpListener::bind("127.0.0.1:1234").await?;
    tower_amp::serve(listener, router).await?;
    Ok(())
}
```

The command's name on the wire defaults to the struct's name; `name = "..."`
overrides it. A command with nothing to report leaves out `response`, and its
handler returns `()`, which is an empty box on the wire.

A request for a command the router does not know is answered with the
`UNHANDLED` error code. `Error::new` goes to the peer as its code and
description. Arguments that fail to parse go as `UNKNOWN` with the parse
error, which is what tells the peer its schema is wrong. Any other error,
and a handler that panics, is answered with `UNKNOWN` and the description
`internal error`: what went wrong is this side's business, and goes to the
log instead. The connection stays up either way.

A server with more than a command or two will want its failures in one
place rather than spelled out as a code and a string at each `Err`. A
command declared with `error = Type` is handled with that type instead,
which chooses its code and description once, in its conversion to `Error`:

```rust,ignore
enum KvError {
    NotFound(String),
    EmptyKey,
}

impl From<KvError> for Error {
    fn from(err: KvError) -> Self {
        match err {
            KvError::NotFound(key) => Error::new("NOT_FOUND", format!("no key {key:?}")),
            KvError::EmptyKey => Error::new("EMPTY_KEY", "a key cannot be empty"),
        }
    }
}

#[derive(Deserialize)]
#[command(response = Value, error = KvError)]
struct Get {
    key: String,
}

impl HandleWith<Store> for Get {
    async fn handle(self, store: Store) -> Result<Value, KvError> {
        store.get(&self.key).ok_or(KvError::NotFound(self.key))
    }
}
```

The `kv_server` example does this for a whole server.

Commands that share state implement `HandleWith` for the state type instead,
and the router is built with `Router::with_state`:

```rust,ignore
impl HandleWith<Store> for Get {
    async fn handle(self, store: Store) -> Result<Value, Error> {
        ...
    }
}

let router = Router::with_state(Store::default())
    .command::<Set>()
    .command::<Get>();
```

Every `Handle` is also a `HandleWith` for any state, so stateless commands
can join a router that has state.

A client
--

```rust,no_run
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tower_amp::{command, Client, Error};

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
    let client = Client::new(TcpStream::connect("127.0.0.1:1234").await?);
    let quotient = client.call(&Divide { numerator: 1234, denominator: 5 }).await?;
    println!("{}", quotient.result);

    match client.call(&Divide { numerator: 1, denominator: 0 }).await {
        Err(Error::Command { code, description }) => println!("{code}: {description}"),
        Err(other) => return Err(other.into()),
        Ok(quotient) => println!("{}", quotient.result),
    }
    Ok(())
}
```

A call returns the command's response type, so there is nothing to name at
the call site. A client needs only the `#[command]` struct, not a `Handle`
implementation, so the structs are the schema a client and server share,
like the command classes in Twisted. Put them in a module both sides use.

A client is cheap to clone and every clone shares the connection, with any
number of calls in flight at once. `notify` sends a command without asking
for a reply.

Set `TCP_NODELAY` on a `TcpStream` before handing it to `Client::new`, so
that a request does not wait for the acknowledgement of the one before it.
`serve` sets it on the connections it accepts.

A call waits until it is answered or the connection ends. `with_timeout`
puts a limit on that, after which the call fails with `Error::Timeout`:

```rust,ignore
let client = Client::new(stream).with_timeout(Duration::from_secs(5));
```

The limit belongs to the handle it was set on and the clones taken from it
afterwards, so a clone can take a longer one for a slow command. A reply
that arrives after its call timed out is discarded. Dropping a call before
it is answered, from your own `tokio::time::timeout` or `select!`, is safe
in the same way: the call is forgotten, and if its request had not been
written yet it is not sent.

Both directions
--

AMP is symmetric: either side of a connection may call commands on the
other. `Client::new` only calls and `serve` only answers; a `Connection`
does both. It hands out `Client` handles for the connection and takes a
service for the peer's requests, so a handler that needs to call the peer
that asked gets a handle through the router's state:

```rust,ignore
impl HandleWith<Client> for Relay {
    async fn handle(self, peer: Client) -> Result<Total, Error> {
        peer.call(&Sum { a: self.a, b: self.b }).await
    }
}

let connection = Connection::new(stream);
let router = Router::with_state(connection.client()).command::<Relay>();
connection.serve(router).await?;
```

A client that answers the commands its server sends is the same thing with
the roles swapped: take a handle, then serve the connection on a task of
its own.

```rust,ignore
let connection = Connection::new(stream);
let client = connection.client();
tokio::spawn(connection.serve(Router::new().command::<Notify>()));
```

`Connection::serve` runs until the peer closes the connection, or returns
the error that ended it. Dropping the handles does not end it; dropping the
`serve` future does. A call on a handle that outlives the connection fails
with `Error::ConnectionClosed`, carrying a copy of that error. A handle
that would rather not wait for a call to fail, such as one that
reconnects, awaits `Client::closed`, which resolves to the same cause once
the connection has ended; `Client::is_closed` says whether it has. `serve` and
`Server` take one service for every connection, so a server whose handlers
call back accepts connections itself and builds a `Connection` and a router
for each, as the `kv_server` example does.

WebAssembly
--

The client and `Connection` build for `wasm32-unknown-unknown` and
`wasm32-wasip2`, so a client in a browser can answer the commands its
server sends. The server does not build there, since tokio has no
networking on WebAssembly. `Client::new` and `Connection::new` take
anything that implements tokio's `AsyncRead` and `AsyncWrite`, so the
connection is whatever the host gives you: in a browser, a WebSocket
through one of the crates that adapt it to those traits; under WASI, a
socket from the host wrapped the same way.

In a browser the connection is driven on the page's event loop, through
`wasm-bindgen-futures`, and `with_timeout` uses the browser's timer. The I/O
type need not be `Send` there, since the browser has one thread, and the
client's futures are not `Send` either.

Under WASI the client behaves as it does natively: the connection is driven
by a task on the current tokio runtime, which must be a current-thread one,
and `with_timeout` uses tokio's timer.

Examples
--

The `examples` directory has runnable programs:

* `sum_server` is the canonical `Sum` and `Divide` server. It speaks the same
  commands as the client in Twisted's AMP documentation.
* `client` calls it.
* `kv_server` is a key/value store: commands sharing state, optional
  arguments, list results, a command with no results, tower middleware, an
  accept loop that reports connection errors, and a `Subscribe` command
  after which the server pushes every change to that client as a command
  on its own connection.
* `kv_watcher` subscribes to it, sets a key, and prints the changes the
  server pushes, so it serves commands on the connection it calls on.
* `middleware` wraps a server in a timing layer of its own and tower's
  timeout and concurrency limit, gives a timeout an error code of its own,
  and calls itself to show what the peer is told when each layer fails.

How it fits together
--

* [amp-protocol](https://crates.io/crates/amp-protocol) owns the wire format
  and the request and response types.
* [serde_amp](https://crates.io/crates/serde_amp) converts between your structs
  and a box of arguments or results. Its README lists the type mapping.
* This crate speaks tower: a service from an `amp_protocol::Request` to an
  `AmpBox` of results, with `tower_amp::Error` as the error. `Router` and
  `Client` are both such services, and `serve` accepts any such service,
  including one wrapped in tower middleware whose error type is a boxed
  error; see Middleware below.
* The `#[command]` attribute comes from the
  [tower-amp-macros](https://crates.io/crates/tower-amp-macros) crate and is
  re-exported here, so only this crate needs to be a dependency. The code it
  generates refers to this crate as `::tower_amp`, so it must be a dependency
  under its own name: renaming it with `package = "tower-amp"` is not
  supported.

Requests on one connection are handled concurrently, each on its own task,
and answered in the order they finish, as the protocol allows. At most 256
requests are in flight on a connection at once; past that the connection
stops reading until one has been answered, so a peer that sends without
reading its replies is held to a bounded amount of memory.

A peer that hangs up takes the handlers of its requests in flight with it:
their replies would have nowhere to go, and a peer that could leave them
running would be charged nothing for them. A cancelled handler is dropped
wherever it is, so a handler whose work must finish once begun should spawn
that work rather than await it. The handler of a notification, a request
that asked for no reply, is not cancelled: the peer never waited for it, and
may have hung up on purpose once it was sent. The idle timeout, five minutes
by default, gives such a handler that long after the hang-up, then cancels
it too, so a peer cannot leave handlers running for longer than the limit.

A box is read in full before it is handled, so a connection refuses one
longer than 1 MiB and closes rather than buffering without bound. It also
refuses one with more than 1,024 pairs, since a decoded pair costs far more
than the few bytes it can take on the wire. Pass an `AmpCodec` to
`Server::codec`, `Connection::with_codec`, or `Client::with_codec` to set
other limits. Under the defaults a connection holds at most the frame limit
for a box still arriving, and the in-flight limit times the frame limit,
256 MiB, for the arguments of requests whose handlers are still running;
the `Server` docs spell this out.

Server options
--

`serve` and `serve_connection` use the defaults above. `Server` is the same
server with its options set: the codec, how many requests a connection may
have in flight, how long one may sit idle, how many there may be at once,
and a future that stops it.

```rust,no_run
# use std::time::Duration;
# use tower_amp::{AmpCodec, Router, Server};
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let router = Router::new();
# let listener = tokio::net::TcpListener::bind("127.0.0.1:1234").await?;
Server::new(router)
    .codec(AmpCodec::with_max_frame_length(64 * 1024))
    .max_in_flight(32)
    .idle_timeout(Duration::from_secs(60))
    .max_connections(1_000)
    .shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .serve(listener)
    .await?;
# Ok(())
# }
```

`max_in_flight` bounds what one connection takes on, not what runs: fewer
holds a peer to less memory when a handler is expensive, more lets a trusted
peer pipeline deeper. Tower's `ConcurrencyLimit` on the service bounds the
handlers running across every connection, and its `Timeout` how long each
runs.

`idle_timeout` closes a connection over which nothing has arrived for that
long while no request was in flight, and one whose peer has not taken a
write of replies in that long, so a peer that connects and sends nothing, a
byte every so often, or requests whose replies it never reads cannot hold a
connection for good. The default is five minutes, so a client that keeps a
connection open between calls further apart than that finds it closed and
reconnects; `Server::no_idle_timeout` removes the limit for trusted peers.
`max_connections` is how many connections `serve` has open at once; at the
limit it stops accepting until one closes, and a peer that connects waits in
the listener's backlog. There is no limit by default. A server open to peers
it does not trust should set one.

The shutdown future stops the server once it completes: the listener is
closed, so a peer that connects from then on is refused, and each
connection stops reading requests, answers the ones it already has, and
closes. `serve` returns once the last connection has closed. A handler that
never finishes holds shutdown open, so wrap the call in `tokio::time::timeout`
if that matters.

`Server::serve_connection` serves one connection with the same options, for
an accept loop of your own; the shutdown future then stops that connection.

Middleware
--

`Router` is a tower service, so a layer from tower, or one of your own,
wraps it before it is served. The first layer in a `ServiceBuilder` is the
outermost.

```rust,no_run
# use std::time::Duration;
# use tower::{BoxError, ServiceBuilder};
# use tower_amp::{Error, Router, Server};
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let router = Router::new();
# let listener = tokio::net::TcpListener::bind("127.0.0.1:1234").await?;
let service = ServiceBuilder::new()
    .map_err(|err: BoxError| match err.downcast::<tower::timeout::error::Elapsed>() {
        Ok(_) => Error::new("TIMEOUT", "the command took too long"),
        Err(err) => Error::from(err),
    })
    .timeout(Duration::from_secs(5))
    .concurrency_limit(64)
    .service(router);
Server::new(service).serve(listener).await?;
# Ok(())
# }
```

`Timeout` and `ConcurrencyLimit` make the stack's error type tower's
`BoxError`, which `serve` accepts. A `tower_amp::Error` inside the box, a
handler's own, comes back out with its code intact. Anything else is
answered `UNKNOWN` with the description `internal error`, and the error
goes to the log: it was not written for the peer, who may be anyone. A
timeout's "request timed out" would be harmless, but a layer that queries a
database fails with the host it could not reach, and there is no telling
the two apart in general. A layer that wants the peer told something
specific says so by mapping its error to `Error::new`, as the `map_err`
above does for a timeout; the peer then gets that code and description, as
it would from a handler. The `middleware` example does this and writes a
layer of its own.

Logging
--

The `tracing` feature logs, through the
[tracing](https://crates.io/crates/tracing) crate, what a connection would
otherwise discard: a command that fails with anything but the handler's own
error, with the command's name, the ask and the error; a handler that
panics, with the panic's message; an accept error that `serve` skipped; and
a connection error that ended one of its connections. A handler's own
error, which the peer is told in full, is logged at `debug`. The feature
changes nothing on the wire.

```toml
tower-amp = { version = "0.3", features = ["tracing"] }
```

Not yet supported: TLS. A connection error inside `serve` ends that
connection and is only logged; write the accept loop yourself with
`serve_connection` or `Server::serve_connection` to handle it, and set
`TCP_NODELAY` on each stream yourself. `serve` itself survives a peer that
hangs up before it is accepted and a process briefly out of file
descriptors, and returns only on an error that means the listener is no
good.

License
--

tower-amp is licensed under the MIT license ([LICENSE-MIT](LICENSE-MIT) or
<http://opensource.org/licenses/MIT>).
