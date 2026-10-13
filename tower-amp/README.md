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
with `Error::ConnectionClosed`, carrying a copy of that error. `serve` and
`serve_with_shutdown` take one service for every connection, so a server
whose handlers call back accepts connections itself and builds a
`Connection` and a router for each, as the `kv_server` example does.

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

How it fits together
--

* [amp-protocol](https://crates.io/crates/amp-protocol) owns the wire format
  and the request and response types.
* [serde_amp](https://crates.io/crates/serde_amp) converts between your structs
  and a box of arguments or results. Its README lists the type mapping.
* This crate speaks tower: a service from an `amp_protocol::Request` to an
  `AmpBox` of results, with `tower_amp::Error` as the error. `Router` and
  `Client` are both such services, and `serve` accepts any such service,
  including one wrapped in tower middleware whose error type is a boxed error.
* The `#[command]` attribute comes from the
  [tower-amp-macros](https://crates.io/crates/tower-amp-macros) crate and is
  re-exported here, so only this crate needs to be a dependency.

Requests on one connection are handled concurrently, each on its own task,
and answered in the order they finish, as the protocol allows. At most 256
requests are in flight on a connection at once; past that the connection
stops reading until one has been answered, so a peer that sends without
reading its replies is held to a bounded amount of memory.

A box is read in full before it is handled, so a connection refuses one
longer than 16 MiB and closes rather than buffering without bound. Pass an
`AmpCodec` to `serve_with_codec`, `serve_connection_with_codec`, or
`Client::with_codec` to set another limit.

Shutdown
--

`serve_with_shutdown` takes a future along with the listener and the service,
and stops once it completes: the listener is closed, so a peer that connects
from then on is refused, and each connection stops reading requests, answers
the ones it already has, and closes. It returns once the last connection has
closed.

```rust,no_run
# use tower_amp::Router;
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let router = Router::new();
# let listener = tokio::net::TcpListener::bind("127.0.0.1:1234").await?;
tower_amp::serve_with_shutdown(listener, router, async {
    let _ = tokio::signal::ctrl_c().await;
})
.await?;
# Ok(())
# }
```

A handler that never finishes holds shutdown open, so wrap the call in
`tokio::time::timeout` if that matters. `serve_with_codec_and_shutdown` takes
a codec as well.

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
tower-amp = { version = "0.2", features = ["tracing"] }
```

Not yet supported: TLS. A
connection error inside `serve` ends that connection and is only logged;
write the accept loop yourself with `serve_connection` to handle it, at the
cost of `serve_with_shutdown`, which has no connection-level counterpart,
and set `TCP_NODELAY` on each stream yourself. `serve` itself survives a peer that hangs up before it is
accepted and a process briefly out of file descriptors, and returns only on
an error that means the listener is no good.

License
--

tower-amp is licensed under the MIT license ([LICENSE-MIT](LICENSE-MIT) or
<http://opensource.org/licenses/MIT>).
