tower-amp
==

![build-and-check](https://github.com/rockstar/serde_amp/actions/workflows/build-and-check.yml/badge.svg) ![crates.io](https://img.shields.io/crates/v/tower-amp.svg) ![docs.rs](https://docs.rs/tower-amp/badge.svg)

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
description; any other error, including arguments that fail to parse, goes
as `UNKNOWN` with the error's message. A handler that panics is answered the
same way, with the panic's message, and the connection stays up.

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

Examples
--

The `examples` directory has runnable programs:

* `sum_server` is the canonical `Sum` and `Divide` server. It speaks the same
  commands as the client in Twisted's AMP documentation.
* `client` calls it.
* `kv_server` is a key/value store: commands sharing state, optional
  arguments, list results, a command with no results, tower middleware, and
  an accept loop that reports connection errors.

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
and answered in the order they finish, as the protocol allows.

A box is read in full before it is handled, so a connection refuses one
longer than 16 MiB and closes rather than buffering without bound. Pass an
`AmpCodec` to `serve_with_codec`, `serve_connection_with_codec`, or
`Client::with_codec` to set another limit.

Not yet supported: commands the server initiates on a client, TLS, graceful
shutdown, and tracing. A connection error inside `serve` ends that connection
silently; write the accept loop yourself with `serve_connection` to see it.

License
--

tower-amp is licensed under the MIT license ([LICENSE-MIT](LICENSE-MIT) or
<http://opensource.org/licenses/MIT>).
