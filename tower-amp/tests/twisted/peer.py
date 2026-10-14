"""The Twisted side of tower-amp's interoperability test.

The commands are the `Sum` and `Divide` from Twisted's AMP documentation,
which `examples/sum_server.rs` and `tests/twisted.rs` implement in Rust,
plus `Relay`, which either side answers by calling `Sum` on the other: the
proof that commands go both ways on one connection, and `KitchenSink`, one
argument of every AMP type, which either side answers by checking each
argument against `KITCHEN_SINK` and echoing it back: the proof that
serde_amp's encoding of every row of its type-mapping table is what Twisted
reads, and that it reads what Twisted writes.

    peer.py client PORT   calls every command on the server at PORT and
                          exits 0 if every answer is what Twisted expects
    peer.py server        serves every command on a free port, prints the
                          port on its first line, and runs until killed

Run by `tests/twisted.rs`, which is ignored unless asked for; see
DEVELOPING.md.
"""

import datetime
import decimal
import sys

from twisted.internet import defer, reactor
from twisted.internet.protocol import ClientCreator, Factory
from twisted.protocols import amp


class Sum(amp.Command):
    arguments = [(b"a", amp.Integer()), (b"b", amp.Integer())]
    response = [(b"total", amp.Integer())]


class Divide(amp.Command):
    arguments = [(b"numerator", amp.Integer()), (b"denominator", amp.Integer())]
    response = [(b"result", amp.Float())]
    errors = {ZeroDivisionError: b"ZERO_DIVISION"}


class Relay(amp.Command):
    arguments = [(b"a", amp.Integer()), (b"b", amp.Integer())]
    response = [(b"total", amp.Integer())]


# One argument per row of serde_amp's type-mapping table. The floats take
# the spellings that differ between Rust and Python (`1e+300` is a
# 301-digit integer from Rust; `inf` is spelled the same by both); the bytes
# are not UTF-8, so they could not pass as text; `never` is left out, which
# is how an optional argument is `None`. The timezone offset is not zero,
# since Twisted writes a zero offset as `-00:00` and the Rust side holds the
# date as text, so an echo of `+00:00` would not compare equal.
KITCHEN_SINK = {
    "count": -9223372036854775808,
    "text": "h\u00e9llo w\u00f6rld \u2713",
    "bytes": b"\x00\xff\x01",
    "yes": True,
    "no": False,
    "floats": [246.8, 1e300, float("inf"), float("-inf")],
    "price": decimal.Decimal("19.99"),
    "when": datetime.datetime(
        2026, 10, 14, 9, 30, 15, 250000, datetime.timezone(datetime.timedelta(hours=2))
    ),
    "ints": [-1, 0, 9223372036854775807],
    "items": [{"name": "a", "size": 1}, {"name": "b", "size": 2}],
    "maybe": 7,
    "never": None,
}


class KitchenSink(amp.Command):
    arguments = [
        (b"count", amp.Integer()),
        (b"text", amp.Unicode()),
        (b"bytes", amp.String()),
        (b"yes", amp.Boolean()),
        (b"no", amp.Boolean()),
        (b"floats", amp.ListOf(amp.Float())),
        (b"price", amp.Decimal()),
        (b"when", amp.DateTime()),
        (b"ints", amp.ListOf(amp.Integer())),
        (b"items", amp.AmpList([(b"name", amp.Unicode()), (b"size", amp.Integer())])),
        (b"maybe", amp.Integer(optional=True)),
        (b"never", amp.Integer(optional=True)),
    ]
    response = arguments


class Math(amp.AMP):
    @Sum.responder
    def sum(self, a, b):
        return {"total": a + b}

    @Divide.responder
    def divide(self, numerator, denominator):
        return {"result": numerator / denominator}

    @Relay.responder
    def relay(self, a, b):
        return self.callRemote(Sum, a=a, b=b)

    @KitchenSink.responder
    def kitchen_sink(self, **arguments):
        assert arguments == KITCHEN_SINK, arguments
        return arguments


@defer.inlineCallbacks
def call_everything(port):
    # `Math` rather than a bare `amp.AMP`, so that the server can call `Sum`
    # back on this client while answering `Relay`.
    proto = yield ClientCreator(reactor, Math).connectTCP("127.0.0.1", port)
    total = yield proto.callRemote(Sum, a=13, b=81)
    assert total == {"total": 94}, total
    total = yield proto.callRemote(Relay, a=5, b=6)
    assert total == {"total": 11}, total
    quotient = yield proto.callRemote(Divide, numerator=1234, denominator=5)
    assert quotient == {"result": 246.8}, quotient
    echo = yield proto.callRemote(KitchenSink, **KITCHEN_SINK)
    assert echo == KITCHEN_SINK, echo
    try:
        yield proto.callRemote(Divide, numerator=1, denominator=0)
    except ZeroDivisionError:
        pass
    else:
        raise AssertionError("dividing by zero was answered")
    proto.transport.loseConnection()


def client(port):
    status = [1]

    def succeeded(_):
        status[0] = 0

    def failed(failure):
        failure.printTraceback(sys.stderr)

    done = call_everything(port)
    done.addCallbacks(succeeded, failed)
    done.addBoth(lambda _: reactor.stop())
    reactor.run()
    sys.exit(status[0])


def server():
    listening = reactor.listenTCP(0, Factory.forProtocol(Math), interface="127.0.0.1")
    print(listening.getHost().port, flush=True)
    reactor.run()


if __name__ == "__main__":
    if sys.argv[1:2] == ["client"] and len(sys.argv) == 3:
        client(int(sys.argv[2]))
    elif sys.argv[1:] == ["server"]:
        server()
    else:
        sys.exit(__doc__)
