"""The Twisted side of tower-amp's interoperability test.

The commands are the `Sum` and `Divide` from Twisted's AMP documentation,
which `examples/sum_server.rs` and `tests/twisted.rs` implement in Rust,
plus `Relay`, which either side answers by calling `Sum` on the other: the
proof that commands go both ways on one connection.

    peer.py client PORT   calls both commands on the server at PORT and
                          exits 0 if every answer is what Twisted expects
    peer.py server        serves both commands on a free port, prints the
                          port on its first line, and runs until killed

Run by `tests/twisted.rs`, which is ignored unless asked for; see
DEVELOPING.md.
"""

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
