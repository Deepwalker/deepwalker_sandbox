from twisted.internet import defer, protocol, reactor
from tx_tokyo import TyrantProtocol

@defer.inlineCallbacks
def test_fail():
    print "kuku!"
    return 0/0

@defer.inlineCallbacks
def test_proto():
    cc = yield protocol.ClientCreator(reactor, TyrantProtocol).connectTCP("127.0.0.1",1978)

    try:
        yield test_fail()
    except:
        print "Error"

    yield cc.put('test','tutu123123123123123213')
    res = yield cc.get('test')
    print res
    reactor.stop()

if __name__=='__main__':
    test_proto()
    reactor.run()
