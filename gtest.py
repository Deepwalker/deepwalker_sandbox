from twisted.internet import defer, protocol, reactor
from tx_green import inlineCallbacks, wait
from greentokyo import Tyrant

@inlineCallbacks
def test_proto():
    t = Tyrant() 
    print t.get_stats()

    t['kuku'] = 'Green Tyrant!'
    print t['kuku']

    reactor.stop()

if __name__=='__main__':
    test_proto()
    reactor.run()
