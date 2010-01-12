from twisted.internet import defer, protocol, reactor
from tx_green import make_it_green, wait

from tx_tokyo import TyrantProtocol, TyrantError

def pr(res):
    print "Callback:",res

@make_it_green
def test_fail():
    print "kuku!"
    return 0/0

from twisted.web import client
@make_it_green
def test_proto():
    cc = wait(protocol.ClientCreator(reactor, TyrantProtocol).connectTCP("127.0.0.1",1978))
    try:
        wait(test_fail())
    except Exception, e:
        print "Really! Exception!", e

    
    client.getPage("http://www.ru").addBoth(pr)

    print 'Set:',wait(cc.put('test','tutu 123123123123123213'))
    print 'Get:',wait(cc.get('test'))
    #reactor.stop()

if __name__=='__main__':
    test_proto()
    reactor.run()

