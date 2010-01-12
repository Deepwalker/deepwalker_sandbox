from twisted.internet import reactor
from tx_green import make_it_green
from tx_tokyo import Tyrant

@make_it_green
def test_proto():
    t = Tyrant() 
    print t.get_stats()

    t['kuku'] = 'Green Tyrant!'
    print t['kuku']

    print ">>> test of query"
    t['guido'] = {'name': 'Guido', 'age': 53}
    t['larry'] = {'name': 'Larry', 'age': 55}
    print t.query.filter(age__gt=53)

    reactor.stop()

if __name__=='__main__':
    test_proto()
    reactor.run()
