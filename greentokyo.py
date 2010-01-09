from twisted.internet import protocol, reactor

from tx_green import inlineCallbacks, wait
from tx_tokyo import TyrantProtocol, TyrantError

# Table Types
DBTYPEBTREE = 'B+ tree'
DBTYPETABLE = 'table'
DBTYPEMEMORY = 'on-memory hash'
DBTYPEHASH = 'hash'

def _parse_elem(elem, dbtype, sep=None):
    if dbtype == DBTYPETABLE:
        # Split element by \x00 which is the column separator
        elems = elem.split('\x00')
        if not elems[0]:
            return None

        return dict((elems[i], elems[i + 1]) \
                        for i in xrange(0, len(elems), 2))
    elif sep and sep in elem:
        return elem.split(sep)

    return elem

class Tyrant(dict):
    def __init__(self, host='127.0.0.1', port=1978, separator=None, literal=False):
        self.proto = wait(protocol.ClientCreator(reactor, TyrantProtocol).connectTCP(host,port))
        self.dbtype = self.get_stats()['type']
        self.separator = separator
        self.literal = literal

    def __contains__(self, key):
        try:
            wait(self.proto.vsiz(key))
        except TyrantError:
            return False
        else:
            return True

    def __delitem__(self, key):
        try:
            return wait(self.proto.out(key))
        except TyrantError:
            raise KeyError(key)

    def __getitem__(self, key):
        try:
            return _parse_elem(wait(self.proto.get(key, self.literal)), self.dbtype,
                               self.separator)
        except TyrantError:
            raise KeyError(key)

    def __setitem__(self, key, value):
        if isinstance(value, dict):
            flat = _itertools.chain([key], *value.iteritems())
            wait(self.proto.misc('put', list(flat)))
            
        elif isinstance(value, (list, tuple)):
            assert self.separator, "Separator is not set"

            flat = self.separator.join(value)
            wait(self.proto.put(key, flat))

        else:
            wait(self.proto.put(key, value))

    def get_stats(self):
        """Get the status string of the database.
        The return value is the status message of the database.The message 
        format is a dictionary. 
        """ 
        return dict(l.split('\t', 1) \
                        for l in wait(self.proto.stat()).splitlines() if l)
