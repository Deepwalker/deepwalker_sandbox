
import itertools as _itertools

from tx_green import wait
from protocol import TyrantProtocol, TyrantError
from query import Query

from twisted.internet import protocol, reactor

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

    def call_func(self, func, key, value, record_locking=False,
                  global_locking=False):
        """Calls specific function."""
        # TODO: write better documentation *OR* move this method to lower level
        opts = ((record_locking and TyrantProtocol.RDBXOLCKREC) |
                (global_locking and TyrantProtocol.RDBXOLCKGLB))
        return wait(self.proto.ext(func, opts, key, value))

    def clear(self):
        """Removes all records from the remote database."""
        wait(self.proto.vanish())

    def concat(self, key, value, width=None):
        """Concatenates columns of the existing record."""
        # TODO: write better documentation, provide example code
        if width is None:
            wait(self.proto.putcat(key, value))
        else:
            wait(self.proto.putshl(key, value, width))

    def get_size(self, key):
        """Returns the size of the value for `key`."""
        try:
            return wait(self.proto.vsiz(key))
        except TyrantError:
            raise KeyError(key)

    def get_stats(self):
        """Returns the status message of the database as dictionary."""
        return utils.csv_to_dict(wait(self.proto.stat()))

    def iterkeys(self):
        """Iterates keys using remote operations."""
        self.proto.iterinit()
        try:
            while True:
                yield wait(self.proto.iternext())
        except TyrantError:
            pass

    def keys(self):
        """Returns the list of keys in the database."""
        return list(self.iterkeys())

    def update(self, dict=None, **kwargs):
        """
        Updates given objets from a dict, list of key and value pairs or a list of named params.

        See update method in python built-in object for more info
        """
        data = {}
        if dict:
            data.update(dict, **kwargs)
        else:
            data.update(**kwargs)
        self.multi_set(data)

    def multi_del(self, keys, no_update_log=False):
        """Removes given records from the database."""
        # TODO: write better documentation: why would user need the no_update_log param?
        opts = (no_update_log and TyrantProtocol.RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)

        wait(self.proto.misc("outlist", keys, opts))

    def multi_get(self, keys, no_update_log=False):
        """Returns a list of records that match given keys."""
        opts = (no_update_log and TyrantProtocol.RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)

        rval = wait(self.proto.misc("getlist", keys, opts))

        if len(rval) <= len(keys):
            # 1.1.10 protocol, may return invalid results
            if len(rval) < len(keys):
                raise KeyError("Missing a result, unusable response in 1.1.10")

            return rval

        # 1.1.11 protocol returns interleaved key, value list
        d = dict((rval[i], to_python(rval[i + 1], self.dbtype,
                                       self.separator)) \
                    for i in xrange(0, len(rval), 2))
        return d

    def multi_set(self, items, no_update_log=False):
        """Stores given records in the database."""
        opts = (no_update_log and TyrantProtocol.RDBMONOULOG or 0)
        lst = []
        for k, v in items.iteritems():
            if isinstance(v, (dict)):
                new_v = []
                for kk, vv in v.items():
                    new_v.append(kk)
                    new_v.append(vv)
                v = new_v
            if isinstance(v, (list, tuple)):
                assert self.separator, "Separator is not set"

                v = self.separator.join(v)
            lst.extend((k, v))

        wait(self.proto.misc("putlist", lst, opts))

    def prefix_keys(self, prefix, maxkeys=None):
        """Get forward matching keys in a database.
        The return value is a list object of the corresponding keys.
        """
        # TODO: write better documentation: describe purpose, provide example code
        if maxkeys is None:
            maxkeys = len(self)

        return wait(self.proto.fwmkeys(prefix, maxkeys))

    def sync(self):
        """Synchronizes updated content with the database."""
        # TODO: write better documentation: when would user need this?
        wait(self.proto.sync())

    @property
    def query(self):
        """Returns a :class:`~pyrant.Query` object for the database."""
        return Query(self.proto, self.dbtype, self.literal)

