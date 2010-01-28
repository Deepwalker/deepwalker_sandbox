"Memcached cache backend"

from django.core.cache.backends.base import BaseCache, InvalidCacheBackendError
from django.utils.encoding import smart_unicode, smart_str

from twisted.internet import protocol, reactor
from twisted.protocols.memcache import MemCacheProtocol, DEFAULT_PORT
from tx_green import wait

class CacheClass(BaseCache):
    def __init__(self, server, params):
        BaseCache.__init__(self, params)

        host, port = server.split(':')
        self._cache = wait(protocol.ClientCreator(reactor, MemCacheProtocol).connectTCP(host, int(port)))

    def add(self, key, value, timeout=0):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        return wait(self._cache.add(smart_str(key), value,expireTime=(timeout or self.default_timeout)))

    def get(self, key, default=None):
        val = wait(self._cache.get(smart_str(key)))
        print val
        if val is None:
            return default
        else:
            val = val[1]
            if isinstance(val, basestring):
                return smart_unicode(val)
            else:
                return val

    def set(self, key, value, timeout=0):
        print key
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        wait(self._cache.set(smart_str(key), value,expireTime=(timeout or self.default_timeout)))

    def delete(self, key):
        wait(self._cache.delete(smart_str(key)))

    def get_many(self, keys):
        return wait(self._cache.get_multi(map(smart_str,keys)))

    def close(self, **kwargs):
        self._cache.timeoutConnection()

    def incr(self, key, delta=1):
        try:
            val = wait(self._cache.increment(key, delta))

        # python-memcache responds to incr on non-existent keys by
        # raising a ValueError. Cmemcache returns None. In both
        # cases, we should raise a ValueError though.
        except ValueError:
            val = None
        if val is None:
            raise ValueError("Key '%s' not found" % key)

        return val

    def decr(self, key, delta=1):
        try:
            val = wait(self._cache.decrement(key, delta))

        # python-memcache responds to decr on non-existent keys by
        # raising a ValueError. Cmemcache returns None. In both
        # cases, we should raise a ValueError though.
        except ValueError:
            val = None
        if val is None:
            raise ValueError("Key '%s' not found" % key)
        return val
