#!/usr/bin/env python
# coding: utf-8

#from py.magic import greenlet
from greenlet import greenlet
from twisted.internet import defer, protocol, reactor
from twisted.python.util import mergeFunctionMetadata
from twisted.python import failure

############################
# main = greenlet().parent

def _inlineCallbacks(result, g, argsp, deferred):
    waiting = [True, # waiting for result?
               None] # result

    while 1:
        try:
            # throw exception into greenlet
            if isinstance(result, failure.Failure):
                result = g.throw(result.type, result.value, result.tb)
            else:
                if argsp and not result:
                    # First start
                    result = g.switch(*argsp[0], **argsp[1])
                else:
                    result = g.switch(result)

        except greenlet.GreenletExit:
            g.throw(greenlet.GreenletExit)

        except Exception, e:
            deferred.errback()
            return deferred

        # Greenlet was ended
        if g.dead:
            deferred.callback(result)
            return deferred

        if isinstance(result, defer.Deferred):
            # a deferred was yielded, get the result.
            def gotResult(r):
                if waiting[0]:
                    waiting[0] = False
                    waiting[1] = r
                else:
                    _inlineCallbacks(r, g, None, deferred)

            result.addBoth(gotResult)
            if waiting[0]:
                # Haven't called back yet, set flag so that we get reinvoked
                # and return from the loop
                waiting[0] = False
                return deferred

            result = waiting[1]
            # Reset waiting to initial values for next loop.  gotResult uses
            # waiting, but this isn't a problem because gotResult is only
            # executed once, and if it hasn't been executed yet, the return
            # branch above would have been taken.


            waiting[0] = True
            waiting[1] = None


    return deferred

def inlineCallbacks(f):
    def unwindGenerator(*args, **kwargs):
        g = greenlet(f)
        # Похоже, что такой тупой ход парента не меняет.
        # Также похоже, что и без него работает, оставляя выполнение в текущем гринлете.
        #g.parent = main
        return _inlineCallbacks(None, g, (args, kwargs), defer.Deferred())
    return mergeFunctionMetadata(f, unwindGenerator)

def wait(d):
    return greenlet.getcurrent().parent.switch(d)