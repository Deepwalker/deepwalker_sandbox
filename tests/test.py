import psyco
psyco.full()

from twisted.internet import reactor
from twisted.web import resource, server


class ClientPart(resource.Resource):
    isLeaf = True
    def render_GET(self, request):
        return "ku"


client_part_site = server.Site(ClientPart())

reactor.listenTCP(1981, client_part_site, 1000)
reactor.run()

