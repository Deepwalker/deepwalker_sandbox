from twisted.internet import reactor
from twisted.application import internet, service 
from twisted.web import resource, server


class ClientPart(resource.Resource):
    isLeaf = True
    def render_GET(self, request):
        return "ku"


application = service.Application('LongPoll service') 
client_part_site = server.Site(ClientPart())
client_service = internet.TCPServer(1981, client_part_site, 1000)
client_service.setServiceParent(application)

