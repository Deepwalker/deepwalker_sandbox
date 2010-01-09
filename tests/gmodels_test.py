from twisted.internet import reactor
from tx_green import inlineCallbacks
from tx_tokyo import Tyrant

from models import Model, Property, Date, Reference

class Country(Model):
    name = Property()

    def __unicode__(self):
        return self.name

    class Meta:
        must_have = {'type': 'country'}


class Person(Model):
    first_name = Property(required=True)
    last_name = Property(required=True)
    gender = Property()
    birth_date = Date()
    birth_place = Reference(Country)

    def __unicode__(self):
        return self.full_name    # full_name is a dynamic attribute, see below

    @property
    def age(self):
        return (datetime.datetime.now().date() - self.birth_date).days / 365

    @property
    def full_name(self):
        return '%s %s' % (self.first_name, self.last_name)

    class Meta:
        must_have = {'type': 'person'}

@inlineCallbacks
def test_proto():
    t = Tyrant() 
    john = Person('test___0001', first_name='John', birth_date='1901-02-03', last_name='Doe')
    john.birth_place = Country('test___0002', name='TestCountry')
    john.save(t)
    print Person.query(t)

    reactor.stop()

if __name__=='__main__':
    test_proto()
    reactor.run()

