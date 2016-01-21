from datetime import datetime

class Parameter(object):
    """
    Cassandra User Defined Type (UDT) for kepler.md_data(parameter).
    """
    def __init__(self, device, property, field):
        self.device = device
        self.property = property
        self.field = field
                
class Cycle():
    """
    Cassandra User Defined Type (UDT) for kepler.md_data(cycle).
    """
    def __init__(self, machine, injection, cyclestamp):
        #super().__init__(self, cyclestamp)
        self.machine = machine
        self.injection = injection
        self.cyclestamp = cyclestamp
        
    def __repr__(self):
        return str(self)
        
    def __str__(self):
        return "%s_%s" % (self.machine, self.injection)
    
class Dataset(object):
    """
    Cassandra User Defined Type (UDT) for kepler.md_data(dataset).
    """
    def __init__(self, name, tag):
        self.name = name
        self.tag = tag