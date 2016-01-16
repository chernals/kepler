class Parameter(object):
    """
    Cassandra User Defined Type (UDT) for kepler.md_data(parameter).
    """
    def __init__(self, device, property, field):
        self.device = device
        self.property = property
        self.field = field