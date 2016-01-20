import hashlib
import cassandra.cluster
import cassandra.util
from kepler.parameters import Parameters

class Cycles():

    def __init__(self, name, tag, devices, cycles, beamstamp=None):
        self.__dict__ = {}
        self._caching = {}
        for c in cycles:
            self.__dict__[str(c)] = Parameters(name, tag, devices, c, beamstamp, self)
    
    #def __call__(self):
    #    return self.__dict__
        
    def _set_caching(self, p):
        h = hashlib.md5((p.device+p.property+p.field).encode('utf-8')).hexdigest()
        self._caching[h] = True
            
    def _get_caching(self, p):
        h = hashlib.md5((p.device+p.property+p.field).encode('utf-8')).hexdigest()
        if self._caching.get(h) is None:
            return False
        else:
            return self._caching[h]