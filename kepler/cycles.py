import hashlib
import cassandra.cluster
import cassandra.util
from kepler.parameters import Parameters

class Cycles(dict):
    
    def __init__(self, name, tag, devices, ids):
        self.__dict__ = {}
        self._caching = {}
        self._p = Parameters(name, tag, devices)
        for k in devices.keys():
            self.__dict__[k] = getattr(self._p, k)
        for id in ids:
            k = cassandra.util.datetime_from_uuid1(id).isoformat()
            self[k] = Parameters(name, tag, devices, id, self)
            
    def __call__(self):
        return self.__dict__
        
    def _set_caching(self, p):
        h = hashlib.md5((p.device+p.property+p.field).encode('utf-8')).hexdigest()
        self._caching[h] = True
            
    def _get_caching(self, p):
        h = hashlib.md5((p.device+p.property+p.field).encode('utf-8')).hexdigest()
        if self._caching.get(h) is None:
            return False
        else:
            return self._caching[h]