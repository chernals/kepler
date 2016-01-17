import threading
import kepler
from kepler.connection import _session
from kepler import cqlstatements
from kepler.utils import _convert_object_from_cassandra

class ParameterData():
    
    class cachingThread(threading.Thread):
        
        def __init__(self, cycles, p):
            threading.Thread.__init__(self)
            self._cycles = cycles
            self._p = p
            
        def run(self):
            p = self._p
            for c in self._cycles.values():
                getattr(getattr(getattr(c, p.device), p.property), p.field).value_async()
    
    def __init__(self, name, tag, p, id, type, cycles):
        self._p = p
        self._name = name
        self._tag = tag
        self._id = id
        self._type = type
        self._value = None
        self._cycles = cycles
        
    def __getattr__(self, a):
        return getattr(self.value,a)
        
    @property
    def value(self):
        if self._value is None:
            if kepler._cache_flag:
                with kepler._thread_lock:
                    if not self._cycles._get_caching(self._p):
                        self._cycles._set_caching(self._p)
                        ParameterData.cachingThread(self._cycles, self._p).start()
            r = _session.execute(cqlstatements._bound_statements['parameter_data'].bind(
                (self._name, self._tag, self._id, self._p)))
            r = r[0]
            self._value = _convert_object_from_cassandra(r[0], [r[1], r[2], r[3]])
        return self._value
        
    @property
    def _value(self):
        return self.__value
        
    @_value.setter
    def _value(self, v):
        with kepler._thread_lock:
            self.__value = v

    def value_async(self):
        if self._value is None:
            future = _session.execute_async(cqlstatements._bound_statements['parameter_data'].bind(
                (self._name, self._tag, self._id, self._p)))
            future.add_callback(self._get_async_success)
        
    def _get_async_success(self, r):
        if r is None:
            print("Error retrieving a value for the cache")
        r = r[0]
        self._value = _convert_object_from_cassandra(r[0],[r[1], r[2], r[3]])
        
    def _get_async_error(self, exception):
        print("Error")
        
    def __call__(self):
        return self.value
        
    def __dir__(self):
        if self._type == 'numpy':
            return dir(np.ndarray)
        else:
            return self.__dict__
        
    def __int__(self):
        return int(self.value)
        
    def __float__(self):
        return float(self.value)
        
    def __str__(self):
        return str(self.value)
        
    def __repr__(self):
        return str(self.value)