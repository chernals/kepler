from kepler.smartdict import SmartDict
from kepler.parameter import Parameter
from kepler.parameterdata import ParameterData
from kepler.parametertimeseries import ParameterTimeseries

class Parameters():
    
    def __init__(self, name, tag, devices, id=None, cycles=None):
        self._name = name
        self._tag = tag
        self._devices = devices
        self._id = id
        self.__cache = None
        self._cycles = cycles
        if id is None:
            self._cache
        
    def __dir__(self):
        return self._cache.keys()
        
    def __getattr__(self, device):
        return self._cache[device]
                
    @property
    def _cache(self):        
        if self.__cache is None:
            self.__cache = {}
            for d in self._devices.keys():
                self.__cache[d] = SmartDict()
                for p in self._devices[d].keys():
                    self.__cache[d][p] = SmartDict()
                    for f in self._devices[d][p].keys():
                        if self._id is None:
                            self.__cache[d][p][f] = ParameterTimeseries(self._name, self._tag, Parameter(d,p,f))
                        else:
                            self.__cache[d][p][f] = ParameterData(self._name, self._tag, Parameter(d,p,f), self._id, self._devices[d][p][f], self._cycles)
        return self.__cache 