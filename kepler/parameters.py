from kepler.smartdict import SmartDict
from kepler.udt import Parameter
from kepler.parameterdata import ParameterData
from kepler.parametertimeseries import ParameterTimeseries

class Parameters():
    #Parameters(name, tag, devices, beamstamp, c, self)
    def __init__(self, name, tag, devices, beamstamp=None, cycle=None, cycles=None):
        self._name = name
        self._tag = tag
        self._devices = devices
        self._beamstamp = beamstamp
        self._cycle = cycle
        self.__cache = None
        self._cycles = cycles
        if beamstamp is None:
            self._cache
        
    def __dir__(self):
        return self._cache.keys()
        
    def __getattr__(self, device):
        return self._cache[device]
                
    @property
    def _cache(self):    
        if self.__cache is None:
            self.__cache = {}
            for d in self._devices[str(self._cycle)].keys():
                self.__cache[d] = SmartDict()
                for p in self._devices[str(self._cycle)][d].keys():
                    self.__cache[d][p] = SmartDict()
                    for f in self._devices[str(self._cycle)][d][p].keys():
                        if self._beamstamp is None:
                            self.__cache[d][p][f] = ParameterTimeseries(self._name, self._tag, Parameter(d,p,f))
                        else:
                            self.__cache[d][p][f] = ParameterData(self._name, self._tag, Parameter(d,p,f), self._beamstamp, self._cycle, self._devices[str(self._cycle)][d][p][f], self._cycles)
        return self.__cache 