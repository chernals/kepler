import hashlib
import cassandra.cluster
import cassandra.util
from kepler.cycles import Cycles
from kepler.parameters import Parameters

class Beams(dict):
    
    def __init__(self, name, tag, devices, beams):
        self.__dict__ = {}
        for beamstamp in beams.keys():
            self[beamstamp.isoformat()] = Cycles(name, tag, devices, beams[beamstamp], beamstamp)
            
        for cycle in devices.keys():
            cc = list(beams.values())[0][0]
            for c in list(beams.values())[0]:
                if str(c) == cycle:
                    cc = c
            self.__dict__[cycle] = Parameters(name, tag, devices, cc)
