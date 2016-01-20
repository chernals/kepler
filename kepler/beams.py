import hashlib
import cassandra.cluster
import cassandra.util
from kepler.cycles import Cycles

class Beams(dict):
    
    def __init__(self, name, tag, devices, beams):
        self.__dict__ = {}
        for beamstamp in beams.keys():
            self[beamstamp.isoformat()] = Cycles(name, tag, devices, beamstamp, beams[beamstamp])
            
    def __call__(self):
        return self.__dict__
