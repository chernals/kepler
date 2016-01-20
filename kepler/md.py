import threading
import numpy as np

from kepler.udt import Parameter, Cycle, Dataset
from kepler.mdnames import MDNames
from kepler.mdtags import MDTags
from kepler.mdusers import MDUsers
from kepler.mdcomment import MDComment
from kepler.beams import Beams
from kepler.connection import _session

class MD():
    names = MDNames(_session)
    
    def __new__(cls, *args, **kwargs):
        """
        Prevent the creation of MD objects when name is not found.
        """
        # Note the call to 'update()'
        if str(args[0]) not in cls.names.update():
            print('MD name not found.')
            return None
        
        # Add another check if tag is defined then it should exist

        return object.__new__(cls)
            
    def __init__(self, name, tag=None):
        """Initialize and load metadata for a MD object.
        
        If the tag is not given the object will be empty and the rest is delayed.
        """
        self.name = name
        self._tag = tag
        self.users = MDUsers(_session, self.name)
        self.comment = MDComment(_session, self.name)
        if self._tag is not None:
            self._tag_init()
       
    @property
    def tag(self):
        """Return the tag name associated with the MD object.
        
        """
        return self._tag
       
    @tag.setter 
    def tag(self, value):
        """Associate a tag name with a MD object.
        
        This triggers the initialization of the metadata.
        """
        if value not in getattr(MD.names, str(self.name)).tags:
            print('Tag not found for the MD.')
            return None
        if self._tag is not None:
            self._tag_clear()
        self._tag = value
        self._tag_init()
        
    def _tag_init(self):
        if self._tag is None:
            return
        self._beams = self._get_beams()
        self._devices = self._get_devices()
        self.beams = Beams(self.name, self.tag, self.devices, self._beams)
        print("MD found with %d beams (%s cycles each)." % (len(self._beams), len(self._devices.keys())))
        
    def _tag_clear(self):
        """
        """
        self._beams = None
        self._devices = None
        self._cycles = None
            
    @property
    def devices(self):
        return self._devices
        
    def _get_beams(self):
        """Return a list of beamstamps for a given dataset and the cycles for each beamstamp.
        
        Simple query where we `select` the `beamstamp` and the `cycles` set.
        """
        beams = {}
        rows = _session.execute("""
        SELECT beamstamp, cycles FROM md_info WHERE name=%s AND tag=%s
        """, (str(self.name), self.tag))
        for r in rows:
            beams[r[0]] = list(r[1])
        return beams
        
    def _get_devices(self):
        devices = {}
        # We assume the beams are all the same in a given dataset
        beamstamp = list(self._beams.keys())[0]
        cycles = self._beams[beamstamp]
        # Get the device list for each cycle in the beam
        for cycle in cycles:
            rows = _session.execute("""
            SELECT parameter, type FROM md_data WHERE dataset=%s AND beamstamp = %s and cycle = %s
            """, (Dataset(self.name, self.tag), beamstamp, cycle))
            dev = {}
            for r in rows:
                p = r[0]
                t = r[1]
                if dev.get(p.device) is None:
                    dev[p.device] = {p.property: {p.field: t}}
                else:
                    if dev[p.device].get(p.property) is None:
                        dev[p.device][p.property] = {p.field: t}
                    else:
                        dev[p.device][p.property][p.field] = t
            devices[str(cycle)] = dev
        return devices
