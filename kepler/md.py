import io
import time
import hashlib
import threading
import numpy as np
import cassandra.cluster
import cassandra.util

import smartdict
import parameter
import parameterdata
import parametertimeseries

_thread_lock = threading.Lock()

class MD():
    #_cluster = cassandra.cluster.Cluster(['188.184.77.145'], load_balancing_policy=cassandra.policies.WhiteListRoundRobinPolicy(['188.184.77.145']))
    _cluster = cassandra.cluster.Cluster()
    _session = _cluster.connect('varilog')
    _cluster.register_user_type('varilog', 'parameter', Parameter)
    
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
        self.name = name
        self._tag = None
        self.users = MDUsers(MD._session, self.name)
        self.comment = MDComment(MD._session, self.name)
        if tag is not None:
            self._tag_init(self)
       
    @property 
    def tag(self):
        return self._tag
       
    @tag.setter 
    def tag(self, value):
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
        self._ids = self._get_ids()
        self._devices = self._get_devices()
        self.cycles = Cycles(self.name, self.tag, self.devices, self._ids)
        print("MD found with %d cycles and %d devices." % (len(self._ids), len(self._devices.keys())))
        
    def _tag_clear(self):
        self._ids = None
        self._devices = None
        self._cycles = None
            
    @property
    def devices(self):
        return self._devices
        
    def _get_ids(self):
        ids = []
        rows = MD._session.execute("""
        SELECT id FROM md_info WHERE name=%s AND tag=%s
        """, (str(self.name), self.tag))
        for r in rows:
            ids.append(r[0])
        return ids    
    
    def _get_devices(self):
        id = self._ids[0]
        rows = MD._session.execute("""
        SELECT parameter, type FROM md_data WHERE name=%s AND tag=%s AND id=%s
        """, (str(self.name), self.tag, id))
        devices = {}
        for r in rows:
            p = r[0]
            t = r[1]
            if devices.get(p.device) is None:
                devices[p.device] = {p.property: {p.field: t}}
            else:
                if devices[p.device].get(p.property) is None:
                    devices[p.device][p.property] = {p.field: t}
                else:
                    devices[p.device][p.property][p.field] = t
        return devices
