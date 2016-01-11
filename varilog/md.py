import io
import time
import threading
import numpy as np
import cassandra.cluster
import cassandra.util

class SmartDict():
    def __init__(self):
        self.__dict__ = {}
    
    def __getitem__(self, k):
        return self.__dict__[k]
    
    def __setitem__(self, k, v):
        self.__dict__[k] = v
        
    def _get(self, k):
        return self.__dict__.get(k)
        
    def __len__(self):
        return len(self.__dict__)
        
    def _keys(self):
        return self.__dict__.keys()

class Parameter(object):
    """
    Cassandra User Defined Type (UDT) for varilog.md_data(parameter).
    """
    def __init__(self, device, property, field):
        self.device = device
        self.property = property
        self.field = field
        
class ParameterData():
    
    def __init__(self, name, tag, p, id, type):
        self._p = p
        self._name = name
        self._tag = tag
        self._id = id
        self._type = type
        self._value = None
        
    def __getattr__(self, a):
        return getattr(self.value,a)
        
    @property
    def value(self):
        if self._value is None:
            r = MD._session.execute(MD._bound_statements['parameter_data'].bind(
                (self._name, self._tag, self._id, self._p)))
            r = r[0]
            self._value = _convert_object_from_cassandra(r[0], [r[1], r[2], r[3]])
        return self._value
        
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
        
def _convert_object_from_cassandra(t, values):
    if t == 'str':
        return values[1]
    elif t == 'scalar':
        return values[0]
    elif t == 'numpy':
        out = io.BytesIO(values[2])
        out.seek(0)
        return np.load(out)
        
class ParameterTimeseries():
    def __init__(self, name, tag, p):
        self._p = p
        self._name = name
        self._tag = tag
        self._values = None
        
    @property
    def values(self):
        if self._values is None:
            start_time = time.time()
            self._values = []
            rows = MD._session.execute(MD._bound_statements['parameter_timeseries'].bind(
                (self._name, self._tag, self._p,)))
            for r in rows:
                cyclestamp = cassandra.util.datetime_from_uuid1(r[4])
                self._values.append([cyclestamp, _convert_object_from_cassandra(r[0], [r[1], r[2], r[3]])])
            self._values = np.array(sorted(list(self._values), key=lambda x: x[0])) 
            print("Data queried in %f seconds." % (time.time()-start_time,))
        return self._values
    
    def __len__(self):
        return len(self.values)
    
    def __iter__(self):
        return self.values.__iter__()
    
    def __getitem__(self,key):
        return self.values[key]

    def __setitem__(self, k, v):
        print('Error')
        
    def __repr__(self):
        return str(self.values)
        
class Parameters():
    
    def __init__(self, name, tag, devices, id=None):
        self._name = name
        self._tag = tag
        self._devices = devices
        self._id = id
        self.__cache = None
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
                            self.__cache[d][p][f] = ParameterData(self._name, self._tag, Parameter(d,p,f), self._id, self._devices[d][p][f])
        return self.__cache 
        
class MDNames(list):
    
    def __init__(self, session):
        self._session = session
        self.update()
            
    def update(self):
        rows = self._session.execute("""
        SELECT DISTINCT name FROM md_info
        """)
        self.__dict__ = {'_session': self._session}
        self.clear()
        for r in rows:
            self.__dict__[r[0]] = MDTags(self._session, r[0])
            self.append(r[0])
        return list(self)
            
    def __dir__(self):
        return self.__dict__.keys()
            
class MDTags(list):
    
    def __init__(self, session, name):
        self._name = name
        rows = session.execute("""
        SELECT tag FROM md_info WHERE name = %s
        """, (self._name,))
        tags = set()
        for r in rows:
            tags.add(r[0])
        for t in tags:
            self.__dict__['tag'+str(t).replace('.', '_')] = t
            self.append(t)
          
    @property
    def tags(self):
        return [self.__dict__[k] for k in self.__dict__.keys() if not k.startswith('_')]
        
    def __repr__(self):
        return self._name
            
    def __str__(self):
        return self._name
            
    def __dir__(self):
        return self.__dict__.keys()

class MD():
    _cluster = cassandra.cluster.Cluster()
    _session = _cluster.connect('varilog')
    _cluster.register_user_type('varilog', 'parameter', Parameter)
    
    _bound_statements = {}
    _bound_statements['parameter_data'] = _session.prepare(
    """
    SELECT type, real_value, text_value, blob_value 
    FROM md_data 
    WHERE name = ? AND tag = ? AND id = ? AND parameter = ?
    """)
    _bound_statements['parameter_timeseries'] = _session.prepare(
    """
    SELECT type, real_value, text_value, blob_value, id 
    FROM md_data 
    WHERE name= ? AND tag= ? AND parameter= ?
    ALLOW FILTERING
    """)
    
    names = MDNames(_session)
    
    def __new__(cls, *args, **kwargs):
        """
        Prevent the creation of MD objects when name or tag is not found.
        """
        
        # Note the call to 'update()'
        if str(args[0]) not in cls.names.update():
            print('MD name not found.')
            return None
        if args[1] not in getattr(cls.names, str(args[0])).tags:
            print('Tag not found for the MD.')
            return None
            
        return object.__new__(cls)
            
    def __init__(self, name, tag): 
        self.name = name
        self.tag = tag
        self._ids = self._get_ids()
        self._devices = self._get_devices()
        self._users = None
        self._comment = None
        self.parameters = Parameters(self.name, self.tag, self._devices)
        self.cycles = {}
        self._init_cycles()
        print("MD found with %d cycles and %d devices." % (len(self._ids), len(self._devices.keys())))
        
    @property
    def comment(self):
        if self._comment is None:
            rows = MD._session.execute("""
            SELECT md_comment FROM md_info WHERE name = %s
            """, (str(self.name),))
            self._comment = rows[0][0]
        return self._comment
        
    @property
    def users(self):
        if self._users is None:
            rows = MD._session.execute("""
            SELECT users FROM md_info WHERE name = %s
            """, (str(self.name), ))
            self._users = rows[0][0]
        if self._users is None:
            return []
        else:
            return list(self._users)
            
    @property
    def devices(self):
        return self._devices
        
    def _init_cycles(self):
        for id in self._ids:
            k = cassandra.util.datetime_from_uuid1(id).isoformat()
            self.cycles[k] = Parameters(self.name, self.tag, self._devices, id)
        
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
        """, (str(self.name), self.tag, id ,))
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
