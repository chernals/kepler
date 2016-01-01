from numbers import Number
import os
import io
from datetime import datetime
import numpy as np
import scipy.io
from cassandra.cluster import Cluster

class Converter(object):
    
    def __init__(self, *args, **kwargs):
        self.cluster = Cluster()
        self.session = self.cluster.connect('varilog')
        self.path = kwargs['path']
        self.md_name = kwargs['name']
        self.md_tag = kwargs['tag']
        self.md_comment = kwargs['comment']
        self._convert_and_insert()

    def _convert_and_insert(self):
        matfiles = os.listdir(self.path)
        for f in matfiles:
            if not f.endswith('.mat'):
                continue
            print(f)
            data = scipy.io.loadmat(self.path+f, squeeze_me=True, struct_as_record=False)
            d = data['myDataStruct']
            self._insert_to_cassandra(d)
    
    def _insert_to_cassandra(self, d):
        cyclestamp = datetime.fromtimestamp(d.headerCycleStamps[0]/1000000000)
        for p in d.parameters:
            device, prop = p.split('/')
            dev = device.replace('.', '_')
            if len(prop.split('#')) == 1:
                for field, value in d.__dict__[dev].__dict__[prop].value.__dict__.items():
                    if field.startswith('_'):
                        continue
                    if isinstance(value, str):
                        self._insert_text_value(cyclestamp, device, prop, field, value)
                    elif isinstance(value, Number):
                        self._insert_scalar_value(cyclestamp, device, prop, field, value)
                    elif isinstance(value, np.ndarray):
                        self._insert_numpy_value(cyclestamp, device, prop, field, value)
                    else:
                        print('Error %s' % type(value))
                        print(field)
                        print(value)
                        break
            if len(prop.split('#')) == 2:
                prop, field = prop.split('#')
                value = d.__dict__[dev].__dict__[prop].__dict__[field].value.real
                self._insert_scalar_value(cyclestamp, device, prop, field, value)
                
    def _insert_scalar_value(self, cyclestamp, device, prop, field, value):
        self.session.execute("""
        INSERT INTO md_data(name, tag, comment, cyclestamp, device, property, field, real_value, type) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (self.md_name, self.md_tag, self.md_comment, cyclestamp, device, prop, field, value, 'scalar'))
        
    def _insert_text_value(self, cyclestamp, device, prop, field, value):
        self.session.execute("""
        INSERT INTO md_data(name, tag, comment, cyclestamp, device, property, field, text_value, type) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (self.md_name, self.md_tag, self.md_comment, cyclestamp, device, prop, field, value, 'str'))
        
    def _insert_numpy_value(self, cyclestamp, device, prop, field, value):
        out = io.BytesIO()
        np.save(out, value)
        out.seek(0)
        value = out.read()
        self.session.execute("""
        INSERT INTO md_data(name, tag, comment, cyclestamp, device, property, field, blob_value, type) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (self.md_name, self.md_tag, self.md_comment, cyclestamp, device, prop, field, value, 'numpy'))