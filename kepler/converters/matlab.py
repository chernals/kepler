from numbers import Number
import re 
import os
import io
from datetime import datetime, timedelta
import numpy as np
import scipy.io
from cassandra.cluster import Cluster
import cassandra.util

class Parameter(object):
    
    def __init__(self, d, p, f):
        self.device = d
        self.property = p
        self.field = f

class Converter(object):
    
    def __init__(self, *args, **kwargs):
        self.path = kwargs['path']
        self.md_name = kwargs['name']
        self.md_tag = str(kwargs['tag'])
        self.host = str(kwargs['host'])
        self.cluster = Cluster([self.host])
        self.cluster.register_user_type('kepler', 'parameter', Parameter)
        self.session = self.cluster.connect('kepler')
        self._prepare_insert_statements()
        self.counter_timestamps = 0
        self._load_archive()
        print('Timestamps inserted: %d' % self.counter_timestamps)
        
    def _prepare_insert_statements(self):
        self.bound_statement_real = self.session.prepare(
        """
        INSERT INTO md_data(
            name,
            tag,
            id,
            parameter, 
            real_value, 
            type
        ) VALUES (?, ?, ?, ?, ?, ?)
        """)
        self.bound_statement_text = self.session.prepare(
        """
        INSERT INTO md_data(
            name,
            tag,
            id,
            parameter, 
            text_value, 
            type
        ) VALUES (?, ?, ?, ?, ?, ?)
        """)
        self.bound_statement_blob = self.session.prepare(
        """
        INSERT INTO md_data(
            name,
            tag,
            id,
            parameter, 
            blob_value, 
            type
        ) VALUES (?, ?, ?, ?, ?, ?)
        """)

    def _load_archive(self):
        matfiles = os.listdir(self.path)
        for f in matfiles:
            if not f.endswith('.mat'):
                continue
            print("Processing %s" % f)
            # Magic values for 'squeeze_me' and 'struct_as_record'
            data = scipy.io.loadmat(self.path+f, squeeze_me=True, struct_as_record=False)
            d = data['myDataStruct']
            cyclestamp = datetime.fromtimestamp(d.headerCycleStamps[0]/1000000000)+timedelta(days=1)
            self.counter_timestamps += 1
            if self._check_not_present(cyclestamp):
                self._process_archive(d, cyclestamp)
    
    def _check_not_present(self, cyclestamp):
        return True
        count = self.session.execute("""
        SELECT COUNT(*) FROM md_info WHERE 
            name = %s and tag = %s and cyclestamp = %s
        """, (self.md_name, self.md_tag, cyclestamp))
        if count[0][0] > 0:
            return False
        else:
            return True
            
    def _insert_telegram(self, d):
        telegram = {}
        telegram['selector'] = d.cycleName
        telegram['seqnumber'] = str(d.seqNumber)
        self.session.execute("""
        INSERT INTO md_data(name, tag, id, telegram) VALUES(%s, %s, %s,%s)
        """,(self.md_name, self.md_tag, self.timeuuid, telegram))
    
    def _process_archive(self, d, cyclestamp):
        comment = d.comment
        self.timeuuid = cassandra.util.uuid_from_time(cyclestamp)
        self.session.execute("""
        INSERT INTO md_info(name, tag, id, comment, cyclestamp)
        VALUES(%s, %s, %s, %s, %s)
        """, (self.md_name,
              self.md_tag,
              self.timeuuid,
              comment,
              cyclestamp))
        self._insert_telegram(d)      
        for p in d.parameters:
            # Special case for LSA settings
            # Parameter: rmi://lsa/name
            if "rmi://lsa" in p:
                self._convert_lsa(p, d)    
                continue
                
            # Property acquired with only one field
            r = re.match(r"(.*)/(.*)#(.*)", p)
            if r:
                self._convert_single_field(r, d)
                continue
            
            # Property acquired with all fields
            # Parameter: device/property
            r = re.match(r"(.*)/(.*)(?!#)(.*)", p)
            if r:
                self._convert_whole_property(r, d)
                continue
     
    def _convert_single_field(self, r, d):
        device = r.group(1)
        device = device.replace('.', '_')
        device = device.replace('-', '_')
        prop = r.group(2)
        field = r.group(3)
        value = d.__dict__[device].__dict__[prop].__dict__[field].value.real
        self._insert(device, prop, field, value)
        
    def _convert_whole_property(self, r, d):
        device = r.group(1)
        device = device.replace('.', '_')
        device = device.replace('-', '_')
        prop = r.group(2)
        val = d.__dict__[device].__dict__[prop].value
        # Property does not contain fields, but only a value (GM)
        if not isinstance(val, scipy.io.matlab.mio5_params.mat_struct):
            self._insert(device, prop, 'value', val)
        # Standard case where a property contains many fields (FESA)
        else:
            for field, value in d.__dict__[device].__dict__[prop].value.__dict__.items():
                if field.startswith('_'):
                    continue
                self._insert(device, prop, field, value)
        
    def _convert_lsa(self, p, d):
        # Example: rmi://lsa/LEIRBEAM/coolerBump_CTRS20_H_1mm
        device = 'lsa'
        sub = re.match(r'rmi://lsa/(.*)/(.*)', p)
        prop = sub.group(1)
        field = sub.group(2)
        value = d.__dict__[prop].__dict__[field].value
        self._insert(device, prop, field, value)
                    
    def _insert(self, device, prop, field, value):
        if isinstance(value, str):
            self._insert_value(device, prop, field, value, 'str', 'text_value')         
        elif isinstance(value, Number):
            self._insert_value(device, prop, field, value, 'scalar', 'real_value')    
        elif isinstance(value, np.ndarray):
            out = io.BytesIO()
            np.save(out, value)
            out.seek(0)
            value = out.read()
            self._insert_value(device, prop, field, value, 'numpy', 'blob_value')
        elif isinstance(value, scipy.io.matlab.mio5_params.mat_struct):
            for k, v in value.__dict__.items():
                if k == '_fieldnames': continue
                self._insert(device, prop, field+'_'+k, v)
                return
        else:
            print('Error with type %s' % type(value))
            print(value)
                
    def _insert_value(self, device, prop, field, value, t, tf):
        if tf == 'text_value':
            self.session.execute(self.bound_statement_text.bind((
                self.md_name, self.md_tag, self.timeuuid, Parameter(device, prop, field), value, t)))
        elif tf == 'blob_value':
            self.session.execute(self.bound_statement_blob.bind((
                self.md_name, self.md_tag, self.timeuuid, Parameter(device, prop, field), value, t)))
        elif tf == 'real_value':
            self.session.execute(self.bound_statement_real.bind((
                self.md_name, self.md_tag, self.timeuuid, Parameter(device, prop, field), value, t)))
        