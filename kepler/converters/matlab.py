from numbers import Number
import re 
import os
import io
from datetime import datetime, timedelta
import numpy as np
import scipy.io
from cassandra.cluster import Cluster
import cassandra.util
import kepler.connection
from kepler.udt import Parameter, Cycle, Dataset

class Converter():
    """
    Import Matlab-Japc .mat files onto Kepler.
    All the device/Property#field are processed.
    Metadata (timing user, etc.) go in the 'telegram' column.
    
    """
    
    def __init__(self, *args, **kwargs):
        self.path = kwargs['path']
        self.md_name = kwargs['name']
        self.md_tag = str(kwargs['tag'])
        self.dataset = Dataset(self.md_name, self.md_tag)
        self.machine = kwargs['machine']
        self.session = kepler.connection._session
        self._prepare_insert_statements()
        self.counter_timestamps = 0
        self._load_archive()
        print('Timestamps inserted: %d' % self.counter_timestamps)
        
    def _prepare_insert_statements(self):
        self.bound_statement_real = self.session.prepare(
        """
        INSERT INTO md_data(
            dataset,
            beamstamp,
            cycle,
            parameter, 
            real_value, 
            type
        ) VALUES (?, ?, ?, ?, ?, ?)
        """)
        self.bound_statement_text = self.session.prepare(
        """
        INSERT INTO md_data(
            dataset,
            beamstamp,
            cycle,
            parameter, 
            text_value, 
            type
        ) VALUES (?, ?, ?, ?, ?, ?)
        """)
        self.bound_statement_blob = self.session.prepare(
        """
        INSERT INTO md_data(
            dataset,
            beamstamp,
            cycle,
            parameter, 
            blob_value, 
            type
        ) VALUES (?, ?, ?, ?, ?, ?)
        """)

    def _load_archive(self):
        matfiles = os.listdir(self.path)
        for f in matfiles[0:1]:
            if not f.endswith('.mat'):
                continue
            print("Processing %s" % f)
            # Magic values for 'squeeze_me' and 'struct_as_record'
            data = scipy.io.loadmat(self.path+f, squeeze_me=True, struct_as_record=False)
            d = data['myDataStruct']
            cyclestamp = datetime.fromtimestamp(d.headerCycleStamps[0]/1000000000)
            self.counter_timestamps += 1
            if self._do_import_cycle_in_beam(cyclestamp):
                self._process_archive(d, cyclestamp)
    
    def _do_import_cycle_in_beam(self, cyclestamp):
        """
        Check if beamstamp is already present in the database.
        If it is not check to which machine and injection the cycle corresponds.
        If it is already present, abort.
        """
        rows = self.session.execute("""
        SELECT beamstamp, cycles FROM md_info WHERE 
            name = %s and tag = %s and beamstamp = %s
        """, (self.md_name, self.md_tag, cyclestamp))
        if not rows.current_rows:
            self.cycle = Cycle(self.machine, 1, cyclestamp)
            return True
        for r in rows:
            beamstamp = r[0]
            cycles = r[1]
            for c in cycles:
                print(self.machine)
                if c.cyclestamp == cyclestamp and c.machine == self.machine:
                    return False
                else:
                    injection = 1
                    if c.machine == self.machine:
                        if cyclestamp >= c.machine:
                            injection = c.injection + 1
                        else:
                            # Should reorder the whole thing
                            pass
                    self.cycle = Cycle(self.machine, 1, cyclestamp)
                    return True
            
    def _insert_telegram(self, d):
        telegram = {}
        telegram['selector'] = d.cycleName
        telegram['seqnumber'] = str(d.seqNumber)
        self.session.execute("""
        UPDATE md_data SET telegram = telegram + {%s: %s} WHERE dataset = %s AND beamstamp = %s
        """, (self.cycle, telegram, self.dataset, self.beamstamp))
    
    def _process_archive(self, d, cyclestamp):
        comment = d.comment
        self.beamstamp = cyclestamp
        
        # Update the md_info table
        self.session.execute("""
        INSERT INTO md_info(name, tag, beamstamp, comment)
        VALUES(%s, %s, %s, %s)
        """, (self.md_name,
              self.md_tag,
              self.beamstamp,
              comment))
        self.session.execute("""
        UPDATE md_info SET cycles = cycles + {%s} WHERE name = %s AND tag = %s AND beamstamp = %s
        """, (self.cycle, self.md_name, self.md_tag, self.beamstamp))
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
                self.dataset, self.beamstamp, self.cycle, Parameter(device, prop, field), value, t)))
        elif tf == 'blob_value':
            self.session.execute(self.bound_statement_blob.bind((
                self.dataset, self.beamstamp, self.cycle, Parameter(device, prop, field), value, t)))
        elif tf == 'real_value':
            self.session.execute(self.bound_statement_real.bind((
                self.dataset, self.beamstamp, self.cycle, Parameter(device, prop, field), value, t)))
        