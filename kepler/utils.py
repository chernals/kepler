import io
import numpy as np
import icalendar as ics
from cassandra.cluster import Cluster
from datetime import datetime

def _convert_object_from_cassandra(t, values):
    if t == 'str':
        return values[1]
    elif t == 'scalar':
        return values[0]
    elif t == 'numpy':
        out = io.BytesIO(values[2])
        out.seek(0)
        return np.load(out)
        
class KepCal():
    
    def __init__(self):
        self._cluster = Cluster(['127.0.0.1'])
        self._session = self._cluster.connect('varilog')
        self._cal = ics.Calendar()
        self.generate()
        
    def save(self, filename):
        print(self._cal.to_ical())
        f = open(filename, 'wb')
        f.write(self._cal.to_ical())
    
    def generate(self):
        self._cal.add('prodid', '-//Kepler MD calendar//mxm.dk//')
        self._cal.add('version', '2.0')
        self._generate_md()
        self._generate_tags()
        
    def _generate_tags(self):
        mds = self._get_mds()
        for r in mds:
            if r[1] is None:
                continue
            for tagname, end in r[2].items():
                tmp = self._session.execute("""
                SELECT cyclestamp FROM md_info WHERE name = %s AND tag = %s LIMIT 1
                """, (r[0], tagname))
                tmp = tmp[0][0]
                event = ics.Event()
                event.add('summary', "Tag %s" % tagname)
                event.add('dtstart', tmp)
                event.add('dtend', end)
                event.add('dtstamp', end)
                self._cal.add_component(event)
        
    def _generate_md(self):
        mds = self._get_mds()
        for r in mds:
            if r[1] is None:
                continue
            event = ics.Event()
            event.add('summary', "MD %s.\nContains %d tags" %(r[0],len(r[2].keys())))
            event.add('dtstart', r[1])
            event.add('dtend', sorted([v for v in list(r[2].values())])[-1])
            event.add('dtstamp', r[1])
            self._cal.add_component(event)
        
    def _get_mds(self):
        rows = self._session.execute("""
        SELECT DISTINCT name, created, tag_info FROM md_info
        """)
        return rows