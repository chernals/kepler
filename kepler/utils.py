import io
import numpy as np
import icalendar as ics
from cassandra.cluster import Cluster
import kepler.connection
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
        self._session = kepler.connection._session
        self._cal = ics.Calendar()
        self.generate()
        
    def save(self, filename):
        print(self._cal.to_ical())
        f = open(filename, 'wb')
        f.write(self._cal.to_ical())
    
    def generate(self):
        self._cal.add('prodid', '-//Kepler calendar//mxm.dk//')
        self._cal.add('version', '2.0')
        self._generate_md()
        self._generate_tags()
        
    def _generate_tags(self):
        mds = self._get_mds()
        for r in mds:
            if r[1] is None:
                continue
            for tagname, end in r[2].items():
                print(tagname, "   ", end)
                tmp = self._session.execute("""
                SELECT cyclestamp FROM md_info WHERE name = %s AND tag = %s LIMIT 1
                """, (r[0], tagname))
                tmp = tmp[0][0]
                event = ics.Event()
                event.add('summary', "Tag %s" % tagname)
                event.add('dtstart', tmp)
                event.add('dtend', end)
                event.add('dtstamp', tmp)
                self._cal.add_component(event)
        
    def _generate_md(self):
        mds = self._get_mds()
        for r in mds:
            if r[1] is None:
                continue
            first_stamp = self._session.execute("""
            SELECT cyclestamp FROM md_info WHERE name=%s LIMIT 1
            """, (r[0],))[0][0]
            print(first_stamp)
            summary = "MD %s.\nContains %d tags. Users:" %(r[0],len(r[2].keys()))
            for u in r[3]:
                summary += " %s," % u
            event = ics.Event()  
            event.add('summary', summary[:-1])  
            event.add('dtstart', first_stamp)
            event.add('dtend', sorted([v for v in list(r[2].values())])[-1])
            event.add('dtstamp', first_stamp)
            self._cal.add_component(event)
        
    def _get_mds(self):
        rows = self._session.execute("""
        SELECT DISTINCT name, created, tag_info, users FROM md_info
        """)
        return rows