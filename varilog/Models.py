import io
import numpy as np
import cassandra.util
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.usertype import UserType

class MD(Model):
    __keyspace__ = 'varilog'
    __table_name__ = 'md_info'
    name = columns.Text(partition_key=True, primary_key=True, required=True)
    tag = columns.Text(primary_key=True)
    id = columns.TimeUUID()
    cyclestamp = columns.DateTime(index=True)
    md_comment = columns.Text(static=True)
    users = columns.Set(columns.Text, static=True)
    created = columns.DateTime(static=True)
    
    def __repr__(self):
        return "MD %s (created on %s)\n   -- with users %s" % (self.name, self.created, self.users)
        
    def getCyclestamps(name, tag=''):
        if name == '':
            return []
        cyclestamps = []
        query = MD.filter(name=name)
        if tag is not '':
            query = query.filter(tag=tag)
        for md in query:
            cyclestamps.append(md.cyclestamp)
        return cyclestamps
        
    def getIds(name, tag=''):
        if name == '':
            return []
        ids = []
        query = MD.filter(name=name)
        if tag is not '':
            query = query.filter(tag=tag)
        for md in query:
            ids.append(md.id)
        return ids
    
class Parameter(UserType):
    device = columns.Text()
    property = columns.Text()
    field = columns.Text()
    
class Data(Model):
    __keyspace__ = 'varilog'
    __table_name__ = 'md_data'
    id = columns.TimeUUID(partition_key=True, primary_key=True, required=True)
    parameter = columns.UserDefinedType(Parameter, primary_key=True, required=True, index=True)
    telegram = columns.Map(columns.Text, columns.Text, static=True)
    type = columns.Text()
    text_value = columns.Text()
    real_value = columns.Float()
    blob_value = columns.Blob()
    
    @property
    def cyclestamp(self):
        return cassandra.util.datetime_from_uuid1(self.id)
    
    @property
    def value(self):
        if self.type == 'str':
            return self.text_value
        elif self.type == 'scalar':
            return self.real_value
        elif self.type == 'numpy':
            out = io.BytesIO(self.blob_value)
            out.seek(0)
            return np.load(out)
