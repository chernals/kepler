import re
import datetime
from kepler.mdtags import MDTags
from kepler.connection import _session

class MDNames(list):
    """
    Hold the MD names declared in Kepler.
    """
    
    def __init__(self):
        self.update()
            
    def update(self):
        """
        Query the `md_info` table for distinct names
        name is the partition key of the table        
        """
        rows = _session.execute("""
        SELECT DISTINCT name FROM md_info
        """)
        self.__dict__ = {'update': self.update, 'create': self.create, 'delete': self.delete}
        self.clear()
        for r in rows:
            self.__dict__[r[0]] = MDTags(r[0])
            self.append(r[0])
        return self
        
    def delete(self):
        print("Please contact a Kepler administrator if you really want to delete a MD!")
        
    def create(self, **kwargs):
        """Create new MD.
    
        Check if MD name already exists.
        If not, create a new one with a comment and
        add time and user information.
        """
        try:
            name = kwargs['name']
        except KeyError:
            print('A name must be provided!')
            return
        try:
            comment = kwargs['comment']
        except KeyError:
            print('A comment must be provided!')
            return
        try:
            usernames = kwargs['users']
        except KeyError:
            print('A user list must be provided')
            return
        if name in self:
            print("This MD name already exists!")
            return
        if re.match("^[a-zA-Z0-9_.-]+$", name) is None:
            print("Please provide a meaningful username.")
        else:
            stamp = datetime.datetime.now()
            _session.execute("""
            INSERT INTO md_info(name, md_comment, created, users) VALUES(%s, %s, %s, %s)
            """, (name, comment, stamp, usernames))
        print("MD successfully created!")
        
    def __dir__(self):
        """
        Allow the autocompletion with just the MD names
        """
        return self.__dict__.keys()
