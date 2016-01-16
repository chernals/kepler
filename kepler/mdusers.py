class MDUsers(list):
    
    def __init__(self, session, name):
        self._session = session
        self._name = name
        self._getusers()
    
    def _getusers(self):
        rows = self._session.execute("""
        SELECT users FROM md_info WHERE name = %s
        """, (str(self._name),))
        users = rows[0][0]
        if users is not None:
            self.clear()
            for u in users:
                self.append(u)
            
    def add(self, username):
        self._session.execute("""
        UPDATE md_info SET users = users + %s WHERE name = %s
        """, ({username}, self._name))
        self._getusers()