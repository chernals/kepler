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