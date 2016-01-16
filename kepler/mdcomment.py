class MDComment():
    
    def __init__(self, session, name):
        self._session = session
        self._name = name
        self._comment = ""
        self._getcomment()
        
    def _getcomment(self):
        rows = self._session.execute("""
        SELECT md_comment FROM md_info WHERE name = %s
        """, (str(self._name),))
        if rows[0][0] is not None:  
            self._comment = rows[0][0]
            
    def update(self, comment):
        self._session.execute("""
        UPDATE md_info SET md_comment =%s WHERE name = %s
        """, (comment, self._name))
        self._getcomment()
        
    def __str__(self):
        return self._comment

    def __repr__(self):
        return str(self)