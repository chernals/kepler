class MDTags(list):
    
    def __init__(self, session, name):
        self._name = name
        rows = session.execute("""
        SELECT tag FROM md_info WHERE name = %s
        """, (self._name,))
        tags = set()
        for r in rows:
            tags.add(r[0])
        for t in tags:
            self.__dict__['tag'+str(t).replace('.', '_')] = t
            self.append(t)
          
    @property
    def tags(self):
        return [self.__dict__[k] for k in self.__dict__.keys() if not k.startswith('_')]
        
    def __repr__(self):
        return self._name
            
    def __str__(self):
        return self._name
            
    def __dir__(self):
        return self.__dict__.keys()