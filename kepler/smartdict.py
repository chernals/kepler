class SmartDict():
    """
    I'm not really what that is...
    """
    def __init__(self):
        self.__dict__ = {}
    
    def __getitem__(self, k):
        return self.__dict__[k]
    
    def __setitem__(self, k, v):
        self.__dict__[k] = v
        
    def _get(self, k):
        return self.__dict__.get(k)
        
    def __len__(self):
        return len(self.__dict__)
        
    def _keys(self):
        return self.__dict__.keys()