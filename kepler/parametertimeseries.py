class ParameterTimeseries():
    def __init__(self, name, tag, p):
        self._p = p
        self._name = name
        self._tag = tag
        self._values = None
        
    @property
    def values(self):
        if self._values is None:
            start_time = time.time()
            self._values = []
            rows = MD._session.execute(MD._bound_statements['parameter_timeseries'].bind(
                (self._name, self._tag, self._p,)))
            for r in rows:
                cyclestamp = cassandra.util.datetime_from_uuid1(r[4])
                self._values.append([cyclestamp, _convert_object_from_cassandra(r[0], [r[1], r[2], r[3]])])
            self._values = np.array(sorted(list(self._values), key=lambda x: x[0])) 
            print("Data queried in %f seconds." % (time.time()-start_time,))
        return self._values
    
    def __len__(self):
        return len(self.values)
    
    def __iter__(self):
        return self.values.__iter__()
    
    def __getitem__(self,key):
        return self.values[key]

    def __setitem__(self, k, v):
        print('Error')
        
    def __repr__(self):
        return str(self.values)