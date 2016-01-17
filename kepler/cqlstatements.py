from kepler.connection import _session

_bound_statements = {}

_bound_statements['parameter_data'] = _session.prepare(
"""
SELECT type, real_value, text_value, blob_value 
FROM md_data 
WHERE name = ? AND tag = ? AND id = ? AND parameter = ?
""")

#_bound_statements['parameter_timeseries'] = _session.prepare(
#"""
#SELECT type, real_value, text_value, blob_value, id 
#FROM md_data 
#WHERE name= ? AND tag= ? AND parameter= ?
#ALLOW FILTERING
#""")