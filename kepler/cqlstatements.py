from kepler.connection import _session

_bound_statements = {}

_bound_statements['parameter_data'] = _session.prepare(
"""
SELECT type, real_value, text_value, blob_value 
FROM md_data 
WHERE dataset = ? AND beamstamp = ? AND cycle = ? AND parameter = ?
""")

_bound_statements['parameter_timeseries'] = _session.prepare(
"""
SELECT type, real_value, text_value, blob_value, cycle 
FROM md_data 
WHERE dataset= ? AND parameter= ?
ALLOW FILTERING
""")