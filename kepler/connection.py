import cassandra.cluster
from kepler.parameter import Parameter

#_cluster = cassandra.cluster.Cluster(['188.184.77.145'], load_balancing_policy=cassandra.policies.WhiteListRoundRobinPolicy(['188.184.77.145']))
default = 'cwe-513-vol078'
host = input('Kepler cluster host [%s]:' % default)
host = host or default
if host == 'cwe-513-vol078':
    _cluster = cassandra.cluster.Cluster([host], load_balancing_policy=cassandra.policies.WhiteListRoundRobinPolicy([host]))
else:
    _cluster = cassandra.cluster.Cluster()
_session = _cluster.connect('kepler')
_cluster.register_user_type('kepler', 'parameter', Parameter)