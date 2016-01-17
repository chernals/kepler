import cassandra.cluster
from kepler.parameter import Parameter

#_cluster = cassandra.cluster.Cluster(['188.184.77.145'], load_balancing_policy=cassandra.policies.WhiteListRoundRobinPolicy(['188.184.77.145']))
_cluster = cassandra.cluster.Cluster()
_session = _cluster.connect('kepler')
_cluster.register_user_type('kepler', 'parameter', Parameter)