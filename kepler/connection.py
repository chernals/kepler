import os
import cassandra.cluster
import cassandra.policies
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from kepler.udt import Parameter, Cycle, Dataset
import logging
import getpass

_session = None

def _try_connect_to_cluster(hosts, policy=None):
    if hosts[0] is 'localhost':
        p = None
    else:
        p = os.environ.get('KEPLER_PASSWORD') or getpass.getpass("Kepler password on host %s: " % hosts[0])
    auth_provider = PlainTextAuthProvider(username='md_user', password=p)
    if policy is not None:
        cluster = cassandra.cluster.Cluster(hosts, auth_provider=auth_provider, load_balancing_policy=policy)
    else:
        cluster = cassandra.cluster.Cluster(hosts, auth_provider=auth_provider)
    session = cluster.connect('kepler')
    session.default_consistency_level = ConsistencyLevel.ONE
    cluster.register_user_type('kepler', 'parameter', Parameter)
    cluster.register_user_type('kepler', 'cycle', Cycle)
    cluster.register_user_type('kepler', 'dataset', Dataset)
    
    return session
    
def _connect(hosts):
    global _session
    if len(hosts) == 1 and hosts[0] == 'cwe-513-vol078':
        _session = _try_connect_to_cluster(hosts, cassandra.policies.WhiteListRoundRobinPolicy(hosts))
    else:
        _session = _try_connect_to_cluster(hosts)

def _get_hosts_and_connect(hosts = None):
    if hosts is None:
        h = [os.environ.get('KEPLER_HOST') or 'cwe-513-vol078']
    else:
        h = hosts
    logging.getLogger('cassandra.cluster').setLevel(logging.CRITICAL)
    try:
        _connect(h)
    except cassandra.cluster.NoHostAvailable:
        print('Error connecting to the cluster via %s' % h)
        host = input('Kepler cluster host [localhost]: ') or 'localhost'
        _get_hosts_and_connect([host])
    
_get_hosts_and_connect()
