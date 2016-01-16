from cassandra.cqlengine import connection
connection.setup(['127.0.0.1'], 'varilog')

from varilog.md import MD