#!/usr/bin/env python3
"""Continuously run a given query on a Cassandra db

Usage:
    query_loop.py [-h HOST]... [-p PORT] [-k KEYSPACE] [-u USER] [-P PASSWORD] [-i INTERVAL] QUERY

Options:
    -h --host HOST  Cassandra host address [default: localhost]
    -p --port PORT  Cassandra port [default: 9042]
    -k --keyspace KEYSPACE  Keyspace to select
    -u --user USERNAME  Username for authentication
    -P --password PASSWORD  Password for authentication
    -i --interval INTERVAL  Interval between query executions (in milliseconds) [default: 1000]
"""

from cassandra import ConsistencyLevel, Unavailable
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from cassandra.protocol import ServerError
from time import sleep
from docopt import docopt
from getpass import getpass
from blessings import Terminal

options = docopt(__doc__)

user = options.get('--user')
password = options.get('--password')
hosts = options.get('--host')
port = options.get('--port')
keyspace = options.get('--keyspace')
query = options.get('QUERY')
interval = int(options.get('--interval'))

if user is not None:
    if password is None:
        password = getpass()

    auth = PlainTextAuthProvider(user, password)
else:
    auth = None


cluster = Cluster(hosts, port=port, auth_provider=auth)
session = None
statement = None

history = (
    (ConsistencyLevel.ONE, []),
    (ConsistencyLevel.TWO, []),
    (ConsistencyLevel.THREE, []),
    (ConsistencyLevel.ALL, []),
    (ConsistencyLevel.QUORUM, []),
    (ConsistencyLevel.SERIAL, []),
    (ConsistencyLevel.EACH_QUORUM, []),
    (ConsistencyLevel.LOCAL_ONE, []),
    (ConsistencyLevel.LOCAL_QUORUM, []),
    (ConsistencyLevel.LOCAL_SERIAL, [])
)

term = Terminal()
print(term.clear)
print(". = Success")
print("C = Not enough live replicas to satisfy the requested consistency level")
print("E = All connections are busy, defunct, closed, or resulted in errors when used.")
print("X = Server error. Session will be reopened.")
print()
y_offset = 6

longest_name = 0
for (consistency, _) in history:
    display_name = ConsistencyLevel.value_to_name[consistency]
    longest_name = max(longest_name, len(display_name))
    print(display_name)

x_offset = longest_name + 1

try:
    while 1:
        if session is None:
            with term.location(0, 0):
                print("Connecting to cluster...", end='', flush=True)
                session = cluster.connect(keyspace)
                statement = session.prepare(query)

            with term.location(0, 0):
                print(query)

        for (i, (consistency, points)) in enumerate(history):
            try:
                statement.consistency_level = consistency
                rows = session.execute(statement)
            except NoHostAvailable:
                points.append('E')
            except ServerError:
                points.append('X')
                session = None
            except Unavailable:
                points.append('C')
            else:
                points.append('.')

            with term.location(x_offset, i + y_offset):
                data_points = points[-(term.width - x_offset):]
                print(''.join(data_points), end='', flush=True)

        sleep(interval / 1000)
except KeyboardInterrupt:
    print("\nExiting.")
