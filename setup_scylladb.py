# setup_scylladb.py
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import time

print("Waiting for ScyllaDB to be ready...")
time.sleep(3)

cluster = Cluster(
    ["localhost"], port=9042,
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1")
)
session = cluster.connect()

session.execute("""
CREATE KEYSPACE IF NOT EXISTS foursquaredb
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
} AND durable_writes = true;
""")
session.set_keyspace("foursquaredb")

session.execute("""
CREATE TABLE IF NOT EXISTS pois (
    venue_id  TEXT PRIMARY KEY,
    latitude  DOUBLE,
    longitude DOUBLE,
    category  TEXT,
    country   TEXT
);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS checkins_by_country (
    country              TEXT,
    utc_time             TIMESTAMP,
    user_id              TEXT,
    venue_id             TEXT,
    timezone_offset_mins INT,
    latitude             DOUBLE,
    longitude            DOUBLE,
    category             TEXT,
    PRIMARY KEY (country, utc_time, user_id, venue_id)
) WITH CLUSTERING ORDER BY (utc_time DESC);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS checkins_by_user (
    user_id              TEXT,
    utc_time             TIMESTAMP,
    venue_id             TEXT,
    timezone_offset_mins INT,
    PRIMARY KEY (user_id, utc_time, venue_id)
) WITH CLUSTERING ORDER BY (utc_time DESC);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS friendship_stable (
    user_id   TEXT,
    friend_id TEXT,
    PRIMARY KEY (user_id, friend_id)
);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS venue_checkin_counts (
    country  TEXT,
    venue_id TEXT,
    cnt      COUNTER,
    PRIMARY KEY (country, venue_id)
);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS pois_by_category (
    category  TEXT,
    venue_id  TEXT,
    latitude  DOUBLE,
    longitude DOUBLE,
    country   TEXT,
    PRIMARY KEY (category, venue_id)
);
""")

print("-------- ScyllaDB schema created!")
rows = session.execute(
    "SELECT table_name FROM system_schema.tables WHERE keyspace_name='foursquaredb'"
)
print("\nTables created:")
for row in rows:
    print(f"  - {row.table_name}")

cluster.shutdown()