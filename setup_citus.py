# setup_citus.py
import psycopg2

conn = psycopg2.connect(
    host="localhost", port=5433,
    dbname="foursquaredb", user="admin", password="admin123"
)
conn.autocommit = True
cur = conn.cursor()

print("Creating tables in Citus...")

cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS pois (
    venue_id  TEXT PRIMARY KEY,
    latitude  DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    category  VARCHAR(255),
    country   CHAR(2)
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS checkins (
    id                   BIGSERIAL,
    user_id              TEXT NOT NULL,
    venue_id             TEXT NOT NULL,
    utc_time             TIMESTAMP,
    timezone_offset_mins INTEGER
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS friendship_before (
    user_id   TEXT NOT NULL,
    friend_id TEXT NOT NULL
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS friendship_after (
    user_id   TEXT NOT NULL,
    friend_id TEXT NOT NULL
);
""")

def distribute_if_needed(cur, table, dist_type, column=None):
    cur.execute("""
        SELECT count(*) FROM pg_dist_partition
        WHERE logicalrelid = %s::regclass
    """, (table,))
    if cur.fetchone()[0] == 0:
        if dist_type == "reference":
            cur.execute(f"SELECT create_reference_table('{table}');")
        else:
            cur.execute(f"SELECT create_distributed_table('{table}', '{column}');")
        print(f"  Distributed: {table}")
    else:
        print(f"  Already distributed (skip): {table}")

distribute_if_needed(cur, "pois", "reference")
distribute_if_needed(cur, "users", "reference")
distribute_if_needed(cur, "checkins", "distributed", "user_id")
distribute_if_needed(cur, "friendship_before", "distributed", "user_id")
distribute_if_needed(cur, "friendship_after", "distributed", "user_id")

cur.execute("CREATE INDEX IF NOT EXISTS idx_citus_checkins_venue ON checkins(venue_id);")

print("---------- Citus schema created!")
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public' ORDER BY table_name;
""")
print("\nTables:")
for row in cur.fetchall():
    print(f"  - {row[0]}")

cur.close()
conn.close()