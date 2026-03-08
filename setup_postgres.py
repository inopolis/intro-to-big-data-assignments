# setup_postgres.py
import psycopg2

conn = psycopg2.connect(
    host="localhost", port=5432,
    dbname="foursquaredb", user="admin", password="admin123"
)
conn.autocommit = True
cur = conn.cursor()

print("Creating tables in PostgreSQL...")

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
    id                   BIGSERIAL PRIMARY KEY,
    user_id              TEXT NOT NULL,
    venue_id             TEXT NOT NULL,
    utc_time             TIMESTAMP,
    timezone_offset_mins INTEGER,
    FOREIGN KEY (user_id)  REFERENCES users(user_id),
    FOREIGN KEY (venue_id) REFERENCES pois(venue_id)
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS friendship_before (
    user_id   TEXT NOT NULL,
    friend_id TEXT NOT NULL,
    PRIMARY KEY (user_id, friend_id),
    FOREIGN KEY (user_id)   REFERENCES users(user_id),
    FOREIGN KEY (friend_id) REFERENCES users(user_id)
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS friendship_after (
    user_id   TEXT NOT NULL,
    friend_id TEXT NOT NULL,
    PRIMARY KEY (user_id, friend_id),
    FOREIGN KEY (user_id)   REFERENCES users(user_id),
    FOREIGN KEY (friend_id) REFERENCES users(user_id)
);
""")

cur.execute("CREATE INDEX IF NOT EXISTS idx_checkins_user_id  ON checkins(user_id);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_checkins_venue_id ON checkins(venue_id);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_pois_country      ON pois(country);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_pois_category_fts ON pois USING GIN(to_tsvector('english', category));")

print("---------- PostgreSQL schema created!")
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public' ORDER BY table_name;
""")
print("\nTables created:")
for row in cur.fetchall():
    print(f"  - {row[0]}")

cur.close()
conn.close()