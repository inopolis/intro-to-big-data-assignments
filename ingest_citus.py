# ingest_citus.py
import warnings
warnings.filterwarnings("ignore")
import psycopg2
import pandas as pd
from io import StringIO
import time

DATA_DIR = "data"
CHUNK = 500_000
DATE_FORMAT = "%a %b %d %H:%M:%S %z %Y"

def parse_dates(series):
    parsed = pd.to_datetime(series, format=DATE_FORMAT, errors="coerce", utc=True)
    return parsed.dt.tz_localize(None)

def pg_copy(cur, df, table):
    buf = StringIO()
    df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
    buf.seek(0)
    cur.copy_from(buf, table, sep="\t", null="\\N", columns=list(df.columns))

conn = psycopg2.connect(
    host="localhost", port=5433,
    dbname="foursquaredb", user="admin", password="admin123"
)
conn.autocommit = False
cur = conn.cursor()
start_total = time.time()

print("Inserting users...")
t = time.time()
df = pd.read_csv(f"{DATA_DIR}/my_users.csv")
df.columns = ["user_id"]
df["user_id"] = df["user_id"].astype(str)
pg_copy(cur, df, "users")
conn.commit()
print(f"  Done in {time.time()-t:.1f}s — {len(df)} rows")

print("Inserting POIs...")
t = time.time()
total = 0
for chunk in pd.read_csv(f"{DATA_DIR}/my_POIs.tsv", sep="\t", chunksize=CHUNK):
    chunk.columns = ["venue_id","latitude","longitude","category","country"]
    chunk["venue_id"] = chunk["venue_id"].astype(str)
    chunk["country"] = chunk["country"].astype(str).str[:2]
    pg_copy(cur, chunk, "pois")
    conn.commit()
    total += len(chunk)
print(f"  Done in {time.time()-t:.1f}s — {total} rows")

print("Inserting check-ins...")
t = time.time()
total = 0
for chunk in pd.read_csv(f"{DATA_DIR}/my_checkins_anonymized.tsv", sep="\t", chunksize=CHUNK):
    chunk.columns = ["user_id","venue_id","utc_time","timezone_offset_mins"]
    chunk["user_id"] = chunk["user_id"].astype(str)
    chunk["venue_id"] = chunk["venue_id"].astype(str)
    chunk["utc_time"] = parse_dates(chunk["utc_time"])
    pg_copy(cur, chunk, "checkins")
    conn.commit()
    total += len(chunk)
print(f"  Done in {time.time()-t:.1f}s — {total} rows")

print("Inserting friendship_before...")
t = time.time()
total = 0
for chunk in pd.read_csv(f"{DATA_DIR}/my_friendship_before.tsv", sep="\t", chunksize=CHUNK):
    chunk.columns = ["user_id","friend_id"]
    chunk["user_id"] = chunk["user_id"].astype(str)
    chunk["friend_id"] = chunk["friend_id"].astype(str)
    pg_copy(cur, chunk, "friendship_before")
    conn.commit()
    total += len(chunk)
print(f"  Done in {time.time()-t:.1f}s — {total} rows")

print("Inserting friendship_after...")
t = time.time()
total = 0
for chunk in pd.read_csv(f"{DATA_DIR}/my_friendship_after.tsv", sep="\t", chunksize=CHUNK):
    chunk.columns = ["user_id","friend_id"]
    chunk["user_id"] = chunk["user_id"].astype(str)
    chunk["friend_id"] = chunk["friend_id"].astype(str)
    pg_copy(cur, chunk, "friendship_after")
    conn.commit()
    total += len(chunk)
print(f"  Done in {time.time()-t:.1f}s — {total} rows")

total_time = time.time() - start_total
print(f"\n----------- Citus total ingestion time: {total_time:.1f}s")
cur.close()
conn.close()