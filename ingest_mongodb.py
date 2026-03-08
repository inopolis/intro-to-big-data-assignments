# ingest_mongodb.py
import warnings
warnings.filterwarnings("ignore")
from pymongo import MongoClient, InsertOne
import pandas as pd
import time

DATA_DIR = "data"
CHUNK = 100_000
DATE_FORMAT = "%a %b %d %H:%M:%S %z %Y"

client = MongoClient("mongodb://localhost:27017/")
db = client["foursquaredb"]
start_total = time.time()

def bulk_insert(collection, records):
    if records:
        collection.bulk_write([InsertOne(r) for r in records], ordered=False)

print("Inserting users...")
t = time.time()
df = pd.read_csv(f"{DATA_DIR}/my_users.csv")
df.columns = ["user_id"]
df["user_id"] = df["user_id"].astype(str)
bulk_insert(db.users, df.to_dict("records"))
print(f"  Done in {time.time()-t:.1f}s — {len(df)} rows")

print("Inserting POIs...")
t = time.time()
total = 0
for chunk in pd.read_csv(f"{DATA_DIR}/my_POIs.tsv", sep="\t", chunksize=CHUNK):
    chunk.columns = ["venue_id","latitude","longitude","category","country"]
    chunk["venue_id"] = chunk["venue_id"].astype(str)
    bulk_insert(db.pois, chunk.to_dict("records"))
    total += len(chunk)
print(f"  Done in {time.time()-t:.1f}s — {total} rows")

print("Inserting check-ins...")
t = time.time()
total = 0
for chunk in pd.read_csv(f"{DATA_DIR}/my_checkins_anonymized.tsv", sep="\t", chunksize=CHUNK):
    chunk.columns = ["user_id","venue_id","utc_time","timezone_offset_mins"]
    chunk["user_id"] = chunk["user_id"].astype(str)
    chunk["venue_id"] = chunk["venue_id"].astype(str)
    chunk["utc_time"] = pd.to_datetime(
        chunk["utc_time"], format=DATE_FORMAT, errors="coerce", utc=True
    ).dt.tz_localize(None).astype(str)
    bulk_insert(db.checkins, chunk.to_dict("records"))
    total += len(chunk)
print(f"  Done in {time.time()-t:.1f}s — {total} rows")

print("Inserting friendship_before...")
t = time.time()
total = 0
for chunk in pd.read_csv(f"{DATA_DIR}/my_friendship_before.tsv", sep="\t", chunksize=CHUNK):
    chunk.columns = ["user_id","friend_id"]
    chunk["user_id"] = chunk["user_id"].astype(str)
    chunk["friend_id"] = chunk["friend_id"].astype(str)
    bulk_insert(db.friendship_before, chunk.to_dict("records"))
    total += len(chunk)
print(f"  Done in {time.time()-t:.1f}s — {total} rows")

print("Inserting friendship_after...")
t = time.time()
total = 0
for chunk in pd.read_csv(f"{DATA_DIR}/my_friendship_after.tsv", sep="\t", chunksize=CHUNK):
    chunk.columns = ["user_id","friend_id"]
    chunk["user_id"] = chunk["user_id"].astype(str)
    chunk["friend_id"] = chunk["friend_id"].astype(str)
    bulk_insert(db.friendship_after, chunk.to_dict("records"))
    total += len(chunk)
print(f"  Done in {time.time()-t:.1f}s — {total} rows")

total_time = time.time() - start_total
print(f"\n------------ MongoDB total ingestion time: {total_time:.1f}s")
client.close()