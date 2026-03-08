# ingest_scylladb.py — with checkpoint
import warnings
warnings.filterwarnings("ignore")
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel as CL
import pandas as pd
import time
from datetime import datetime

DATA_DIR = "data"
CHUNK = 10_000
DATE_FORMAT = "%a %b %d %H:%M:%S %z %Y"
MAX_INFLIGHT = 200
CHECKPOINT_FILE = "scylla_checkpoint.txt"

cluster = Cluster(
    ["localhost"], port=9042,
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
    executor_threads=8,
)
session = cluster.connect("foursquaredb")
session.default_timeout = 60
start_total = time.time()

def flush_futures(futures):
    for f in futures:
        try:
            f.result()
        except Exception:
            pass
    return []

# ── Load checkpoint ────────────────────────────────────────────────────────
try:
    with open(CHECKPOINT_FILE) as f:
        skip_rows = int(f.read().strip())
    print(f"Resuming from row {skip_rows:,}")
except:
    skip_rows = 0
    print("Starting fresh")

# ── POIs — skip if already done ────────────────────────────────────────────
if skip_rows == 0:
    print("Inserting POIs...")
    t = time.time()
    ins_poi = session.prepare("INSERT INTO pois (venue_id,latitude,longitude,category,country) VALUES (?,?,?,?,?)")
    ins_cat = session.prepare("INSERT INTO pois_by_category (category,venue_id,latitude,longitude,country) VALUES (?,?,?,?,?)")
    ins_poi.consistency_level = CL.ONE
    ins_cat.consistency_level = CL.ONE
    total = 0
    for chunk in pd.read_csv(f"{DATA_DIR}/my_POIs.tsv", sep="\t", chunksize=CHUNK):
        chunk.columns = ["venue_id","latitude","longitude","category","country"]
        chunk = chunk.fillna("")
        futures = []
        for row in chunk.itertuples(index=False):
            vid,lat,lon,cat,ctry = str(row.venue_id),float(row.latitude),float(row.longitude),str(row.category),str(row.country)
            futures.append(session.execute_async(ins_poi, (vid,lat,lon,cat,ctry)))
            futures.append(session.execute_async(ins_cat, (cat,vid,lat,lon,ctry)))
            if len(futures) >= MAX_INFLIGHT:
                futures = flush_futures(futures)
        flush_futures(futures)
        total += len(chunk)
        print(f"  Progress: {total:,}", end="\r")
    print(f"\n  Done in {time.time()-t:.1f}s — {total} rows")

    print("Building stable friendships...")
    t = time.time()
    fb = pd.read_csv(f"{DATA_DIR}/my_friendship_before.tsv", sep="\t")
    fa = pd.read_csv(f"{DATA_DIR}/my_friendship_after.tsv", sep="\t")
    fb.columns = fa.columns = ["user_id","friend_id"]
    stable = pd.merge(fb, fa, on=["user_id","friend_id"])
    ins_fs = session.prepare("INSERT INTO friendship_stable (user_id,friend_id) VALUES (?,?)")
    ins_fs.consistency_level = CL.ONE
    futures = []
    for row in stable.itertuples(index=False):
        futures.append(session.execute_async(ins_fs, (str(int(row.user_id)), str(int(row.friend_id)))))
        if len(futures) >= MAX_INFLIGHT:
            futures = flush_futures(futures)
    flush_futures(futures)
    print(f"  Done in {time.time()-t:.1f}s — {len(stable)} rows")
else:
    print("Skipping POIs and friendships (already loaded)")

# ── Load POIs map ──────────────────────────────────────────────────────────
print("Loading POIs map into memory...")
pois_df = pd.read_csv(f"{DATA_DIR}/my_POIs.tsv", sep="\t",
                      names=["venue_id","latitude","longitude","category","country"])
pois_df["venue_id"] = pois_df["venue_id"].astype(str)
pois_map = pois_df.set_index("venue_id").to_dict("index")
print(f"  Loaded {len(pois_map):,} venues")

# ── Check-ins with checkpoint ──────────────────────────────────────────────
print(f"Inserting check-ins (skipping first {skip_rows:,} rows)...")
t = time.time()

ins_cbc = session.prepare("""
    INSERT INTO checkins_by_country
    (country,utc_time,user_id,venue_id,timezone_offset_mins,latitude,longitude,category)
    VALUES (?,?,?,?,?,?,?,?)
""")
ins_cbu = session.prepare("""
    INSERT INTO checkins_by_user (user_id,utc_time,venue_id,timezone_offset_mins)
    VALUES (?,?,?,?)
""")
ins_vcc = session.prepare("""
    UPDATE venue_checkin_counts SET cnt = cnt + 1 WHERE country=? AND venue_id=?
""")
ins_cbc.consistency_level = CL.ONE
ins_cbu.consistency_level = CL.ONE
ins_vcc.consistency_level = CL.ONE

total = skip_rows
rows_processed = 0

for chunk in pd.read_csv(
    f"{DATA_DIR}/my_checkins_anonymized.tsv",
    sep="\t", chunksize=CHUNK,
    skiprows=range(1, skip_rows + 1) if skip_rows > 0 else None,
    header=0
):
    chunk.columns = ["user_id","venue_id","utc_time","timezone_offset_mins"]
    chunk["utc_time"] = pd.to_datetime(
        chunk["utc_time"], format=DATE_FORMAT, errors="coerce", utc=True
    )
    # Drop rows with invalid timestamps instead of crashing
    chunk = chunk.dropna(subset=["utc_time"])
    chunk = chunk.fillna("")

    futures_insert = []
    futures_counter = []

    for row in chunk.itertuples(index=False):
        uid  = str(int(float(row.user_id))) if row.user_id != "" else "0"
        vid  = str(row.venue_id)
        tz   = int(float(row.timezone_offset_mins)) if row.timezone_offset_mins != "" else 0
        ts   = row.utc_time
        poi  = pois_map.get(vid, {})
        ctry = str(poi.get("country", "XX"))
        lat  = float(poi.get("latitude", 0.0))
        lon  = float(poi.get("longitude", 0.0))
        cat  = str(poi.get("category", ""))
        dt   = ts.to_pydatetime().replace(tzinfo=None) if hasattr(ts, "to_pydatetime") else datetime.utcnow()

        futures_insert.append(session.execute_async(ins_cbc, (ctry,dt,uid,vid,tz,lat,lon,cat)))
        futures_insert.append(session.execute_async(ins_cbu, (uid,dt,vid,tz)))
        futures_counter.append(session.execute_async(ins_vcc, (ctry,vid)))

        if len(futures_insert) >= MAX_INFLIGHT:
            futures_insert = flush_futures(futures_insert)
        if len(futures_counter) >= 50:
            futures_counter = flush_futures(futures_counter)

    flush_futures(futures_insert)
    flush_futures(futures_counter)

    rows_processed += len(chunk)
    total = skip_rows + rows_processed

    # Save checkpoint every chunk
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(total))

    elapsed = time.time() - t
    rate = rows_processed / elapsed if elapsed > 0 else 1
    eta = (22_561_807 - total) / rate / 60
    print(f"  {total:,} / 22,561,807 — {rate:.0f} rows/s — ETA {eta:.1f} min", end="\r")

print(f"\n  Done in {time.time()-t:.1f}s — {total} rows")
total_time = time.time() - start_total
print(f"\n------------ ScyllaDB total ingestion time: {total_time:.1f}s")

# Remove checkpoint file on success
import os
if os.path.exists(CHECKPOINT_FILE):
    os.remove(CHECKPOINT_FILE)

cluster.shutdown()