# prepare_data.py
# Reads large dataset files in chunks and filters data for your user slice
# Uses chunked reading to avoid memory overflow on large files

import pandas as pd
import os

# Path to data folder
DATA_DIR = "data"
CHUNK_SIZE = 500_000

print("Loading my users slice...")
my_users = pd.read_csv(f"{DATA_DIR}/my_users.csv")
user_ids = set(my_users["userid"].tolist())
print(f"Total users in slice: {len(user_ids)}")

# ─────────────────────────────────────────────
# 1. Prepare check-ins
# ─────────────────────────────────────────────
print("\nPreparing check-ins...")
output_checkins = f"{DATA_DIR}/my_checkins_anonymized.tsv"
if os.path.exists(output_checkins):
    os.remove(output_checkins)

checkin_cols = ["user_id", "venue_id", "utc_time", "timezone_offset_mins"]
total_checkins = 0
first_chunk = True

for chunk in pd.read_csv(
    f"{DATA_DIR}/checkins_anonymized.txt",
    sep="\t", header=None, names=checkin_cols,
    chunksize=CHUNK_SIZE, on_bad_lines="skip"
):
    filtered = chunk[chunk["user_id"].isin(user_ids)]
    if not filtered.empty:
        filtered.to_csv(output_checkins, sep="\t", index=False,
                        header=first_chunk, mode="a")
        total_checkins += len(filtered)
        first_chunk = False

print(f"Saved {total_checkins} check-ins to {output_checkins}")

# ─────────────────────────────────────────────
# 2. Prepare friendship_before
# ─────────────────────────────────────────────
print("\nPreparing friendship_before...")
output_fb = f"{DATA_DIR}/my_friendship_before.tsv"
if os.path.exists(output_fb):
    os.remove(output_fb)

friend_cols = ["user_id", "friend_id"]
total_fb = 0
first_chunk = True

for chunk in pd.read_csv(
    f"{DATA_DIR}/friendship_before_old.txt",
    sep="\t", header=None, names=friend_cols,
    chunksize=CHUNK_SIZE, on_bad_lines="skip"
):
    filtered = chunk[
        chunk["user_id"].isin(user_ids) & chunk["friend_id"].isin(user_ids)
    ]
    if not filtered.empty:
        filtered.to_csv(output_fb, sep="\t", index=False,
                        header=first_chunk, mode="a")
        total_fb += len(filtered)
        first_chunk = False

print(f"Saved {total_fb} friendships_before to {output_fb}")

# ─────────────────────────────────────────────
# 3. Prepare friendship_after
# ─────────────────────────────────────────────
print("\nPreparing friendship_after...")
output_fa = f"{DATA_DIR}/my_friendship_after.tsv"
if os.path.exists(output_fa):
    os.remove(output_fa)

total_fa = 0
first_chunk = True

for chunk in pd.read_csv(
    f"{DATA_DIR}/friendship_after_new.txt",
    sep="\t", header=None, names=friend_cols,
    chunksize=CHUNK_SIZE, on_bad_lines="skip"
):
    filtered = chunk[
        chunk["user_id"].isin(user_ids) & chunk["friend_id"].isin(user_ids)
    ]
    if not filtered.empty:
        filtered.to_csv(output_fa, sep="\t", index=False,
                        header=first_chunk, mode="a")
        total_fa += len(filtered)
        first_chunk = False

print(f"Saved {total_fa} friendships_after to {output_fa}")

# ─────────────────────────────────────────────
# 4. Prepare POIs
# ─────────────────────────────────────────────
print("\nPreparing POIs...")
print("  Loading venue IDs from my checkins...")
my_venue_ids = set(
    pd.read_csv(output_checkins, sep="\t", usecols=["venue_id"])["venue_id"].tolist()
)
print(f"  Found {len(my_venue_ids)} unique venues")

output_pois = f"{DATA_DIR}/my_POIs.tsv"
if os.path.exists(output_pois):
    os.remove(output_pois)

poi_cols = ["venue_id", "latitude", "longitude", "category", "country"]
total_pois = 0
first_chunk = True

for chunk in pd.read_csv(
    f"{DATA_DIR}/POIs.txt",
    sep="\t", header=None, names=poi_cols,
    chunksize=CHUNK_SIZE, on_bad_lines="skip"
):
    filtered = chunk[chunk["venue_id"].isin(my_venue_ids)]
    if not filtered.empty:
        filtered.to_csv(output_pois, sep="\t", index=False,
                        header=first_chunk, mode="a")
        total_pois += len(filtered)
        first_chunk = False

print(f"Saved {total_pois} POIs to {output_pois}")
print("Data preparation complete!")