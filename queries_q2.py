# queries_q2.py
# Q2: Find POIs visited by friends of each user
# Only stable friendships (present in BOTH before and after snapshots)
# Strategy: intersect friendship_before and friendship_after, then get friend checkins

import warnings
warnings.filterwarnings("ignore")
import psycopg2
import time
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

RUNS = 3

# ── PostgreSQL ─────────────────────────────────────────────────────────────
def q2_postgres(port=5432, label="PostgreSQL"):
    conn = psycopg2.connect(host="localhost", port=port,
                            dbname="foursquaredb", user="admin", password="admin123")
    cur = conn.cursor()
    sql = """
        WITH stable_friends AS (
            SELECT fb.user_id, fb.friend_id
            FROM friendship_before fb
            INNER JOIN friendship_after fa
                ON fb.user_id = fa.user_id
                AND fb.friend_id = fa.friend_id
        ),
        friend_pois AS (
            SELECT DISTINCT sf.user_id, c.venue_id
            FROM stable_friends sf
            JOIN checkins c ON c.user_id = sf.friend_id
        )
        SELECT fp.user_id, p.venue_id, p.category, p.country,
               p.latitude, p.longitude
        FROM friend_pois fp
        JOIN pois p ON p.venue_id = fp.venue_id
        ORDER BY fp.user_id
        LIMIT 100;
    """
    times = []
    rows = []
    for _ in range(RUNS):
        t = time.time()
        cur.execute(sql)
        rows = cur.fetchall()
        times.append(time.time() - t)
    avg = sum(times) / len(times)
    print(f"\n{label} Q2: {len(rows)} rows")
    for r in rows[:3]:
        print(f"  user={r[0]} venue={r[1]} cat={r[2]} country={r[3]}")
    print(f"  Avg time ({RUNS} runs): {avg:.3f}s")
    cur.close()
    conn.close()
    return avg

# ── ScyllaDB ───────────────────────────────────────────────────────────────
def q2_scylladb():
    cluster = Cluster(
        ["localhost"], port=9042,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1")
    )
    session = cluster.connect("foursquaredb")
    times = []
    results = []
    for _ in range(RUNS):
        results = []
        t = time.time()
        friendships = session.execute(
            "SELECT user_id, friend_id FROM friendship_stable LIMIT 1000"
        )
        for f in friendships:
            checkins = session.execute(
                "SELECT venue_id FROM checkins_by_user WHERE user_id = %s LIMIT 10",
                [str(f.friend_id)]
            )
            for c in checkins:
                poi = session.execute(
                    "SELECT venue_id, category, country FROM pois WHERE venue_id = %s",
                    [c.venue_id]
                ).one()
                if poi:
                    results.append((f.user_id, poi.venue_id, poi.category, poi.country))
        times.append(time.time() - t)
    avg = sum(times) / len(times)
    print(f"\nScyllaDB Q2: {len(results)} rows")
    for r in results[:3]:
        print(f"  user={r[0]} venue={r[1]} cat={r[2]} country={r[3]}")
    print(f"  Avg time ({RUNS} runs): {avg:.3f}s")
    cluster.shutdown()
    return avg

# ── MongoDB ────────────────────────────────────────────────────────────────
def q2_mongodb():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["foursquaredb"]
    pipeline = [
        {"$lookup": {
            "from": "friendship_after",
            "let": {"uid": "$user_id", "fid": "$friend_id"},
            "pipeline": [{"$match": {"$expr": {
                "$and": [
                    {"$eq": ["$user_id", "$$uid"]},
                    {"$eq": ["$friend_id", "$$fid"]}
                ]
            }}}],
            "as": "also_after"
        }},
        {"$match": {"also_after": {"$ne": []}}},
        {"$lookup": {
            "from": "checkins",
            "localField": "friend_id",
            "foreignField": "user_id",
            "as": "friend_checkins"
        }},
        {"$unwind": "$friend_checkins"},
        {"$lookup": {
            "from": "pois",
            "localField": "friend_checkins.venue_id",
            "foreignField": "venue_id",
            "as": "poi"
        }},
        {"$unwind": "$poi"},
        {"$group": {
            "_id": {"user_id": "$user_id", "venue_id": "$friend_checkins.venue_id"},
            "category": {"$first": "$poi.category"},
            "country": {"$first": "$poi.country"}
        }},
        {"$limit": 100}
    ]
    times = []
    results = []
    for _ in range(RUNS):
        t = time.time()
        results = list(db.friendship_before.aggregate(pipeline, allowDiskUse=True))
        times.append(time.time() - t)
    avg = sum(times) / len(times)
    print(f"\nMongoDB Q2: {len(results)} rows")
    for r in results[:3]:
        print(f"  user={r['_id']['user_id']} venue={r['_id']['venue_id']}")
    print(f"  Avg time ({RUNS} runs): {avg:.3f}s")
    client.close()
    return avg

print("=" * 60)
print("Q2: POIs Shared by Friends (Stable Friendships)")
print("=" * 60)
t_pg     = q2_postgres(5432, "PostgreSQL")
t_citus  = q2_postgres(5433, "Citus")
t_scylla = q2_scylladb()
t_mongo  = q2_mongodb()

print("\n" + "=" * 60)
print("Q2 SUMMARY (avg seconds):")
print(f"  PostgreSQL : {t_pg:.3f}s")
print(f"  Citus      : {t_citus:.3f}s")
print(f"  ScyllaDB   : {t_scylla:.3f}s")
print(f"  MongoDB    : {t_mongo:.3f}s")
print("=" * 60)