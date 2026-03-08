# queries_q1.py
# Q1: Find top 10 countries with the highest total number of check-ins
# Strategy: JOIN checkins with pois on venue_id, GROUP BY country, ORDER DESC

import warnings
warnings.filterwarnings("ignore")
import psycopg2
import time
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from collections import Counter

RUNS = 3

# ── PostgreSQL ─────────────────────────────────────────────────────────────
def q1_postgres(port=5432, label="PostgreSQL"):
    conn = psycopg2.connect(host="localhost", port=port,
                            dbname="foursquaredb", user="admin", password="admin123")
    cur = conn.cursor()
    sql = """
        SELECT p.country, COUNT(*) AS total_checkins
        FROM checkins c
        JOIN pois p ON c.venue_id = p.venue_id
        GROUP BY p.country
        ORDER BY total_checkins DESC
        LIMIT 10;
    """
    times = []
    rows = []
    for _ in range(RUNS):
        t = time.time()
        cur.execute(sql)
        rows = cur.fetchall()
        times.append(time.time() - t)
    avg = sum(times) / len(times)
    print(f"\n{label} Q1 results:")
    for r in rows:
        print(f"  {r[0]}: {r[1]}")
    print(f"  Avg time ({RUNS} runs): {avg:.3f}s")
    cur.close()
    conn.close()
    return avg

# ── ScyllaDB ───────────────────────────────────────────────────────────────
def q1_scylladb():
    cluster = Cluster(
        ["localhost"], port=9042,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1")
    )
    session = cluster.connect("foursquaredb")
    session.default_timeout = 300
    session.default_fetch_size = 10000
    times = []
    top10 = []
    for _ in range(RUNS):
        t = time.time()
        counter = Counter()
        rows = session.execute(
            "SELECT country, cnt FROM venue_checkin_counts",
            timeout=300
        )
        for r in rows:
            counter[r.country] += r.cnt
        top10 = counter.most_common(10)
        times.append(time.time() - t)
    avg = sum(times) / len(times)
    print(f"\nScyllaDB Q1 results:")
    for country, cnt in top10:
        print(f"  {country}: {cnt}")
    print(f"  Avg time ({RUNS} runs): {avg:.3f}s")
    cluster.shutdown()
    return avg

# ── MongoDB ────────────────────────────────────────────────────────────────
def q1_mongodb():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["foursquaredb"]
    pipeline = [
        {"$lookup": {
            "from": "pois",
            "localField": "venue_id",
            "foreignField": "venue_id",
            "as": "poi"
        }},
        {"$unwind": "$poi"},
        {"$group": {"_id": "$poi.country", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}},
        {"$limit": 10}
    ]
    times = []
    results = []
    for _ in range(RUNS):
        t = time.time()
        results = list(db.checkins.aggregate(pipeline, allowDiskUse=True))
        times.append(time.time() - t)
    avg = sum(times) / len(times)
    print(f"\nMongoDB Q1 results:")
    for r in results:
        print(f"  {r['_id']}: {r['total']}")
    print(f"  Avg time ({RUNS} runs): {avg:.3f}s")
    client.close()
    return avg

# ── Run all ────────────────────────────────────────────────────────────────
print("=" * 60)
print("Q1: Top 10 Countries by Check-ins")
print("=" * 60)
t_pg    = q1_postgres(5432, "PostgreSQL")
t_citus = q1_postgres(5433, "Citus")
t_scylla = q1_scylladb()
t_mongo = q1_mongodb()

print("\n" + "=" * 60)
print("Q1 SUMMARY (avg seconds):")
print(f"  PostgreSQL : {t_pg:.3f}s")
print(f"  Citus      : {t_citus:.3f}s")
print(f"  ScyllaDB   : {t_scylla:.3f}s")
print(f"  MongoDB    : {t_mongo:.3f}s")
print("=" * 60)