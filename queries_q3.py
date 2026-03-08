# queries_q3.py
# Q3: Find most attractive venues by country and location
# Attractive = most check-ins (counting same user multiple times)

import warnings
warnings.filterwarnings("ignore")
import psycopg2
import time
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from collections import defaultdict

RUNS = 3

# ── PostgreSQL ─────────────────────────────────────────────────────────────
def q3_postgres(port=5432, label="PostgreSQL"):
    conn = psycopg2.connect(host="localhost", port=port,
                            dbname="foursquaredb", user="admin", password="admin123")
    cur = conn.cursor()
    sql = """
        SELECT p.country, p.venue_id, p.latitude, p.longitude,
               p.category, COUNT(*) AS visit_count
        FROM checkins c
        JOIN pois p ON c.venue_id = p.venue_id
        GROUP BY p.country, p.venue_id, p.latitude, p.longitude, p.category
        ORDER BY p.country, visit_count DESC
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
    print(f"\n{label} Q3: {len(rows)} rows")
    for r in rows[:3]:
        print(f"  country={r[0]} venue={r[1]} lat={r[2]:.4f} visits={r[5]}")
    print(f"  Avg time ({RUNS} runs): {avg:.3f}s")
    cur.close()
    conn.close()
    return avg

# ── ScyllaDB ───────────────────────────────────────────────────────────────
def q3_scylladb():
    cluster = Cluster(
        ["localhost"], port=9042,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1")
    )
    session = cluster.connect("foursquaredb")
    session.default_timeout = 300
    session.default_fetch_size = 10000
    times = []
    result = []
    for _ in range(RUNS):
        t = time.time()
        rows = session.execute(
            "SELECT country, venue_id, cnt FROM venue_checkin_counts",
            timeout=300
        )
        by_country = defaultdict(list)
        for r in rows:
            by_country[r.country].append((r.venue_id, r.cnt))
        result = []
        for country, venues in by_country.items():
            top = sorted(venues, key=lambda x: -x[1])[:10]
            for vid, cnt in top:
                result.append((country, vid, cnt))
        times.append(time.time() - t)
    avg = sum(times) / len(times)
    print(f"\nScyllaDB Q3: {len(result)} rows")
    for r in result[:3]:
        print(f"  country={r[0]} venue={r[1]} cnt={r[2]}")
    print(f"  Avg time ({RUNS} runs): {avg:.3f}s")
    cluster.shutdown()
    return avg

# ── MongoDB ────────────────────────────────────────────────────────────────
def q3_mongodb():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["foursquaredb"]
    # First get top venues by checkin count, then join with pois
    pipeline = [
        {"$group": {
            "_id": "$venue_id",
            "visit_count": {"$sum": 1}
        }},
        {"$sort": {"visit_count": -1}},
        {"$limit": 1000},
        {"$lookup": {
            "from": "pois",
            "localField": "_id",
            "foreignField": "venue_id",
            "as": "poi"
        }},
        {"$unwind": "$poi"},
        {"$project": {
            "venue_id": "$_id",
            "visit_count": 1,
            "country": "$poi.country",
            "latitude": "$poi.latitude",
            "longitude": "$poi.longitude",
            "category": "$poi.category"
        }},
        {"$sort": {"country": 1, "visit_count": -1}}
    ]
    times = []
    results = []
    for _ in range(RUNS):
        t = time.time()
        results = list(db.checkins.aggregate(pipeline, allowDiskUse=True))
        times.append(time.time() - t)
    avg = sum(times) / len(times)
    print(f"\nMongoDB Q3: {len(results)} rows")
    for r in results[:3]:
        print(f"  country={r.get('country')} venue={r['venue_id']} visits={r['visit_count']}")
    print(f"  Avg time ({RUNS} runs): {avg:.3f}s")
    client.close()
    return avg

print("=" * 60)
print("Q3: Attractive Venues by Country and Location")
print("=" * 60)
t_pg     = q3_postgres(5432, "PostgreSQL")
t_citus  = q3_postgres(5433, "Citus")
t_scylla = q3_scylladb()
t_mongo  = q3_mongodb()

print("\n" + "=" * 60)
print("Q3 SUMMARY (avg seconds):")
print(f"  PostgreSQL : {t_pg:.3f}s")
print(f"  Citus      : {t_citus:.3f}s")
print(f"  ScyllaDB   : {t_scylla:.3f}s")
print(f"  MongoDB    : {t_mongo:.3f}s")
print("=" * 60)