# queries_q4.py
# Q4: Count venues per custom category using full-text search
# Categories: Restaurant, Club, Museum, Shop, Others

import warnings
warnings.filterwarnings("ignore")
import psycopg2
import time
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from collections import Counter

RUNS = 3

KEYWORD_MAP = {
    "Restaurant": ["restaurant","food","dining","cafe","bistro","pizza","burger","sushi","grill"],
    "Club":       ["club","bar","nightclub","lounge","pub","brewery","cocktail","tavern"],
    "Museum":     ["museum","gallery","exhibit","art","history","science","cultural"],
    "Shop":       ["shop","store","market","mall","boutique","retail","supermarket"],
}

# ── PostgreSQL ─────────────────────────────────────────────────────────────
def q4_postgres(port=5432, label="PostgreSQL"):
    conn = psycopg2.connect(host="localhost", port=port,
                            dbname="foursquaredb", user="admin", password="admin123")
    cur = conn.cursor()
    sql = """
        SELECT
            CASE
                WHEN to_tsvector('english', COALESCE(category,'')) @@
                     to_tsquery('english', 'restaurant|food|dining|cafe|bistro|pizza|burger|sushi|grill')
                    THEN 'Restaurant'
                WHEN to_tsvector('english', COALESCE(category,'')) @@
                     to_tsquery('english', 'club|bar|nightclub|lounge|pub|brewery|cocktail|tavern')
                    THEN 'Club'
                WHEN to_tsvector('english', COALESCE(category,'')) @@
                     to_tsquery('english', 'museum|gallery|exhibit|art|history|science|cultural')
                    THEN 'Museum'
                WHEN to_tsvector('english', COALESCE(category,'')) @@
                     to_tsquery('english', 'shop|store|market|mall|boutique|retail|supermarket')
                    THEN 'Shop'
                ELSE 'Others'
            END AS custom_category,
            COUNT(*) AS venue_count
        FROM pois
        GROUP BY custom_category
        ORDER BY venue_count DESC;
    """
    times = []
    rows = []
    for _ in range(RUNS):
        t = time.time()
        cur.execute(sql)
        rows = cur.fetchall()
        times.append(time.time() - t)
    avg = sum(times) / len(times)
    print(f"\n{label} Q4:")
    for r in rows:
        print(f"  {r[0]}: {r[1]} venues")
    print(f"  Avg time ({RUNS} runs): {avg:.3f}s")
    cur.close()
    conn.close()
    return avg

# ── ScyllaDB ───────────────────────────────────────────────────────────────
def q4_scylladb():
    cluster = Cluster(
        ["localhost"], port=9042,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1")
    )
    session = cluster.connect("foursquaredb")
    session.default_timeout = 300
    session.default_fetch_size = 10000
    times = []
    counts = Counter({"Restaurant":0,"Club":0,"Museum":0,"Shop":0,"Others":0})
    for _ in range(RUNS):
        counts = Counter({"Restaurant":0,"Club":0,"Museum":0,"Shop":0,"Others":0})
        t = time.time()
        rows = session.execute(
            "SELECT category FROM pois",
            timeout=300
        )
        for r in rows:
            cat_lower = (r.category or "").lower()
            matched = "Others"
            for custom_cat, keywords in KEYWORD_MAP.items():
                if any(kw in cat_lower for kw in keywords):
                    matched = custom_cat
                    break
            counts[matched] += 1
        times.append(time.time() - t)
    avg = sum(times) / len(times)
    print(f"\nScyllaDB Q4:")
    for cat, cnt in counts.most_common():
        print(f"  {cat}: {cnt} venues")
    print(f"  Avg time ({RUNS} runs): {avg:.3f}s")
    cluster.shutdown()
    return avg

# ── MongoDB ────────────────────────────────────────────────────────────────
def q4_mongodb():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["foursquaredb"]
    pipeline = [
        {"$project": {
            "custom_category": {"$switch": {
                "branches": [
                    {"case": {"$regexMatch": {"input": {"$ifNull": ["$category",""]},
                        "regex": "restaurant|food|dining|cafe|bistro|pizza|burger|sushi|grill",
                        "options": "i"}}, "then": "Restaurant"},
                    {"case": {"$regexMatch": {"input": {"$ifNull": ["$category",""]},
                        "regex": "club|bar|nightclub|lounge|pub|brewery|cocktail|tavern",
                        "options": "i"}}, "then": "Club"},
                    {"case": {"$regexMatch": {"input": {"$ifNull": ["$category",""]},
                        "regex": "museum|gallery|exhibit|art|history|science|cultural",
                        "options": "i"}}, "then": "Museum"},
                    {"case": {"$regexMatch": {"input": {"$ifNull": ["$category",""]},
                        "regex": "shop|store|market|mall|boutique|retail|supermarket",
                        "options": "i"}}, "then": "Shop"},
                ],
                "default": "Others"
            }}
        }},
        {"$group": {"_id": "$custom_category", "venue_count": {"$sum": 1}}},
        {"$sort": {"venue_count": -1}}
    ]
    times = []
    results = []
    for _ in range(RUNS):
        t = time.time()
        results = list(db.pois.aggregate(pipeline, allowDiskUse=True))
        times.append(time.time() - t)
    avg = sum(times) / len(times)
    print(f"\nMongoDB Q4:")
    for r in results:
        print(f"  {r['_id']}: {r['venue_count']} venues")
    print(f"  Avg time ({RUNS} runs): {avg:.3f}s")
    client.close()
    return avg

print("=" * 60)
print("Q4: Venues by Custom Category (Full-Text Search)")
print("=" * 60)
t_pg     = q4_postgres(5432, "PostgreSQL")
t_citus  = q4_postgres(5433, "Citus")
t_scylla = q4_scylladb()
t_mongo  = q4_mongodb()

print("\n" + "=" * 60)
print("Q4 SUMMARY (avg seconds):")
print(f"  PostgreSQL : {t_pg:.3f}s")
print(f"  Citus      : {t_citus:.3f}s")
print(f"  ScyllaDB   : {t_scylla:.3f}s")
print(f"  MongoDB    : {t_mongo:.3f}s")
print("=" * 60)