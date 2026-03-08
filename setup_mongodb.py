# setup_mongodb.py
from pymongo import MongoClient, ASCENDING, TEXT
import time

time.sleep(2)
print("Connecting to MongoDB...")

# Standalone connection (no replica set)
client = MongoClient("mongodb://localhost:27017/")
db = client["foursquaredb"]

print("Creating indexes...")
db.users.create_index([("user_id", ASCENDING)], unique=True)
db.pois.create_index([("venue_id", ASCENDING)], unique=True)
db.pois.create_index([("country", ASCENDING)])
db.pois.create_index([("category", TEXT)])
db.checkins.create_index([("user_id", ASCENDING)])
db.checkins.create_index([("venue_id", ASCENDING)])
db.checkins.create_index([("utc_time", ASCENDING)])
db.friendship_before.create_index([("user_id", ASCENDING)])
db.friendship_after.create_index([("user_id", ASCENDING)])

print("------- MongoDB schema created!")
print("\nCollections with indexes:")
for name in ["users", "pois", "checkins", "friendship_before", "friendship_after"]:
    indexes = list(db[name].list_indexes())
    print(f"  - {name}: {len(indexes)} indexes")

client.close()