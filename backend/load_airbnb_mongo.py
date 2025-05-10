import pandas as pd
import ast
from pymongo import MongoClient

CSV_FILE_PATH = r"../sample_data/airbnb_listing_500.csv"

# These values must be replaced with actual credentials before deployment
MONGO_URI = "mongodb://localhost:27017/" # MongoDB server address, replace with your actual MongoDB URI
DB_NAME = "airbnb_db" # MongoDB database name, replace with your actual database name


df = pd.read_csv(CSV_FILE_PATH) 

# Convert ID columns to Int64 type to handle potential large integers and missing values
df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
df["host_id"] = pd.to_numeric(df["host_id"], errors="coerce").astype("Int64")

# Sort and remove duplicate listings
df.sort_values("id", inplace=True)
df.drop_duplicates(subset=["id"], inplace=True)
df.dropna(subset=["id"], inplace=True)

# Remove whitespace to clean string values
df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

# Replace empty strings and NaN values with None for proper MongoDB document serialization
df = df.where(df.ne(""), None).where(pd.notnull(df), None)

# MongoDB document schema design
meta_fields = [
    "scrape_id", "last_scraped", "source",
    "host_id", "host_response_time", "host_response_rate", "host_acceptance_rate",
    "instant_bookable", "license",
    "neighbourhood_cleansed", "neighbourhood_group_cleansed"
]

# Amenities will be stored separately as arrays in a dedicated collection
amenities_field = "amenities"

# URLs for listing and host images will be stored in a media collection
media_fields = [
    "picture_url",
    "host_thumbnail_url",
    "host_picture_url"
]

print("MongoDB listing fields:", meta_fields)
print("MongoDB amenities field:", amenities_field)
print("MongoDB media fields:", media_fields)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
db.listings_meta.drop()
db.amenities.drop()
db.media.drop()

meta_docs     = []
amenity_docs  = []
media_docs    = []

for _, row in df.iterrows():
    lid = int(row["id"])
    mdoc = {"_id": lid}
    for f in meta_fields:
        mdoc[f] = row.get(f)
    meta_docs.append(mdoc)

    raw = row.get(amenities_field)
    if raw:
        try:
            alist = ast.literal_eval(raw)
        except Exception:
            alist = [s.strip() for s in raw.split(",")]
        amenity_docs.append({"listing_id": lid, "amenities": alist})

    media_doc = {"listing_id": lid}
    for f in media_fields:
        media_doc[f] = row.get(f)
    media_docs.append(media_doc)

if meta_docs:
    db.listings_meta.insert_many(meta_docs)
if amenity_docs:
    db.amenities.insert_many(amenity_docs)
if media_docs:
    db.media.insert_many(media_docs)

print(f"Inserted {db.listings_meta.count_documents({})} into listings_meta")
print(f"Inserted {db.amenities.count_documents({})} into amenities")
print(f"Inserted {db.media.count_documents({})} into media")

client.close()