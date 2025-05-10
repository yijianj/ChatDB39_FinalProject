import mysql.connector
import pandas as pd
import numpy as np

CSV_FILE_PATH = r"../sample_data/airbnb_listing_500.csv"


# MySQL server address, port, user, password, and database name, 
# replace with your actual MySQL configuration
# These values must be replaced with actual credentials before deployment
MYSQL_CONFIG = dict(
    host="localhost",
    port=3306,
    user="root",
    password="Dsci-551",
    database="airbnb_db") 

df = pd.read_csv(CSV_FILE_PATH, dtype=str)

# Convert ID columns to nullable integer type to handle missing values
df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
df["host_id"] = pd.to_numeric(df["host_id"], errors="coerce").astype("Int64")

# Sort and remove duplicates
df.sort_values("id", inplace=True)
df.drop_duplicates("id", inplace=True)
df.dropna(subset=["id", "host_id"], inplace=True)

# Remove redundant whitespace from string fields
df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

# Convert NaN values to None for proper SQL handling
df = df.astype(object).where(pd.notnull(df), None)

# Define the columns for each table
hosts_cols = [
    "host_id",
    "host_url",
    "host_name",
    "host_since",
    "host_location",
    "host_about",
    "host_is_superhost",
    "host_thumbnail_url",
    "host_picture_url",
    "host_listings_count"
]

listings_cols = [
    "id",
    "listing_url",
    "name",
    "property_type",
    "room_type",
    "accommodates",
    "bathrooms",
    "bathrooms_text",
    "bedrooms",
    "beds",
    "description",
    "neighborhood_overview",
    "neighbourhood_cleansed",
    "neighbourhood_group_cleansed",
    "latitude",
    "longitude"
]

reviews_cols = [
    "id",
    "number_of_reviews",
    "number_of_reviews_ltm",
    "first_review",
    "last_review",
    "review_scores_rating",
    "review_scores_accuracy",
    "review_scores_cleanliness",
    "review_scores_checkin",
    "review_scores_communication",
    "review_scores_location",
    "review_scores_value",
    "reviews_per_month"
]

print("Hosts columns:", hosts_cols)
print("Listings columns:", listings_cols)
print("Reviews columns:", reviews_cols)

tmp = mysql.connector.connect(**{**MYSQL_CONFIG, "database": None})
cur = tmp.cursor()
cur.execute("CREATE DATABASE IF NOT EXISTS airbnb_db;")
tmp.commit()
cur.close()
tmp.close()

conn = mysql.connector.connect(**MYSQL_CONFIG)
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS Reviews;")
cursor.execute("DROP TABLE IF EXISTS Listings;")
cursor.execute("DROP TABLE IF EXISTS Hosts;")
conn.commit()

cursor.execute("""
CREATE TABLE Hosts (
  host_id BIGINT PRIMARY KEY,
  host_url VARCHAR(255),
  host_name VARCHAR(255),
  host_since DATE,
  host_location VARCHAR(255),
  host_about TEXT,
  host_is_superhost BOOLEAN,
  host_thumbnail_url VARCHAR(255),
  host_picture_url VARCHAR(255),
  host_listings_count INT
);
""")

cursor.execute("""
CREATE TABLE Listings (
  id BIGINT PRIMARY KEY,
  listing_url VARCHAR(255),
  name VARCHAR(255),
  property_type VARCHAR(100),
  room_type VARCHAR(100),
  accommodates INT,
  bathrooms FLOAT,
  bathrooms_text VARCHAR(100),
  bedrooms INT,
  beds INT,
  description TEXT,
  neighborhood_overview TEXT,
  neighbourhood_cleansed VARCHAR(100),
  neighbourhood_group_cleansed VARCHAR(100),
  latitude DOUBLE,
  longitude DOUBLE,
  host_id BIGINT,
  FOREIGN KEY (host_id) REFERENCES Hosts(host_id)
);
""")

cursor.execute("""
CREATE TABLE Reviews (
  listing_id BIGINT,
  number_of_reviews INT,
  number_of_reviews_ltm INT,
  first_review DATE,
  last_review DATE,
  review_scores_rating FLOAT,
  review_scores_accuracy FLOAT,
  review_scores_cleanliness FLOAT,
  review_scores_checkin FLOAT,
  review_scores_communication FLOAT,
  review_scores_location FLOAT,
  review_scores_value FLOAT,
  reviews_per_month FLOAT,
  PRIMARY KEY (listing_id),
  FOREIGN KEY (listing_id) REFERENCES Listings(id)
);
""")

conn.commit()

# Insert host data
hosts_df = df[hosts_cols].drop_duplicates("host_id")
hosts_q = f"""
  INSERT IGNORE INTO Hosts ({', '.join(hosts_cols)})
  VALUES ({', '.join(['%s'] * len(hosts_cols))});
"""

for _, row in hosts_df.iterrows():
    cursor.execute(hosts_q, tuple(row[col] for col in hosts_cols))
conn.commit()

# Insert listing data, foreign key to hosts
listings_df = df[listings_cols + ["host_id"]].drop_duplicates("id")
listings_q = f"""
  INSERT IGNORE INTO Listings ({','.join(listings_cols + ['host_id'])})
  VALUES ({','.join(['%s']*(len(listings_cols)+1))});
"""
for _, row in listings_df.iterrows():
    vals = [ row[col] for col in listings_cols ] + [ row["host_id"] ]
    cursor.execute(listings_q, tuple(vals))
conn.commit()

# Insert review data, using listing ID as foreign key and primary key
reviews_df = df[reviews_cols].dropna(subset=["id"])
reviews_q = f"""
  INSERT IGNORE INTO Reviews ({', '.join(['listing_id'] + reviews_cols[1:])})
  VALUES ({', '.join(['%s'] * (len(reviews_cols)))});
"""
for _, row in reviews_df.iterrows():
    vals = [row["id"]] + [row[col] for col in reviews_cols if col != "id"]
    cursor.execute(reviews_q, tuple(vals))
conn.commit()

cursor.close()
conn.close()

print("MySQL load complete")