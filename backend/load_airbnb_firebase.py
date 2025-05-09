import pandas as pd
import firebase_admin
from firebase_admin import credentials, db
from tqdm import tqdm

CSV_FILE_PATH = r"../sample_data/airbnb_listing_500.csv"

FIREBASE_CRED = "dir_path_to_firebase_credential_file" # replace with your firebase actual credential file path
DATABASE_URL = "firebase_database_url" # replace with your actual firebase database url
# These values must be replaced with actual credentials before deployment

df = pd.read_csv(CSV_FILE_PATH)

# Convert ID columns to Int64 type to handle potential large integers and missing values
df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
df["host_id"] = pd.to_numeric(df["host_id"], errors="coerce").astype("Int64")

df.sort_values("id", inplace=True)
df.drop_duplicates("id", inplace=True)

df.dropna(subset=["id", "host_id"], inplace=True)

# Clean string values by removing whitespace
df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

# Replace empty strings and NaN values with None for proper Firebase JSON serialization
df = df.where(df.ne(""), None).where(pd.notnull(df), None)

# Remove currency symbols in pricing column and reformatting
for col in ["price"]:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

# Convert availability columns to numeric values
avail_cols = ["availability_30", "availability_60", "availability_90", "availability_365"]
for c in avail_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

# Define field groupings for structured data storage in Firebase
PRICING_FIELDS = ["price", "weekly_price", "monthly_price", "security_deposit", "cleaning_fee", "guests_included", "extra_people"]
AVAIL_FIELDS =  ["availability_30", "availability_60", "availability_90", "availability_365", "calendar_last_scraped"]
HOST_FIELDS = ["host_is_superhost", "host_listings_count"]

print("firebase listing fields:", PRICING_FIELDS)
print("firebase availability fields:", AVAIL_FIELDS)
print("firebase host fields:", HOST_FIELDS)

cred = credentials.Certificate(FIREBASE_CRED)
firebase_admin.initialize_app(cred, {"databaseURL": DATABASE_URL})
root_ref = db.reference("/")
listings_ref = root_ref.child("listings")
hosts_ref = root_ref.child("hosts")
listings_ref.delete()
hosts_ref.delete()

# Upload listings data
for _, row in tqdm(df.iterrows(), total=len(df), desc="Loading listings"):
    lid = row["id"]
    if lid is None:
        continue

    pricing = {f: row.get(f) for f in PRICING_FIELDS if f in row.index}
    availability = {f: row.get(f) for f in AVAIL_FIELDS if f in row.index}

    # Store data with listing ID as the key and nested objects for each category
    entry_ref = listings_ref.child(str(int(lid)))
    entry_ref.child("pricing").set(pricing)
    entry_ref.child("availability").set(availability)

# Create a separate dataframe for host data to avoid duplicates
hosts_df = df[["host_id"] + HOST_FIELDS].drop_duplicates("host_id")
for _, row in tqdm(hosts_df.iterrows(), total=len(hosts_df), desc="Loading hosts"):
    hid = row["host_id"]
    if hid is None:
        continue
    data = {f: row.get(f) for f in HOST_FIELDS}
    hosts_ref.child(str(int(hid))).set(data)

print("Firebase load complete")