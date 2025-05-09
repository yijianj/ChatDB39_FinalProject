import requests
import json

API_URL = "http://127.0.0.1:8000/explore"

def query_backend(query_text, db_type=None):
    payload = {"query": query_text}
    if db_type:
        payload["db_type"] = db_type
    
    print(f"\nTesting: {query_text}")
    if db_type:
        print(f"Database: {db_type}")
    print("-" * 50)
    
    response = requests.post(API_URL, json=payload)
    
    if response.status_code == 200:
        result = response.json()
        print(f"Exploration Type: {result.get('exploration_type')}")
        print(f"Message: {result.get('message')}")
        print("\nData:")
        print(json.dumps(result.get('data'), indent=2))
        return result
    else:
        print(f"ERROR ({response.status_code}): {response.text}")
        return None

def test_list_tables():
    print("\n=== TESTING LIST TABLES FUNCTIONALITY ===")
    
    # Test for MySQL tables
    query_backend("What tables exist in MySQL?", "mysql")
    
    # Test for MongoDB collections
    query_backend("Show me all MongoDB collections", "mongodb")
    
    # Test for Firebase nodes
    query_backend("List the Firebase nodes", "firebase")

def test_table_schemas():
    print("\n=== TESTING TABLE SCHEMA FUNCTIONALITY ===")
    
    # Test MySQL table schema
    query_backend("What columns are in the Listings table?", "mysql")
    
    # Test MongoDB collection schema
    query_backend("Show me the structure of the listings_meta collection", "mongodb")
    
    # Test Firebase schema
    query_backend("What is the schema of the listings node in Firebase?", "firebase")

def test_sample_data():
    print("\n=== TESTING SAMPLE DATA FUNCTIONALITY ===")
    
    # Test MySQL sample data
    query_backend("Show me 3 rows from the Hosts table", "mysql")
    
    # Test MongoDB sample data
    query_backend("Give me 3 sample documents from the amenities collection", "mongodb")
    
    # Test Firebase sample data
    query_backend("Show me 3 entries from the listings path", "firebase")

def main():
    print("=== DATABASE SCHEMA EXPLORATION TEST ===")
    
    test_list_tables()
    test_table_schemas()
    test_sample_data()
    
    print("\n=== TEST COMPLETED ===")

if __name__ == "__main__":
    main()
