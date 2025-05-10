from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any, List

import uvicorn
import re
import json

from google import genai
from database.mysql_connector import query_mysql, validate_table_exists, get_table_schema, modify_mysql
from database.mongodb_connector import query_mongodb, get_collection, get_database, convert_objectid_to_str, COLLECTIONS, modify_mongodb
from database.firebase_connector import query_firebase, get_reference, initialize_firebase, modify_firebase
from firebase_admin import db

app = FastAPI()

# Enable CORS for all origins and methods to support frontend-backend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    query: str
    db_type: Optional[str] = None

class ExploreRequest(BaseModel):
    query: str
    db_type: Optional[str] = None

class ModificationRequest(BaseModel):
    modification: str
    db_type: Optional[str] = None 

# Removes markdown code fences (``` or ```python) from AI-generated responses
def remove_code_fences(text: str) -> str:
    text = re.sub(r"```[a-zA-Z]*\n?", "", text)
    text = re.sub(r"\n?```", "", text)
    return text.strip()

# Extracts the main text content from a Gemini API candidate response
def extract_candidate_text(candidate) -> str:
    if not candidate.content or not candidate.content.parts:
        return ""
    for part in candidate.content.parts:
        if isinstance(part, dict):
            if "text" in part and isinstance(part["text"], str):
                return part["text"]
            else:
                return json.dumps(part)
        elif isinstance(part, str):
            return part
        elif hasattr(part, "text"):
            return part.text
    return ""

# Converts a natural language query into structured queries
def convert_nl_to_query(nl_query: str, max_attempts: int = 3) -> Dict[str, Any]:
    client = genai.Client(api_key=api_key)
    attempt = 0
    while attempt < max_attempts:

        prompt = f"""
            You are given a natural language query: "{nl_query}"
            You must produce a valid JSON object with exactly three keys: "mysql", "mongodb", and "firebase".

            Below are the database schemas and a description of the data in each field:

            MySQL Database:
            ---------------
            Hosts table columns:
            - host_id: BIGINT, unique host identifier
            - host_url: VARCHAR(255)
            - host_name: VARCHAR(255)
            - host_since: DATE
            - host_location: VARCHAR(255)
            - host_about: TEXT
            - host_is_superhost: BOOLEAN
            - host_thumbnail_url: VARCHAR(255)
            - host_picture_url: VARCHAR(255)
            - host_listings_count: INT

            Listings table columns:
            - id: BIGINT, unique listing identifier
            - listing_url: VARCHAR(255)
            - name: VARCHAR(255)
            - property_type: VARCHAR(100)
            - room_type: VARCHAR(100)
            - accommodates: INT
            - bathrooms: FLOAT
            - bathrooms_text: VARCHAR(100)
            - bedrooms: INT
            - beds: INT
            - description: TEXT
            - neighborhood_overview: TEXT
            - neighbourhood_cleansed: VARCHAR(100)
            - neighbourhood_group_cleansed: VARCHAR(100)
            - latitude: DOUBLE
            - longitude: DOUBLE
            - host_id: BIGINT (foreign key to Hosts)
            
            Note: The MySQL database does NOT contain price information. Price data is only in Firebase.

            Reviews table columns:
            - listing_id: BIGINT (foreign key to Listings)
            - number_of_reviews: INT
            - number_of_reviews_ltm: INT
            - first_review: DATE
            - last_review: DATE
            - review_scores_rating: FLOAT
            - review_scores_accuracy: FLOAT
            - review_scores_cleanliness: FLOAT
            - review_scores_checkin: FLOAT
            - review_scores_communication: FLOAT
            - review_scores_location: FLOAT
            - review_scores_value: FLOAT
            - reviews_per_month: FLOAT

            MongoDB Database:
            -----------------
            listings_meta collection fields:
            - _id: INTEGER (listing ID)
            - scrape_id: STRING
            - last_scraped: STRING
            - source: STRING
            - host_id: INTEGER
            - host_response_time: STRING
            - host_response_rate: STRING
            - host_acceptance_rate: STRING
            - instant_bookable: STRING
            - license: STRING
            - neighbourhood_cleansed: STRING
            - neighbourhood_group_cleansed: STRING
            
            Note: The MongoDB database does NOT contain price information. Price data is only in Firebase.

            amenities collection fields:
            - listing_id: INTEGER (references the _id in listings_meta)
            - amenities: ARRAY of strings

            media collection fields:
            - listing_id: INTEGER (references the _id in listings_meta)
            - picture_url: STRING
            - host_thumbnail_url: STRING
            - host_picture_url: STRING

            Firebase Realtime Database:
            ---------------------------
            /listings/listing_id:
            - pricing:
                - price: INTEGER
                - weekly_price: INTEGER
                - monthly_price: INTEGER
                - security_deposit: INTEGER
                - cleaning_fee: INTEGER
                - guests_included: INTEGER
                - extra_people: INTEGER
            - availability:
                - availability_30: INTEGER
                - availability_60: INTEGER
                - availability_90: INTEGER
                - availability_365: INTEGER
                - calendar_last_scraped: STRING

            /hosts/host_id:
            - host_is_superhost: BOOLEAN
            - host_listings_count: INTEGER

            Instructions:
            - For "mysql": Generate a comprehensive SQL query that selects appropriate columns from tables. Support the following SQL constructs:
              - SELECT: Select appropriate columns based on the query
              - FROM: Include all necessary tables
              - JOIN: Use appropriate JOIN syntax for related tables
              - WHERE: Include appropriate conditions
              - GROUP BY: Include when aggregations are needed
              - HAVING: Include when filtering on aggregated values
              - ORDER BY: Order results appropriately
              - LIMIT: Limit results to a reasonable number (default to 10 if not specified)
              - Do NOT include price in any clause (price is not in MySQL)
              - Always include semicolons and use proper SQL keywords capitalization
            
            - For "mongodb": Generate a comprehensive MongoDB query for the appropriate collection.
              If the query involves finding documents with certain criteria, generate:
              {{
                "collection": "listings_meta",
                "filter": {{ "neighbourhood_cleansed": "Downtown" }}
              }}
              
              If the query involves projection, add a projection field:
              {{
                "collection": "listings_meta",
                "filter": {{}},
                "projection": {{ "_id": 1, "host_id": 1 }}
              }}
              
              If the query involves sorting, add a sort field:
              {{
                "collection": "listings_meta",
                "filter": {{}},
                "sort": {{ "host_response_rate": -1 }}
              }}
              
              If the query involves limiting results, add a limit field:
              {{
                "collection": "listings_meta",
                "filter": {{}},
                "limit": 5
              }}
              
              If the query requires aggregation (like grouping, complex sorting, etc.), use an aggregation pipeline:
              {{
                "collection": "listings_meta", 
                "aggregate": [
                  {{ "$match": {{ "neighbourhood_cleansed": {{ "$exists": true }} }} }},
                  {{ "$group": {{ "_id": "$neighbourhood_cleansed", "count": {{ "$sum": 1 }} }} }},
                  {{ "$sort": {{ "count": -1 }} }},
                  {{ "$limit": 10 }}
                ]
              }}
              
              Important: For queries about neighborhood statistics or "include neighborhoods with X listings", use proper aggregation:
              {{
                "collection": "listings_meta",
                "aggregate": [
                  {{ "$group": {{ 
                    "_id": "$neighbourhood_cleansed", 
                    "listingCount": {{ "$sum": 1 }},
                    "avgRating": {{ "$avg": "$review_scores_rating" }}
                  }} }},
                  {{ "$match": {{ "listingCount": {{ "$gte": 5 }} }} }}, 
                  {{ "$sort": {{ "avgRating": -1 }} }},
                  {{ "$limit": 10 }}
                ]
              }}
              
              For complex operations like joining collections (lookups), use:
              [
                {{ "$match": {{ "_id": {{ "$exists": true }} }} }},
                {{ "$lookup": {{
                  "from": "amenities",
                  "localField": "_id",
                  "foreignField": "listing_id",
                  "as": "amenities_data"
                }} }}
              ]
              
              If a specific MongoDB operation is requested (LIMIT, SKIP, PROJECT, MATCH, GROUP, SORT, etc.), make sure to include it.
            
            - For "firebase": Generate a query for Firebase that filters listings based on pricing and availability:
              {{
                "orderBy": "pricing/price",
                "limitToFirst": 10,
                "pricing": {{
                  "price": {{"$lt": 150}}
                }}
              }}

            IMPORTANT: For a query about price, you must use the Firebase database only. MySQL and MongoDB do not have price information.

            Return only the valid JSON with the three keys. Example:
            {{
                "mysql": "SELECT l.id, l.name, l.room_type, r.review_scores_rating, AVG(r.review_scores_value) AS avg_value FROM Listings l JOIN Reviews r ON l.id = r.listing_id WHERE r.review_scores_rating > 4.5 GROUP BY l.id, l.name, l.room_type, r.review_scores_rating HAVING avg_value > 4.0 ORDER BY r.review_scores_rating DESC LIMIT 10;",
                "mongodb": {{
                    "collection": "listings_meta",
                    "filter": {{}}
                }},
                "firebase": {{
                    "orderBy": "pricing/price",
                    "limitToFirst": 10,
                    "pricing": {{
                        "price": {{"$lt": 150}}
                    }}
                }}
            }}

            Now, convert the given natural language query.
            
            ANALYZE THE QUERY CAREFULLY. If it's asking for specific MongoDB features like PROJECTION, MATCH, GROUP, SORT, LIMIT, SKIP, etc., make sure to include those in your MongoDB query.
        """

        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )
    
        if not response or not response.candidates:
            attempt += 1
            continue

        candidate = response.candidates[0]
        candidate_text = extract_candidate_text(candidate)
        if not isinstance(candidate_text, str):
            candidate_text = json.dumps(candidate_text)
        candidate_text = remove_code_fences(candidate_text)
        
        # Validate that all three keys are present in the Gemini response
        try:
            result = json.loads(candidate_text)
            if all(k in result for k in ["mysql", "mongodb", "firebase"]):
                return result
        except json.JSONDecodeError:
            pass
        attempt += 1

    print("Invalid JSON from AI after multiple attempts:", candidate_text)
    return {}

# Use Gemini to classify the user's query as schema exploration or general data query
def identify_schema_exploration_query(query: str) -> Dict[str, Any]:
    client = genai.Client(api_key=api_key)
    prompt = f"""
        Analyze this database exploration question: "{query}"
        
        You need to categorize this query into one of these types, focusing ONLY on database schema and metadata exploration:
        
        1. LIST_TABLES - User wants to know what tables exist in the database
           Example: "What tables are in the database?", "Show me all available tables"
           Parameters: db_type (string, optional - one of "mysql", "mongodb", "firebase")
        
        2. TABLE_SCHEMA - User wants to know the schema/structure of a specific table
           Example: "What columns are in the Listings table?", "Show me the structure of Hosts"
           Parameters: table_name (string), db_type (string, optional - one of "mysql", "mongodb", "firebase")
        
        3. SAMPLE_DATA - User wants to see sample data from a table
           Example: "Show me some sample data from Listings", "Give me 5 rows from the Reviews table"
           Parameters: table_name (string), row_count (integer, default 5), db_type (string, optional - one of "mysql", "mongodb", "firebase")
           
        4. GENERAL_QUERY - User is asking for actual data with conditions (not schema exploration)
           Example: "Show me listings with high ratings", "Get all hosts from New York"
        
        Return a JSON object with these fields:
        - query_type: One of the types above
        - parameters: An object with parameters if needed
        - db_type: "mysql", "mongodb", "firebase", or null if not specified
        
        Example output:
        {{
          "query_type": "TABLE_SCHEMA",
          "parameters": {{"table_name": "Listings"}},
          "db_type": "mysql"
        }}
        
        Or:
        {{
          "query_type": "SAMPLE_DATA", 
          "parameters": {{"table_name": "Reviews", "row_count": 5}},
          "db_type": "mongodb"
        }}
        
        If the user is clearly asking about MongoDB collections or Firebase nodes, set db_type appropriately.
    """
    
    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=prompt,
    )
    
    if not response or not response.candidates:
        return {"query_type": "GENERAL_QUERY"}
    
    candidate = response.candidates[0]
    candidate_text = extract_candidate_text(candidate)
    if not isinstance(candidate_text, str):
        candidate_text = json.dumps(candidate_text)
    candidate_text = remove_code_fences(candidate_text)
    
    try:
        result = json.loads(candidate_text)
        if "query_type" in result:
            return result
    except json.JSONDecodeError:
        pass
    
    return {"query_type": "GENERAL_QUERY"}

def get_mysql_tables() -> List[str]:
    query = "SHOW TABLES;"
    results = query_mysql(query)
    tables = []
    for row in results:
        # MySQL returns each table as a dict with a single key; extract the table name
        tables.append(list(row.values())[0])
    return tables

def get_sample_data(table_name: str, row_count: int = 5) -> List[Dict[str, Any]]:
    query = f"SELECT * FROM {table_name} LIMIT {row_count};"
    return query_mysql(query)

def get_mongodb_collections() -> List[str]:
    print("Getting MongoDB collections...")
    try:
        db = get_database()
        collections = db.list_collection_names()
        print(f"Found {len(collections)} MongoDB collections: {collections}")
        return collections
    except Exception as e:
        print(f"Error getting MongoDB collections: {str(e)}")
        return []

# Infers the schema of a MongoDB collection by inspecting a sample document
def get_mongodb_schema(collection_name: str) -> List[Dict[str, Any]]:
    print(f"Getting MongoDB schema for collection: {collection_name}")
    try:
        collection = get_collection(collection_name)
        sample_docs = list(collection.find().limit(5))
        print(f"Found {len(sample_docs)} sample documents")
        
        schema = []
        if sample_docs:
            reference_doc = sample_docs[0]
            print(f"Reference document keys: {list(reference_doc.keys())}")
            for field_name, value in reference_doc.items():
                field_type = type(value).__name__
                schema.append({
                    "Field": field_name,
                    "Type": field_type,
                    "Example": str(value)
                })
        
        print(f"Extracted schema with {len(schema)} fields")
        return schema
    except Exception as e:
        print(f"Error getting MongoDB schema: {str(e)}")
        raise

def get_mongodb_sample(collection_name: str, count: int = 5) -> List[Dict[str, Any]]:
    print(f"Getting MongoDB sample data for collection: {collection_name}, count: {count}")
    try: 
        collection = get_collection(collection_name)
        sample_docs = list(collection.find().limit(count))
        print(f"Found {len(sample_docs)} sample documents")
        
        # Convert ObjectId fields to string for JSON serialization
        converted_docs = convert_objectid_to_str(sample_docs)
        print(f"Converted documents: {len(converted_docs)}")
        return converted_docs
    except Exception as e:
        print(f"Error getting MongoDB samples: {str(e)}")
        raise

def get_firebase_nodes() -> List[str]:
    """Get available top-level nodes in Firebase."""
    print("Getting Firebase nodes...")
    try:
        initialize_firebase()
        ref = db.reference("/")
        data = ref.get()
        print(f"Firebase root data type: {type(data)}")
        
        if isinstance(data, dict):
            nodes = list(data.keys())
            print(f"Found {len(nodes)} Firebase nodes: {nodes}")
            return nodes
        else:
            print(f"Firebase root data is not a dictionary: {type(data)}")
            # Return default nodes if we can't get actual data
            return ["listings", "hosts"]
    except Exception as e:
        print(f"Error getting Firebase nodes: {str(e)}")
        # Return default nodes on error
        return ["listings", "hosts"]

# Attempts to infer the schema of a Firebase node by examining a sample entry
def get_firebase_schema(node_path: str) -> Dict[str, Any]:
    print(f"Getting Firebase schema for path: {node_path}")
    try:
        initialize_firebase()
        # Firebase expects paths to start with a slash
        if not node_path.startswith("/"):
            node_path = f"/{node_path}"
            
        ref = db.reference(node_path)
        data = ref.get()
        print(f"Firebase data for {node_path}: {type(data)}")
        
        # If no data is found, return a sample schema
        if data is None:
            print(f"No data found at path: {node_path}")
            return {
                "schema": "No data found at this path",
                "example_structure": {
                    "listings": {
                        "123456": {
                            "pricing": {
                                "price": 100,
                                "weekly_price": 650,
                                "monthly_price": 2500
                            },
                            "availability": {
                                "availability_30": 15,
                                "availability_60": 30
                            }
                        }
                    }
                }
            }
        
        if isinstance(data, dict):
            if data:
                sample_key = next(iter(data))
                sample_data = data[sample_key]
                print(f"Firebase sample key: {sample_key}, data type: {type(sample_data)}")
                
                # Recursively infer the structure of the sample data
                def infer_schema(data_obj, path=""):
                    if isinstance(data_obj, dict):
                        schema = {}
                        for k, v in data_obj.items():
                            new_path = f"{path}/{k}" if path else k
                            schema[k] = infer_schema(v, new_path)
                        return schema
                    elif isinstance(data_obj, list):
                        return f"Array[{len(data_obj)}]"
                    else:
                        return f"{type(data_obj).__name__}"
                
                return {
                    "schema": infer_schema(sample_data),
                    "example_key": sample_key
                }
            else:
                print(f"Empty dictionary for Firebase path: {node_path}")
                return {
                    "schema": "Empty object at this path",
                    "structure": "Dictionary with no keys"
                }
        else:
            print(f"Firebase data is not a dictionary for path: {node_path}")
            return {
                "schema": f"Data at this path is of type: {type(data).__name__}",
                "value": str(data)
            }
    except Exception as e:
        print(f"Error getting Firebase schema: {str(e)}")
        # Provide a sample schema for known node types if error occurs
        if "listings" in node_path:
            return {
                "error": str(e),
                "sample_schema": {
                    "pricing": {
                        "price": "int",
                        "weekly_price": "int"
                    },
                    "availability": {
                        "availability_30": "int",
                        "availability_60": "int"
                    }
                }
            }
        elif "hosts" in node_path:
            return {
                "error": str(e),
                "sample_schema": {
                    "host_is_superhost": "boolean",
                    "host_listings_count": "int"
                }
            }
        return {"error": str(e)}

def get_firebase_sample(node_path: str, count: int = 5) -> List[Dict[str, Any]]:
    print(f"Getting Firebase sample data for path: {node_path}, count: {count}")
    try:
        initialize_firebase()
        if not node_path.startswith("/"):
            node_path = f"/{node_path}"
            
        ref = db.reference(node_path)
        data = ref.get()
        print(f"Firebase data type for {node_path}: {type(data)}")
        
        results = []
        if data is None:
            print(f"No data found at path: {node_path}")
            # Return example data if nothing is found at the path
            return [
                {"id": "example_id_1", "note": "This is example data since no actual data was found"},
                {"id": "example_id_2", "note": "Try with path 'listings' or 'hosts'"}
            ]
        
        if isinstance(data, dict):
            sample_keys = list(data.keys())[:count]
            print(f"Sample keys: {sample_keys}")
            for key in sample_keys:
                sample = data[key]
                if isinstance(sample, dict):
                    sample["id"] = key
                else:
                    sample = {"id": key, "value": sample}
                results.append(sample)
            
            print(f"Returning {len(results)} Firebase samples")
            return results
        else:
            print(f"Firebase data is not a dictionary: {type(data)}")
            return [{"value": str(data), "type": type(data).__name__}]
    except Exception as e:
        print(f"Error getting Firebase samples: {str(e)}")
        # Return example data on error
        return [
            {"id": "example_id", "error": str(e)},
            {"id": "sample", "note": "This is example data due to an error"}
        ]

api_key = "AIzaSyBb4lNTOjji1fUJhNvkf4CpKo_yQDrTn0M" 
# API key for Google Gemini APII 
# This value must be replaced with actual credential before deployment

@app.post("/explore")
async def explore_database(request: ExploreRequest):

    try:
        # Classify the exploration query and extract parameters
        exploration = identify_schema_exploration_query(request.query)
        query_type = exploration.get("query_type", "GENERAL_QUERY")
        parameters = exploration.get("parameters", {})
        
        db_type = request.db_type or exploration.get("db_type")
        
        if query_type == "LIST_TABLES":
            if db_type == "mongodb":
                collections = get_mongodb_collections()
                return {
                    "exploration_type": "collections",
                    "db_type": "mongodb",
                    "message": "Available collections in MongoDB",
                    "data": collections
                }
            elif db_type == "firebase":
                nodes = get_firebase_nodes()
                return {
                    "exploration_type": "nodes",
                    "db_type": "firebase",
                    "message": "Available top-level nodes in Firebase",
                    "data": nodes
                }
            else:  # Default to MySQL
                tables = get_mysql_tables()
                return {
                    "exploration_type": "tables",
                    "db_type": "mysql",
                    "message": "Available tables in MySQL",
                    "data": tables
                }
            
        elif query_type == "TABLE_SCHEMA":
            table_name = parameters.get("table_name")
            if not table_name:
                return {
                    "exploration_type": "error", 
                    "message": "No table/collection name provided for schema exploration"
                }
                
            if db_type == "mongodb":
                # Allow any collection name in MongoDB
                try:
                    schema = get_mongodb_schema(table_name)
                    return {
                        "exploration_type": "schema",
                        "db_type": "mongodb",
                        "message": f"Schema for collection '{table_name}'",
                        "data": schema
                    }
                except Exception as e:
                    return {
                        "exploration_type": "error",
                        "message": f"Error accessing MongoDB collection '{table_name}': {str(e)}"
                    }
            elif db_type == "firebase":
                try:
                    schema = get_firebase_schema(table_name)
                    return {
                        "exploration_type": "schema",
                        "db_type": "firebase",
                        "message": f"Schema for Firebase path '{table_name}'",
                        "data": schema
                    }
                except Exception as e:
                    return {
                        "exploration_type": "error",
                        "message": f"Error accessing Firebase path '{table_name}': {str(e)}"
                    }
            else:  # Default to MySQL
                if not validate_table_exists(table_name):
                    return {
                        "exploration_type": "error",
                        "message": f"Table '{table_name}' does not exist"
                    }
                    
                schema = get_table_schema(table_name)
                return {
                    "exploration_type": "schema",
                    "db_type": "mysql",
                    "message": f"Schema for table '{table_name}'",
                    "data": schema
                }
            
        elif query_type == "SAMPLE_DATA":
            table_name = parameters.get("table_name")
            if not table_name:
                return {
                    "exploration_type": "error",
                    "message": "No table/collection name provided for sample data"
                }
                
            row_count = parameters.get("row_count", 5)
            try:
                row_count = int(row_count)
            except (ValueError, TypeError):
                row_count = 5
                
            if db_type == "mongodb":
                try:
                    samples = get_mongodb_sample(table_name, row_count)
                    return {
                        "exploration_type": "sample_data",
                        "db_type": "mongodb",
                        "message": f"Sample data from collection '{table_name}'",
                        "data": samples
                    }
                except Exception as e:
                    return {
                        "exploration_type": "error",
                        "message": f"Error accessing MongoDB collection '{table_name}': {str(e)}"
                    }
            elif db_type == "firebase":
                try:
                    samples = get_firebase_sample(table_name, row_count)
                    return {
                        "exploration_type": "sample_data",
                        "db_type": "firebase",
                        "message": f"Sample data from Firebase path '{table_name}'",
                        "data": samples
                    }
                except Exception as e:
                    return {
                        "exploration_type": "error",
                        "message": f"Error accessing Firebase path '{table_name}': {str(e)}"
                    }
            else:  # Default to MySQL
                if not validate_table_exists(table_name):
                    return {
                        "exploration_type": "error",
                        "message": f"Table '{table_name}' does not exist"
                    }
                
                samples = get_sample_data(table_name, row_count)
                return {
                    "exploration_type": "sample_data",
                    "db_type": "mysql",
                    "message": f"Sample data from table '{table_name}'",
                    "data": samples
                }
            
        else:
            # If not a schema exploration query, process as a general query
            return await process_query(QueryRequest(query=request.query, db_type=db_type))
            
    except Exception as e:
        print(f"Exploration error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def process_query(request: QueryRequest):
    try:
        converted_queries = convert_nl_to_query(request.query)
        if not converted_queries:
            return {
                "message": "No valid queries could be generated for this request.",
                "converted_queries": {}
            }
        
        results = {}
        nl_lower = request.query.lower()
        
        # Check if a specific database type was requested
        if request.db_type:
            # Only query the specified database type
            if request.db_type == "mysql" and "mysql" in converted_queries:
                results["mysql"] = query_mysql(converted_queries["mysql"])
            elif request.db_type == "mongodb" and "mongodb" in converted_queries:
                mongo_query = converted_queries["mongodb"]
                if isinstance(mongo_query, str):
                    try:
                        mongo_query = json.loads(mongo_query)
                    except json.JSONDecodeError:
                        mongo_query = {}
                results["mongodb"] = query_mongodb(mongo_query)
            elif request.db_type == "firebase" and "firebase" in converted_queries:
                firebase_query = converted_queries["firebase"]
                if isinstance(firebase_query, str):
                    try:
                        firebase_query = json.loads(firebase_query)
                    except json.JSONDecodeError:
                        firebase_query = None
                fb_result = query_firebase("listings", firebase_query)
                if fb_result is not None:
                    results["firebase"] = fb_result
            
            # Add the requested database results to merged results
            if request.db_type in results and results[request.db_type]:
                results["merged"] = results[request.db_type]
            else:
                results["merged"] = []
                
            return {"converted_queries": converted_queries, "results": results}
            
        # If no specific db_type was provided, continue with the original logic
        if "firebase" in converted_queries:
            firebase_query = converted_queries["firebase"]
            if isinstance(firebase_query, str):
                try:
                    firebase_query = json.loads(firebase_query)
                except json.JSONDecodeError:
                    firebase_query = None
            fb_result = query_firebase("listings", firebase_query)
            if fb_result is not None:
                results["firebase"] = fb_result
                
                listing_ids = []
                for item in fb_result:
                    if "id" in item and item["id"] is not None:
                        try:
                            listing_ids.append(int(item["id"]))
                        except (ValueError, TypeError):
                            listing_ids.append(str(item["id"]).strip())
                
                if listing_ids:
                    if "mysql" in converted_queries:
                        mysql_query = converted_queries["mysql"]
                        
                        if listing_ids:
                            if all(isinstance(id, int) for id in listing_ids):
                                ids_sql = ", ".join(str(id) for id in listing_ids)
                            else:
                                ids_sql = ", ".join(f"'{id}'" for id in listing_ids)
                            
                            # Insert the listing_ids filter into the MySQL query
                            if " WHERE " in mysql_query.upper():
                                mysql_query = mysql_query.replace(" WHERE ", f" WHERE id IN ({ids_sql}) AND ", 1)
                            elif " LIMIT " in mysql_query.upper():
                                mysql_query = mysql_query.replace(" LIMIT ", f" WHERE id IN ({ids_sql}) LIMIT ", 1)
                            elif ";" in mysql_query:
                                mysql_query = mysql_query.replace(";", f" WHERE id IN ({ids_sql});", 1)
                            else:
                                mysql_query = f"{mysql_query} WHERE id IN ({ids_sql})"
                        
                        try:
                            results["mysql"] = query_mysql(mysql_query)
                        except Exception as e:
                            print(f"MySQL query error: {str(e)}")
                            results["mysql"] = []
                        
                    if "mongodb" in converted_queries:
                        try:
                            mongo_query = converted_queries["mongodb"]
                            if isinstance(mongo_query, str):
                                try:
                                    mongo_query = json.loads(mongo_query)
                                except json.JSONDecodeError:
                                    mongo_query = {}
                            
                            if isinstance(mongo_query, dict):
                                collection = mongo_query.get("collection", "listings_meta")
                                
                                # Filter MongoDB query by listing_ids, handling collection-specific keys
                                if listing_ids:
                                    if "filter" in mongo_query:
                                        mongo_query["filter"]["_id"] = {"$in": listing_ids}
                                    else:
                                        mongo_query["filter"] = {"_id": {"$in": listing_ids}}
                                        
                                    if collection in ["amenities", "media"]:
                                        mongo_query["filter"]["listing_id"] = {"$in": listing_ids}
                                        if "_id" in mongo_query["filter"]:
                                            del mongo_query["filter"]["_id"]
                            
                            print(f"MongoDB query: {json.dumps(mongo_query, default=str)}")
                            results["mongodb"] = query_mongodb(mongo_query)
                            print(f"MongoDB results count: {len(results['mongodb'])}")
                        except Exception as e:
                            print(f"MongoDB query error: {str(e)}")
                            results["mongodb"] = []
        
        if not results.get("firebase") or not listing_ids:
            query_mysql_needed = True
            query_mongodb_needed = True
            query_firebase_needed = True
            
            # If the query is about availability, only Firebase is relevant
            if "avail" in nl_lower or "available" in nl_lower:
                query_mysql_needed = False
                query_mongodb_needed = False
            
            # If the query is about reviews or room type, Firebase is not relevant
            if "review" in nl_lower or "number_of_reviews" in nl_lower or "room_type" in nl_lower:
                query_firebase_needed = False

            if query_mysql_needed and "mysql" in converted_queries:
                results["mysql"] = query_mysql(converted_queries["mysql"])
            
            if query_mongodb_needed and "mongodb" in converted_queries:
                try:
                    mongo_query = converted_queries["mongodb"]
                    if isinstance(mongo_query, str):
                        mongo_query = json.loads(mongo_query)
                    results["mongodb"] = query_mongodb(mongo_query)
                except json.JSONDecodeError:
                    results["mongodb"] = []
            
            if query_firebase_needed and "firebase" in converted_queries:
                firebase_query = converted_queries["firebase"]
                if isinstance(firebase_query, str):
                    try:
                        firebase_query = json.loads(firebase_query)
                    except json.JSONDecodeError:
                        firebase_query = None
                fb_result = query_firebase("listings", firebase_query)
                if fb_result is not None:
                    results["firebase"] = fb_result
        
        merged_results = []
        # Merge results from MySQL and MongoDB using listing id as the join key
        if "mysql" in results and "mongodb" in results and results["mysql"] and results["mongodb"]:
            mysql_data = {str(item["id"]): item for item in results["mysql"] if "id" in item}
            for mongo_item in results["mongodb"]:
                mongo_id = str(mongo_item.get("_id", ""))
                if mongo_id in mysql_data:
                    merged_item = {**mysql_data[mongo_id], **mongo_item}
                    if "firebase" in results:
                        fb_item = next((item for item in results["firebase"] if str(item.get("id", "")) == mongo_id), None)
                        if fb_item:
                            merged_item.update(fb_item)
                    merged_results.append(merged_item)

        # Prefer merged results if available, otherwise fallback to the most complete single source
        if merged_results:
            results["merged"] = merged_results
        else:
            if "firebase" in results and results["firebase"]:
                results["merged"] = results["firebase"]
            elif "mysql" in results and results["mysql"]:
                results["merged"] = results["mysql"] 
            elif "mongodb" in results and results["mongodb"]:
                results["merged"] = results["mongodb"]
            else:
                results["merged"] = []
        
        return {"converted_queries": converted_queries, "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# modification

def convert_nl_to_modification(nl_modification: str) -> dict:
    client = genai.Client(api_key=api_key)
    prompt = f"""
    You are processing a natural language modification command and must generate valid modification queries
    for three databases: MySQL, MongoDB, and Firebase.
    
    The command could be for inserting, updating, or deleting data. Follow these rules:
    
    1) For MySQL:
    • Schema:
        -Listings(id PK, listing_url, name, property_type, room_type, accommodates, bathrooms, bedrooms, beds,
        description, neighborhood_overview, neighborhood_cleansed, latitude, longitude, host_id FK)  
        -Hosts(host_id PK, host_url, host_name, host_since, host_location, host_about, host_is_superhost,
        host_thumbnail_url, host_picture_url, host_listings_count)
        -Reviews(listing_id FK, number_of_reviews, first_review, last_review, review_scores_rating)
    • Output:
        -If it's an INSERT: generate a complete INSERT statement with all required fields
        Example: "INSERT INTO Listings (id, name, property_type, host_id) VALUES (12345, 'Cozy Apartment', 'Apartment', 67890);"
        -If it's an UPDATE: generate a complete UPDATE statement
        Example: "UPDATE Listings SET name = 'Luxury Apartment', beds = 2 WHERE id = 12345;"
        -If it's a DELETE: generate a complete DELETE statement
        Example: "DELETE FROM Listings WHERE id = 12345;"
    • Make sure to:
        - Include all necessary quotes around string values (use single quotes)
        - End all statements with semicolons
        - For numeric values, don't include quotes
        - For dates, use format 'YYYY-MM-DD'
    • If MySQL isn't affected, set `"mysql":""`.

    2) For MongoDB:
    • Schema:
        -listings_meta collection: 
        _id (listing ID, numeric), 
        scrape_id, 
        last_scraped, 
        source, 
        host_id, 
        host_response_time,
        host_response_rate, 
        host_acceptance_rate, 
        instant_bookable, 
        license,
        neighbourhood_cleansed, 
        neighbourhood_group_cleansed,
        market,
        smart_location,
        country_code,
        country
        -amenities collection: listing_id (references listings_meta._id), amenities (array of strings)
        -media collection: listing_id (references listings_meta._id), picture_url, host_thumbnail_url, host_picture_url
    • IMPORTANT: smart_location and neighbourhood_cleansed are DIFFERENT fields and should not be confused
    • Output: a single JSON object:
        - For SINGLE INSERT:
        {{
            "operation": "insert",
            "document": {{
            "_id": 12345,
            "host_id": 67890,
            "host_response_rate": "95%",
            "neighbourhood_cleansed": "Downtown",
            "smart_location": "Los Angeles, California"
            }}
        }}
        - For BULK INSERT (multiple documents):
        {{
            "operation": "insert",
            "documents": [
            {{
                "_id": 12345,
                "host_id": 67890,
                "neighbourhood_cleansed": "Downtown",
                "smart_location": "Los Angeles, California"
            }},
            {{
                "_id": 12346,
                "host_id": 67891,
                "neighbourhood_cleansed": "Midtown",
                "smart_location": "New York, New York"
            }}
            ]
        }}
        - For UPDATE:
        {{
            "operation": "update",
            "filter": {{ "_id": 12345 }},
            "update": {{ 
            "host_response_rate": "100%",
            "smart_location": "San Francisco, California"
            }}
        }}
        - For BULK UPDATE (updating multiple documents):
        {{
            "operation": "update",
            "filter": {{ "neighbourhood_cleansed": "Downtown" }},
            "update": {{ 
            "host_response_rate": "100%",
            "smart_location": "San Francisco, California"
            }},
            "multi": true
        }}
        - For DELETE:
        {{
            "operation": "delete",
            "filter": {{ "_id": 12345 }}
        }}
        - For BULK DELETE (deleting multiple documents):
        {{
            "operation": "delete",
            "filter": {{ "neighbourhood_cleansed": "Downtown" }},
            "multi": true
        }}
    • IMPORTANT: 
        - Make sure _id is a NUMBER not a string when using it as an identifier
        - If the natural language indicates multiple documents (plural, "all", "many", etc.), set multi:true
        - For operations that should affect multiple documents, include "multi": true in the object
        - Be extremely precise with field names - DO NOT confuse smart_location with neighbourhood_cleansed
    • If MongoDB isn't affected, set `"mongodb":""`.

    3) For Firebase Realtime DB:
    • Schema:
        -`/listings` node keyed by numeric `id`, each child has:
        pricing: {{ price, weekly_price, monthly_price, security_deposit, cleaning_fee, guests_included, extra_people }}
        availability: {{ availability_30, availability_60, availability_90, availability_365, calendar_last_scraped }}
    • Output: a single JSON object:
        {{
        "operation": "insert"|"update"|"delete",
        "key": "<string id>",
        "data": {{…}}        // for insert/update
        }}
    • If Firebase isn't affected, set `"firebase":""`.
        
        Now, convert the following natural language modification command:
    '{nl_modification}'
    """

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=prompt,
    )
    if not response or not response.candidates:
        return {}
    candidate = response.candidates[0]
    if not candidate.content or not candidate.content.parts or len(candidate.content.parts) == 0:
        return {}
    generated_modifications = candidate.content.parts[0].text.strip()

    try:
        clean_modifications = json.loads(remove_code_fences(generated_modifications))
        print(f"Generated modifications: {clean_modifications}")
        return clean_modifications if isinstance(clean_modifications, dict) else {}
    except json.JSONDecodeError:
        print("Invalid JSON from AI (modification):", generated_modifications)
        return {}

@app.post("/modify")
async def process_modification(request: ModificationRequest):
    try:
        print(f"Processing modification request: {request.modification}")
        converted_modifications = convert_nl_to_modification(request.modification)
        db_choice = request.db_type.lower() if request.db_type else None
        print(f"Selected database type: {db_choice}")
        print(f"Converted modifications: {converted_modifications}")

        if not converted_modifications:
            return {
                "message": "No valid modifications could be generated for this request.",
                "converted_modifications": {}
            }

        if db_choice:
            # Only keep modifications for the selected database
            filtered_mods = {}
            if db_choice in converted_modifications and converted_modifications[db_choice]:
                filtered_mods[db_choice] = converted_modifications[db_choice]
            converted_modifications = filtered_mods
            print(f"Filtered modifications for {db_choice}: {converted_modifications}")

        results = {}

        # MySQL: executes SQL modification statements
        if "mysql" in converted_modifications:
            mysql_mod = converted_modifications["mysql"]
            if isinstance(mysql_mod, str) and mysql_mod.strip():
                print(f"Executing MySQL modification: {mysql_mod}")
                # Ensure MySQL statements are properly formatted
                if not mysql_mod.strip().endswith(';'):
                    mysql_mod = mysql_mod.strip() + ';'
                results["mysql"] = modify_mysql(mysql_mod)
            elif isinstance(mysql_mod, (list, tuple)) and mysql_mod:
                # Format and execute each statement in a batch
                formatted_stmts = []
                for stmt in mysql_mod:
                    if isinstance(stmt, str) and stmt.strip():
                        if not stmt.strip().endswith(';'):
                            stmt = stmt.strip() + ';'
                        formatted_stmts.append(stmt)
                
                if formatted_stmts:
                    print(f"Executing MySQL modifications (multiple): {formatted_stmts}")
                    results["mysql"] = modify_mysql(formatted_stmts)
                else:
                    print("No valid MySQL statements after formatting")
                    results["mysql"] = {"message": "No valid MySQL modification statements"}
            else:
                print(f"Skipping MySQL modification - invalid format: {type(mysql_mod)}")
                print(f"Value: {mysql_mod}")
                results["mysql"] = {"message": "No valid MySQL modification provided"}

        # MongoDB: executes modification instructions on the specified collection
        if "mongodb" in converted_modifications:
            mongo_mod_val = converted_modifications.get("mongodb")
            if mongo_mod_val:
                if isinstance(mongo_mod_val, str):
                    try:
                        print(f"Parsing MongoDB modification from string: {mongo_mod_val}")
                        mongo_mod = json.loads(mongo_mod_val)
                    except json.JSONDecodeError as ex:
                        print(f"Error parsing MongoDB JSON: {str(ex)}")
                        raise HTTPException(400, f"Bad MongoDB mod JSON: {ex}")
                elif isinstance(mongo_mod_val, dict):
                    mongo_mod = mongo_mod_val
                else:
                    print(f"Unexpected MongoDB modification type: {type(mongo_mod_val)}")
                    raise HTTPException(400, "Unsupported MongoDB mod format.")
                
                # If the operation type is missing, try to infer it from the fields
                if "operation" not in mongo_mod:
                    print("Adding missing 'operation' field to MongoDB modification")
                    if "document" in mongo_mod or "documents" in mongo_mod:
                        mongo_mod["operation"] = "insert"
                    elif "update" in mongo_mod:
                        mongo_mod["operation"] = "update"
                    elif "filter" in mongo_mod:
                        mongo_mod["operation"] = "delete"
                    else:
                        raise HTTPException(400, "Cannot determine MongoDB operation type")

                collection = "listings_meta"
                # Use a different collection if specified
                if "collection" in mongo_mod:
                    collection = mongo_mod.pop("collection")
                
                print(f"Executing MongoDB modification on collection {collection}: {json.dumps(mongo_mod, default=str)}")
                results["mongodb"] = modify_mongodb(mongo_mod, collection)

        # Firebase: executes modification on the listings node
        if db_choice is None or db_choice == "firebase":
            fb_mod_val = converted_modifications.get("firebase")
            if fb_mod_val:
                if isinstance(fb_mod_val, str):
                    try:
                        firebase_mod = json.loads(fb_mod_val)
                    except json.JSONDecodeError as ex:
                        raise HTTPException(400, f"Bad Firebase mod JSON: {ex}")
                elif isinstance(fb_mod_val, dict):
                    firebase_mod = fb_mod_val
                else:
                    raise HTTPException(400, "Unsupported Firebase mod format.")

                op  = firebase_mod.get("operation")
                key = firebase_mod.get("key", "")
                data = firebase_mod.get("data", {})
                results["firebase"] = modify_firebase("listings", key, op, data)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "converted_modifications": converted_modifications,
        "results": results
    }

if __name__ == "__main__": 
    uvicorn.run(app, host="127.0.0.1", port=8000)
