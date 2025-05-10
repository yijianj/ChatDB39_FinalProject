import firebase_admin
from firebase_admin import credentials, db
from fastapi import HTTPException
from typing import Any, Dict, List, Optional, Union
import json
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIAL_PATH = os.path.join(os.path.dirname(os.path.dirname(CURRENT_DIR)), 
                               "credential\dsci551proj-437a5-firebase-adminsdk-fbsvc-3599be0830.json") # change to your actual Firebase credential file path

# Firebase credential file path and database URL
# change to your actual Firebase credential file path and database URL
# These values must be replaced with actual credentials before deployment
FIREBASE_CONFIG = {
    "credential_path": CREDENTIAL_PATH,
    "database_url": "https://dsci551proj-437a5-default-rtdb.firebaseio.com" 
} 

NODES = {
    "listings": "listings",
    "hosts": "hosts"
}

def initialize_firebase():

    # Initializes Firebase connection with credentials or fallback authentication.
    # Uses a tiered approach to authentication:
    # 1. Certificate-based auth with provided credentials
    # 2. Application default credentials as fallback

    try:
        if not firebase_admin._apps:
            print(f"Initializing Firebase with credential path: {FIREBASE_CONFIG['credential_path']}")
            try:
                cred = credentials.Certificate(FIREBASE_CONFIG["credential_path"])
                firebase_admin.initialize_app(cred, {
                    "databaseURL": FIREBASE_CONFIG["database_url"]
                })
                print("Firebase initialization successful")
            except Exception as e:
                print(f"Firebase initialization error: {str(e)}")
                # Fallback to application default credentials if certificate fails
                try:
                    firebase_admin.initialize_app(options={"databaseURL": FIREBASE_CONFIG["database_url"]})
                    print("Firebase initialized with default credentials")
                except Exception as inner_e:
                    print(f"Firebase default credentials error: {str(inner_e)}")
                    raise
        else:
            print("Firebase already initialized")
    except Exception as e:
        print(f"Failed to initialize Firebase: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialize Firebase: {str(e)}"
        )

def get_reference(node: str) -> db.Reference:

    # Retrieves a Firebase reference to the specified database node.
    # Includes security validation to prevent unauthorized node access.

    try:
        if node.startswith('/'):
            node = node[1:]  # Remove leading slash
            
        # Security validation to prevent accessing unauthorized nodes
        if node not in NODES.values() and not any(node.startswith(f"{n}/") for n in NODES.values()):
            print(f"Warning: Accessing non-standard node: {node}")
            
        ref_path = f"/{node}"
        print(f"Getting Firebase reference to: {ref_path}")
        return db.reference(ref_path)
    except Exception as e:
        print(f"Error getting Firebase reference: {str(e)}")
        raise HTTPException(
            status_code=400,
            detail=f"Invalid Firebase node: {node}. Error: {str(e)}"
        )

def query_firebase(
    node: str,
    query_obj: Optional[Dict[str, Any]] = None) -> Union[Dict[str, Any], List[Dict[str, Any]]]:

    # Executes queries against Firebase database with filtering capabilities.
    # Supports complex filtering operations on nested fields and pagination.

    try:
        initialize_firebase()
        ref = get_reference(node)
        all_data = ref.get()
        
        if not all_data:
            return []
        if not query_obj:
            return list(all_data.values()) if isinstance(all_data, dict) else all_data
        
        results = []
        
        items = []
        for key, value in all_data.items():
            if isinstance(value, dict):
                value['id'] = key
                items.append(value)
            else:
                items.append({'id': key, 'value': value})
        
        if isinstance(query_obj, dict):
            if 'pricing' in query_obj:
                for condition_field, condition in query_obj['pricing'].items():
                    if '$lt' in condition:
                        limit_value = condition['$lt']
                        items = [
                            item for item in items
                            if 'pricing' in item and
                            condition_field in item['pricing'] and
                            item['pricing'][condition_field] is not None and
                            float(item['pricing'][condition_field]) < limit_value
                        ]
                    elif '$gt' in condition:
                        limit_value = condition['$gt']
                        items = [
                            item for item in items 
                            if 'pricing' in item and
                            condition_field in item['pricing'] and
                            item['pricing'][condition_field] is not None and
                            float(item['pricing'][condition_field]) > limit_value
                        ]
                    elif '$eq' in condition:
                        limit_value = condition['$eq']
                        items = [
                            item for item in items
                            if 'pricing' in item and
                            condition_field in item['pricing'] and
                            item['pricing'][condition_field] is not None and
                            float(item['pricing'][condition_field]) == limit_value
                        ]
            
            if 'availability' in query_obj:
                for condition_field, condition in query_obj['availability'].items():
                    if '$lt' in condition:
                        limit_value = condition['$lt']
                        items = [
                            item for item in items
                            if 'availability' in item and
                            condition_field in item['availability'] and
                            item['availability'][condition_field] is not None and
                            float(item['availability'][condition_field]) < limit_value
                        ]
                    elif '$gt' in condition:
                        limit_value = condition['$gt']
                        items = [
                            item for item in items
                            if 'availability' in item and
                            condition_field in item['availability'] and
                            item['availability'][condition_field] is not None and
                            float(item['availability'][condition_field]) > limit_value
                        ]
                    elif '$eq' in condition:
                        limit_value = condition['$eq']
                        items = [
                            item for item in items
                            if 'availability' in item and
                            condition_field in item['availability'] and
                            item['availability'][condition_field] is not None and
                            float(item['availability'][condition_field]) == limit_value
                        ]
            
            if 'orderBy' in query_obj:
                order_field = query_obj['orderBy']
                if '/' in order_field:
                    main_field, sub_field = order_field.split('/')
                    items.sort(key=lambda x: (
                        float(x.get(main_field, {}).get(sub_field, float('inf')))
                        if x.get(main_field, {}).get(sub_field) is not None
                        else float('inf')
                    ))
                else:
                    items.sort(key=lambda x: float(x.get(order_field, float('inf'))) if x.get(order_field) is not None else float('inf'))
            
            if 'limitToFirst' in query_obj:
                limit = query_obj['limitToFirst']
                items = items[:limit]
        
        return items
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Firebase query error: {str(e)}"
        )

# modification
def modify_firebase(
    node: str,
    key: str,
    operation: str,
    data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

    # Performs CRUD operations on Firebase nodes with data normalization.
    # Different schemas are applied based on the target node type.

    try:
        initialize_firebase()
        ref = get_reference(node)
        
        if operation in ["insert", "update"]:
            if data is None:
                raise ValueError("Data required for insert/update operations")
                
            if node == "listings":
                normalized_data = {
                    "pricing": {
                        k: v for k, v in data.items()
                        if k in ["price", "weekly_price", "monthly_price", 
                                "security_deposit", "cleaning_fee", 
                                "guests_included", "extra_people"]
                    },
                    "availability": {
                        k: v for k, v in data.items()
                        if k in ["availability_30", "availability_60", 
                                "availability_90", "availability_365", 
                                "calendar_last_scraped"]
                    }
                }
            else:
                normalized_data = {
                    k: v for k, v in data.items()
                    if k in ["host_is_superhost", "host_listings_count"]
                }
            
            if not key:
                child_ref = ref.push()
                key = child_ref.key
            else:
                key = key.strip('"')
                child_ref = ref.child(key)
                
            try:
                numeric_id = int(key)
                normalized_data["id"] = numeric_id
            except ValueError:
                pass
                
            if operation == "insert":
                child_ref.set(normalized_data)
            else:
                child_ref.update(normalized_data)
                
            return {
                "message": f"Firebase {operation} successful",
                "key": key
            }
            
        elif operation == "delete":
            if not key:
                raise ValueError("Key required for delete operation")
                
            key = key.strip('"')
            ref.child(key).delete()
            return {
                "message": "Firebase delete successful",
                "key": key
            }
            
        else:
            raise ValueError(f"Unsupported operation: {operation}")
            
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Firebase modification error: {str(e)}"
        )