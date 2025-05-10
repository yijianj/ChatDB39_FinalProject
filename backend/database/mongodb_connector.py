import pymongo
import json
import numpy as np
from fastapi import HTTPException
from bson import ObjectId
from typing import List, Dict, Any, Optional, Union
from pymongo.collection import Collection
from pymongo.database import Database

# replace with actual MongoDB Server config values on your system if needed
# These values must be replaced with actual credentials before deployment
MONGO_CONFIG = {
    "host": "localhost",
    "port": 27017,
    "database": "airbnb_db"
}

COLLECTIONS = {
    "listings_meta": "listings_meta",
    "amenities": "amenities",
    "media": "media"
}

def get_database() -> Database:
    try:
        client = pymongo.MongoClient(**{k: v for k, v in MONGO_CONFIG.items() if k != "database"})
        return client[MONGO_CONFIG["database"]]
    except pymongo.errors.ConnectionError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to connect to MongoDB: {str(e)}"
        )

def get_collection(collection_name: str) -> Collection:
    if collection_name not in COLLECTIONS.values():
        raise HTTPException(
            status_code=400,
            detail=f"Invalid collection name. Must be one of: {', '.join(COLLECTIONS.values())}"
        )
    return get_database()[collection_name]

def convert_objectid_to_str(doc: Any) -> Any:
    if isinstance(doc, list):
        return [convert_objectid_to_str(d) for d in doc]
    if isinstance(doc, dict):
        return {k: convert_objectid_to_str(v) for k, v in doc.items()}
    if isinstance(doc, ObjectId):
        return str(doc)
    if isinstance(doc, float) and np.isnan(doc):
        return None
    if not isinstance(doc, (str, int, float, bool, type(None))):
        return str(doc)
    return doc

def query_mongodb(
    mongo_filter: Union[Dict[str, Any], List[Dict[str, Any]]],
    collection_name: str = "listings_meta"
) -> List[Dict[str, Any]]:
    try:
        print(f"MongoDB Query: {json.dumps(mongo_filter, default=str)}")
        coll = get_collection(collection_name)
        
        if isinstance(mongo_filter, dict):
            if "collection" in mongo_filter:
                collection_name = mongo_filter["collection"]
                coll = get_collection(collection_name)
            
            if "aggregate" in mongo_filter:
                pipeline = mongo_filter["aggregate"]
                if isinstance(pipeline, list):
                    try:
                        print(f"Executing aggregate pipeline: {json.dumps(pipeline, default=str)}")
                        results = list(coll.aggregate(pipeline))
                        return convert_objectid_to_str(results)
                    except pymongo.errors.OperationFailure as e:
                        print(f"MongoDB aggregation error: {str(e)}")
                        if "$group" in str(e) and "$sort" in str(e):
                            # Try to fix common group+sort issues by adding allowDiskUse
                            try:
                                print("Retrying with allowDiskUse=True")
                                results = list(coll.aggregate(pipeline, allowDiskUse=True))
                                return convert_objectid_to_str(results)
                            except Exception as retry_err:
                                print(f"Retry failed: {str(retry_err)}")
                                return []
                        return []
                
            # Handle standard query with filter
            filter_obj = {}
            if "filter" in mongo_filter:
                filter_obj = mongo_filter["filter"]
            elif "query" in mongo_filter:
                filter_obj = mongo_filter["query"]
                
            # Handle projection
            projection = None
            if "projection" in mongo_filter:
                projection = mongo_filter["projection"]
                
            # Handle sort and limit
            sort_obj = None
            if "sort" in mongo_filter:
                sort_obj = mongo_filter["sort"]
            elif "$sort" in mongo_filter:
                sort_obj = mongo_filter["$sort"]
            elif "$orderby" in mongo_filter:
                sort_obj = mongo_filter["$orderby"]
                
            limit_val = 0
            if "limit" in mongo_filter:
                limit_val = mongo_filter["limit"]
            elif "$limit" in mongo_filter:
                limit_val = mongo_filter["$limit"]
                
            # Create a pipeline if needed
            if sort_obj or limit_val:
                pipeline = []
                # Add match stage if filter exists
                if filter_obj:
                    pipeline.append({"$match": filter_obj})
                # Add projection if exists
                if projection:
                    pipeline.append({"$project": projection})
                # Add sort if exists
                if sort_obj:
                    pipeline.append({"$sort": sort_obj})
                # Add limit if exists
                if limit_val > 0:
                    pipeline.append({"$limit": limit_val})
                    
                try:
                    print(f"Executing built pipeline: {json.dumps(pipeline, default=str)}")
                    results = list(coll.aggregate(pipeline))
                    return convert_objectid_to_str(results)
                except pymongo.errors.OperationFailure as e:
                    print(f"MongoDB pipeline error: {str(e)}")
                    # Use allowDiskUse for large datasets since MongoDB has a 100MB memory limit for aggregation pipelines
                    # allowDiskUse=True permits operations to use temporary files on disk
                    try:
                        print("Retrying with allowDiskUse=True")
                        results = list(coll.aggregate(pipeline, allowDiskUse=True))
                        return convert_objectid_to_str(results)
                    except Exception as retry_err:
                        print(f"Retry failed: {str(retry_err)}")
                        return []
            
            # Standard find query
            # Used when no special sorting or complex operations are needed
            if projection:
                results = list(coll.find(filter_obj, projection))
            else:
                results = list(coll.find(filter_obj))
                
            # Apply limit after find if specified
            # This is a client-side limit (not a database cursor limit)
            if limit_val > 0:
                results = results[:limit_val]
                
            return convert_objectid_to_str(results)
            
        # Handle direct aggregation pipeline
        elif isinstance(mongo_filter, list):
            try:
                print(f"Executing direct aggregation: {json.dumps(mongo_filter, default=str)}")
                results = list(coll.aggregate(mongo_filter))
                return convert_objectid_to_str(results)
            except pymongo.errors.OperationFailure as e:
                print(f"MongoDB direct aggregation error: {str(e)}")
                # Try with allowDiskUse for large datasets or complex aggregations
                # Particularly important for operations like $sort with large datasets
                # or $group operations that consume significant memory
                try:
                    print("Retrying direct aggregation with allowDiskUse=True")
                    results = list(coll.aggregate(mongo_filter, allowDiskUse=True))
                    return convert_objectid_to_str(results)
                except Exception as retry_err:
                    print(f"Retry failed: {str(retry_err)}")
                    return []
            
        else:
            raise ValueError("Unsupported MongoDB query format. Provide a dict or list (aggregation pipeline).")

    except pymongo.errors.OperationFailure as e:
        print(f"MongoDB OperationFailure: {str(e)}")
        return []
    except Exception as e:
        print(f"MongoDB Query Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def normalize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    normalized = doc.copy()
    
    if "id" in normalized and isinstance(normalized["id"], str):
        try:
            normalized["id"] = int(normalized["id"].strip().replace('"', ''))
        except ValueError:
            pass
            
    if "price" in normalized and isinstance(normalized["price"], str):
        try:
            normalized["price"] = float(normalized["price"].replace("$", "").strip())
        except ValueError:
            pass
            
    return normalized

# modification

def modify_mongodb(
    mod_query: Dict[str, Any],
    collection_name: str = "listings_meta") -> Dict[str, Any]:

    try:
        print(f"MongoDB modification request: {json.dumps(mod_query, default=str)}")
        
        # Validate operation type
        op = mod_query.get("operation", "").lower()
        if not op or op not in ["insert", "update", "delete"]:
            raise ValueError(f"Invalid or missing operation: {op}. Must be 'insert', 'update', or 'delete'.")
        
        # Get the correct collection
        try:
            coll = get_collection(collection_name)
            print(f"Using MongoDB collection: {collection_name}")
        except Exception as e:
            print(f"Error accessing collection '{collection_name}': {str(e)}")
            # Fallback to listings_meta collection
            collection_name = "listings_meta" 
            coll = get_collection(collection_name)
            print(f"Fell back to collection: {collection_name}")
        
        if op == "insert":
            # Check if inserting multiple documents
            if "documents" in mod_query and isinstance(mod_query["documents"], list):
                documents = [normalize_doc(doc) for doc in mod_query["documents"]]
                if not documents:
                    raise ValueError("Empty documents list provided for bulk insert operation")
                
                print(f"Bulk inserting {len(documents)} documents")
                
                for doc in documents:
                    if "_id" in doc and isinstance(doc["_id"], str):
                        try:
                            doc["_id"] = int(doc["_id"])
                        except ValueError:
                            pass
                
                result = coll.insert_many(documents)
                response = {
                    "message": f"{len(documents)} documents inserted successfully",
                    "inserted_ids": [str(id) for id in result.inserted_ids]
                }
                print(f"Bulk insert result: {json.dumps(response, default=str)}")
                return response
            else:
                # Single document insert (existing code)
                document = normalize_doc(mod_query.get("document", {}))
                if not document:
                    raise ValueError("No document provided for insert operation")
                
                print(f"Inserting document: {json.dumps(document, default=str)}")
                
                # Ensure _id is properly formatted
                if "_id" in document and isinstance(document["_id"], str):
                    try:
                        document["_id"] = int(document["_id"])
                        print(f"Converted _id from string to int: {document['_id']}")
                    except ValueError:
                        # Keep as string if not convertible to int
                        pass
                
                result = coll.insert_one(document)
                response = {
                    "message": "Document inserted successfully",
                    "inserted_id": str(result.inserted_id)
                }
                print(f"Insert result: {json.dumps(response, default=str)}")
                return response
            
        elif op == "update":
            filter_query = mod_query.get("filter", {})
            update_data = mod_query.get("update", {})
            
            if not filter_query:
                raise ValueError("Filter is required for update operation")
            if not update_data:
                raise ValueError("Update data is required for update operation")
            
            # Convert string IDs to integers where appropriate
            if "_id" in filter_query and isinstance(filter_query["_id"], str):
                try:
                    filter_query["_id"] = int(filter_query["_id"])
                    print(f"Converted filter _id from string to int: {filter_query['_id']}")
                except ValueError:
                    # Keep as string if not convertible to int
                    pass
            
            # Determine if this should be a multi-update operation
            multi = mod_query.get("multi", False)
            
            # Auto-detect if this is likely a multi-update operation
            if not multi:
                # If the filter contains operators like $in, $gt, etc, or doesn't have a specific _id, it's likely intended to update multiple documents
                uses_array_operator = any(isinstance(v, dict) and any(k.startswith('$') for k in v.keys()) 
                                         for k, v in filter_query.items())
                has_id_exact_match = "_id" in filter_query and not isinstance(filter_query["_id"], dict)
                
                # If using array operators or not targeting a specific ID, assume multi=True
                if uses_array_operator or not has_id_exact_match:
                    multi = True
                    print("Auto-detected multi-update operation based on filter criteria")
            
            upsert = mod_query.get("upsert", False)
            
            # Ensure update has proper MongoDB operators
            if not any(k.startswith("$") for k in update_data):
                update_data = {"$set": update_data}
                print("Added $set operator to update data")
            
            print(f"Update operation:")
            print(f"  Filter: {json.dumps(filter_query, default=str)}")
            print(f"  Update: {json.dumps(update_data, default=str)}")
            print(f"  Multi: {multi}, Upsert: {upsert}")
            
            if multi:
                result = coll.update_many(filter_query, update_data, upsert=upsert)
            else:
                result = coll.update_one(filter_query, update_data, upsert=upsert)
            
            response = {
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }
            print(f"Update result: {json.dumps(response, default=str)}")
            return response
            
        elif op == "delete":
            filter_query = mod_query.get("filter", {})
            if not filter_query:
                raise ValueError("Filter is required for delete operation")
            
            # Convert string IDs to integers where appropriate
            if "_id" in filter_query and isinstance(filter_query["_id"], str):
                try:
                    filter_query["_id"] = int(filter_query["_id"])
                    print(f"Converted filter _id from string to int: {filter_query['_id']}")
                except ValueError:
                    # Keep as string if not convertible to int
                    pass
            
            # Determine if this should be a multi-delete operation
            multi = mod_query.get("multi", False)
            
            # Auto-detect if this is likely a multi-delete operation
            if not multi:
                # If the filter contains operators like $in, $gt, etc, or doesn't have a specific _id, it's likely intended to delete multiple documents
                uses_array_operator = any(isinstance(v, dict) and any(k.startswith('$') for k in v.keys()) 
                                         for k, v in filter_query.items())
                has_id_exact_match = "_id" in filter_query and not isinstance(filter_query["_id"], dict)
                
                # If using array operators or not targeting a specific ID, assume multi=True
                if uses_array_operator or not has_id_exact_match:
                    multi = True
                    print("Auto-detected multi-delete operation based on filter criteria")
            
            print(f"Delete operation with filter: {json.dumps(filter_query, default=str)}")
            print(f"  Multi: {multi}")
            
            if multi:
                result = coll.delete_many(filter_query)
            else:
                result = coll.delete_one(filter_query)
                
            response = {
                "deleted_count": result.deleted_count
            }
            print(f"Delete result: {json.dumps(response, default=str)}")
            return response
            
        else:
            raise ValueError(f"Unsupported operation: {op}")
            
    except ValueError as e:
        print(f"MongoDB modification error (ValueError): {str(e)}")
        traceback = __import__('traceback')
        traceback.print_exc()
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        print(f"MongoDB modification error: {str(e)}")
        traceback = __import__('traceback')
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"MongoDB modification error: {str(e)}"
        )
