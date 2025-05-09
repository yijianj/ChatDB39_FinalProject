import requests
import json
import sys
from pprint import pprint

API_URL = "http://127.0.0.1:8000/query"

test_queries = [
    # Find with projections
    {
        "feature": "FIND_PROJECTION",
        "query": "Show me 3 listing ID and host ID from listings_meta collection"
    },
    # $match
    {
        "feature": "MATCH",
        "query": "Find 3 listings in the Downtown neighborhood"
    },
    # $group
    {
        "feature": "GROUP",
        "query": "Count how many listings are in each neighborhood"
    },
    # $sort
    {
        "feature": "SORT",
        "query": "Show 3 listings sorted by host response rate"
    },
    # $limit
    {
        "feature": "LIMIT",
        "query": "Show only the first 5 listings"
    },
    # $skip
    {
        "feature": "SKIP",
        "query": "Skip the first 10 listings and show the next 5"
    },
    # $project
    {
        "feature": "PROJECT",
        "query": "Show only the listing ID, host ID and neighborhood for 3 listing"
    },
    # $lookup (join-like)
    {
        "feature": "LOOKUP",
        "query": "Show 3listings with their amenities details"
    },
    # Complex aggregation with multiple stages
    {
        "feature": "COMPLEX",
        "query": "Find the average host response rate for each neighborhood, only include neighborhoods with at least 5 listings, sort by average rate descending"
    }
]

def test_mongodb_query(test_item):
    print(f"\n{'=' * 80}")
    print(f"Testing {test_item['feature']} query: \"{test_item['query']}\"")
    print(f"{'=' * 80}")
    
    try:
        response = requests.post(API_URL, json={"query": test_item['query'], "db_type": "mongodb"})
        
        if response.status_code == 200:
            result = response.json()
            
            mongodb_query = result.get("converted_queries", {}).get("mongodb", {})
            
            print("\nGenerated MongoDB Query:")
            print(json.dumps(mongodb_query, indent=2))
            
            print("\nFull Response Structure:")
            print(f"Keys in response: {list(result.keys())}")
            if "results" in result:
                print(f"Keys in results: {list(result['results'].keys())}")
                for db_type, db_results in result['results'].items():
                    result_type = type(db_results).__name__
                    result_count = len(db_results) if isinstance(db_results, list) else "N/A"
                    print(f"  - {db_type}: {result_type} with {result_count} items")
            
            feature_present = False
            
            if isinstance(mongodb_query, dict):
                if test_item['feature'] == "FIND_PROJECTION":
                    feature_present = "projection" in mongodb_query
                elif test_item['feature'] == "MATCH":
                    if isinstance(mongodb_query.get("filter"), dict):
                        feature_present = "neighbourhood_cleansed" in mongodb_query.get("filter", {})
                    elif isinstance(mongodb_query.get("aggregate"), list):
                        for stage in mongodb_query.get("aggregate", []):
                            if "$match" in stage and "neighbourhood_cleansed" in stage.get("$match", {}):
                                feature_present = True
                                break
                elif test_item['feature'] == "LIMIT":
                    feature_present = "limit" in mongodb_query or "$limit" in mongodb_query
                elif test_item['feature'] == "SORT":
                    feature_present = "sort" in mongodb_query or "$sort" in mongodb_query or "$orderby" in mongodb_query
            elif isinstance(mongodb_query, list):
                pipeline_ops = []
                for stage in mongodb_query:
                    if isinstance(stage, dict):
                        pipeline_ops.extend(list(stage.keys()))
                
                if test_item['feature'] == "GROUP":
                    feature_present = "$group" in pipeline_ops
                elif test_item['feature'] == "MATCH":
                    feature_present = "$match" in pipeline_ops
                elif test_item['feature'] == "SORT":
                    feature_present = "$sort" in pipeline_ops
                elif test_item['feature'] == "LIMIT":
                    feature_present = "$limit" in pipeline_ops
                elif test_item['feature'] == "SKIP":
                    feature_present = "$skip" in pipeline_ops
                elif test_item['feature'] == "PROJECT":
                    feature_present = "$project" in pipeline_ops
                elif test_item['feature'] == "LOOKUP":
                    feature_present = "$lookup" in pipeline_ops
                elif test_item['feature'] == "COMPLEX":
                    complex_features = ["$match", "$group", "$sort", "$limit", "$project"]
                    present_features = [f for f in complex_features if f in pipeline_ops]
                    feature_present = len(present_features) >= 3
            
            print(f"\nFeature '{test_item['feature']}' present: {feature_present}")
            
            mongodb_results = result.get("results", {}).get("mongodb", [])
            if mongodb_results:
                print(f"\nMongoDB Results (first 3 of {len(mongodb_results)}):")
                for i, res in enumerate(mongodb_results[:3]):
                    print(f"Result {i+1}:")
                    pprint(res)
            else:
                print("\nNo MongoDB results returned")
                merged_results = result.get("results", {}).get("merged", [])
                if merged_results:
                    print(f"\nFound results in 'merged' field (first 3 of {len(merged_results)}):")
                    for i, res in enumerate(merged_results[:3]):
                        print(f"Result {i+1}:")
                        pprint(res)
            
            return feature_present
        else:
            print(f"ERROR ({response.status_code}):", response.text)
            return False
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("Starting MongoDB Query Tests")
    successes = 0
    
    for test_item in test_queries:
        if test_mongodb_query(test_item):
            successes += 1
    
    print("\n" + "=" * 80)
    print(f"Test Summary: {successes}/{len(test_queries)} tests passed")
    print("=" * 80)
    
    if successes < len(test_queries):
        sys.exit(1)

if __name__ == "__main__":
    main()
