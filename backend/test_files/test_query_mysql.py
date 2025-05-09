import requests
import json
import sys
from pprint import pprint

API_URL = "http://127.0.0.1:8000/query"

test_queries = [
    # SELECT
    {
        "feature": "SELECT",
        "query": "Show me the name and room type of 3 listings"
    },
    # WHERE
    {
        "feature": "WHERE",
        "query": "Find 3 listings where the neighborhood is Downtown"
    },
    # JOIN
    {
        "feature": "JOIN",
        "query": "Get 3 listings with their host information"
    },
    # GROUP BY
    {
        "feature": "GROUP BY",
        "query": "Count the number of listings in each neighborhood"
    },
    # HAVING
    {
        "feature": "HAVING",
        "query": "Show neighborhoods having more than 5 listings"
    },
    # ORDER BY
    {
        "feature": "ORDER BY",
        "query": "List 5 hosts ordered by their listings count"
    },
    # LIMIT
    {
        "feature": "LIMIT",
        "query": "Show me the top 5 highest-rated listings"
    },
    # Complex query with multiple features
    {
        "feature": "COMPLEX",
        "query": "Find the average review score for each room type in Downtown, showing only those with average score above 4.5, sorted by average score"
    }
]

def test_mysql_query(test_item):
    print(f"\n{'=' * 80}")
    print(f"Testing {test_item['feature']} query: \"{test_item['query']}\"")
    print(f"{'=' * 80}")
    
    try:
        response = requests.post(API_URL, json={"query": test_item['query']})
        
        if response.status_code == 200:
            result = response.json()
            
            mysql_query = result.get("converted_queries", {}).get("mysql", "")
            
            print("\nGenerated MySQL Query:")
            print(mysql_query)
            
            feature_present = False
            if test_item['feature'] == "SELECT":
                feature_present = "SELECT" in mysql_query.upper()
            elif test_item['feature'] == "WHERE":
                feature_present = "WHERE" in mysql_query.upper()
            elif test_item['feature'] == "JOIN":
                feature_present = "JOIN" in mysql_query.upper()
            elif test_item['feature'] == "GROUP BY":
                feature_present = "GROUP BY" in mysql_query.upper()
            elif test_item['feature'] == "HAVING":
                feature_present = "HAVING" in mysql_query.upper()
            elif test_item['feature'] == "ORDER BY":
                feature_present = "ORDER BY" in mysql_query.upper()
            elif test_item['feature'] == "LIMIT":
                feature_present = "LIMIT" in mysql_query.upper()
            elif test_item['feature'] == "COMPLEX":
                complex_features = ["SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY"]
                present_features = [f for f in complex_features if f in mysql_query.upper()]
                feature_present = len(present_features) >= 3
            
            print(f"\nFeature '{test_item['feature']}' present: {feature_present}")
            
            mysql_results = result.get("results", {}).get("mysql", [])
            if mysql_results:
                print(f"\nMySQL Results (first 3 of {len(mysql_results)}):")
                for i, res in enumerate(mysql_results[:3]):
                    print(f"Result {i+1}:")
                    pprint(res)
            else:
                print("\nNo MySQL results returned")
            
            return feature_present
        else:
            print(f"ERROR ({response.status_code}):", response.text)
            return False
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        return False

def main():
    print("Starting MySQL Query Tests")
    successes = 0
    
    for test_item in test_queries:
        if test_mysql_query(test_item):
            successes += 1
    
    print("\n" + "=" * 80)
    print(f"Test Summary: {successes}/{len(test_queries)} tests passed")
    print("=" * 80)
    
    if successes < len(test_queries):
        sys.exit(1)

if __name__ == "__main__":
    main()
