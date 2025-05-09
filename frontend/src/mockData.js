export const mockResponse = {
    "converted_queries": {
        "mysql": "SELECT id, name, price FROM Listings WHERE price < 150 LIMIT 5;",
        "mongodb": "{ 'price': { '$lt': 150 } }"
    },
    "results": {
        "mysql": [
            {
                "id": 1,
                "name": "Cozy Apartment",
                "price": 120
            },
            {
                "id": 2,
                "name": "Small Studio",
                "price": 100
            }
        ],
        "mongodb": [
            {
                "id": 1,
                "description": "A cozy apartment in the city center"
            }
        ],
        "merged": [
            {
                "id": 1,
                "name": "Cozy Apartment",
                "price": 120,
                "description": "A cozy apartment in the city center"
            }
        ]
    }
}; 