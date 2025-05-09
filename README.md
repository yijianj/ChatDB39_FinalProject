# DSCI 551 Project - Multi-Database Airbnb Data Explorer

This project implements a natural language interface to query and manipulate Airbnb data across multiple databases (MySQL, MongoDB, and Firebase). The application allows users to query data using natural language, explore database schemas, and modify data through a unified interface.

## Prerequisites

Before running this project, ensure the following are installed/initiated:

- **Python 3.8+** - For the backend server
- **Node.js and npm** - For the frontend application
- **MySQL Server** - For relational data storage
- **MongoDB Server** - For document data storage
- **Firebase Project** - For real-time database
- **Google Gemini API key** with Gemini API access - For natural language processing

### API Keys Required

1. **Firebase Admin SDK Private Key**
   - Save the JSON key file in the project root directory

2. **Google Gemini API Key**
   - replace the API key in the backend/app.py file (line 608)

## Installation

### Clone the repository using Git

### Backend Setup

1. Create and activate a virtual environment:

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows
venv\Scripts\activate
# On macOS/Linux
source venv/bin/activate
```

2. Install Python dependencies:

```bash
pip install -r requirements.txt
```

3. Configure database connections:
   - MySQL: Update connection parameters in `backend/database/mysql_connector.py`
   - MongoDB: Update connection parameters in `backend/database/mongodb_connector.py`
   - Firebase: Ensure the Firebase Admin SDK JSON file is in the correct location and update the path in `backend/database/firebase_connector.py` if necessary

4. Set up Google Gemini API Key

### Frontend Setup

1. Navigate to the frontend directory:

```bash
cd frontend
```

2. Install Node.js dependencies:

```bash
npm install
```

### Database Setup

1. Start MySQL and MongoDB servers.
2. Create the database and load sample data:

```bash
# navigate to the backend folder
cd backend

# Create and load MySQL data
python backend/load_airbnb_mysql.py

# Create and load MongoDB data
python backend/load_airbnb_mongo.py

# Load Firebase data
python backend/load_airbnb_firebase.py
```

## Running the Application

1. Start the backend server:

```bash
# navigate to the backend folder
cd backend

# start the Backend Server
python app.py
```

2. Start the frontend development server:

```bash
# From another terminal, navigate to the frontend folder
cd frontend
npm start
```

3. *The frontend will be avilable at `http://localhost:3000` using a web browser*

4. Using the application
   - Use the interface to select operation type (Query, Explore Schema, Modify Data), choose a database, and enter your natural language query.

## Sample Queries:
1. Schema Exploration
   - MySQL:
      1. What tables exist?
         - Prompt: What tables are in the databases?
      2. View table attributes.
         - Prompt: Show me the schema of the Hosts/listings/reviews table
      3. Retrieve sample rows.
         - Prompt: Show me 5 sample data from the Hosts/listings/reviews table
   - MongoDB:
      1. what collections exist?
         - Prompt: What collections are in the database?
      2. View collection attributes.
         - Prompt: What attributes are in listings_meta/media/amenities table?
      3. Retrieve sample rows.
         - Prompt: Show me 5 sample data from the listings_meta/media/amenities table.
2. Database Query
   - MySQL：
      1. SELECT ... FROM
         - Prompt: Show me the name and room type of 3 listings.
      2. WHERE
         - Prompt: Find 3 listings (name, id, and neighborhood) where the neighborhood is Downtown.
      3. GROUP BY
         - Prompt: Count the number of listings in each neighborhood.
      4. HAVING
         - Prompt: Show neighborhoods having more than 5 listings.
      5. ORDER BY
         - Prompt: List 5 hosts ordered by their listings count in descending order (use group by).
      6. OFFSET ... LIMIT ...
         - Prompt: Show me the top 5 highest-rated listings (Skip the first 5 results)
      7. Join:
         - Prompt: Get 3 listings with their host information.
      8. Complex Situation:
         - Prompt:Find the average review score for each room type in Downtown, showing only those with average score above 4.5, sorted by average score

   - MongoDB:
      1. find with projections
         - Prompt: Show me 3 listing ID and host ID from listings_meta collection.
      2. $match
         - Prompt: Find listings in the Downtown neighborhood (use match instead of filter in MongoDB query).
      3. $group
         - Prompt: Count how many listings are in each neighborhood.
      4. $sort
         - Prompt: Show 3 listings sorted by host response rate in descending order.
      5. $limit / $Skip
         - Prompt: Skip the first 10 listings and show the next 5.
      6. $project
         - Prompt: Show only the listing ID, host ID and neighborhood for 3 listings.
      7. joins using $lookup
         - Prompt: Show 3 listings with their amenities details.
      8. Aggregate Complex
         - Prompt: Find the average host response rate for each neighborhood, only include neighborhoods with at least 5 listings, sort by average rate descending.
3. Modification:
   - MySQL:
      1. Insertion
         - Prompt: Insert a new listing with id 3003, name 'OneLineTest', property_type 'Condo', room_type 'Entire home/apt', accommodates 5 person;
         - Checking Query: Show everything about listing with id 3003 in the listing table.
      2. Insertion (Many)
         - Prompt: Insert these listings into MySQL: 1. "id 3004, name 'BeachHouse', property_type 'House', room_type 'Entire home/apt', accommodates 8"; 2. "id 3005, name 'CityLoft', property_type 'Apartment', room_type 'Private room', accommodates 2"; 3. "id 3006, name 'SuburbanRetreat', property_type 'House', room_type 'Entire home/apt', accommodates 6"
         - Checking Query: Show everything about listings with ids 3004, 3005, and 3006 in the listing table.

      3. Modification (Support Many)
         - Prompt: Update the listing with id 3003 to set its accommodates to 6.
         - Checking Query: Show the accommodate information about the listing with id 3003.

         - Prompt: Update listings with id 3004, 3005, 3006 and set their room_type to Studio.
         - Checking Query: Show the room_type information about listings with id 3004, 3005, 3006
         
      4. Deletion (Support Many)
         - Prompt: Delete listing with id 3004, 3005, 3006
         - Checking Query: Show everything about listing with id 3004, 3005, 3006 in the listing table.

   - MongoDB:
      1. Insertion (one)
         - Prompt: Insert a new listing into MongoDB with metaid '123', scrape_id 'abc', last_scraped '2025-04-18', source 'manual', host_id 456, host_response_time '1 day', host_response_rate 0.85, host_acceptance_rate 0.9, instant_bookable true, license 'XYZ123', neighbourhood_cleansed 'Downtown', neighbourhood_group_cleansed 'Central', market 'LA', smart_location 'Los Angeles', country_code 'US', country 'USA
         - Checking Query: Show listing with id 123.
      2. Insertion (many)
         - Prompt: Insert these listings into MongoDB: 1. "metaid '124', scrape_id 'def', last_scraped '2025-04-19', source 'manual', host_id 457, host_response_time '2 days', host_response_rate 0.75, host_acceptance_rate 0.8, instant_bookable false, license 'ABC789', neighbourhood_cleansed 'Midtown', neighbourhood_group_cleansed 'Central', market 'SF', smart_location 'San Francisco', country_code 'US', country 'USA'"; 2. "metaid '125', scrape_id 'ghi', last_scraped '2025-04-20', source 'manual', host_id 458, host_response_time '1 hour', host_response_rate 0.95, host_acceptance_rate 0.85, instant_bookable true, license 'DEF456', neighbourhood_cleansed 'SOMA', neighbourhood_group_cleansed 'Central', market 'SF', smart_location 'San Francisco', country_code 'US', country 'USA'" 3. "metaid '126', scrape_id 'jkl', last_scraped '2025-04-21', source 'manual', host_id 459, host_response_time '3 days', host_response_rate 0.65, host_acceptance_rate 0.7, instant_bookable false, license 'GHI123', neighbourhood_cleansed 'Mission', neighbourhood_group_cleansed 'Central', market 'SF', smart_location 'San Francisco', country_code 'US', country 'USA'"
         - Checking Query: Show listings with ids 124, 125, and 126.
      3. Update (one)
         - Prompt: Update the listing in MongoDB with metaid '123' to set smart_location to 'San Francisco'.
         - Checking Query: Show listing with id 123.

      4. Update (Many)
         - Prompt: Update listings with id "124", "125", "126" in MongoDB set smart_location to 'Los Angeles'.
         - Checking Query: Show listings with ids 124, 125, and 126.

      5. Deletion
         - Prompt: Delete listing with metaid '123' from the database.
         - prompt: Delete listings with metaid 124, 125, and 126.
         - Checking Query: Show listings with ids 123, 124, 125, and 126.

## Troubleshooting

- **Database Connection Issues**: Ensure all database services are running and connection parameters are correct, make sure the sample data are loaded correctly.
- **API Key Issues**: Verify Google Gemini API key and Firebase credentials
- **Data Loading Problems**: Check that the sample data file exists and is correctly formatted.