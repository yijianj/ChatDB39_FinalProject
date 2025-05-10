import pymysql
import re
from fastapi import HTTPException
from typing import List, Dict, Any, Optional


# config for MySQL connection
# These values must be replaced with actual credentials before deployment
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'Dsci-551',
    'database': 'airbnb_db',
    'cursorclass': pymysql.cursors.DictCursor  # Returns results as dictionaries instead of tuples
}

def get_connection():

    # Creates and returns a connection to the MySQL database.
    # Uses the global configuration and handles connection errors.

    try:
        return pymysql.connect(**MYSQL_CONFIG)
    except pymysql.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to connect to MySQL database: {str(e)}"
        )

def query_mysql(sql_query: str) -> List[Dict[str, Any]]:

    # Executes a SQL query and returns results as a list of dictionaries.
    # Uses connection pooling pattern with proper resource cleanup.

    connection = None
    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(sql_query)  # Note: Direct query execution，used with trusted inputs only
            results = cursor.fetchall()
            return results
    except pymysql.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"MySQL query error: {str(e)}"
        )
    finally:
        if connection:
            connection.close()  # Ensures connection is closed even if an exception occurs

def validate_table_exists(table_name: str) -> bool:

    # Safely checks if a table exists in the database using parameterized query.
    # Returns True if the table exists, False otherwise.

    connection = None
    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))  # Parameterized query for SQL injection prevention
            return cursor.fetchone() is not None  # Returns True if at least one row is returned
    except pymysql.Error:
        # Silently fails and returns False rather than raising an exception
        return False
    finally:
        if connection:
            connection.close()

def get_table_schema(table_name: str) -> Optional[List[Dict[str, Any]]]:

    # Retrieves the schema of a table as a list of dictionaries containing column details. 
    # Returns None if the table does not exist or an error occurs.

    connection = None
    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(f"DESCRIBE {table_name}")
            return cursor.fetchall()
    except pymysql.Error:
        return None
    finally:
        if connection:
            connection.close()

# modification

def modify_mysql(sql_query: str) -> Dict[str, str]:

    # Executes a modification query (INSERT, UPDATE, DELETE) on the MySQL database.
    # Accepts a single query as a string or multiple queries as a list of strings.
    # Commits the transaction if successful, rolls back in case of error.

    if isinstance(sql_query, str):
        # split on semicolon, strip whitespace, ignore empties
        stmts = [s.strip() for s in sql_query.split(";") if s.strip()]
    elif isinstance(sql_query, (list, tuple)):
        stmts = sql_query
    else:
        raise HTTPException(400, "`mysql` mods must be a string or list of strings")
    
    connection = None
    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            for raw_sql in stmts:
                # Clean SQL to handle dollar signs in strings like '$100'
                clean_sql = re.sub(
                    r"'\$([0-9]+(?:\.[0-9]+)?)'",
                    r"'\1'",
                    raw_sql
                )
                print(f"Executing MySQL query: {clean_sql}")
                cursor.execute(clean_sql)
        connection.commit()
        return {"message": "MySQL modification executed successfully."}
    except pymysql.Error as e:
        if connection:
            connection.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"MySQL modification error: {str(e)}"
        )
    finally:
        if connection:
            connection.close()
