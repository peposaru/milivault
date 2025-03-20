import logging
import json
from psycopg2 import pool
from datetime import datetime
from decimal import Decimal


class AwsRdsManager:
    def __init__(self, credentials_file, min_connections=5, max_connections=10):
        """
        Initialize a PostgreSQL connection pool using credentials from a file.
        """
        self._initialize_connection_pool(credentials_file, min_connections, max_connections)

    def _initialize_connection_pool(self, credentials_file, min_connections, max_connections):
        try:
            with open(credentials_file, 'r') as file:
                credentials = json.load(file)

            self.connection_pool = pool.SimpleConnectionPool(
                min_connections, max_connections,
                user=credentials.get("userName"),
                password=credentials.get("pwd"),
                host=credentials.get("hostName"),
                database=credentials.get("dataBase"),
                port=credentials.get("portId")
            )
            logging.info("Connection pool initialized successfully.")
        except Exception as e:
            logging.error(f"Failed to initialize connection pool: {e}")
            raise

    def _execute_query(self, query, params=None, fetch=False):
        """
        Execute a query with optional parameters. Fetch results if specified.
        """
        connection = self.connection_pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchall()
                connection.commit()
        except Exception as e:
            logging.error(f"Error executing query: {e}")
            if not fetch:
                connection.rollback()
            raise
        finally:
            self.connection_pool.putconn(connection)

    def fetch(self, query, params=None):
        """
        Fetch results from a query.
        """
        return self._execute_query(query, params, fetch=True)

    def execute(self, query, params=None):
        """
        Execute a query without fetching results (e.g., INSERT, UPDATE).
        """
        self._execute_query(query, params)

    def create_comparison_list(self):
        """
        Fetch all existing URLs from the database for comparison.

        Returns:
            dict: A dictionary with URLs as keys and (title, price, available, description, price_history) as values.
        """
        try:
            all_url_query = "SELECT url, title, price, available, description, price_history FROM militaria;"
            all_url_query_result = self.fetch(all_url_query)  # Fetch all rows
            
            if not all_url_query_result:
                logging.warning("No data fetched from the database.")
                return {}

            return {
                row[0]: (
                    row[1],  # title
                    float(row[2]) if row[2] else 0.0,  # price as float
                    row[3],  # available
                    row[4],  # description
                    row[5] if row[5] is not None else "[]"  # price_history as valid JSON
                )
                for row in all_url_query_result
            }
        except Exception as e:
            logging.error(f"Error fetching comparison data from database. Query: {all_url_query}. Exception: {e}")
            return {}


    def get_record_id(self, query, params):
        """
        Fetch a single ID from a query result.
        """
        result = self.fetch(query, params)
        return result[0][0] if result else None

    def update_record(self, update_query, params):
        """
        Update a record in the database.
        """
        self.execute(update_query, params)

    def close(self):
        """
        Close the connection pool.
        """
        try:
            self.connection_pool.closeall()
            logging.info("Connection pool closed.")
        except Exception as e:
            logging.error(f"Error closing connection pool: {e}")

    def should_skip_update(self, check_query, params):
        """
        Check if a record update should be skipped based on query conditions.
        """
        result = self.fetch(check_query, params)
        if not result:
            return False  # No record found, proceed with update
        record = result[0]
        # Logic for skipping can vary based on business rules
        return all(record)  # Example: skip if all fields are populated
    
    def should_skip_image_upload(self, product_url):
        # Check if the product already has images or needs them.
        try:
            query = """
            SELECT original_image_urls, s3_image_urls
            FROM militaria
            WHERE url = %s;
            """
            result = self.fetch(query, (product_url,))
            
            if not result:
                logging.info(f"No record found for product URL: {product_url}.")
                return False

            original_image_urls, s3_image_urls = result[0]
            # Ensure both columns have values and their lengths match
            if original_image_urls and s3_image_urls and len(original_image_urls) == len(s3_image_urls):
                logging.debug(f"Image upload already completed for product URL: {product_url}.")
                return True

            logging.debug(f"Image upload needed for product URL: {product_url}.")
            return False
        except Exception as e:
            logging.error(f"Error checking image upload status for URL {product_url}: {e}")
            return False
        
    def new_product_input(self, clean_details_data):
        """
        Insert product data into the database without conflict checks, skipping blank or empty fields.

        Args:
            clean_details_data (dict): The clean product details to upload.

        Returns:
            None
        """
        try:
            # Ensure price is never None, but do not replace a valid DB price
            if 'price' not in clean_details_data or clean_details_data['price'] is None:
                clean_details_data['price'] = 0.0
                
            # Filter out empty fields
            filtered_data = {k: v for k, v in clean_details_data.items() if v not in (None, "", [], {})}

            # Convert Decimal fields to float and handle JSON serialization
            json_fields = ["original_image_urls", "categories_site_designated"]
            for key, value in filtered_data.items():
                if isinstance(value, Decimal):
                    filtered_data[key] = float(value)  # Convert Decimal to float
                if key in json_fields:
                    filtered_data[key] = json.dumps(value, default=float)  # Handle serialization for JSON fields

            # Prepare the column names and placeholders dynamically
            columns = ", ".join(filtered_data.keys())
            placeholders = ", ".join(["%s"] * len(filtered_data))
            insert_query = f"""
            INSERT INTO militaria ({columns})
            VALUES ({placeholders})
            """

            # Debug the prepared data
            logging.debug(f"Prepared data for insertion: {filtered_data}")

            # Execute the query
            self.execute(insert_query, tuple(filtered_data.values()))
            logging.info(f"Successfully inserted product: {clean_details_data.get('title')}")

        except Exception as e:
            logging.error(f"Error inserting product to RDS: {e}")







