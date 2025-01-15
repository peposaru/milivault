import logging
import json
from psycopg2 import pool


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

        Args:
            db_manager: Database manager with a `fetch` method for executing SQL queries.

        Returns:
            dict: A dictionary with URLs as keys and (title, price, available) as values.
        """
        try:
            all_url_query = "SELECT url, title, price, available FROM militaria;"
            all_url_query_result = self.fetch(all_url_query)  # Fetch all rows
            return {
                row[0]: (row[1], row[2], row[3])  # Key: url, Value: (title, price, available)
                for row in all_url_query_result
            }
        except Exception as e:
            logging.error(f"Error fetching comparison data from database: {e}")
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
        