import re
from html import unescape

class CleanData:
    @staticmethod
    def clean_url(url):
        """
        Cleans and validates a URL.

        Args:
            url (str): The URL to clean.

        Returns:
            str: The cleaned and validated URL.
        Raises:
            ValueError: If the URL is invalid.
        """
        if not isinstance(url, str):
            raise ValueError("URL must be a string.")

        # Strip whitespace
        url = url.strip()

        # Validate URL structure
        url_pattern = re.compile(
            r"^(https?://)"  # http or https
            r"([a-zA-Z0-9.-]+)"  # Domain
            r"(\.[a-zA-Z]{2,})"  # Top-level domain
            r"(:[0-9]+)?(/.*)?$"  # Port and path
        )
        if not url_pattern.match(url):
            raise ValueError(f"Invalid URL format: {url}")

        return url
    

    @staticmethod
    def clean_title(title):
        """
        Cleans and normalizes a product title, ensuring single quotes are used consistently.

        Args:
            title (str): The title to clean.

        Returns:
            str: The cleaned title with normalized quotes.
        Raises:
            ValueError: If the title is not a string or is empty.
        """
        if not isinstance(title, str):
            raise ValueError("Title must be a string.")

        # Decode HTML entities
        title = unescape(title)

        # Remove leading and trailing whitespace
        title = title.strip()

        # Replace special quotes with standard single quotes
        special_quotes = {
            "“": "'", "”": "'",  # Double quotes
            "‘": "'", "’": "'",  # Single quotes
            '"': "'",                # Replace double quotes with single quotes
        }
        for special, standard in special_quotes.items():
            title = title.replace(special, standard)

        # Replace multiple spaces with a single space
        title = " ".join(title.split())

        if not title:
            raise ValueError("Title cannot be empty after cleaning.")

        return title


    @staticmethod
    def clean_description(description):
        """
        Cleans and normalizes a product description to use single quotes
        and removes the leading 'Description' if present.

        Args:
            description (str): The raw description text.

        Returns:
            str: The cleaned description text with normalized quotes.
        Raises:
            ValueError: If the description is not a string or is empty.
        """
        if not isinstance(description, str):
            raise ValueError("Description must be a string.")

        # Decode HTML entities
        description = unescape(description)

        # Remove leading and trailing whitespace
        description = description.strip()

        # Remove leading "Description" if present
        if description.lower().startswith("description"):
            description = description[len("description"):].strip()

        # Replace special quotes with standard single quotes
        special_quotes = {
            "“": "'", "”": "'",  # Double quotes
            "‘": "'", "’": "'",  # Single quotes
            '"': "'",            # Replace double quotes with single quotes
        }
        for special, standard in special_quotes.items():
            description = description.replace(special, standard)

        # Replace multiple spaces with a single space
        description = " ".join(description.split())

        if not description:
            raise ValueError("Description cannot be empty after cleaning.")

        return description


    @staticmethod
    def clean_price(price):
        """
        Cleans and normalizes price strings to a float.

        Args:
            price (str or None): The raw price string to clean.

        Returns:
            float or None: The cleaned price as a float, or None if the input is None.
        
        Raises:
            ValueError: If the price cannot be parsed or is invalid.
        """
        if price is None:
            return None  # If price is None, return None immediately

        if not isinstance(price, str):
            raise ValueError("Price must be a string or None.")

        # Remove unnecessary words and symbols
        unwanted_phrases = ["NEW PRICE", "Non-EU Price", "PRICE", "excl. VAT"]
        for phrase in unwanted_phrases:
            price = price.replace(phrase, "")

        # Remove currency symbols and normalize spaces
        price = re.sub(r"[€$£]", "", price)  # Remove common currency symbols
        price = price.replace(" ", "")  # Remove spaces for consistent parsing

        # Handle European-style thousand separators and decimals
        if "," in price and "." in price:
            # Identify if the format is European (e.g., €1.550,00)
            if price.index(",") > price.index("."):
                price = price.replace(".", "").replace(",", ".")
            else:
                price = price.replace(",", "")

        # Handle commas as decimal separators (e.g., €3750,00)
        elif "," in price:
            price = price.replace(",", ".")

        # Extract the numeric portion using a regular expression
        match = re.search(r"\d+(\.\d+)?", price)
        if match:
            numeric_price = match.group(0)
        else:
            raise ValueError(f"Could not parse price: {price}")

        # Convert to float
        try:
            return float(numeric_price)
        except ValueError:
            raise ValueError(f"Invalid numeric value in price: {price}")

    @staticmethod
    def clean_available(available):
        """
        Normalizes the availability field to a boolean.

        Args:
            available: The raw availability value (string, int, or bool).

        Returns:
            bool: True if available, False otherwise.
        Raises:
            ValueError: If the availability value cannot be interpreted.
        """
        if isinstance(available, bool):
            return available

        if isinstance(available, (int, float)):
            return bool(available)

        if isinstance(available, str):
            available = available.strip().lower()
            if available in ["true", "yes", "in stock", "1"]:
                return True
            elif available in ["false", "no", "sold", "unavailable", "out of stock", "0"]:
                return False

        raise ValueError(f"Cannot interpret availability value: {available}")

    @staticmethod
    def clean_url_list(urls):
        """
        Cleans and validates a list of URLs.

        Args:
            urls (list): A list of URLs to clean.

        Returns:
            list: A list of cleaned and validated URLs.

        Raises:
            ValueError: If any URL in the list is invalid.
        """
        import re

        # URL validation regex
        url_pattern = re.compile(
            r"^(https?://)"  # http or https
            r"([a-zA-Z0-9.-]+)"  # Domain
            r"(\.[a-zA-Z]{2,})"  # Top-level domain
            r"(:[0-9]+)?(/.*)?$"  # Port and path
        )

        if not isinstance(urls, list):
            raise ValueError("Input must be a list of URLs.")

        cleaned_urls = []
        for url in urls:
            if not isinstance(url, str):
                raise ValueError("Each URL must be a string.")
            
            url = url.strip()  # Remove surrounding whitespace
            if not url_pattern.match(url):
                raise ValueError(f"Invalid URL format: {url}")
            
            cleaned_urls.append(url)

        return cleaned_urls
    
    @staticmethod
    def clean_nation(nation):
        """
        Clean and standardize the nation data: all caps, no leading/trailing spaces.
        """
        if not nation:
            return None
        return nation.strip().upper()
    
    @staticmethod
    def clean_conflict(conflict):
        """
        Clean and standardize the conflict data: all caps, no leading/trailing spaces.
        """
        if not conflict:
            return None
        return conflict.strip().upper()
    
    @staticmethod
    def clean_item_type(item_type):
        """
        Clean and standardize the item type data:
        - Removes "CATEGORIES:" if present.
        - Removes "NEW" if it appears.
        - Keeps only the part after the '-' (to remove Dutch/Belgian part).
        - Extracts text inside parentheses if it exists (e.g., "MULTIPLE (WEHRMACHT)" → "WEHRMACHT").
        - Converts everything to uppercase.
        - Returns None if the type is "SOLD" or "NOT SPECIFIED".
        """
        if not item_type:
            return None

        # Convert to uppercase and strip spaces
        item_type = item_type.strip().upper()

        # Remove "CATEGORIES:" from the beginning
        if item_type.startswith("CATEGORIES:"):
            item_type = item_type[len("CATEGORIES:"):].strip()

        # Remove "NEW" if it appears alone or in a list
        item_parts = [part.strip() for part in item_type.split(",") if part.strip() and part.upper() != "NEW"]

        # Process each category: remove everything before the '-' and extract from parentheses if present
        cleaned_parts = []
        for part in item_parts:
            # Extract text inside parentheses if it exists (e.g., "MULTIPLE (WEHRMACHT)" → "WEHRMACHT")
            match = re.search(r"\(([^)]+)\)", part)
            if match:
                part = match.group(1).strip()  # Keep only the text inside parentheses

            # Remove everything before the '-' (Dutch/Belgian text)
            if '-' in part:
                part = part.split('-')[-1].strip()  # Keep only the part after '-'

            # Skip if item type is "SOLD" or "NOT SPECIFIED"
            if part in {"SOLD", "NOT SPECIFIED"}:
                return None

            cleaned_parts.append(part)

        # Join cleaned categories back into a single string
        return ", ".join(cleaned_parts) if cleaned_parts else None
    
    @staticmethod
    def clean_extracted_id(extracted_id):
        """
        Clean and validate the extracted ID:
        - Removes leading/trailing spaces.
        - Converts to uppercase.
        - Returns None and logs an error if the ID is longer than 20 characters.
        """
        if not extracted_id:
            return None

        extracted_id = extracted_id.strip().upper()

        # If ID is longer than 20 characters, log an error and return None
        if len(extracted_id) > 20:
            print(f"ERROR: EXTRACTED ID TOO LONG: '{extracted_id}' exceeds 20 characters. Returning None.")
            return None

        return extracted_id
    
    @staticmethod
    def clean_grade(grade):
        """
        Clean and standardize the grade data: no leading/trailing spaces.
        """
        if not grade:
            return None
        return grade.strip()
    
    @staticmethod
    def clean_categories(categories):
        """
        Clean and standardize site categories: strip spaces and title-case each category.
        """
        if not categories or not isinstance(categories, list):
            return []
        return [category.strip().title() for category in categories if isinstance(category, str)]

