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
    
    def clean_title(self, title):
        """
        Cleans and normalizes a product title, ensuring safe handling of quotes.

        Args:
            title (str): The title to clean.

        Returns:
            str: The cleaned and normalized title.
        Raises:
            ValueError: If the title is not a string or is empty.
        """
        if not isinstance(title, str):
            raise ValueError("Title must be a string.")
        
        # Remove leading and trailing whitespace
        title = title.strip()

        # Replace special quotes with standard ones
        special_quotes = {
            "“": '"', "”": '"',  # Double quotes
            "‘": "'", "’": "'",  # Single quotes
        }
        for special, standard in special_quotes.items():
            title = title.replace(special, standard)
        
        # Escape single quotes for PostgreSQL
        title = title.replace("'", "''")

        # Replace multiple spaces with a single space
        title = " ".join(title.split())

        # Remove specific unnecessary phrases (if applicable)
        unnecessary_phrases = [
            "Click to View Larger Image",  # Example phrase
            "Description:",  # Example prefix
        ]
        for phrase in unnecessary_phrases:
            title = title.replace(phrase, "").strip()

        if not title:
            raise ValueError("Title cannot be empty after cleaning.")

        return title

    def clean_description(self, description):
        """
        Cleans and normalizes a product description.

        Args:
            description (str): The raw description text.

        Returns:
            str: The cleaned description text.
        Raises:
            ValueError: If the description is not a string or is empty.
        """
        if not isinstance(description, str):
            raise ValueError("Description must be a string.")
        
        # Remove leading and trailing whitespace
        description = description.strip()

        # Replace escape characters
        description = description.replace("\n", " ").replace("\t", " ").replace("\r", " ")

        # Decode HTML entities
        description = unescape(description)

        # Replace multiple spaces with a single space
        description = " ".join(description.split())

        # Remove unwanted phrases (if applicable)
        unwanted_phrases = ["Full Description Below", "See details above"]
        for phrase in unwanted_phrases:
            description = description.replace(phrase, "").strip()

        if not description:
            raise ValueError("Description cannot be empty after cleaning.")

        return description

    
    def clean_price(self, price):
        """
        Cleans and normalizes price strings to a float.

        Args:
            price (str): The raw price string to clean.

        Returns:
            float: The cleaned price as a float.
        Raises:
            ValueError: If the price cannot be parsed or is invalid.
        """
        if not isinstance(price, str):
            raise ValueError("Price must be a string.")

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
        
    def clean_available(self, available):
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
