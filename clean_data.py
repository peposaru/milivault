import re
from html import unescape
import html
import logging
from price_parser import Price
from bs4 import BeautifulSoup


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
            logging.error("CLEAN URL: Input is not a string.")
            raise ValueError("URL must be a string.")

        url = url.strip()
        logging.debug(f"CLEAN URL: Stripped URL → {url}")

        url_pattern = re.compile(
            r"^(https?://)"          # http or https
            r"([a-zA-Z0-9.-]+)"      # Domain
            r"(\.[a-zA-Z]{2,})"      # Top-level domain
            r"(:[0-9]+)?(/.*)?$"     # Optional port and path
        )

        if not url_pattern.match(url):
            logging.warning(f"CLEAN URL: Failed to match pattern → {url}")
            raise ValueError(f"Invalid URL format: {url}")

        logging.debug(f"CLEAN URL: Validated URL → {url}")
        return url
    

    @staticmethod
    def clean_title(title):
        """
        Cleans and normalizes a product title, ensuring single quotes are used consistently
        and HTML tags are removed.

        Args:
            title (str): The title to clean.

        Returns:
            str: The cleaned title with normalized quotes.
        Raises:
            ValueError: If the title is not a string or is empty.
        """
        import logging

        try:
            if not isinstance(title, str):
                logging.error("CLEAN TITLE: Input is not a string.")
                raise ValueError("Title must be a string.")

            logging.debug(f"CLEAN TITLE: Raw input → {title}")

            # Decode HTML entities
            title = unescape(title)
            logging.debug(f"CLEAN TITLE: After unescape → {title}")

            # Remove HTML tags
            title = re.sub(r'<[^>]+>', '', title)
            logging.debug(f"CLEAN TITLE: After removing HTML tags → {title}")

            # Strip whitespace
            title = title.strip()

            # Replace special quotes
            special_quotes = {
                "“": "'", "”": "'",
                "‘": "'", "’": "'",
                '"': "'"
            }
            for special, standard in special_quotes.items():
                title = title.replace(special, standard)
            logging.debug(f"CLEAN TITLE: After normalizing quotes → {title}")

            # Collapse multiple spaces
            title = " ".join(title.split())
            logging.debug(f"CLEAN TITLE: After collapsing spaces → {title}")

            if not title:
                logging.warning("CLEAN TITLE: Title is empty after cleaning.")
                raise ValueError("Title cannot be empty after cleaning.")

            logging.debug(f"CLEAN TITLE: Final cleaned title → {title}")
            return title

        except Exception as e:
            logging.error(f"CLEAN TITLE: Failed to clean title: {title} ({e})")
            raise




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
        import logging

        try:
            if description is None:
                logging.debug("CLEAN DESCRIPTION: Description is None.")
                description = None
                return description
            
            if not isinstance(description, str):
                logging.error("CLEAN DESCRIPTION: Input is not a string.")
                raise ValueError("Description must be a string.")

            logging.debug(f"CLEAN DESCRIPTION: Raw input → {description}")

            # Decode HTML entities
            description = unescape(description)
            logging.debug(f"CLEAN DESCRIPTION: After unescape → {description}")

            # Remove leading and trailing whitespace
            description = description.strip()

            # Remove leading "Description" label if present
            if description.lower().startswith("description"):
                description = description[len("description"):].strip()
                logging.debug(f"CLEAN DESCRIPTION: Removed leading label → {description}")

            # Normalize quotes
            special_quotes = {
                "“": "'", "”": "'",
                "‘": "'", "’": "'",
                '"': "'"
            }
            for special, standard in special_quotes.items():
                description = description.replace(special, standard)
            logging.debug(f"CLEAN DESCRIPTION: Normalized quotes → {description}")

            # Collapse multiple spaces
            description = " ".join(description.split())
            logging.debug(f"CLEAN DESCRIPTION: Collapsed whitespace → {description}")

            if not description:
                logging.warning("CLEAN DESCRIPTION: Description is empty after cleaning.")
                raise ValueError("Description cannot be empty after cleaning.")

            logging.debug(f"CLEAN DESCRIPTION: Final cleaned description → {description}")
            return description

        except Exception as e:
            logging.error(f"CLEAN DESCRIPTION: Failed to clean description: {description} ({e})")
            raise


# This is the old version of clean_price. I am testing price_parser.
    # @staticmethod
    # def clean_price(price):
    #     """
    #     Cleans and normalizes price strings to a float.
    #     Handles mixed formats: US, European, currency symbols, HTML wrappers.
    #     """
    #     import re
    #     import logging
    #     from bs4 import BeautifulSoup

    #     try:
    #         if price is None:
    #             logging.debug("CLEAN PRICE: Price is None.")
    #             return None

    #         if not isinstance(price, str):
    #             raise ValueError("CLEAN PRICE: Price must be a string.")

    #         price_raw = price  # for logging/debug
    #         logging.debug(f"CLEAN PRICE: Raw input → {price}")

    #         # Step 1: Remove HTML if present
    #         price = BeautifulSoup(price, "html.parser").get_text(strip=True)
    #         logging.debug(f"CLEAN PRICE: After stripping HTML → {price}")

    #         # Step 2: Remove known phrases and currency symbols
    #         currency_symbols = ["$", "€", "£", "Kč", "USD", "EUR", ",-"]
    #         for symbol in currency_symbols:
    #             price = price.replace(symbol, "")
    #         price = price.replace("\u00A0", "").replace(" ", "")
    #         logging.debug(f"CLEAN PRICE: After removing currency/symbols/spaces → {price}")

    #         # Step 3: Normalize separators
    #         # 1.375,00 → 1375.00
    #         if re.match(r'^\d{1,3}(\.\d{3})*(,\d{2})?$', price):
    #             price = price.replace(".", "").replace(",", ".")
    #         # 1,375.00 → 1375.00
    #         elif re.match(r'^\d{1,3}(,\d{3})*\.\d{2}$', price):
    #             price = price.replace(",", "")
    #         # 7,000 → 7000 (treat comma as thousand separator if no dot)
    #         elif "," in price and "." not in price:
    #             if re.match(r'^\d{1,3}(,\d{3})+$', price):  # thousands format
    #                 price = price.replace(",", "")
    #             else:
    #                 price = price.replace(",", ".")
    #         # 1.400 → 1400 (dot used as thousands, no decimals)
    #         elif re.match(r'^\d+\.\d{3}$', price):
    #             price = price.replace(".", "")
    #         logging.debug(f"CLEAN PRICE: Normalized format → {price}")

    #         # Step 4: Extract number
    #         match = re.search(r"\d+(\.\d+)?", price)
    #         if not match:
    #             raise ValueError(f"Could not parse numeric portion from → {price}")
    #         result = float(match.group(0))
    #         logging.debug(f"CLEAN PRICE: Final numeric value → {result}")

    #         if result < 10:
    #             logging.warning(f"CLEAN PRICE: Suspiciously low parsed price: {result} ← from input '{price_raw}'")

    #         return result

    #     except Exception as e:
    #         logging.error(f"CLEAN PRICE: Failed to clean price: {price} ({e})")
    #         raise


    @staticmethod
    def clean_price(price_string):
        """
        Cleans and normalizes price input to a float.
        Accepts HTML, raw strings, integers, or floats.

        Args:
            price_string (str|float|int): Raw price input.

        Returns:
            float: Parsed float price.

        Raises:
            ValueError: If the price can't be parsed.
        """
        try:
            if price_string is None:
                logging.debug("CLEAN PRICE: Price is None.")
                return None

            # Accept numbers and cast to string
            if isinstance(price_string, (int, float)):
                price_string = str(price_string)

            if not isinstance(price_string, str):
                raise ValueError("CLEAN PRICE: Price must be a string or convertible to string.")

            price_raw = price_string
            price_string = BeautifulSoup(price_string, "html.parser").get_text(strip=True)
            logging.debug(f"CLEAN PRICE: Stripped text → {price_string}")

            from price_parser import Price
            price = Price.fromstring(price_string)

            if price.amount_float is None:
                logging.warning(f"CLEAN PRICE: Unable to parse price from '{price_string}'")
                raise ValueError(f"Could not parse numeric portion from → {price_string}")

            if price.amount_float < 10:
                logging.warning(f"CLEAN PRICE: Suspiciously low parsed price: {price.amount_float} ← from input '{price_raw}'")

            logging.debug(f"CLEAN PRICE: Parsed value → {price.amount_float}")
            return float(price.amount_float)

        except Exception as e:
            logging.error(f"CLEAN PRICE: Exception while parsing '{price_string}': {e}")
            raise





    @staticmethod
    def clean_available(available):
        """
        Normalizes the availability field to a boolean.

        Args:
            available: The raw availability value (string, int, or bool).

        Returns:
            bool: True if available, False otherwise.
        """
        try:
            logging.debug(f"CLEAN AVAILABLE: Raw value → {available} ({type(available).__name__})")

            # ✅ Exit early if already a boolean
            if isinstance(available, bool):
                logging.debug(f"CLEAN AVAILABLE: Already boolean → {available}")
                return available

            if isinstance(available, bool):
                logging.debug(f"CLEAN AVAILABLE: Interpreted as boolean → {available}")
                return available

            if isinstance(available, (int, float)):
                result = bool(available)
                logging.debug(f"CLEAN AVAILABLE: Interpreted as numeric → {result}")
                return result

            if isinstance(available, str):
                available = available.strip().lower()
                if available in ["true", "yes", "in stock", "available", "1", '1 in stock','stock in-stock']:
                    logging.debug("CLEAN AVAILABLE: Interpreted as available string → True")
                    return True
                elif available in ["false", "no", "sold", "unavailable", "out of stock", "0", "Out of stock", "Sold", "Sold out", "Sold Out", "SOLD OUT", "SOLD","Sold out"]:
                    logging.debug("CLEAN AVAILABLE: Interpreted as unavailable string → False")
                    return False

            if hasattr(available, "get_text"):
                text = available.get_text(strip=True).lower()
                result = "in stock" in text or "add to cart" in text
                logging.debug(f"CLEAN AVAILABLE: Extracted from tag → '{text}', Result → {result}")
                return result

            text = str(available).strip().lower()
            result = text in ["true", "yes", "in stock", "available", "1"]
            logging.debug(f"CLEAN AVAILABLE: Fallback to string → '{text}', Result → {result}")
            return result

        except Exception as e:
            logging.warning(f"CLEAN AVAILABLE: Failed to normalize availability: {available} ({e})")
            return False


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
        try:
            logging.debug(f"CLEAN URL LIST: Raw input → {urls}")

            if not isinstance(urls, list):
                logging.error("CLEAN URL LIST: Input is not a list.")
                raise ValueError("Input must be a list of URLs.")

            url_pattern = re.compile(
                r"^(https?://)"        # http or https
                r"([a-zA-Z0-9.-]+)"    # Domain
                r"(\.[a-zA-Z]{2,})"    # Top-level domain
                r"(:[0-9]+)?(/.*)?$"   # Port and path
            )

            cleaned_urls = []
            for url in urls:
                if not isinstance(url, str):
                    logging.error(f"CLEAN URL LIST: URL is not a string → {url}")
                    raise ValueError("Each URL must be a string.")
                
                url = url.strip()
                if not url_pattern.match(url):
                    logging.warning(f"CLEAN URL LIST: Invalid URL format → {url}")
                    raise ValueError(f"Invalid URL format: {url}")

                cleaned_urls.append(url)

            logging.debug(f"CLEAN URL LIST: Cleaned URLs → {cleaned_urls}")
            return cleaned_urls

        except Exception as e:
            logging.error(f"CLEAN URL LIST: Failed to clean list: {urls} ({e})")
            raise

    
    @staticmethod
    def clean_nation(nation):
        """
        Clean and standardize the nation data:
        - Strips whitespace
        - Converts to uppercase
        """
        try:
            logging.debug(f"CLEAN NATION: Raw input → {nation}")
            if not nation:
                logging.debug("CLEAN NATION: Input is empty or None.")
                return None

            cleaned = nation.strip().upper()
            logging.debug(f"CLEAN NATION: Cleaned → {cleaned}")
            return cleaned

        except Exception as e:
            logging.error(f"CLEAN NATION: Failed to clean nation: {nation} ({e})")
            return None

    
    @staticmethod
    def clean_conflict(conflict):
        """
        Clean and standardize the conflict data:
        - Strips whitespace
        - Converts to uppercase
        """
        try:
            logging.debug(f"CLEAN CONFLICT: Raw input → {conflict}")
            if not conflict:
                logging.debug("CLEAN CONFLICT: Input is empty or None.")
                return None

            cleaned = conflict.strip().upper()
            logging.debug(f"CLEAN CONFLICT: Cleaned → {cleaned}")
            return cleaned

        except Exception as e:
            logging.error(f"CLEAN CONFLICT: Failed to clean conflict: {conflict} ({e})")
            return None

    
    @staticmethod
    def clean_item_type(item_type):
        """
        Clean and standardize the item type data:
        - Removes prefixes like "CATEGORIES:", "CATEGORY:", and "ARCHIVE:"
        - Removes "NEW" and "SOLD"
        - Extracts text in parentheses (e.g., "FOO (BAR)" → "BAR")
        - Keeps only the part after '-' if present
        - Decodes HTML (e.g., &AMP; → &)
        - Converts to uppercase
        - Filters out generic values like "MILITARIA"
        - Returns None if no valid type remains
        """
        try:
            logging.debug(f"CLEAN ITEM TYPE: Raw input → {item_type}")

            if not item_type:
                logging.debug("CLEAN ITEM TYPE: Input is empty or None.")
                return None

            item_type = html.unescape(item_type).strip().upper()
            logging.debug(f"CLEAN ITEM TYPE: After decode and uppercase → {item_type}")

            for prefix in ["CATEGORIES:", "CATEGORY:", "ARCHIVE:"]:
                if item_type.startswith(prefix):
                    item_type = item_type[len(prefix):].strip()
                    logging.debug(f"CLEAN ITEM TYPE: Removed prefix '{prefix}' → {item_type}")

            parts = [p.strip() for p in item_type.split(",") if p.strip() and p not in {"NEW", "SOLD"}]

            cleaned_parts = []
            for part in parts:
                # Extract from parentheses
                match = re.search(r"\(([^)]+)\)", part)
                if match:
                    part = match.group(1).strip()
                    logging.debug(f"CLEAN ITEM TYPE: Extracted from parentheses → {part}")

                # Take only part after last hyphen
                if '-' in part:
                    part = part.split('-')[-1].strip()
                    logging.debug(f"CLEAN ITEM TYPE: Trimmed after '-' → {part}")

                # Filter generic or unhelpful values
                if part in {"SOLD", "NOT SPECIFIED", "ARCHIVE", "MILITARIA"}:
                    continue

                cleaned_parts.append(part)

            result = ", ".join(cleaned_parts) if cleaned_parts else None
            logging.debug(f"CLEAN ITEM TYPE: Final result → {result}")
            return result

        except Exception as e:
            logging.error(f"CLEAN ITEM TYPE: Failed to clean item type: {item_type} ({e})")
            return None



    
    @staticmethod
    def clean_extracted_id(extracted_id):
        """
        Clean and validate the extracted ID:
        - Strips whitespace
        - Converts to uppercase
        - Logs a warning and returns None if longer than 20 characters
        """
        try:
            logging.debug(f"CLEAN EXTRACTED ID: Raw input → {extracted_id}")

            if not extracted_id:
                logging.debug("CLEAN EXTRACTED ID: Input is empty or None.")
                return None

            if not isinstance(extracted_id, str):
                logging.warning(f"CLEAN EXTRACTED ID: Input is not a string → {type(extracted_id).__name__}")
                return None

            extracted_id = extracted_id.strip().upper()
            logging.debug(f"CLEAN EXTRACTED ID: Cleaned → {extracted_id}")

            if len(extracted_id) > 20:
                logging.warning(f"CLEAN EXTRACTED ID: Too long (>20): '{extracted_id}'")
                return None

            return extracted_id

        except Exception as e:
            logging.error(f"CLEAN EXTRACTED ID: Failed to clean ID: {extracted_id} ({e})")
            return None

    
    @staticmethod
    def clean_grade(grade):
        """
        Clean and standardize the grade data:
        - Strips whitespace
        """
        try:
            logging.debug(f"CLEAN GRADE: Raw input → {grade}")
            if not grade:
                logging.debug("CLEAN GRADE: Input is empty or None.")
                return None

            if not isinstance(grade, str):
                logging.warning(f"CLEAN GRADE: Input is not a string → {type(grade).__name__}")
                return None

            cleaned = grade.strip()
            logging.debug(f"CLEAN GRADE: Cleaned → {cleaned}")
            return cleaned

        except Exception as e:
            logging.error(f"CLEAN GRADE: Failed to clean grade: {grade} ({e})")
            return None

    
    @staticmethod
    def clean_categories(categories):
        """
        Clean and standardize site categories:
        - Title-cases each category
        - Strips whitespace
        """
        try:
            logging.debug(f"CLEAN CATEGORIES: Raw input → {categories}")

            if not categories or not isinstance(categories, list):
                logging.debug("CLEAN CATEGORIES: Input is empty or not a list.")
                return []

            cleaned = [
                category.strip().title()
                for category in categories
                if isinstance(category, str) and category.strip()
            ]

            logging.debug(f"CLEAN CATEGORIES: Cleaned → {cleaned}")
            return cleaned

        except Exception as e:
            logging.error(f"CLEAN CATEGORIES: Failed to clean categories: {categories} ({e})")
            return []


