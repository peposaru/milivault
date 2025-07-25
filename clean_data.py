import re
import html
import logging
from price_parser import Price
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from html import unescape


class CleanData:
    @staticmethod
    def clean_url(url: str) -> str:
        if not isinstance(url, str):
            raise ValueError("URL must be a string.")
        url = url.strip()
        if not url:
            raise ValueError("URL is empty after cleaning.")
        return url


    @staticmethod
    def clean_title(title, allow_empty: bool = True):
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
        try:
            if not isinstance(title, str):
                logging.error("CLEAN TITLE: Input is not a string.")
                if allow_empty:
                    return ""
                raise ValueError("Title must be a string.")

            logging.debug(f"CLEAN TITLE: Raw input → {title}")

            # Decode HTML entities
            title = unescape(title)

            # Remove HTML tags
            title = re.sub(r'<[^>]+>', '', title)

            # Strip whitespace
            title = title.strip()

            # Replace special quotes with single quote
            SPECIAL_QUOTES = {"“": "'", "”": "'", "‘": "'", "’": "'", '"': "'"}
            for special, standard in SPECIAL_QUOTES.items():
                title = title.replace(special, standard)

            # Collapse multiple spaces
            title = " ".join(title.split())

            if not title:
                msg = "Title is empty after cleaning."
                logging.warning(f"CLEAN TITLE: {msg}")
                if allow_empty:
                    return ""
                raise ValueError("Title cannot be empty after cleaning.")

            logging.debug(f"CLEAN TITLE: Final cleaned title → {title}")
            return title

        except Exception as e:
            logging.error(f"CLEAN TITLE: Failed to clean title ({e})")
            if allow_empty:
                return ""
            raise


    @staticmethod
    def clean_description(description, allow_empty: bool = True):
        """
        Clean and normalize a product description.
        If allow_empty=True, return "" (or a provided fallback upstream) instead of raising.
        """
        try:
            if description is None:
                logging.debug("CLEAN DESCRIPTION: Description is None.")
                return "" if allow_empty else None  # your previous code returned None

            if not isinstance(description, str):
                logging.error("CLEAN DESCRIPTION: Input is not a string.")
                if allow_empty:
                    return ""
                raise ValueError("Description must be a string.")

            logging.debug(f"CLEAN DESCRIPTION: Raw input → {description}")

            # Decode HTML entities
            description = unescape(description)

            # Trim
            description = description.strip()

            # Remove leading "Description"
            if description.lower().startswith("description"):
                description = description[len("description"):].strip()

            # Normalize quotes
            SPECIAL_QUOTES = {"“": "'", "”": "'", "‘": "'", "’": "'", '"': "'"}
            for special, standard in SPECIAL_QUOTES.items():
                description = description.replace(special, standard)

            # Collapse multiple spaces
            description = " ".join(description.split())

            # Strip leading/trailing colons
            description = description.strip(":").strip()

            if not description:
                msg = "Description is empty after cleaning."
                logging.warning(f"CLEAN DESCRIPTION: {msg}")
                if allow_empty:
                    return ""
                # your old fallback text:
                return "No description available."

            logging.debug(f"CLEAN DESCRIPTION: Final cleaned description → {description}")
            return description

        except Exception as e:
            logging.error(f"CLEAN DESCRIPTION: Failed to clean description ({e})")
            if allow_empty:
                return ""
            raise



    @staticmethod
    def clean_price(price_input) -> float | None:
        """
        Normalize a price string to a float.
        Returns None if input is None.
        Raises ValueError if input isn’t a string or can’t be parsed.
        """
        # 1) None → None
        if price_input is None:
            logging.debug("CLEAN_PRICE: received None, returning None")
            return None

        # 2) Must be a string
        if not isinstance(price_input, str):
            raise ValueError("must be a string")

        logging.debug(f"CLEAN_PRICE: raw input = {price_input!r}")

        # 3) Strip any HTML
        text = BeautifulSoup(price_input, "html.parser").get_text(strip=True)

        # 4) Handle mixed comma & dot cases
        if "." in text and "," in text:
            last_dot   = text.rfind(".")
            last_comma = text.rfind(",")
            if last_comma > last_dot:
                # European style: dot thousands, comma decimal
                logging.debug(f"CLEAN_PRICE: European style detected, remove dots → {text!r}")
                text = text.replace(".", "")
            else:
                # US style: comma thousands, dot decimal
                logging.debug(f"CLEAN_PRICE: US style detected, remove commas → {text!r}")
                text = text.replace(",", "")

        # 5) Single‑dot thousands shorthand (e.g. "1.400" → "1400")
        elif "," not in text and re.match(r"^\d+\.\d{3}$", text):
            logging.debug(f"CLEAN_PRICE: single‑dot thousands detected, remove dot → {text!r}")
            text = text.replace(".", "")

        # 6) Pure comma decimal (no other dots)
        if "," in text and "." not in text:
            logging.debug(f"CLEAN_PRICE: comma decimal only, comma→dot → {text!r}")
            text = text.replace(",", ".")

        # 7) Collapse malformed multi‑dot cases (e.g. "1.250.00" → "1250.00")
        if text.count(".") > 1 and "," not in price_input:
            parts = text.split(".")
            fixed = "".join(parts[:-1]) + "." + parts[-1]
            logging.debug(f"CLEAN_PRICE: collapsed multi‑dot {text!r} → {fixed!r}")
            text = fixed

        # 8) Parse with Price.fromstring
        p = Price.fromstring(text)
        if p.amount_float is None:
            raise ValueError(f"Could not parse price from '{text}'")
        result = float(p.amount_float)

        logging.debug(f"CLEAN_PRICE: FINAL → {result}")

        return result


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
    def clean_nation(nation: str | None) -> str | None:
        if not nation:
            return None
        return nation.strip().upper()


    @staticmethod
    def clean_conflict(conflict: str | None) -> str | None:
        if not conflict:
            return None
        return conflict.strip().upper()


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
                # TEMP FIX for MILITARIA_PLAZA: remove specific (RELATED) tag
                if part.endswith("(RELATED)"):
                    part = part.replace("(RELATED)", "").strip()
                    logging.debug(f"CLEAN ITEM TYPE: Removed '(RELATED)' → {part}")

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
    def clean_grade(grade: str | None) -> str | None:
        if not grade or not isinstance(grade, str):
            return None
        return grade.strip()


    @staticmethod
    def clean_categories(categories: list[str] | None) -> list[str]:
        if not isinstance(categories, list):
            return []
        return [c.strip().title() for c in categories if isinstance(c, str) and c.strip()]
