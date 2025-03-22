import logging
from clean_data import CleanData

class TileProcessor:
    def __init__(self, site_profile):
        self.site_profile = site_profile
        self.site_profile_tile_selectors = site_profile.get("product_tile_selectors", {})

    def tile_process_main(self, products_tile_list):
        """
        Process all product tiles to extract relevant data points into a list of dictionaries,
        ensuring all required fields are populated and no duplicates are added.
        """
        tile_product_data = []
        seen_products = set()  # To track unique URLs
        clean_data = CleanData()

        for product_tile in products_tile_list:
            try:
                # Extract and clean URL
                product_tile_url = self.extract_tile_url(product_tile)
                if not product_tile_url:
                    logging.debug(f"TILE PROCESSOR: skipped due to missing URL: {product_tile}")
                    continue
                clean_product_tile_url = clean_data.clean_url(product_tile_url.strip())

                # Extract and clean title
                product_tile_title = self.extract_tile_title(product_tile)
                if not product_tile_title:
                    logging.debug(f"TILE PROCESSOR: skipped due to missing title: {product_tile}")
                    continue
                clean_product_tile_title = clean_data.clean_title(product_tile_title.strip())

                # Extract and clean price
                product_tile_price = self.extract_tile_price(product_tile)

                if not product_tile_price:
                    logging.debug(f"TILE PROCESSOR: Missing price, leaving as None: {product_tile_url}")
                    clean_product_tile_price = None  # Keeps price unchanged in DB if missing
                else:
                    clean_product_tile_price = clean_data.clean_price(product_tile_price.strip())  # Only strip if not None

                # Extract and clean availability
                clean_product_tile_available = self.extract_tile_available(product_tile)
                if clean_product_tile_available is None:
                    logging.debug(f"TILE PROCESSOR: skipped due to missing availability: {product_tile}")
                    continue


                # Deduplication logic based on cleaned URL
                if clean_product_tile_url in seen_products:
                    logging.debug(f"TILE PROCESSOR: Duplicate product skipped: {clean_product_tile_url}")
                    continue

                # Construct the product dictionary and add it to the final list
                product_dict = {
                    "url"      : clean_product_tile_url,
                    "title"    : clean_product_tile_title,
                    "price"    : clean_product_tile_price,
                    "available": clean_product_tile_available
                }
                tile_product_data.append(product_dict)
                seen_products.add(clean_product_tile_url)

            except Exception as e:
                logging.error(f"TILE PROCESSOR: Error processing tile: {e}, Tile: {product_tile}")

        return tile_product_data



    def parse_tile_config(self, selector_key):
        try:
            product_tile_selectors = self.site_profile_tile_selectors.get(selector_key, {})
            return (
                product_tile_selectors.get("method", "find"),
                product_tile_selectors.get("args", []),
                product_tile_selectors.get("kwargs", {}),
                product_tile_selectors.get("attribute")
            )
        except Exception as e:
            raise ValueError(f"TILE PROCESSOR: Error parsing configuration for {selector_key}: {e}")

    def extract_data_from_tile(self, product_tile, method, args, kwargs, attribute):
        """
        Extract data from a product tile based on the provided method, arguments, and attribute.
        """
        try:
            # Use the specified method (e.g., 'find', 'find_all') on the product tile
            element = getattr(product_tile, method)(*args, **kwargs)

            # If an attribute is provided, extract its value
            if attribute:
                return element.get(attribute).strip() if element and element.get(attribute) else None

            # If no attribute is specified, return the element's text content
            return element.get_text(strip=True) if element else None
        except AttributeError:
            print(f"TILE PROCESSOR: Error: Method '{method}' not found on the product tile.")
            return None
        except Exception as e:
            print(f"TILE PROCESSOR: Error extracting data from tile: {e}")
            return None

    def extract_tile_url(self, product_tile):
        try:
            # Get the configuration for the details URL
            url_config = self.parse_tile_config("details_url")

            # Extract the URL using the configuration
            product_url = self.extract_data_from_tile(product_tile, *url_config)

            # Validate and return the URL
            if product_url and product_url.startswith("http"):
                logging.info(f"TILE Extracted product URL: {product_url}")
                return product_url.strip()  # Ensure the URL is clean
            else:
                print(f"TILE PROCESSOR: Invalid or missing URL extracted: {product_url}")
                return None
        except Exception as e:
            print(f"TILE PROCESSOR: Error extracting product URL: {e}")
            return None
        
    def extract_tile_available(self, product_tile):
        """
        Determine whether a product is available or unavailable.

        Args:
            product_tile: The product tile element to check.

        Returns:
            bool: True if the product is available, False if unavailable.
        """
        try:
            # Check if the product is explicitly marked as available
            if self.is_product_available(product_tile):
                logging.debug("TILE PROCESSOR: Product is marked as available.")
                return True

            # Check if the product is explicitly marked as unavailable
            if self.is_product_unavailable(product_tile):
                logging.debug("TILE PROCESSOR: Product is marked as unavailable.")
                return False

            # Default to unavailable if neither condition is met
            logging.debug("TILE PROCESSOR: Defaulting to unavailable.")
            return False
        except Exception as e:
            print(f"TILE PROCESSOR: Error determining product stock status: {e}")
            return False

    def is_product_available(self, product_tile):
        """
        Check if a product is marked as available based on the JSON profile.

        Args:
            product_tile: The product tile element to check.

        Returns:
            bool: True if the product is available, False otherwise.
        """
        try:
            # Check if the JSON config explicitly states availability
            config_value = self.site_profile_tile_selectors.get("tile_availability")

            if isinstance(config_value, str):
                config_value_lower = config_value.strip().lower()

                # If "tile_availability" is explicitly "False", return False immediately
                if config_value_lower == "false":
                    return False  # The entire site is an archive or out-of-stock source

                # If "tile_availability" is explicitly "True", return True immediately
                elif config_value_lower == "true":
                    return True  # All products are available

            # If "tile_availability" is not a hardcoded "False"/"True", proceed with normal checking
            availability_keys = ["tile_availability"]
            for key in availability_keys:
                if key not in self.site_profile_tile_selectors:
                    continue

                config = self.site_profile_tile_selectors.get(key, {})
                method = config.get("method", "find")
                args = config.get("args", [])
                kwargs = config.get("kwargs", {})
                value = config.get("value", None)

                # Extract element based on method
                element = getattr(product_tile, method)(*args, **kwargs)

                # Handle attribute-based availability (e.g., checking class names)
                if method == "has_attr" and "class" in args:
                    attribute_value = product_tile.get("class", [])
                    if value in attribute_value:
                        return True
                    continue

                # If an element is found, check for string values ("True" / "False")
                if element is not None:
                    element_text = element.get_text(strip=True).lower()

                    if element_text == "true":
                        return True
                    elif element_text == "false":
                        return False
                    else:
                        return True  # If unknown value, assume available.

            return False  # Default to False if no availability info is found
        except Exception as e:
            print(f"TILE PROCESSOR: Error checking availability: {e}")
            return False


    def is_product_unavailable(self, product_tile):
        """
        Check if a product is marked as unavailable based on the JSON profile.

        Args:
            product_tile: The product tile element to check.

        Returns:
            bool: True if the product is unavailable, False otherwise.
        """
        try:
            unavailability_keys = ["tile_unavailability_reserved", "tile_unavailability_sold"]

            for key in unavailability_keys:
                if key not in self.site_profile_tile_selectors:
                    continue

                config = self.site_profile_tile_selectors.get(key, {})
                method = config.get("method", "find")
                args = config.get("args", [])
                kwargs = config.get("kwargs", {})
                exists = config.get("exists", False)
                value = config.get("value", None)

                if method == "has_attr" and "class" in args:
                    attribute_value = product_tile.get("class", [])
                    if value in attribute_value:
                        return True
                    continue

                # Extract element based on other methods
                element = getattr(product_tile, method)(*args, **kwargs)
                if exists and element is not None:
                    return True

            return False
        except Exception as e:
            print(f"TILE PROCESSOR: Error checking unavailability: {e}")
            return False

        
    def extract_tile_title(self, product_tile):
        """
        Extract the title of the product from the product tile.
        """
        try:
            # Get the configuration for the tile title
            title_config = self.parse_tile_config("tile_title")

            # Extract the title using the configuration
            title = self.extract_data_from_tile(product_tile, *title_config)
            return title.strip() if title else None  # Strip whitespace if title is found
        except Exception as e:
            print(f"TILE Error extracting title from tile: {e}")
            return None


    def extract_tile_price(self, product_tile):
        """
        Extract the price of the product from the product tile.
        """
        try:
            # Get the configuration for the tile price
            price_config = self.parse_tile_config("tile_price")

            # Extract the price using the configuration
            price = self.extract_data_from_tile(product_tile, *price_config)
            return price.strip() if price else None  # Strip whitespace if price is found
        except Exception as e:
            print(f"TILE PROCESSOR: Error extracting price from tile: {e}")
            return None


    def validate_product_dict(self, product_dict):
        """
        Validates that all required fields in the product dictionary are populated.
        """
        return all([
            product_dict.get("url"),
            product_dict.get("title"),
            product_dict.get("price"),
            product_dict.get("available") is not None
        ])


