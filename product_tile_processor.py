import logging
from clean_data import CleanData
import post_processors as post_processors
from urllib.parse import urlparse
from post_processors import normalize_input, apply_post_processors

class TileProcessor:
    def __init__(self, site_profile):
        self.site_profile = site_profile
        self.site_profile_tile_selectors = site_profile.get("product_tile_selectors", {})
        self.site_profile.get("base_url", None)

    def tile_process_main(self, products_tile_list):
        """
        Process all product tiles to extract relevant data points into a list of dictionaries,
        ensuring all required fields are populated and no duplicates are added.
        """
        tile_product_data = []
        seen_products = set()  
        clean_data = CleanData()
        skipped_duplicate_urls = set()

        # A product tile is like a product card or item card on a website.
        for product_tile in products_tile_list:
            try:
                # Extract and clean URL
                product_tile_url = self.extract_tile_url(product_tile)
                if not product_tile_url:
                    tile_preview = str(product_tile).strip().splitlines()[0][:500]
                    logging.debug(f"TILE PROCESSOR: skipped due to missing URL: {tile_preview}")
                    continue
                clean_product_tile_url = clean_data.clean_url(product_tile_url.strip())

                # Remove duplicate URLs immediately
                if clean_product_tile_url in seen_products:
                    skipped_duplicate_urls.add(clean_product_tile_url)
                    continue
                seen_products.add(clean_product_tile_url) 

                # Extract and clean title
                product_tile_title = self.extract_tile_title(product_tile)
                if not product_tile_title:
                    logging.debug(f"TILE PROCESSOR: Raw extracted title → {product_tile_title}")
                    continue
                clean_product_tile_title = clean_data.clean_title(product_tile_title.strip())

                # Extract and clean price
                product_tile_price = self.extract_tile_price(product_tile)
                clean_product_tile_price = (
                    clean_data.clean_price(product_tile_price.strip())
                    if product_tile_price else None
                )

                # Extract and clean availability
                logging.debug(f"[BEFORE AVAIL] {product_tile}")
                clean_product_tile_available = self.extract_tile_available(product_tile)
                if clean_product_tile_available is None:
                    logging.debug(f"TILE PROCESSOR: skipped due to missing availability: {product_tile_url}")
                    continue

                # Construct the product dictionary and add it to the final list
                product_dict = {
                    "url"      : clean_product_tile_url,
                    "title"    : clean_product_tile_title,
                    "price"    : clean_product_tile_price,
                    "available": clean_product_tile_available,
                    "site"     : self.site_profile.get("site")
                }

                logging.info(
                    f"""
                ======== TILE PRODUCT SUMMARY ========
                Raw URL            : {product_tile_url}
                Cleaned URL        : {clean_product_tile_url}

                Raw Title          : {product_tile_title}
                Cleaned Title      : {clean_product_tile_title}

                Raw Price          : {product_tile_price}
                Cleaned Price      : {clean_product_tile_price}

                Raw Availability   : {product_tile.get('class')}
                Cleaned Availability: {clean_product_tile_available}
                ======================================
                    """
                )

                tile_product_data.append(product_dict)

            except Exception as e:
                tile_preview = str(product_tile).strip().splitlines()[0][:500]
                logging.debug(f"TILE PROCESSOR: Product tile preview: {tile_preview}...")

        # Final deduplicated log of skipped URLs
        if skipped_duplicate_urls:
            logging.info("TILE PROCESSOR: Skipped duplicate URLs (deduplicated list):")
            for url in sorted(skipped_duplicate_urls):
                logging.info(f"  → {url}")

        return tile_product_data




    def parse_tile_config(self, selector_key):
        try:
            product_tile_selectors = self.site_profile_tile_selectors.get(selector_key, {})
            return (
                product_tile_selectors.get("method", "find"),
                product_tile_selectors.get("args", []),
                product_tile_selectors.get("kwargs", {}),
                product_tile_selectors.get("attribute"),
                product_tile_selectors
            )
        except Exception as e:
            raise ValueError(f"TILE PROCESSOR: Error parsing configuration for {selector_key}: {e}")

    def extract_data_from_tile(self, product_tile, method, args, kwargs, attribute):
        try:
            # Special case: direct attribute check (e.g., has_attr)
            if method == "has_attr" and args:
                attr_name = args[0]
                attr_value = product_tile.get(attr_name)
                if isinstance(attr_value, list):
                    result = " ".join(attr_value)
                else:
                    result = attr_value or ""
                logging.debug(f"TILE PROCESSOR: has_attr result → {result}")
                return result

            # Execute method like find, find_all, select, etc.
            element = getattr(product_tile, method)(*args, **kwargs)
            if not element:
                # Generates a lot of spam in logger.
                #logging.debug("TILE PROCESSOR: Element not found.")
                return None

            # Attribute extraction
            if attribute:
                attr_val = element.get(attribute, "")

                # Defensive fix: If attribute is a list (e.g., class_), join into a string
                if isinstance(attr_val, list):
                    attr_val = " ".join(attr_val)

                return attr_val.strip()

            # If it's a BeautifulSoup tag, extract text
            if hasattr(element, "get_text"):
                text = element.get_text(strip=True)
                logging.debug(f"TILE PROCESSOR: Extracted text from tag → {text}")
                return text

            # Fallback: return string conversion
            logging.debug(f"TILE PROCESSOR: Fallback to string → {str(element)}")
            return str(element)

        except AttributeError as e:
            logging.error(f"TILE PROCESSOR: Method '{method}' not found on the product tile. {e}")
            return None
        except Exception as e:
            logging.error(f"TILE PROCESSOR: Error extracting data from tile: {e}")
            return None



    def extract_tile_url(self, product_tile):
        """
        Extracts and post-processes the product detail URL from a tile.

        Filters known invalid or non-product URLs using both exact match and substring rules.
        """
        try:
            method, args, kwargs, attribute, config = self.parse_tile_config("details_url")
            product_url = self.extract_data_from_tile(product_tile, method, args, kwargs, attribute)

            if product_url:
                product_url = self.apply_post_processing(product_url, config)

            if product_url:
                product_url = product_url.strip()

                # Filter out base URLs and non-product links
                base_url = self.site_profile.get("base_url", "").rstrip("/")
                invalid_urls = {
                    "/", "#", "#MainContent", "", None,
                    base_url, base_url + "/", base_url + "/#"
                }
                if not product_url or product_url in invalid_urls or product_url.rstrip("/") == base_url:
                    return None

            # Custom hardcoded bad URLs
            CUSTOM_BAD_URLS = {
                "https://militariaplaza.nl/archive-38/dirAsc/results,1-1",
                "https://militariaplaza.nl/archive-38/dirAsc",
                "https://www.therupturedduck.com/"
            }

            if product_url in CUSTOM_BAD_URLS:
                return None

            if product_url and isinstance(product_url, str) and product_url.startswith("http"):
                return product_url

            return None

        except Exception as e:
            logging.error(f"TILE PROCESSOR: Error extracting product URL: {e}")
            return None
        

    def extract_tile_available(self, product_tile):
        """
        Determine whether a product is available or unavailable.
        Applies JSON-based post-processing logic if defined.
        Falls back to fallback rules if detection fails.
        """
        try:
            logging.debug(f"TILE ROOT ELEMENT: {product_tile.name}, attrs: {product_tile.attrs}")
            # === 1. Load and log the raw availability config ===
            raw_config = self.site_profile_tile_selectors.get("tile_availability")
            logging.debug(f"EXTRACT AVAILABILITY CONFIG: {raw_config}")

            # === 2. Handle static bool or string configs ===
            if isinstance(raw_config, bool):
                return raw_config

            if isinstance(raw_config, str):
                val = raw_config.strip().lower()
                if val == "true":
                    return True
                if val == "false":
                    return False

            # === 3. Ensure config is a dictionary going forward ===
            config = raw_config if isinstance(raw_config, dict) else {}

            # === 4. Extract raw value from tile ===
            method = config.get("method", "find")
            args = config.get("args", [])
            kwargs = config.get("kwargs", {})
            attribute = config.get("attribute")

            value = self.extract_data_from_tile(product_tile, method, args, kwargs, attribute)
            logging.debug(f"RAW AVAILABILITY VALUE: {value}")

            # === 5. Normalize and apply post-processing ===
            value = normalize_input(value)
            logging.debug(f"NORMALIZED AVAILABILITY VALUE: {value}")

            if "post_process" in config:
                value = apply_post_processors(value, config["post_process"])
                logging.debug(f"POST-PROCESSED AVAILABILITY VALUE: {value}")

            # === 6. Interpret common boolean values ===
            if isinstance(value, bool):
                return value

            if isinstance(value, str):
                val = value.lower().strip()
                if val in ("true", "yes", "available", "in stock", "add to cart"):
                    return True
                if val in ("false", "no", "sold out", "unavailable", "out of stock"):
                    return False

            if value is None or value == "":
                logging.debug("AVAILABILITY: Value is empty or None — fallback will be used.")

            # === 7. Fallback logic ===
            if self.is_product_available(product_tile):
                logging.debug("TILE PROCESSOR: Fallback availability = True")
                return True

            if self.is_product_unavailable(product_tile):
                logging.debug("TILE PROCESSOR: Fallback availability = False")
                return False

            # === 8. Final default if no availability info was found ===
            logging.warning("TILE PROCESSOR: No availability info found. Defaulting to False.")
            return False

        except Exception as e:
            logging.error(f"TILE PROCESSOR: Error determining product stock status: {e}")
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
            # Handle static "true"/"false" string override
            config_value = self.site_profile_tile_selectors.get("tile_availability")
            if isinstance(config_value, str):
                config_value_lower = config_value.strip().lower()
                if config_value_lower == "false":
                    return False
                elif config_value_lower == "true":
                    return True

            config = self.site_profile_tile_selectors.get("tile_availability", {})
            method = config.get("method", "find")
            args = config.get("args", [])
            kwargs = config.get("kwargs", {})
            value = config.get("value", None)

            element = getattr(product_tile, method)(*args, **kwargs)
            if element is None:
                return False

            # Handle post-process if configured
            if "post_process" in config:
                for func_name, arg in config["post_process"].items():
                    func = getattr(post_processors, func_name, None)
                    if func and func(element.get_text(strip=True), arg):
                        return True
                return False  # If post-processing exists but none matched

            # Fallback interpretation from text
            element_text = element.get_text(strip=True).lower()
            if element_text == "true":
                return True
            elif element_text == "false":
                return False
            else:
                return True

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
            from post_processors import find_text_contains  # or import all as needed

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

                # Handle class check via has_attr
                if method == "has_attr" and "class" in args:
                    attribute_value = product_tile.get("class", [])
                    if value in attribute_value:
                        return True
                    continue

                # Extract element using method
                element = getattr(product_tile, method)(*args, **kwargs)

                if element is None:
                    continue

                # Check for existence only
                if exists and element is not None:
                    return True

                # Check post-processing if defined
                if "post_process" in config:
                    for func_name, arg in config["post_process"].items():
                        func = getattr(post_processors, func_name, None)
                        if func and func(element.get_text(strip=True), arg):
                            return True

            return False
        except Exception as e:
            print(f"TILE PROCESSOR: Error checking unavailability: {e}")
            return False

        
    def extract_tile_title(self, product_tile):
        """
        Extract the title of the product from the tile and apply post-processing if defined.
        Ensures the result is a string, not a tag object.
        """
        try:
            method, args, kwargs, attribute, config = self.parse_tile_config("tile_title")
            title = self.extract_data_from_tile(product_tile, method, args, kwargs, attribute)

            # If still a tag, convert to string or extract text
            if hasattr(title, "get_text"):
                title = title.get_text(strip=True)

            return self.apply_post_processing(title, config) if title else None

        except Exception as e:
            logging.error(f"TILE PROCESSOR: Error extracting title: {e}")
            return None




    def extract_tile_price(self, product_tile):
        """
        Extract the price of the product from the tile and apply post-processing if defined.
        """
        try:
            method, args, kwargs, attribute, config = self.parse_tile_config("tile_price")
            price = self.extract_data_from_tile(product_tile, method, args, kwargs, attribute)
            return self.apply_post_processing(price, config) if price else None
        except Exception as e:
            logging.error(f"TILE PROCESSOR: Error extracting price: {e}")
            return None

    def extract_tile_image_url(self, product_tile):
        """
        Extract the image URL from the product tile and apply post-processing if defined.
        """
        try:
            method, args, kwargs, attribute, config = self.parse_tile_config("tile_image_url")
            image_url = self.extract_data_from_tile(product_tile, method, args, kwargs, attribute)
            return self.apply_post_processing(image_url, config) if image_url else None
        except Exception as e:
            logging.error(f"TILE PROCESSOR: Error extracting image URL: {e}")
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

    def apply_post_processing(self, value, config):
        post_process_config = config.get("post_process", None)
        if not post_process_config or not isinstance(post_process_config, dict):
            return value

        for func_name, arg in post_process_config.items():
            try:
                func = getattr(post_processors, func_name, None)
                if func is None:
                    logging.warning(f"Post-process function '{func_name}' not found.")
                    continue

                # Handle boolean-style no-arg functions (if you keep any in the future)
                if isinstance(arg, bool) and arg:
                    value = func(value)
                else:
                    value = func(value, arg)
            except Exception as e:
                logging.error(f"Error applying post-processing '{func_name}': {e}")
        return value
