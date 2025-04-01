import logging, json, pprint
from clean_data import CleanData
import image_extractor
from datetime import datetime
from decimal import Decimal


# This will handle the dictionary of data extracted from the tile on the products page tile.
class ProductTileDictProcessor:
    def __init__(self,site_profile, comparison_list, managers):
        self.site_profile       = site_profile
        self.comparison_list    = comparison_list
        self.managers           = managers
        self.counter            = managers.get('counter')
        self.rds_manager        = managers.get('rdsManager')
        self.log_print          = managers.get('logPrint')
        
    def product_tile_dict_processor_main(self, tile_product_data_list):
        processing_required_list = []
        availability_update_list = []

        # Categorize products into old and new
        try:
            processing_required_list, availability_update_list = self.compare_tile_url_to_rds(tile_product_data_list)
            self.counter.add_availability_update_count(count=len(availability_update_list))
            self.counter.add_processing_required_count(count=len(processing_required_list))
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: compare_tile_url_to_rds: {e}")

        # Update availability list
        try:
            self.process_availability_update_list(availability_update_list)
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: process_availability_update_list: {e}")

        return processing_required_list, availability_update_list


    # If in rds, compare availability status, title, price and update if needed
    # If not in rds, create
    def compare_tile_url_to_rds(self, tile_product_data_list):
        processing_required_list = []  # Products needing full processing
        availability_update_list = []  # Products needing only availability updates
        ignored_update_count = 0       # Count products with no updates required

        for tile_product_dict in tile_product_data_list:
            try:
                url       = tile_product_dict['url']            
                title     = tile_product_dict['title']
                price     = tile_product_dict['price']
                available = tile_product_dict['available']
            except Exception as e:
                logging.error(f'PRODUCT PROCESSOR: Error retrieving tile_product_dict values: {e}')
                continue

            product_category = "Processing Required"
            reason = "New product or mismatched details"

            # Check if the URL exists in the comparison dictionary
            if url in self.comparison_list:
                try:
                    # Unpack database values
                    db_title, db_price, db_available, db_description, db_price_history = self.comparison_list[url]
                except ValueError as e:
                    logging.error(f"PRODUCT PROCESSOR: Error unpacking comparison data for URL {url}: {e}")
                    continue

                # Determine price match logic
                if db_available == False and available == False and self.is_empty_price(price):
                    price_match = True
                    logging.debug(f"PRODUCT PROCESSOR: [MATCH] Sold item with no price: DB={db_price}, TILE={price}")
                elif self.is_empty_price(db_price) and not self.is_empty_price(price):
                    price_match = False
                    tile_product_dict["force_details_process"] = True
                    logging.debug(f"PRODUCT PROCESSOR: [FORCE PROCESS] DB has 0 price, tile has value → DB={db_price}, TILE={price}")
                else:
                    try:
                        price_match = float(db_price) == float(price)
                        if not price_match:
                            logging.debug(f"PRODUCT PROCESSOR: [MISMATCH] Price changed → DB={db_price}, TILE={price}")
                    except (TypeError, ValueError):
                        price_match = False
                        logging.debug(f"PRODUCT PROCESSOR: [MISMATCH] Could not compare prices → DB={db_price}, TILE={price}")

                # Check for exact matches of title, price, and availability
                if title == db_title and price_match:
                    if available != db_available:
                        # Only availability differs, add to availability update list
                        availability_update_list.append({"url": url, "available": available})
                        product_category = "Availability Update"
                        reason = "Availability status changed"
                        self.counter.add_availability_update_count(1)
                    else:
                        # No changes required
                        ignored_update_count += 1
                        continue
                else:
                    # Add non-matching products to the full processing list
                    processing_required_list.append(tile_product_dict)
                    reason = "Mismatch in title or price"
                    self.counter.add_processing_required_count(1)
            else:
                # Add new products not in the database to the full processing list
                processing_required_list.append(tile_product_dict)
                reason = "New product"
                self.counter.add_processing_required_count(1)

            logging.info(
                f"""
======== Product Summary ========
URL                  : {url}
DB Title             : {db_title if 'db_title' in locals() else 'N/A'}
Tile Title           : {tile_product_dict['title']}
DB Price             : {db_price if 'db_price' in locals() else 'N/A'}
Tile Price           : {tile_product_dict['price']}
DB Availability      : {db_available if 'db_available' in locals() else 'N/A'}
Tile Availability    : {tile_product_dict['available']}
==================================
Product Category     : {product_category}
Reason               : {reason}
==================================
                """
            )

        # Logging summary of processing
        logging.info(f'PRODUCT PROCESSOR: Products needing full processing  : {len(processing_required_list)}')
        logging.info(f'PRODUCT PROCESSOR: Products needing availability updates: {len(availability_update_list)}')
        logging.info(f'PRODUCT PROCESSOR: Products ignored (no updates needed): {ignored_update_count}')

        return processing_required_list, availability_update_list



    def is_empty_price(self, value):
        """Check if a price is empty (None, 0, 0.0, or an empty string)."""
        return value is None or value == 0 or value == 0.0 or value == ""

    def process_availability_update_list(self, availability_update_list):
        for product in availability_update_list:
            try:
                url       = product['url']
                available = product['available']
                logging.debug(f"PRODUCT PROCESSOR: Preparing to update: URL={url}, Available={available}")
                update_query = """
                UPDATE militaria
                SET available = %s
                WHERE url = %s;
                """
                params = (available, url)
                self.rds_manager.update_record(update_query, params)
                logging.info(f"PRODUCT PROCESSOR: Successfully updated availability for URL: {url}")
            except Exception as e:
                logging.error(f"PRODUCT PROCESSOR: Failed to update record for URL: {url}. Error: {e}")
        return
    


class ProductDetailsProcessor:
    def __init__(self, site_profile, managers, comparison_list):
        self.site_profile       = site_profile
        self.managers           = managers
        self.comparison_list    = comparison_list
        self.counter            = managers.get('counter')
        self.rds_manager        = managers.get('rdsManager')
        self.html_manager       = managers.get('html_manager')
        self.details_selectors  = site_profile.get("product_details_selectors", {})

    def product_details_processor_main(self, processing_required_list):
        """
        Process product details for new and old products, adding debugging information.

        Args:
            processing_required_list (list): List of products needing processing.

        Returns:
            None
        """
        # Debug start of processing
        logging.debug(f"PRODUCT PROCESSOR: Processing required list count: {len(processing_required_list)}")

        # Iterate through all the products needing processing
        for product in processing_required_list:
            product_url = product.get('url')
            logging.debug(f'PRODUCT PROCESSOR: ******************PRODUCT CHANGE******************')
            logging.debug(f"PRODUCT PROCESSOR: Processing product URL: {product_url}")

            # Create beautiful soup for deciphering HTML / CSS
            try:
                product_url_soup = self.html_manager.parse_html(product_url)
                logging.debug(f"PRODUCT PROCESSOR: Successfully parsed product URL into BeautifulSoup.")
            except Exception as e:
                logging.error(f"PRODUCT PROCESSOR: Error parsing product URL {product_url}: {e}")
                continue

            # Does the product need image upload?
            # try:
            #     image_upload_required = not self.rds_manager.should_skip_image_upload(product_url)
            #     logging.debug(f"Image upload required for {product_url}: {image_upload_required}")
            # except Exception as e:
            #     logging.error(f"Error determining image upload requirement for {product_url}: {e}")
            #     continue

            # Construct details data and clean data
            try:
                details_data = self.construct_details_data(product_url, product_url_soup)
                preview = pprint.pformat({k: str(v)[:200] for k, v in details_data.items()})
                logging.debug(f"PRODUCT PROCESSOR: Constructed details data (preview) for {product_url}: {preview}")
            except Exception as e:
                logging.error(f"PRODUCT PROCESSOR: Error constructing details data for {product_url}: {e}")
                continue

            try:
                clean_details_data = self.construct_clean_details_data(details_data)
                logging.debug(f"PRODUCT PROCESSOR: Constructed clean details data for {product_url}: {clean_details_data}")
            except Exception as e:
                preview = pprint.pformat({k: str(v)[:200] for k, v in details_data.items()})
                logging.debug(f"PRODUCT PROCESSOR: Constructed details data (preview) for {product_url}: {preview}")
                continue

            # Process new products
            if product_url not in self.comparison_list or product.get("force_new_upload"):
                logging.debug(f"PRODUCT PROCESSOR: Product URL {product_url} identified as new.")
                self.counter.add_new_product_count()

                try:
                    self.new_product_processor(clean_details_data, details_data)
                    logging.info(f"PRODUCT PROCESSOR: New product processed successfully: {product_url}")
                except Exception as e:
                    logging.error(f"PRODUCT PROCESSOR: Error processing new product {product_url}: {e}")

            # Process old products
            elif product_url in self.comparison_list:
                logging.debug(f"PRODUCT PROCESSOR: Product URL {product_url} identified as old.")
                self.counter.add_old_product_count()

                try:
                    self.old_product_processor(clean_details_data)
                    logging.info(f"PRODUCT PROCESSOR: Old product processed successfully: {product_url}")
                except Exception as e:
                    logging.error(f"PRODUCT PROCESSOR: Error processing old product {product_url}: {e}")

        logging.debug(f"PRODUCT PROCESSOR: Processing completed for {len(processing_required_list)} products.")
        

    def new_product_processor(self, clean_details_data, details_data):

        # Log the cleaning process
        try:
            logging.info('PRODUCT PROCESS: Starting data cleaning process...')
            logging.info(
                f"""
====== Data Cleaning Summary ======
Pre-clean URL                : {details_data.get('url')}
Post-clean URL               : {clean_details_data.get('url')}
Pre-clean Title              : {details_data.get('title')}
Post-clean Title             : {clean_details_data.get('title')}
Pre-clean Description        : {details_data.get('description')}
Post-clean Description       : {clean_details_data.get('description')}
Pre-clean Price              : {details_data.get('price')}
Post-clean Price             : {clean_details_data.get('price')}
Pre-clean Availability       : {details_data.get('available')}
Post-clean Availability      : {clean_details_data.get('available')}
Pre-clean Image URLs         : {details_data.get('original_image_urls')}
Post-clean Image URLs        : {clean_details_data.get('original_image_urls')}
Pre-clean Nation             : {details_data.get('nation_site_designated')}
Post-clean Nation            : {clean_details_data.get('nation_site_designated')}
Pre-clean Conflict           : {details_data.get('conflict_site_designated')}
Post-clean Conflict          : {clean_details_data.get('conflict_site_designated')}
Pre-clean Item Type          : {details_data.get('item_type_site_designated')}
Post-clean Item Type         : {clean_details_data.get('item_type_site_designated')}
Pre-clean Extracted ID       : {details_data.get('extracted_id')}
Post-clean Extracted ID      : {clean_details_data.get('extracted_id')}
Pre-clean Grade              : {details_data.get('grade')}
Post-clean Grade             : {clean_details_data.get('grade')}
Pre-clean Site Categories    : {details_data.get('categories_site_designated')}
Post-clean Site Categories   : {clean_details_data.get('categories_site_designated')}
===================================
                """
            )
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error logging cleaning summary: {e}")

        # Insert the cleaned data into the database
        self.rds_manager.new_product_input(clean_details_data)
        return

    def old_product_processor(self, clean_details_data):
        """
        Process existing products to compare and update changes in the database.

        Args:
            clean_details_data (dict): The cleaned product details to compare and update.

        Returns:
            None
        """
        url = clean_details_data.get('url')

        if url in self.comparison_list:
            try:
                # Unpack database values
                db_title, db_price, db_available, db_description, db_price_history = self.comparison_list[url]

                updates = {}
                now = datetime.now().isoformat()
                only_availability_changed = True  # Track if only availability changed

                # Check and update title
                if clean_details_data.get('title') and clean_details_data.get('title') != db_title:
                    updates['title'] = clean_details_data.get('title')
                    only_availability_changed = False  

                # Check and update description
                if clean_details_data.get('description') and clean_details_data.get('description') != db_description:
                    updates['description'] = clean_details_data.get('description')
                    only_availability_changed = False  

                # Prevent overwriting price with NULL or 0 when a valid price exists
                if clean_details_data.get('price') is not None and clean_details_data.get('price') != db_price:
                    # Prevent overwriting a valid price with 0
                    if clean_details_data['price'] == 0.0 and db_price not in (None, 0.0):
                        logging.info(f"PRODUCT PROCESSOR: Skipping price update for {url}, keeping {db_price}")
                    else:
                        updates['price'] = clean_details_data['price']

                        # Ensure price_history is initialized correctly
                        if isinstance(db_price_history, str):
                            try:
                                price_history = json.loads(db_price_history)  # Convert string JSON to list
                            except json.JSONDecodeError:
                                logging.error(f"PRODUCT PROCESSOR: Invalid JSON in price history for {url}")
                                price_history = []
                        else:
                            price_history = db_price_history if db_price_history is not None else []

                        # Append previous price to history
                        price_history.append({"price": db_price, "date": now})
                        updates['price_history'] = json.dumps(price_history)  # Convert back to JSON

                        only_availability_changed = False  # Mark that more than just availability changed


                # Check and update availability separately
                if clean_details_data.get('available') != db_available:
                    if only_availability_changed:
                        update_query = """
                        UPDATE militaria
                        SET available = %s
                        WHERE url = %s;
                        """
                        self.rds_manager.execute(update_query, (clean_details_data.get('available'), url))
                        logging.info(f"PRODUCT PROCESSOR: Updated availability for {url} to {clean_details_data.get('available')}")
                        return  
                    else:
                        updates['available'] = clean_details_data.get('available')

                # Check and update image URLs safely
                if clean_details_data.get('original_image_urls'):
                    original_images = clean_details_data.get('original_image_urls')
                    updates['original_image_urls'] = json.dumps(original_images) if isinstance(original_images, list) else original_images
                    only_availability_changed = False  

                # Check and update nation
                if clean_details_data.get('nation_site_designated'):
                    updates['nation_site_designated'] = clean_details_data.get('nation_site_designated')
                    only_availability_changed = False  

                # Check and update conflict
                if clean_details_data.get('conflict_site_designated'):
                    updates['conflict_site_designated'] = clean_details_data.get('conflict_site_designated')
                    only_availability_changed = False  

                # Check and update item type
                if clean_details_data.get('item_type_site_designated'):
                    updates['item_type_site_designated'] = clean_details_data.get('item_type_site_designated')
                    only_availability_changed = False  

                # Check and update extracted ID
                if clean_details_data.get('extracted_id'):
                    updates['extracted_id'] = clean_details_data.get('extracted_id')
                    only_availability_changed = False  

                # Check and update grade
                if clean_details_data.get('grade'):
                    updates['grade'] = clean_details_data.get('grade')
                    only_availability_changed = False  

                # Check and update site categories safely
                if clean_details_data.get('categories_site_designated'):
                    categories = clean_details_data.get('categories_site_designated')
                    updates['categories_site_designated'] = json.dumps(categories) if isinstance(categories, list) else categories
                    only_availability_changed = False  

                # Log the processing summary
                logging.info(f"PRODUCT PROCESSOR: Updated product {url} with changes: {updates}")

                # Perform database update if there are changes
                if updates:
                    set_clause = ', '.join([f"{key} = %s" for key in updates.keys()])
                    update_query = f"""
                    UPDATE militaria
                    SET {set_clause}
                    WHERE url = %s
                    """

                    # Execute the query
                    self.rds_manager.execute(update_query, list(updates.values()) + [url])
                    logging.info(f"PRODUCT PROCESSOR: Successfully updated product {url}")

            except ValueError as e:
                logging.error(f"PRODUCT PROCESSOR: Error unpacking comparison data for URL {url}: {e}")
            except Exception as e:
                logging.error(f"PRODUCT PROCESSOR: Error processing old product {url}: {e}")
        else:
            # 🛠 If product is NEW and price is missing, set it to 0 before inserting
            if clean_details_data.get('price') is None:
                clean_details_data['price'] = 0

            logging.info(f"PRODUCT PROCESSOR: New product detected, setting price to 0 if missing: {url}")
            
            # Now insert the new product into the database
            insert_query = """
            INSERT INTO militaria (url, title, description, price, available, original_image_urls, 
                                nation_site_designated, conflict_site_designated, item_type_site_designated, 
                                extracted_id, grade, categories_site_designated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            values = (
                clean_details_data.get('url'),
                clean_details_data.get('title'),
                clean_details_data.get('description'),
                clean_details_data.get('price'),  # Now guaranteed to be 0 if missing
                clean_details_data.get('available'),
                json.dumps(clean_details_data.get('original_image_urls')) if clean_details_data.get('original_image_urls') else None,
                clean_details_data.get('nation_site_designated'),
                clean_details_data.get('conflict_site_designated'),
                clean_details_data.get('item_type_site_designated'),
                clean_details_data.get('extracted_id'),
                clean_details_data.get('grade'),
                json.dumps(clean_details_data.get('categories_site_designated')) if clean_details_data.get('categories_site_designated') else None
            )

            try:
                self.rds_manager.execute(insert_query, values)
                logging.info(f"PRODUCT PROCESSOR: Inserted new product {url} with price {clean_details_data['price']}")
            except Exception as e:
                logging.error(f"PRODUCT PROCESSOR: Error inserting new product {url}: {e}")


    def extract_data(self, soup, method, args, kwargs, attribute, config=None):
        try:
            logging.debug(f"PRODUCT PROCESSOR: EXTRACT DATA: method={method}, args={args}, kwargs={kwargs}, attribute={attribute}")

            # Handle has_attr checks
            if method == "has_attr" and args:
                attr_name = args[0]
                attr_value = soup.get(attr_name)
                if isinstance(attr_value, list):
                    result = " ".join(attr_value)
                else:
                    result = attr_value or ""
                logging.debug(f"PRODUCT PROCESSOR: EXTRACT DATA: has_attr result → {result}")
                return result

            # Call method like .find(), .find_all(), etc.
            element = getattr(soup, method)(*args, **kwargs)
            if not element:
                logging.debug("PRODUCT PROCESSOR: EXTRACT DATA: Element not found.")
                return None

            # If checking presence only
            if config is None and attribute is None:
                logging.debug("PRODUCT PROCESSOR: EXTRACT DATA: Presence-based selector → returning element.")
                return element

            # Extract text if requested
            if config and config.get("extract") == "text":
                text = element.get_text(strip=True)
                logging.debug(f"PRODUCT PROCESSOR: EXTRACT DATA: Extracted text → {text}")
                return text

            # Extract attribute if specified
            if attribute:
                attr_val = element.get(attribute, "").strip()
                logging.debug(f"PRODUCT PROCESSOR: EXTRACT DATA: Extracted attribute '{attribute}' → {attr_val}")
                return attr_val

            # If still a tag, extract text
            if hasattr(element, "get_text"):
                text = element.get_text(strip=True)
                logging.debug(f"PRODUCT PROCESSOR: EXTRACT DATA: Default tag text → {text}")
                return text

            # Fallback to string conversion
            logging.debug(f"PRODUCT PROCESSOR: EXTRACT DATA: Fallback str() → {str(element)}")
            return str(element)

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting data: {e}")
            return None


    def extract_details_title(self, soup):
        """
        Extract and optionally post-process the product title from the details page.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_title", {})
            method = selector_config.get("method", "find")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            # Extract raw value using the generic extractor
            title = self.extract_data(soup, method, args, kwargs, attribute, selector_config)

            # Fallback: if title is still a tag, get its text
            if hasattr(title, "get_text"):
                logging.warning("EXTRACT DETAILS TITLE: Received tag instead of text, auto-converting with .get_text()")
                title = title.get_text(strip=True)

            # Apply post-processing if defined
            if title and "post_process" in selector_config:
                import post_processors as post_processors
                for func_name, arg in selector_config["post_process"].items():
                    func = getattr(post_processors, func_name, None)
                    if func:
                        title = func(title, arg) if not isinstance(arg, bool) else func(title)

            logging.debug(f"EXTRACT DETAILS TITLE: Final value → {title}")
            return title

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting title: {e}")
            return None



    def extract_details_description(self, soup):
        """
        Extract and optionally post-process the product description.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_description", {})
            method = selector_config.get("method", "find")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            # Submethod support (nested find)
            submethod = selector_config.get("submethod")
            element = getattr(soup, method)(*args, **kwargs)

            if submethod and element:
                sub_element = getattr(element, submethod.get("method", "find"))(
                    *submethod.get("args", []),
                    **submethod.get("kwargs", {})
                )
                if sub_element:
                    attribute = submethod.get("attribute", attribute)
                    description = sub_element.get(attribute).strip() if attribute else sub_element.get_text(strip=True)
                else:
                    description = None
            else:
                description = (
                    element.get(attribute).strip() if attribute and element and element.get(attribute)
                    else element.get_text(strip=True) if element else None
                )

            # Post-processing if defined
            if description and "post_process" in selector_config:
                import post_processors as post_processors
                for func_name, arg in selector_config["post_process"].items():
                    func = getattr(post_processors, func_name, None)
                    if func:
                        description = func(description, arg) if not isinstance(arg, bool) else func(description)

            return description
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting description: {e}")
            return None


    def extract_details_price(self, soup):
        """
        Extract and optionally post-process the product price from the details page.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_price", {})
            method = selector_config.get("method", "find")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            # Extract raw value
            raw_price = self.extract_data(soup, method, args, kwargs, attribute)

            # Apply post-processing if defined
            if raw_price and "post_process" in selector_config:
                import post_processors as post_processors
                for func_name, arg in selector_config["post_process"].items():
                    func = getattr(post_processors, func_name, None)
                    if func:
                        raw_price = func(raw_price, arg) if not isinstance(arg, bool) else func(raw_price)

            return str(raw_price) if raw_price else "0"
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting price: {e}")
            return "0"


    def process_price_updates(clean_details_data, db_price, db_price_history, now):
        """
        Process price updates and manage the price history.

        Args:
            clean_details_data (dict): The new product details data.
            db_price (Decimal): The existing price in the database.
            db_price_history (list): The existing price history (if any).
            now (str): The current timestamp.

        Returns:
            dict: A dictionary of updates for price and price history.
        """
        updates = {}

        # Check if the price has changed
        if clean_details_data.get('price') != float(db_price):
            # Update price
            new_price = clean_details_data.get('price')
            updates['price'] = new_price

            # Initialize or update the price history
            updated_price_history = db_price_history or []
            updated_price_history.append({'price': float(db_price), 'date': now})
            updates['price_history'] = json.dumps(updated_price_history)

        return updates


    def extract_details_availability(self, soup):
        """
        Extract availability and apply post-processing if defined.
        Supports static booleans: true/false.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_availability", {})

            # 🔁 Static boolean values
            if selector_config is True:
                logging.debug("PRODUCT PROCESSOR: Availability hardcoded as True in JSON.")
                return True
            elif selector_config is False:
                logging.debug("PRODUCT PROCESSOR: Availability hardcoded as False in JSON.")
                return False
            elif isinstance(selector_config, str):
                if selector_config.lower() == "true":
                    logging.debug("PRODUCT PROCESSOR: Availability string 'true' treated as True.")
                    return True
                elif selector_config.lower() == "false":
                    logging.debug("PRODUCT PROCESSOR: Availability string 'false' treated as False.")
                    return False

            # Proceed with extraction if it's a dict (selector object)
            method = selector_config.get("method", "find")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            raw_value = self.extract_data(soup, method, args, kwargs, attribute, selector_config)
            logging.debug(f"PRODUCT PROCESSOR: Extracted availability raw: {raw_value}")

            # Post-process
            if raw_value and "post_process" in selector_config:
                import post_processors as post_processors
                for func_name, arg in selector_config["post_process"].items():
                    func = getattr(post_processors, func_name, None)
                    if func:
                        raw_value = func(raw_value, arg) if not isinstance(arg, bool) else func(raw_value)

            if isinstance(raw_value, bool):
                return raw_value

            return raw_value.strip().lower() == "in stock" if isinstance(raw_value, str) else False

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting availability: {e}")
            return None



    def extract_details_image_url(self, soup):
        """
        Extract image URLs using the specified function in the JSON profile.
        
        Args:
            soup (BeautifulSoup): The BeautifulSoup object for the product page.
            
        Returns:
            list: A list of extracted image URLs or an empty list if the function is not defined or fails.
        """
        try:
            # Fetch the function name from the JSON profile
            image_extractor_function_version = self.site_profile.get("product_details_selectors", {}).get("details_image_url", {}).get("function")

            # ✅ Ensure it's a string before calling .lower()
            if isinstance(image_extractor_function_version, str) and image_extractor_function_version.lower() == "skip":
                return []

            logging.debug(f"PRODUCT PROCESSOR: Function name retrieved: {image_extractor_function_version}")

            if not image_extractor_function_version:
                raise ValueError("PRODUCT PROCESSOR: Image extraction function name not specified in the JSON profile.")
            
            # Ensure the function exists in the image_extractor module
            if not hasattr(image_extractor, image_extractor_function_version):
                raise AttributeError(f"PRODUCT PROCESSOR: Function '{image_extractor_function_version}' not found in image_extractor.")
            
            # Dynamically call the function from image_extractor
            details_image_urls = getattr(image_extractor, image_extractor_function_version)(soup)
            if not isinstance(details_image_urls, list):
                raise TypeError(f"PRODUCT PROCESSOR: Function '{image_extractor_function_version}' did not return a list of URLs.")
        
            return details_image_urls

        except AttributeError as ae:
            logging.error(f"PRODUCT PROCESSOR: AttributeError: {ae}")
            return []

        except ValueError as ve:
            logging.error(f"PRODUCT PROCESSOR: ValueError: {ve}")
            return []

        except TypeError as te:
            logging.error(f"PRODUCT PROCESSOR: TypeError: {te}")
            return []

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Unexpected error in extract_details_image_url: {e}")
            return []


    def extract_details_nation(self, soup):
        """
        Extract the nation information from the product page.
        Currently not implemented in the JSON, returns None.
        """
        return None  # Placeholder as the nation field is not defined in the JSON profile

    def extract_details_conflict(self, soup):
        """
        Extract the conflict information from the product page.
        Currently not implemented in the JSON, returns None.
        """
        return None  # Placeholder as the conflict field is not defined in the JSON profile

    def extract_details_item_type(self, soup):
        """
        Extract the item type from the product page and apply post-processing if defined.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_item_type", {})
            method = selector_config.get("method", "find")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            item_type = self.extract_data(soup, method, args, kwargs, attribute, selector_config)

            if item_type and "post_process" in selector_config:
                import post_processors as post_processors
                for func_name, arg in selector_config["post_process"].items():
                    func = getattr(post_processors, func_name, None)
                    if func:
                        item_type = func(item_type, arg) if not isinstance(arg, bool) else func(item_type)

            return item_type if item_type else None
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting item type: {e}")
            return None


    def extract_details_extracted_id(self, soup):
        """
        Extract the product ID or SKU and apply post-processing if defined.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_extracted_id", {})
            method          = selector_config.get("method", "find")
            args            = selector_config.get("args", [])
            kwargs          = selector_config.get("kwargs", {})
            attribute       = selector_config.get("attribute")

            extracted_id = self.extract_data(soup, method, args, kwargs, attribute, selector_config)

            if extracted_id and "post_process" in selector_config:
                import post_processors as post_processors
                for func_name, arg in selector_config["post_process"].items():
                    func = getattr(post_processors, func_name, None)
                    if func:
                        extracted_id = func(extracted_id, arg) if not isinstance(arg, bool) else func(extracted_id)

            return extracted_id if extracted_id else None
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting extracted_id: {e}")
            return None


    def extract_details_grade(self, soup):
        """
        Extract the grade information from the product page.
        Currently not implemented in the JSON, returns None.
        """
        return None  # Placeholder as the grade field is not defined in the JSON profile
        
    def extract_details_site_categories(self, soup):
        """
        Extract site-specific product categories and apply post-processing if defined.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_site_categories", {})
            method = selector_config.get("method", "find_all")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            categories = self.extract_data(soup, method, args, kwargs, attribute, selector_config)

            # Apply post-processing
            if categories and "post_process" in selector_config:
                import post_processors as post_processors
                for func_name, arg in selector_config["post_process"].items():
                    func = getattr(post_processors, func_name, None)
                    if func:
                        categories = func(categories, arg) if not isinstance(arg, bool) else func(categories)

            return categories if categories else []
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting site categories: {e}")
            return []

        
    def parse_details_config(self, selector_key):
        """
        Returns full selector config, including method/args/kwargs/attribute and post_process.
        """
        try:
            selector_config = self.details_selectors.get(selector_key, {})

            return (
                selector_config.get("method", "find"),
                selector_config.get("args", []),
                selector_config.get("kwargs", {}),
                selector_config.get("attribute"),
                selector_config  # full config including post_process
            )
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error parsing configuration for {selector_key}: {e}")
            return None, None, None, None, {}


        
    def construct_details_data(self, product_url, product_url_soup):
        sel = self.details_selectors  # shortcut for readability

        try:
            data = {
                "url": product_url,
                "title": self.extract_details_title(product_url_soup) if sel.get("details_title") else None,
                "description": self.extract_details_description(product_url_soup) if sel.get("details_description") else None,
                "price": self.extract_details_price(product_url_soup) if sel.get("details_price") else "0",
                "available": self.extract_details_availability(product_url_soup) if sel.get("details_availability") else None,
                "original_image_urls": self.extract_details_image_url(product_url_soup) if sel.get("details_image_url") else [],
                "nation_site_designated": self.extract_details_nation(product_url_soup) if sel.get("details_nation") else None,
                "conflict_site_designated": self.extract_details_conflict(product_url_soup) if sel.get("details_conflict") else None,
                "item_type_site_designated": self.extract_details_item_type(product_url_soup) if sel.get("details_item_type") else None,
                "extracted_id": self.extract_details_extracted_id(product_url_soup) if sel.get("details_extracted_id") else None,
                "grade": self.extract_details_grade(product_url_soup) if sel.get("details_grade") else None,
                "categories_site_designated": self.extract_details_site_categories(product_url_soup) if sel.get("details_site_categories") else [],
            }

            import pprint, logging
            logging.debug(f"CONSTRUCT DETAILS DATA: Extracted fields →\n{pprint.pformat(data)}")

            return data

        except Exception as e:
            logging.error(f"CONSTRUCT DETAILS DATA: Error while constructing data for {product_url} → {e}")
            return {
                "url": product_url,
                "title": None,
                "description": None,
                "price": "0",
                "available": False,
                "original_image_urls": [],
                "nation_site_designated": None,
                "conflict_site_designated": None,
                "item_type_site_designated": None,
                "extracted_id": None,
                "grade": None,
                "categories_site_designated": []
            }

    
    def construct_clean_details_data(self, details_data):
        """
        Process and clean details data dynamically.

        Args:
            details_data (dict): Raw details data.

        Returns:
            dict: Cleaned details data.
        """
        clean_data = CleanData()

        # Map keys to their corresponding cleaning functions
        cleaning_functions = {
            "url"                       : clean_data.clean_url,
            "title"                     : clean_data.clean_title,
            "description"               : clean_data.clean_description,
            "price"                     : clean_data.clean_price,
            "available"                 : clean_data.clean_available,
            "original_image_urls"       : clean_data.clean_url_list,
            "nation_site_designated"    : clean_data.clean_nation,
            "conflict_site_designated"  : clean_data.clean_conflict,
            "item_type_site_designated" : clean_data.clean_item_type,
            "extracted_id"              : clean_data.clean_extracted_id,
            "grade"                     : clean_data.clean_grade,
            "categories_site_designated": clean_data.clean_categories,
        }

        # Apply the corresponding cleaning function to each key
        cleaned_data = {}
        for key, value in details_data.items():
            if key in cleaning_functions:
                cleaned_data[key] = cleaning_functions[key](value)
            else:
                cleaned_data[key] = value  # Preserve keys that don't have cleaning functions

        # Add additional data like site and currency
        cleaned_data.update({
            "site": self.site_profile['source_name'],
            "currency": self.site_profile.get("access_config", {}).get("currency_code"),
        })

        return cleaned_data
    
    def convert_decimal_to_float(self,data):
        """
        Recursively convert Decimal objects in a nested structure to float.
        
        Args:
            data: The data structure (dict, list, or scalar) to process.
        
        Returns:
            The data structure with all Decimal objects converted to float.
        """
        if isinstance(data, dict):
            return {key: self.convert_decimal_to_float(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.convert_decimal_to_float(item) for item in data]
        elif isinstance(data, Decimal):
            return float(data)
        else:
            return data