import logging, json
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
        
    def product_tile_dict_processor_main(self, tile_product_data_list):
        processing_required_list = []
        availability_update_list = []

        # Categorize products into old and new
        try:
            processing_required_list, availability_update_list = self.compare_tile_url_to_rds(tile_product_data_list)
            self.counter.add_availability_update_count(count=len(availability_update_list))
            self.counter.add_processing_required_count(count=len(processing_required_list))
        except Exception as e:
            logging.error(f"compare_tile_url_to_rds: {e}")

        # Update availability list
        try:
            self.process_availability_update_list(availability_update_list)
        except Exception as e:
            logging.error(f"process_availability_update_list: {e}")

        return processing_required_list, availability_update_list


    # If in rds, compare availability status, title, price and update if needed
    # If not in rds, create
    def compare_tile_url_to_rds(self, tile_product_data_list):
        processing_required_list = []  # Products needing full processing
        availability_update_list = []  # Products needing only availability updates

        for tile_product_dict in tile_product_data_list:
            try:
                url       = tile_product_dict['url']            
                title     = tile_product_dict['title']
                price     = tile_product_dict['price']
                available = tile_product_dict['available']
            except Exception as e:
                logging.error(f'Error retrieving tile_product_dict values: {e}')
                continue

            product_category = "Processing Required"
            reason           = "New product or mismatched details"

            # Check if the URL exists in the comparison dictionary
            if url in self.comparison_list:
                try:
                    # Unpack database values
                    db_title, db_price, db_available, db_description, db_price_history = self.comparison_list[url]
                except ValueError as e:
                    logging.error(f"Error unpacking comparison data for URL {url}: {e}")
                    continue

                # Check for exact matches of title, price, and availability
                if title == db_title and price == db_price:
                    if available != db_available:
                        # Only availability differs, add to availability update list
                        availability_update_list.append({
                            "url": url,
                            "available": available
                        })
                        product_category = "Availability Update"
                        reason = "Availability status changed"
                    else:
                        continue  # Skip exact matches if nothing else differs
                else:
                    # Add non-matching products to the full processing list
                    processing_required_list.append(tile_product_dict)
                    reason = "Mismatch in title or price"
            else:
                # Add new products not in the database to the full processing list
                processing_required_list.append(tile_product_dict)
                reason = "New product"

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

        logging.info(f'Products needing further processing  : {len(processing_required_list)}')
        logging.info(f'Products needing availability updates: {len(availability_update_list)}')
        return processing_required_list, availability_update_list

    def process_availability_update_list(self, availability_update_list):
        for product in availability_update_list:
            try:
                url       = product['url']
                available = product['available']
                logging.debug(f"Preparing to update: URL={url}, Available={available}")
                update_query = """
                UPDATE militaria
                SET available = %s
                WHERE url = %s;
                """
                params = (available, url)
                self.rds_manager.update_record(update_query, params)
                logging.info(f"Successfully updated availability for URL: {url}")
            except Exception as e:
                logging.error(f"Failed to update record for URL: {url}. Error: {e}")
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
        logging.debug(f"Processing required list count: {len(processing_required_list)}")

        # Iterate through all the products needing processing
        for product in processing_required_list:
            product_url = product.get('url')
            logging.debug(f"Processing product URL: {product_url}")

            # Create beautiful soup for deciphering HTML / CSS
            try:
                product_url_soup = self.html_manager.parse_html(product_url)
                logging.debug(f"Successfully parsed product URL into BeautifulSoup.")
            except Exception as e:
                logging.error(f"Error parsing product URL {product_url}: {e}")
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
                logging.debug(f"Constructed details data for {product_url}: {details_data}")
            except Exception as e:
                logging.error(f"Error constructing details data for {product_url}: {e}")
                continue

            try:
                clean_details_data = self.construct_clean_details_data(details_data)
                logging.debug(f"Constructed clean details data for {product_url}: {clean_details_data}")
            except Exception as e:
                logging.error(f"Error constructing clean details data for {product_url}: {e}")
                continue

            # Process new products
            if product_url not in self.comparison_list:
                logging.debug(f"Product URL {product_url} identified as new.")
                self.counter.add_new_product_count()

                try:
                    self.new_product_processor(clean_details_data, details_data)
                    logging.info(f"New product processed successfully: {product_url}")
                except Exception as e:
                    logging.error(f"Error processing new product {product_url}: {e}")

            # Process old products
            elif product_url in self.comparison_list:
                logging.debug(f"Product URL {product_url} identified as old.")
                self.counter.add_old_product_count()

                try:
                    self.old_product_processor(clean_details_data)
                    logging.info(f"Old product processed successfully: {product_url}")
                except Exception as e:
                    logging.error(f"Error processing old product {product_url}: {e}")

        logging.debug(f"Processing completed for {len(processing_required_list)} products.")



    def new_product_processor(self, clean_details_data, details_data):

        # Log the cleaning process
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
            Pre-clean Site Categories    : {details_data.get('catgories_site_designated')}
            Post-clean Site Categories   : {clean_details_data.get('catgories_site_designated')}
            ===================================
            """
        )

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

                # Check and update title
                if clean_details_data.get('title') != db_title:
                    updates['title'] = clean_details_data.get('title')

                # Check and update description
                if clean_details_data.get('description') != db_description:
                    updates['description'] = clean_details_data.get('description')

                # Check and update price
                if clean_details_data.get('price') != db_price:
                    updates['price'] = clean_details_data.get('price')
                    # Handle price history
                    price_history = json.loads(db_price_history) if db_price_history else []
                    price_history.append({"price": db_price, "date": now})
                    updates['price_history'] = json.dumps(price_history)

                # Check and update availability
                if clean_details_data.get('available') != db_available:
                    updates['available'] = clean_details_data.get('available')

                # Check and update image URLs
                if clean_details_data.get('original_image_urls'):
                    updates['original_image_urls'] = json.dumps(clean_details_data.get('original_image_urls'))

                # Check and update nation
                if clean_details_data.get('nation'):
                    updates['nation_site_designated'] = clean_details_data.get('nation_site_designated')

                # Check and update conflict
                if clean_details_data.get('conflict'):
                    updates['conflict_site_designated'] = clean_details_data.get('conflict_site_designated')

                # Check and update item type
                if clean_details_data.get('item_type'):
                    updates['item_type_site_designated'] = clean_details_data.get('item_type_site_designated')

                # Check and update extracted ID
                if clean_details_data.get('extracted_id'):
                    updates['extracted_id'] = clean_details_data.get('extracted_id')

                # Check and update grade
                if clean_details_data.get('grade'):
                    updates['grade'] = clean_details_data.get('grade')

                # Check and update site categories
                if clean_details_data.get('site_categories'):
                    updates['categories_site_designated'] = json.dumps(clean_details_data.get('categories_site_designated'))

                # Log the processing summary
                logging.info(
                    f"""
                    ====== Old Product Processing Summary ======
                    Product URL                   : {url}

                    Existing Title                : {db_title}
                    New Title                     : {clean_details_data.get('title')}

                    Existing Description          : {db_description}
                    New Description               : {clean_details_data.get('description')}

                    Existing Price                : {db_price}
                    New Price                     : {clean_details_data.get('price')}

                    Existing Availability         : {db_available}
                    New Availability              : {clean_details_data.get('available')}

                    Existing Image URLs           : {self.comparison_list[url][4]}
                    New Image URLs                : {clean_details_data.get('original_image_urls')}

                    Existing Nation               : {self.comparison_list[url][5] if len(self.comparison_list[url]) > 5 else 'N/A'}
                    New Nation                    : {clean_details_data.get('nation_site_designated')}

                    Existing Conflict             : {self.comparison_list[url][6] if len(self.comparison_list[url]) > 6 else 'N/A'}
                    New Conflict                  : {clean_details_data.get('conflict_site_designated')}

                    Existing Item Type            : {self.comparison_list[url][7] if len(self.comparison_list[url]) > 7 else 'N/A'}
                    New Item Type                 : {clean_details_data.get('item_type_site_designated')}

                    Existing Extracted ID         : {self.comparison_list[url][8] if len(self.comparison_list[url]) > 8 else 'N/A'}
                    New Extracted ID              : {clean_details_data.get('extracted_id')}

                    Existing Grade                : {self.comparison_list[url][9] if len(self.comparison_list[url]) > 9 else 'N/A'}
                    New Grade                     : {clean_details_data.get('grade')}

                    Existing Site Categories      : {self.comparison_list[url][10] if len(self.comparison_list[url]) > 10 else 'N/A'}
                    New Site Categories           : {clean_details_data.get('catgories_site_designated')}
                    ==============================================
                    """
                )

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
                    logging.info(f"Updated product: {url} with changes: {updates}")

            except ValueError as e:
                logging.error(f"Error unpacking comparison data for URL {url}: {e}")
            except Exception as e:
                logging.error(f"Error processing old product {url}: {e}")
        else:
            logging.error(f"old_product_processor received new URL: {url}")






    def extract_data(self, soup, method, args, kwargs, attribute):
        """
        Extract data from the soup object based on the configuration.
        """
        try:
            element = getattr(soup, method)(*args, **kwargs)
            if attribute:
                return element.get(attribute).strip() if element and element.get(attribute) else None
            return element.get_text(strip=True) if element else None
        except Exception as e:
            logging.error(f"Error extracting data: {e}")
            return None
        
    def extract_details_title(self, soup):
        """
        Extract the title of the product.
        """
        config = self.parse_details_config("details_title")
        return self.extract_data(soup, *config)

    def extract_details_description(self, soup):
        """
        Extract the description of the product.
        """
        config = self.parse_details_config("details_description")
        return self.extract_data(soup, *config)

    def extract_details_price(self, soup):
        """
        Extract the price of the product and normalize it to a float.

        Args:
            soup (BeautifulSoup): The BeautifulSoup object for the product page.

        Returns:
            str: The raw price string extracted from the page.
        """
        try:
            # Parse configuration for price extraction
            config = self.parse_details_config("details_price")
            raw_price = self.extract_data(soup, *config)

            if raw_price:
                # Ensure the price is returned as a string
                return str(raw_price)

            logging.warning("No price found on the page.")
            return None
        except Exception as e:
            logging.error(f"Error extracting price: {e}")
            return None

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
        try:
            config = self.parse_details_config("details_availability")
            availability = self.extract_data(soup, *config)

            logging.debug(f"Extracted availability raw: {availability}")

            # Normalize text for comparison
            if availability:
                normalized = availability.strip().lower()
                return "in stock" in normalized
            return False
        except Exception as e:
            logging.error(f"Error extracting availability: {e}")
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
            logging.debug(f"Function name retrieved: {image_extractor_function_version}")

            if image_extractor_function_version.lower() == "skip":
                return []

            if not image_extractor_function_version:
                raise ValueError("Image extraction function name not specified in the JSON profile.")
            
            # Ensure the function exists in the image_extractor module
            if not hasattr(image_extractor, image_extractor_function_version):
                raise AttributeError(f"Function '{image_extractor_function_version}' not found in image_extractor.")
            
            # Dynamically call the function from image_extractor
            details_image_urls = getattr(image_extractor, image_extractor_function_version)(soup)
            if not isinstance(details_image_urls, list):
                raise TypeError(f"Function '{image_extractor_function_version}' did not return a list of URLs.")
        
            return details_image_urls

        except AttributeError as ae:
            logging.error(f"AttributeError: {ae}")
            return []

        except ValueError as ve:
            logging.error(f"ValueError: {ve}")
            return []

        except TypeError as te:
            logging.error(f"TypeError: {te}")
            return []

        except Exception as e:
            logging.error(f"Unexpected error in extract_details_image_url: {e}")
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
        Extract the item type from the product page.
        """
        config = self.parse_details_config("details_item_type")
        item_type = self.extract_data(soup, *config)
        return item_type if item_type else None

    def extract_details_extracted_id(self, soup):
        """
        Extract the SKU or product ID from the product page.
        """
        config = self.parse_details_config("details_extracted_id")
        extracted_id = self.extract_data(soup, *config)
        return extracted_id if extracted_id else None

    def extract_details_grade(self, soup):
        """
        Extract the grade information from the product page.
        Currently not implemented in the JSON, returns None.
        """
        return None  # Placeholder as the grade field is not defined in the JSON profile
        
    def extract_details_site_categories(self, soup):
        """
        Extract site-specific categories from the product page in a universal manner.
        """
        config = self.parse_details_config("details_site_categories")
        categories = self.extract_data(soup, *config)
        return categories if categories else []

    def parse_details_config(self, selector_key):
        try:
            product_tile_selectors = self.details_selectors.get(selector_key, {})
            return (
                product_tile_selectors.get("method", "find"),
                product_tile_selectors.get("args", []),
                product_tile_selectors.get("kwargs", {}),
                product_tile_selectors.get("attribute")
            )
        except Exception as e:
            raise ValueError(f"Error parsing configuration for {selector_key}: {e}")
        
    def construct_details_data(self, product_url, product_url_soup ):
        return {
            "url"                       : product_url,
            "title"                     : self.extract_details_title(product_url_soup),
            "description"               : self.extract_details_description(product_url_soup),
            "price"                     : self.extract_details_price(product_url_soup),
            "available"                 : self.extract_details_availability(product_url_soup),
            "original_image_urls"       : self.extract_details_image_url(product_url_soup),
            "nation_site_designated"    : self.extract_details_nation(product_url_soup),
            "conflict_site_designated"  : self.extract_details_conflict(product_url_soup),
            "item_type_site_designated" : self.extract_details_item_type(product_url_soup),
            "extracted_id"              : self.extract_details_extracted_id(product_url_soup),
            "grade"                     : self.extract_details_grade(product_url_soup),
            "catgories_site_designated" : self.extract_details_site_categories(product_url_soup),
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
            "url"                      : clean_data.clean_url,
            "title"                    : clean_data.clean_title,
            "description"              : clean_data.clean_description,
            "price"                    : clean_data.clean_price,
            "available"                : clean_data.clean_available,
            "original_image_urls"      : clean_data.clean_url_list,
            "nation_site_designated"   : clean_data.clean_nation,
            "conflict_site_designated" : clean_data.clean_conflict,
            "item_type_site_designated": clean_data.clean_item_type,
            "extracted_id"             : clean_data.clean_extracted_id,
            "grade"                    : clean_data.clean_grade,
            "catgories_site_designated": clean_data.clean_categories,
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