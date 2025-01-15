import logging, re, time
from clean_data import CleanData
import image_extractor

# This will handle the dictionary of data extracted from the tile on the products page tile.
class ProductTileDictProcessor:
    def __init__(self,site_profile, comparison_list, managers):
        self.site_profile       = site_profile
        self.comparison_list    = comparison_list
        self.managers           = managers
        self.counter            = managers.get('counter')
        self.rds_manager        = managers.get('rdsManager')
        
    def product_tile_dict_processor_main(self, tile_product_data_list):

        # Categorize products into old and new
        try:
            processing_required_list, availability_update_list = self.compare_tile_url_to_rds(tile_product_data_list)  # Remove extra self
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
            url       = tile_product_dict['url']
            logging.debug(f'{url}')
            
            title     = tile_product_dict['title']
            logging.debug(f'{title}')

            price     = tile_product_dict['price']
            logging.debug(f'{price}')

            available = tile_product_dict['available']
            logging.debug(f'{available}')


            # Check if the URL exists in the comparison dictionary
            if url in self.comparison_list:
                try:
                    # Unpack database values
                    db_title, db_price, db_available = self.comparison_list[url]
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
                    continue  # Skip exact matches if nothing else differs
                else:
                    # Add non-matching products to the full processing list
                    processing_required_list.append(tile_product_dict)
            else:
                # Add new products not in the database to the full processing list
                processing_required_list.append(tile_product_dict)

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

    def product_details_processor_main(self,processing_required_list):

        # Iterate through all the products needing processing
        for product in processing_required_list:
            product_url = product['url']
            logging.debug(f'Product Url for Details: {product_url}')

            # Create beautiful soup for decyphering html / css
            try:
                product_url_soup = self.html_manager.parse_html(product_url)
                logging.debug(f'product_url_soup loaded.')
            except Exception as e:
                logging.error(f"product_url_soup: {e}")
                continue

            # Does product need to upload image?
            image_upload_required = False
            if not self.rds_manager.should_skip_image_upload(product_url):
                image_upload_required = True

            # New products processer
            if product_url not in self.comparison_list:
                self.counter.add_new_product_count()
                # If the product is not in the database send the url to a processor
                self.new_product_processor(product_url_soup)

            # Old products processer
            elif product_url in self.comparison_list:
                self.counter.add_old_product_count()
                # If the product is in the database, compare them.
                self.old_product_processor(product_url_soup, image_upload_required)

            # Check if url has changed to different page.



    def new_product_processor(self, product_url_soup):
        clean_data = CleanData()

        details_data = {
            "title"       : self.extract_details_title(product_url_soup),
            "description" : self.extract_details_description(product_url_soup),
            "price"       : self.extract_details_price(product_url_soup),
            "availability": self.extract_details_availability(product_url_soup),
            "image_urls"  : self.extract_details_image_url(product_url_soup),
        }

        # Clean and process each field in details_data if it has a value, otherwise set it to None
        clean_product_detail_title = clean_data.clean_title(details_data['title'])
        logging.debug(f'Pre-clean Title : {details_data['title']}')
        logging.debug(f'Post-clean Title: {clean_product_detail_title}')

        clean_product_detail_description = clean_data.clean_description(details_data['description'])
        logging.debug(f'Pre-clean description : {details_data['description']}')
        logging.debug(f'Post-clean description: {clean_product_detail_description}')

        clean_product_detail_price = clean_data.clean_price(details_data['price'])
        logging.debug(f'Pre-clean price : {details_data['price']}')
        logging.debug(f'Post-clean price: {clean_product_detail_price}')

        clean_product_detail_availability = clean_data.clean_available(details_data['availability'])
        logging.debug(f'Pre-clean availability : {details_data['availability']}')
        logging.debug(f'Post-clean availability: {clean_product_detail_availability}')

        clean_product_detail_image_url_list = clean_data.clean_url_list(details_data['image_urls'])
        logging.debug(f'Pre-clean image_urls : {details_data['image_urls']}')
        logging.debug(f'Post-clean image_urls: {clean_product_detail_image_url_list}')
        time.sleep(5)
        return
    



    def old_product_processor(self, product_url_soup, image_upload_required):



        return


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
        Extract the price of the product.
        """
        config = self.parse_details_config("details_price")
        return self.extract_data(soup, *config)

    def extract_details_availability(self, soup):
        """
        Extract the availability status of the product.
        """
        config = self.parse_details_config("details_availability")
        availability = self.extract_data(soup, *config)
        return availability == "True" if availability is not None else None

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