# This will be a refactoring of my old prduct processing module.
from datetime import datetime, date
import logging, json
import time
from urllib.parse import urlparse
from product_tile_processor import TileProcessor
from product_processor import ProductTileDictProcessor, ProductDetailsProcessor


class SiteProcessor:
    def __init__(self,managers):

        self.managers            = managers

        self.rds_manager         = managers.get("rdsManager")
        self.s3_manager          = managers.get("s3_manager")
        self.log_print           = managers.get("log_print")
        self.webScrapeManager    = managers.get('webScrapeManager')
        self.jsonManager         = managers.get('jsonManager')
        self.counter             = managers.get('counter')
        self.html_manager        = managers.get('html_manager')

    def site_processor_main(self, comparison_list, selected_site):
        # site_profile is all the site selectors for one the selected site
        try:
            site_profile = self.jsonManager.json_unpacker(selected_site)
            logging.debug(f'site_profile loaded.')
        except Exception as e:
            logging.error(f"Failed to load site_profile: {e}")

        # Get the base url from the json profile config
        try:
            base_url = self.construct_base_url(selected_site)
            logging.debug(f'Debug: base_url: {base_url}')
        except Exception as e:
            logging.error(f"Error base_url: {e}")

        # Reset configs for new site
        self.counter.set_continue_state_true()
        self.counter.reset_current_page_count()
        self.counter.reset_empty_page_count()

        # Keep looping through pages until current_continue_state becomes False
        while self.counter.get_current_continue_state():
            # Generate a list of products by scraping product urls from store page
            try:
                products_list_page = self.construct_products_list_directory(site_profile)
                logging.debug(f'products_list_page loaded')
            except Exception as e:
                logging.error(f"products_list_page: {e}")
                continue

            # Create beautiful soup for decyphering html / css
            try:
                products_list_page_soup = self.html_manager.parse_html(products_list_page)
                logging.debug(f'Debug: products_list_page_soup loaded.')
            except Exception as e:
                logging.error(f"Error products_list_page_soup: {e}")
                continue

            # Create a list of the product tiles on the given product page
            try:
                products_tile_list = self.construct_products_tile_list(products_list_page_soup,site_profile)
                logging.debug(f'Debug: Length of products_tile_list: {len(products_tile_list)} ')
            
                if not products_tile_list:
                    logging.info("No products found on the current page. Stopping processing.")
                    self.counter.set_continue_state_false()
                    break
            except Exception as e:
                logging.error(f"Error products_tile_list: {e}")
                continue

            # Create and categorize products tile list into urls and separate them by available and not available.
            try:
                tile_processor = TileProcessor(site_profile)
                tile_product_data_list = tile_processor.tile_process_main(products_tile_list)
                self.counter.increment_total_products_count(len(tile_product_data_list))
                logging.info(f'Product Dictionaries count: {len(tile_product_data_list)}')
                logging.info(f'Source: {selected_site['source_name']}')
                # Add a page to go to the next page.
                self.counter.add_current_page_count(
                    count=site_profile.get("access_config", {}).get("page_increment_step", 1)
                )
                logging.info(f"Moved to next page: {self.counter.get_current_page_count()}")
            except Exception as e:
                logging.error(f"Error tile_processor / categorized_product_urls: {e}")
                continue

            # Send the list of products to be checked individually
            try:
                product_tile_dict_processor                           = ProductTileDictProcessor(site_profile, comparison_list, self.managers)
                processing_required_list, availability_update_list    = product_tile_dict_processor.product_tile_dict_processor_main(tile_product_data_list)
            except Exception as e:
                logging.error(f"Error product_tile_dict_processor: {e}")
                continue

            try:
                product_details_processor = ProductDetailsProcessor(site_profile, self.managers, comparison_list)
                output = product_details_processor.product_details_processor_main(processing_required_list)

            except Exception as e:
                logging.error(f"Error product_details_processor: {e}")
                continue

            # Check to see if we have hit multiple empty product pages and possibly terminate
            if self.empty_page_check(processing_required_list, availability_update_list):
                logging.debug("Exiting site_processor_main.")
                break

            time.sleep(10)
            



        return


    ###############FUNCTIONS FOR THE MAIN PART######################




    def construct_base_url(self, site_profile):
        return site_profile['access_config']['base_url']
    
    def construct_products_page_path(self, site_profile):
        return site_profile['access_config']['products_page_path']

    # Sample products_page_directory: shop/page/{page}/
    def construct_products_list_directory(self,site_profile):
        # Create the initial list of products from site's products page.
        try:
            base_url           = self.construct_base_url(site_profile)
            products_page_path = self.construct_products_page_path(site_profile)
            current_page       = self.counter.get_current_page_count()
            return f"{base_url}{products_page_path.format(page=current_page)}"
        except Exception as e:
            logging.warning(f"Error during construct_products_list_directory: {site_profile['source_name']}, Error: {e}")

    # Create a list of all of the product tiles on given page.
    def construct_products_tile_list(self, products_list_page_soup, site_profile):
        try:
            # Extract product selectors from the site profile
            product_selectors = site_profile.get("product_selectors", {})
            
            # Get this site's configurations / selectors to scrape the product tiles
            tiles_config        = product_selectors.get("tiles", {})
            tiles_config_method = tiles_config.get("method", "find_all")
            tiles_config_args   = tiles_config.get("args", [])
            tiles_config_kwargs = tiles_config.get("kwargs", {})
            
            # Create a list of all product tiles
            product_tiles = getattr(products_list_page_soup, tiles_config_method)(*tiles_config_args, **tiles_config_kwargs)
            
            # Filter out tiles that don't contain essential data (e.g., a valid URL or title)
            valid_tiles = [
                tile for tile in product_tiles 
                if self.is_tile_valid(tile, site_profile.get("product_tile_selectors", {}))
            ]
            
            logging.debug(f"Total tiles found: {len(product_tiles)}, Valid tiles: {len(valid_tiles)}")
            return valid_tiles

        except Exception as e:
            logging.error(f"Error constructing product tile list: {e}")
            return []

    def is_tile_valid(self, tile, selectors):
        """
        Check if a product tile contains essential data like URL or title.
        """
        try:
            # Check for URL validity
            url_selector = selectors.get("details_url", {})
            method       = url_selector.get("method", "find")
            args         = url_selector.get("args", [])
            kwargs       = url_selector.get("kwargs", {})
            attribute    = url_selector.get("attribute")
            
            # Attempt to extract URL
            element = getattr(tile, method)(*args, **kwargs)
            url     = element.get(attribute).strip() if element and element.get(attribute) else None
            
            # Ensure at least URL is present for the tile to be valid
            return url is not None

        except Exception as e:
            logging.warning
    
    def empty_page_check(self, processing_required_list, availability_update_list):
        # If the program detects X pages in a row that are empty, stop processing site 
        if len(processing_required_list) == 0 and len(availability_update_list) == 0:
            self.counter.add_empty_page_count()
            self.counter.check_empty_page_tolerance()
            logging.info("No products require further processing. Stopping site processing.")
            logging.info(
    f"""
    ====== Processing Summary ======
    Total Products Count       : {self.counter.get_total_count()}
    New Products Count         : {self.counter.get_new_products_count()}
    Old Products Count         : {self.counter.get_old_products_count()}
    Sites Processed Count      : {self.counter.get_sites_processed_count()}
    Current Page Count         : {self.counter.get_current_page_count()}
    Availability Updates Count : {self.counter.get_availability_update_count()}
    Processing Required Count  : {self.counter.get_processing_required_count()}
    ===============================
    """
)



 