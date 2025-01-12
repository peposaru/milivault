# This will be a refactoring of my old prduct processing module.
from datetime import datetime, date
import logging, json
import time
from urllib.parse import urlparse
from product_tile_processor import TileProcessor


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

    def site_processor_main(self,selected_site):

        # site_profile is all the site selectors for one the selected site
        try:
            site_profile = self.jsonManager.json_unpacker(selected_site)
            logging.debug(f'Debug: site_profile: {site_profile}')
        except Exception as e:
            logging.error(f"Error site_profile: {e}")

        # Get the base url from the json profile config
        try:
            base_url = self.construct_base_url(selected_site)
            logging.debug(f'Debug: base_url: {base_url}\n\n\n')
        except Exception as e:
            logging.error(f"Error base_url: {e}")

        # Generate a list of products by scraping product urls from store page
        try:
            products_list_page = self.construct_products_list_directory(site_profile)
            logging.debug(f'Debug: products_list_page: {products_list_page}\n\n\n')
        except Exception as e:
            logging.error(f"Error products_list_page: {e}")

        # Create beautiful soup for decyphering html / css
        try:
            products_list_page_soup = self.html_manager.construct_products_page_list_soup(products_list_page)
            logging.debug(f'Debug: products_list_page_soup: {products_list_page_soup}\n\n\n')
        except Exception as e:
            logging.error(f"Error products_list_page_soup: {e}")
            
        # Create a list of the product tiles on the given product page
        try:
            products_tile_list = self.construct_products_tile_list(products_list_page_soup,site_profile)
            logging.debug(f'Debug: products_tile_list: {products_tile_list}\n\n\n')
        except Exception as e:
            logging.error(f"Error products_tile_list: {e}")

            # Create and categorize products tile list into urls and separate them by available and not available.
            """
            THIS IS WHERE THERE NEEDS TO BE A CHECK ON THE PRODUCTS PAGE TILES
            CHECK EACH TILE SOUP FOR CHANGE IN AVAILABILITY

            """
        try:
            tile_processor = TileProcessor(site_profile)
            categorized_product_urls = tile_processor.construct_categorized_product_urls(products_tile_list)
            logging.debug(f'Debug: categorized_product_urls: {categorized_product_urls}\n\n\n')
        except Exception as e:
            logging.error(f"Error tile_processor / categorized_product_urls: {e}")


            # Check each url in RDS database to see if product already exists
            # Check each title and description in RDS to see if product already exists
            # Check products list page to check availability

        

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
            return getattr(products_list_page_soup, tiles_config_method)(*tiles_config_args, **tiles_config_kwargs)

        except Exception as e:
            logging.error(f"Error constructing product tile list: {e}")
            return []

    def construct_categorized_product_urls(self, products_tile_list, site_profile):
        categorized_urls = {"available": [], "unavailable": []}

        try:
            product_tile_selectors = site_profile.get("product_tile_selectors", {})
            details_url         = product_tile_selectors.get('details_url')
            tile_availability   = product_tile_selectors.get('tile_availability')
            tile_unavailability = product_tile_selectors.get('tile_unavailability')
            tile_image_url      = product_tile_selectors.get('tile_image_url')
        except Exception as e:
            logging.error(f"Error product_tile_selectors: {e}")
    

    def check_rds_url():
        return

    def check_rds_title_desc():
        return




 