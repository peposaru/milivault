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

    def site_processor_main(self, comparison_list, selected_site, targetMatch, use_comparison_row):
        # site_profile is all the site selectors for one the selected site
        try:
            site_profile = self.jsonManager.json_unpacker(selected_site)   
        except Exception as e:
            logging.error(f"SITE PROCESSOR: Failed to load site_profile: {e}")

        # Load the targetMatch
        try:
            logging.info(f"SITE PROCESSOR: Running with targetMatch = {targetMatch}")
        except Exception as e:
            logging.error(f"SITE PROCESSOR: Failed to load targetMatch: {e}")

        # Get the base url from the json profile config
        try:
            base_url = self.construct_base_url(selected_site)
            logging.debug(f'SITE PROCESSOR: Debug: base_url: {base_url}')
        except Exception as e:
            logging.error(f"SITE PROCESSOR: Error base_url: {e}")

        self.log_print.newInstance(
            source=site_profile["source_name"],
            productsPage=base_url,
            runCycle=site_profile.get("run_availability_check", False),
            productsProcessed=self.counter.get_total_count()
            ) 
        
        # Reset configs for new site
        self.counter.set_continue_state_true()
        self.counter.reset_current_page_count()
        self.counter.reset_empty_page_count()

        # Keep looping through pages until current_continue_state becomes False
        while self.counter.get_current_continue_state():
            # Generate a list of products by scraping product urls from store page
            try:
                products_list_page = self.construct_products_list_directory(site_profile)
                logging.debug(f'SITE PROCESSOR: products_list_page loaded')
                logging.info(f'SITE PROCESSOR: Current product page: {products_list_page}')
                logging.info(f'SITE PROCESSOR: Current page: {self.counter.get_current_page_count()}')
            except Exception as e:
                logging.error(f"SITE PROCESSOR: products_list_page: {e}")
                continue

            # Create beautiful soup for deciphering html / css
            try:
                products_list_page_soup = self.html_manager.parse_html(products_list_page)
                if not products_list_page_soup:
                    logging.warning(f"SITE PROCESSOR: Empty/fetch failed for page: {products_list_page}")
                    self.counter.set_continue_state_false()
                    self.log_print.terminating(
                        source=site_profile["source_name"],
                        consecutiveMatches=self.counter.get_current_page_count(),
                        targetMatch = targetMatch,
                        runCycle=site_profile.get("run_availability_check", False),
                        productsProcessed=self.counter.get_total_count()
                    )
                    break
                logging.debug(f'SITE PROCESSOR: products_list_page_soup loaded.')
            except Exception as e:
                logging.error(f"SITE PROCESSOR: Error products_list_page_soup: {e}")
                self.counter.set_continue_state_false()
                break

            # This is where the loop will break if the page is empty or has no products.
            # Create a list of the product tiles on the given product page
            try:
                products_tile_list = self.construct_products_tile_list(products_list_page_soup,site_profile)
                logging.debug(f'SITE PROCESSOR: Length of products_tile_list: {len(products_tile_list)} ')
            
                if len(products_tile_list) == 0:
                    logging.info("SITE PROCESSOR: No products found on the current page. Stopping processing.")
                    self.log_print.terminating(
                        source=site_profile["source_name"],
                        consecutiveMatches=self.counter.get_current_page_count(),
                        targetMatch = targetMatch,
                        runCycle=site_profile.get("run_availability_check", False),
                        productsProcessed=self.counter.get_total_count()
                    )
                    self.counter.set_continue_state_false()
                    break
            except Exception as e:
                logging.error(f"SITE PROCESSOR: Error products_tile_list: {e}")
                continue

            # Create and categorize products tile list into urls and separate them by available and not available.
            try:
                tile_processor = TileProcessor(site_profile)
                tile_product_data_list = tile_processor.tile_process_main(products_tile_list)
                self.counter.increment_total_products_count(len(tile_product_data_list))
                logging.info(f'SITE PROCESSOR: Product Dictionaries count: {len(tile_product_data_list)}')
            except Exception as e:
                logging.error(f"SITE PROCESSOR: Error tile_processor / categorized_product_urls: {e}")
                continue

            # Send the list of products to be checked individually
            try:
                product_tile_dict_processor = ProductTileDictProcessor(
                    site_profile,
                    comparison_list,
                    self.managers,
                    use_comparison_row=use_comparison_row
                )
                processing_required_list, availability_update_list    = product_tile_dict_processor.product_tile_dict_processor_main(tile_product_data_list)
            except Exception as e:
                logging.error(f"SITE PROCESSOR: Error product_tile_dict_processor: {e}")
                continue

            try:
                product_details_processor = ProductDetailsProcessor(site_profile, self.managers, comparison_list, use_comparison_row)
                # I don't remember why I made output but maybe it will come to me later.
                output = product_details_processor.product_details_processor_main(processing_required_list)

            except Exception as e:
                logging.error(f"SITE PROCESSOR: Error product_details_processor: {e}")
                continue

            # Add a page to go to the next page.
            try:
                self.counter.add_current_page_count(
                    count=site_profile.get("access_config", {}).get("page_increment_step", 1)
                )
                logging.info(f"SITE PROCESSOR: Moved to next page: {self.counter.get_current_page_count()}")
            except Exception as e:
                logging.error(f'SITE PROCESSOR: Failed to add to page count.')


            # Check to see if we have hit multiple empty product pages and possibly terminate
            if self.empty_page_check(processing_required_list, availability_update_list, targetMatch):
                logging.debug("SITE PROCESSOR: Exiting site_processor_main.")
                break
                
        logging.info(f"SITE PROCESSOR: Finished processing site: {site_profile['source_name']}")
        self.log_print.terminating(
            source=site_profile["source_name"],
            consecutiveMatches=self.counter.get_current_page_count(),
            targetMatch=site_profile.get("targetMatch", 1),
            runCycle=site_profile.get("run_availability_check", False),
            productsProcessed=self.counter.get_total_count()
        )

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
            logging.warning(f"SITE PROCESSOR: Error during construct_products_list_directory: {site_profile['source_name']}, Error: {e}")

    def construct_products_tile_list(self, products_list_page_soup, site_profile):
        try:
            # Extract the correct selectors
            tile_selectors = site_profile.get("product_tile_selectors", {})
            tiles_config = tile_selectors.get("tiles", {})

            # Extract config parameters
            tiles_config_method = tiles_config.get("method", "find_all")
            tiles_config_args = tiles_config.get("args", [])
            tiles_config_kwargs = tiles_config.get("kwargs", {})

            # Defensive check for missing tile selector args
            if not tiles_config_args or tiles_config_method != "find_all":
                logging.warning("TILE EXTRACTOR: Missing or invalid tiles config — skipping tile extraction.")
                return []

            # Extract tiles from the soup
            product_tiles = getattr(products_list_page_soup, tiles_config_method)(
                *tiles_config_args, **tiles_config_kwargs
            )

            tile_processor = TileProcessor(site_profile)
            seen_urls = set()
            valid_tiles = []

            for tile in product_tiles:
                raw_url = tile_processor.extract_tile_url(tile)
                if not raw_url:
                    continue

                if raw_url in seen_urls:
                    logging.debug(f"SITE PROCESSOR: Skipping duplicate tile URL → {raw_url}")
                    continue

                if tile_processor.extract_tile_title(tile):
                    seen_urls.add(raw_url)
                    valid_tiles.append(tile)

            logging.debug(f"SITE PROCESSOR: Total tiles found: {len(product_tiles)}, Valid tiles: {len(valid_tiles)}")
            return valid_tiles

        except Exception as e:
            logging.error(f"SITE PROCESSOR: Error constructing product tile list: {e}")
            return []



    def is_tile_valid(self, tile, selectors):
        try:
            url_selector = selectors.get("details_url", {})
            method       = url_selector.get("method", "find")
            args         = url_selector.get("args", [])
            kwargs       = url_selector.get("kwargs", {})
            submethod    = url_selector.get("submethod", None)
            attribute    = url_selector.get("attribute")

            # Step 1: find base element
            element = getattr(tile, method)(*args, **kwargs)

            # Step 2: apply submethod if defined
            if element and submethod:
                sub_meth     = submethod.get("method", "find")
                sub_args     = submethod.get("args", [])
                sub_kwargs   = submethod.get("kwargs", {})
                attribute    = submethod.get("attribute", attribute)  # ⚠️ Make sure this line is here

                element = getattr(element, sub_meth)(*sub_args, **sub_kwargs) if element else None

            # Step 3: extract href (or other attribute)
            url = element.get(attribute).strip() if element and attribute and element.get(attribute) else None

            return url is not None

        except Exception as e:
            logging.warning(f"is_tile_valid failed: {e}")
            return False


    
    def empty_page_check(self, processing_required_list, availability_update_list, targetMatch):
        if len(processing_required_list) == 0 and len(availability_update_list) == 0:
            self.counter.add_empty_page_count()

            if self.counter.get_empty_page_count() >= targetMatch:
                self.counter.set_continue_state_false()
                logging.info("SITE PROCESSOR: No products require further processing. Stopping site processing.")
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
                return True  # ← this is important for the caller to `break`
        return False



 