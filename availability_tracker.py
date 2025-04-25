import logging
import post_processors as post_processors
from product_tile_processor import TileProcessor
from site_processor import SiteProcessor
from datetime import datetime
from collections import defaultdict

"""This program is a redesign of the availability tracker to be much leaner.
    I realize that we need the availability and the url for the availbility check.
    Make a list of sold items and 

"""
class SiteAvailabilityTracker:
# This gets us all the urls from the current page of product tiles.
    def __init__(self, managers):
        self.managers            = managers

        self.rds_manager         = managers.get("rdsManager")
        self.s3_manager          = managers.get("s3_manager")
        self.log_print           = managers.get("log_print")
        self.webScrapeManager    = managers.get('webScrapeManager')
        self.jsonManager         = managers.get('jsonManager')
        self.counter             = managers.get('counter')
        self.html_manager        = managers.get('html_manager')

    # Which mode to use for availability processing    
    def avail_track_main(self, site_profile):
        """
        Routes availability processing based on the site's configured mode.
        """
        mode = site_profile.get("bulk_availability_mode", "tile").lower()

        if mode == "tile":
            logging.info("AVAIL TRACKER: TILE MODE")
            return self._process_tile_mode(site_profile)

        elif mode == "last_seen":
            logging.info("AVAIL TRACKER: LAST SEEN MODE")
            return self._process_last_seen_mode(site_profile)

        else:
            raise ValueError(f"Unknown availability mode: {mode}")

    def _process_tile_mode(self, site_profile):
        # Retrieve the product tile list.
        # Reset configs for new site

        logging.info(f"AVAIL TRACKER: INITIALIZING AVAILABILITY TRACKER FOR {site_profile['source_name']}")

        self.counter.set_continue_state_true()
        self.counter.reset_current_page_count()
        self.counter.reset_empty_page_count()

        site_processor = SiteProcessor(self.managers)

        all_product_tiles = []

        # Keep looping through pages until current_continue_state becomes False
        while self.counter.get_current_continue_state():
            # Generate a list of products by scraping product urls from store page
            try:
                products_list_page = site_processor.construct_products_list_directory(site_profile)
                logging.info(f'AVAIL TRACKER: Current Site: {site_profile["source_name"]}')
                logging.debug(f'AVAIL TRACKER: products_list_page loaded')
                logging.info(f'AVAIL TRACKER: Current product page: {products_list_page}')
                logging.info(f'AVAIL TRACKER: Current page: {self.counter.get_current_page_count()}')
            except Exception as e:
                logging.error(f"AVAIL TRACKER: products_list_page: {e}")
                continue


            # Create beautiful soup for the current products list page
            try:
                products_list_page_soup = self.html_manager.parse_html(products_list_page)
                if not products_list_page_soup:
                    logging.warning(f"AVAIL TRACKER: Empty/fetch failed for page: {products_list_page}")
                    self.counter.set_continue_state_false()
                    self.log_print.terminating(
                        source=site_profile["source_name"],
                        consecutiveMatches=self.counter.get_current_page_count(),
                        #targetMatch = targetMatch,
                        runCycle=site_profile.get("run_availability_check", False),
                        productsProcessed=self.counter.get_total_count()
                    )
                    break
                logging.debug(f'AVAIL TRACKER: products_list_page_soup loaded.')
            except Exception as e:
                logging.error(f"AVAIL TRACKER: Error products_list_page_soup: {e}")
                self.counter.set_continue_state_false()
                break



            # This is where the loop will break if the page is empty or has no products.
            # Create a list of the product tiles on the given product page
            try:
                products_tile_list = site_processor.construct_products_tile_list(products_list_page_soup,site_profile)
                logging.debug(f'AVAIL TRACKER: Length of products_tile_list: {len(products_tile_list)} ')
            
                if len(products_tile_list) == 0:
                    current_page   = self.counter.get_current_page_count()
                    total_products = self.counter.get_total_count()
                    total_seen     = len(all_product_tiles)
                    total_sold     = len([p for p in all_product_tiles if not p["available"]])
                    total_available= len([p for p in all_product_tiles if p["available"]])

                    logging.info(f"AVAIL TRACKER: Current page            : {current_page}")
                    logging.info(f"AVAIL TRACKER: Total pages processed   : {current_page}")
                    logging.info(f"AVAIL TRACKER: Total products processed: {total_products}")
                    logging.info(f"AVAIL TRACKER: Total products seen     : {total_seen}")
                    logging.info(f"AVAIL TRACKER: Total products sold     : {total_sold}")
                    logging.info(f"AVAIL TRACKER: Total products available: {total_available}")
                    self.counter.set_continue_state_false()
                    break
            except Exception as e:
                logging.error(f"AVAIL TRACKER: Error products_tile_list: {e}")
                continue





            # This breaks down the products page / tiles into a list of products.
            try:
                tile_processor = TileProcessor(site_profile)
                tile_data = tile_processor.tile_process_main(products_tile_list)

                # Not sure if this is needed but it is here for now.
                avail_tile_product_data = [p for p in tile_data if p["available"]]
                logging.debug(f'AVAIL TRACKER: Available urls count: {len(avail_tile_product_data)} ')

                # This part updates any products that have been sold to in the database column 'available' to False.
                sold_tile_product_data = [p for p in tile_data if not p["available"]]
                logging.debug(f'AVAIL TRACKER: Sold urls count     : {len(sold_tile_product_data)} ')
                sold_urls = [p["url"] for p in sold_tile_product_data if "url" in p]
                for count, url in enumerate(sold_urls, start=1):
                    logging.info(f"AVAIL TRACKER:  Sold URL:{count}/{len(sold_urls)} {url}")

                # Calculate ratio of sold products on this page
                unseen_ratio = len(sold_urls) / max(len(all_product_tiles), 1)

                if unseen_ratio > 0.10:
                    logging.warning("🚨 Abnormally high number of sold products detected.")
                    logging.warning(f"Total on this page      : {len(total_available + total_sold)}")
                    logging.warning(f"Marked as sold on page  : {len(sold_urls)}")
                    logging.warning("⚠️  Listing sold URLs:")
                    for url in sold_urls[:20]:  # limit to first 20 for readability
                        logging.warning(f"  → {url}")
                    if len(sold_urls) > 20:
                        logging.warning(f"...and {len(sold_urls) - 20} more")

                # ✅ Always update the DB
                logging.info(f"AVAIL TRACKER: Updating sold products in DB: {len(sold_urls)}")
                self.rds_manager.mark_urls_as_sold(sold_urls)



                # Add all the urls / availability to the list of all product tiles (global variable).
                all_product_tiles.extend(tile_data)
                logging.debug(f'AVAIL TRACKER: Length of all_product_tiles: {len(all_product_tiles)} ')



            except Exception as e:
                logging.error(f"Error processing tile mode: {e}")
                return None
            
            self.counter.add_current_page_count(
                count=site_profile.get("access_config", {}).get("page_increment_step", 1)
            )

        # This prints out samples of the first and last 10 products seen.
        # This is useful for debugging and checking the availability of products.
        sample_size = 10

        logging.info("🔍 Sample of first 10 products seen:")
        for p in all_product_tiles[:sample_size]:
            logging.info(f"- {p.get('url')} | Available: {p.get('available')} | Title: {p.get('title')}")

        logging.info("🔍 Sample of last 10 products seen:")
        for p in all_product_tiles[-sample_size:]:
            logging.info(f"- {p.get('url')} | Available: {p.get('available')} | Title: {p.get('title')}")


        # Seen in current scrape
        total_seen         = len(all_product_tiles)
        seen_available     = len([p for p in all_product_tiles if p.get("available")])
        seen_sold          = len([p for p in all_product_tiles if not p.get("available")])
        all_tile_urls      = [p["url"] for p in all_product_tiles if "url" in p]

        # Count of existing DB products by availability
        try:
            source_name = site_profile["source_name"]
            db_counts_query = """
                SELECT 
                    COUNT(*) FILTER (WHERE available = TRUE)  AS available_count,
                    COUNT(*) FILTER (WHERE available = FALSE) AS sold_count
                FROM militaria
                WHERE site = %s;
            """
            result = self.rds_manager.fetch(db_counts_query, (source_name,))
            db_available, db_sold = result[0] if result else (0, 0)
        except Exception as e:
            logging.error(f"AVAIL TRACKER: Failed to fetch DB product counts for {source_name}: {e}")
            db_available, db_sold = 0, 0

        # Compute newly marked-as-sold URLs
        try:
            db_urls = self.rds_manager.fetch(
                "SELECT url FROM militaria WHERE site = %s AND available = TRUE;", (source_name,)
            )
            db_urls_flat = [row[0] for row in db_urls]
            unseen_urls = list(set(db_urls_flat) - set(all_tile_urls))
            marked_sold_count = len(unseen_urls)
        except Exception as e:
            logging.error(f"AVAIL TRACKER: Error calculating unseen URLs for {source_name}: {e}")
            unseen_urls = []
            marked_sold_count = 0

        # Log availability summary
        logging.info(self.log_print.create_log_header("AVAILABILITY SUMMARY"))
        logging.info(f"{site_profile['source_name']:>60}")
        logging.info("📊 AVAILABILITY SUMMARY:")
        logging.info(f"- Products seen this run        : {total_seen}")
        logging.info(f"  ↳ Available                   : {seen_available}")
        logging.info(f"  ↳ Sold                        : {seen_sold}")
        logging.info(f"- Products in DB for this site  : {db_available + db_sold}")
        logging.info(f"  ↳ Available in DB             : {db_available}")
        logging.info(f"  ↳ Sold in DB                  : {db_sold}")
        logging.info(f"- Newly marked sold this run    : {marked_sold_count}")

        # Check for discrepancy and log detailed mismatch
        expected_marked_sold = db_available - seen_available
        self._log_discrepant_urls(
            unseen_urls=unseen_urls,
            expected_count=expected_marked_sold,
            actual_count=marked_sold_count,
            site_name=site_profile["source_name"]
)
        # Log URLs that were marked as sold but not seen in the current scrape
        if unseen_urls:
            logging.info(f"AVAIL TRACKER: Marking {len(unseen_urls)} unseen URLs as sold (not found in this scrape).")
            self.rds_manager.mark_urls_as_sold(unseen_urls)
        else:
            logging.info("AVAIL TRACKER: No unseen products to mark as sold.")






    def _log_discrepant_urls(self, unseen_urls, expected_count, actual_count, site_name):
        discrepancy = actual_count - expected_count

        if discrepancy == 0:
            return  # No mismatch, no need to log

        # Header
        logging.warning(self.log_print.create_log_header("⚠️ AVAILABILITY DISCREPANCY DETECTED ⚠️"))
        logging.warning(f"{site_name:>60}")
        logging.warning(f"Expected to mark {expected_count} as sold based on DB and seen products.")
        logging.warning(f"Actually marked {actual_count} as sold.")
        logging.warning(f"Discrepancy = {discrepancy:+}")

        # Log each unseen URL
        logging.warning(f"🔍 Discrepant URLs not seen in scrape but marked sold:")
        for url in unseen_urls:
            logging.warning(f"  → {url}")



    # Does this make sense when tile mode is used?
    def _process_last_seen_mode(self, site_profile):
        return


