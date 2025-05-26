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
    def avail_track_main(self, site_profiles):
        """
        Runs availability check across all profiles sharing the same source_name.
        site_profiles = list of site profiles (same source_name)
        """

        # If user didn't select any sites.
        if not site_profiles:
            logging.error("AVAIL TRACKER: No site profiles provided.")
            return

        # Name of the source site.
        source_name = site_profiles[0]['source_name']

        # Keeping a master list for all the urls across all sites.
        all_seen_urls = set()

        # Processing sites based on avail tracking mode in json file.
        for site_profile in site_profiles:
            try:
                mode = site_profile.get("bulk_availability_mode", "tile").lower()

                # This is almost all the websites. Basically finds all urls in their available products pages and if it isn't on there, mark as sold.
                if mode == "tile":
                    logging.info(f"AVAIL TRACKER: TILE MODE - {site_profile['source_name']} [{site_profile.get('json_desc', 'unknown')}]")
                    seen_urls = self._process_tile_mode(site_profile)
                    all_seen_urls.update(seen_urls)

                # This has not been developed and currently has no use.
                elif mode == "last_seen":
                    logging.info(f"AVAIL TRACKER: LAST SEEN MODE - {site_profile['source_name']} [{site_profile.get('json_desc', 'unknown')}]")
                    seen_urls = self._process_last_seen_mode(site_profile)
                    all_seen_urls.update(seen_urls)

                else:
                    raise ValueError(f"Unknown availability mode '{mode}' for site {site_profile['source_name']}")

            except Exception as e:
                logging.error(f"AVAIL TRACKER: Error processing site {site_profile['source_name']}: {e}")

        # After all profiles processed, update DB once
        logging.info(f"AVAIL TRACKER: Total combined seen URLs: {len(all_seen_urls)}")
        # Takes the master list and compares to what is in the DB. If url is in DB but not the master list, mark as sold.
        self.rds_manager.mark_unseen_products_unavailable(source_name, all_seen_urls)     


    def _process_tile_mode(self, site_profile):
        logging.info(f"AVAIL TRACKER: INITIALIZING AVAILABILITY TRACKER FOR {site_profile['source_name']}")

        self.counter.set_continue_state_true()
        self.counter.reset_current_page_count()
        self.counter.reset_empty_page_count()

        site_processor = SiteProcessor(self.managers)

        all_product_tiles = []
        already_marked_sold_urls = set()
        last_page_urls = None
        repeat_page_count = 0

        while self.counter.get_current_continue_state():
            try:
                products_list_page = site_processor.construct_products_list_directory(site_profile)
                logging.info(f'AVAIL TRACKER: Current Site: {site_profile["source_name"]}')
                logging.debug(f'AVAIL TRACKER: products_list_page loaded')
                logging.info(f'AVAIL TRACKER: Current product page: {products_list_page}')
                logging.info(f'AVAIL TRACKER: Current page: {self.counter.get_current_page_count()}')
            except Exception as e:
                logging.error(f"AVAIL TRACKER: products_list_page: {e}")
                continue

            try:
                products_list_page_soup = self.html_manager.parse_html(products_list_page)
                if not products_list_page_soup:
                    logging.warning(f"AVAIL TRACKER: Empty/fetch failed for page: {products_list_page}")
                    self.counter.set_continue_state_false()
                    self.log_print.terminating(
                        source=site_profile["source_name"],
                        consecutiveMatches=self.counter.get_current_page_count(),
                        runCycle=site_profile.get("run_availability_check", False),
                        productsProcessed=self.counter.get_total_count()
                    )
                    break
                logging.debug('AVAIL TRACKER: products_list_page_soup loaded.')
            except Exception as e:
                logging.error(f"AVAIL TRACKER: Error products_list_page_soup: {e}")
                self.counter.set_continue_state_false()
                break

            try:
                products_tile_list = site_processor.construct_products_tile_list(products_list_page_soup, site_profile)
                logging.debug(f'AVAIL TRACKER: Length of products_tile_list: {len(products_tile_list)}')

                if len(products_tile_list) == 0:
                    self.counter.set_continue_state_false()
                    break
            except Exception as e:
                logging.error(f"AVAIL TRACKER: Error products_tile_list: {e}")
                continue

            try:
                tile_processor = TileProcessor(site_profile)
                tile_data = tile_processor.tile_process_main(products_tile_list)

                # ✅ Repeat-page detection moved here (after cleaning)
                current_page_urls = set(p.get("url") for p in tile_data if p.get("url"))
                if last_page_urls is not None and current_page_urls == last_page_urls:
                    repeat_page_count += 1
                    if repeat_page_count >= 2 and self.counter.get_current_page_count() >= 2:
                        prev_page = self.counter.get_current_page_count() - 1
                        logging.warning(f"AVAIL TRACKER: Page {prev_page} and {prev_page + 1} are identical again. Ending pagination.")
                        self.counter.set_continue_state_false()
                        break
                    else:
                        logging.warning(f"AVAIL TRACKER: Detected first repeated page. Continuing scrape with leniency.")
                else:
                    repeat_page_count = 0

                last_page_urls = current_page_urls

                avail_tile_product_data = [p for p in tile_data if p["available"]]
                sold_tile_product_data = [p for p in tile_data if not p["available"]]
                sold_urls = [p["url"] for p in sold_tile_product_data if "url" in p]

                logging.debug(f'AVAIL TRACKER: Available urls count: {len(avail_tile_product_data)}')
                logging.debug(f'AVAIL TRACKER: Sold urls count     : {len(sold_tile_product_data)}')

                for count, url in enumerate(sold_urls, start=1):
                    logging.info(f"AVAIL TRACKER: Sold URL:{count}/{len(sold_urls)} {url}")

                if sold_urls:
                    logging.info(f"AVAIL TRACKER: Updating sold products in DB: {len(sold_urls)}")
                    self.rds_manager.mark_urls_as_sold(sold_urls)
                    already_marked_sold_urls.update(sold_urls)

                all_product_tiles.extend(tile_data)

            except Exception as e:
                logging.error(f"Error processing tile mode: {e}")
                return None

            self.counter.add_current_page_count(
                count=site_profile.get("access_config", {}).get("page_increment_step", 1)
            )


        # 🛡️ SAFETY CHECKS before marking unseen products as sold
        total_seen = len(all_product_tiles)
        current_page_count = self.counter.get_current_page_count()
        source_name = site_profile["source_name"]

        try:
            db_counts_query = """
                SELECT 
                    COUNT(*) FILTER (WHERE available = TRUE)  AS available_count,
                    COUNT(*) FILTER (WHERE available = FALSE) AS sold_count
                FROM militaria
                WHERE site = %s;
            """
            result = self.rds_manager.fetch(db_counts_query, (source_name,))
            db_available, db_sold = result[0] if result else (0, 0)
            total_in_db = db_available + db_sold
        except Exception as e:
            logging.error(f"AVAIL TRACKER: Failed to fetch DB product counts for {source_name}: {e}")
            db_available, db_sold, total_in_db = 0, 0, 0

        if current_page_count < 5:
            logging.critical(f"AVAIL TRACKER: CRITICAL: Only {current_page_count} pages processed for {source_name}. Skipping marking sold to protect database.")
            return

        elif current_page_count < 10:
            logging.warning(f"AVAIL TRACKER: WARNING: Only {current_page_count} pages processed for {source_name}. Proceeding cautiously.")

        if total_seen == 0:
            logging.critical(f"AVAIL TRACKER: CRITICAL: 0 products seen for {source_name}. Skipping marking sold to protect database.")
            return

        scrape_success_rate = total_seen / max(total_in_db, 1)
        if scrape_success_rate < 0.10:
            logging.critical(f"AVAIL TRACKER: CRITICAL: Only {scrape_success_rate:.2%} of products seen for {source_name}. Skipping marking sold to protect database.")
            return

        # 🧹 Calculate unseen URLs
        try:
            db_urls = self.rds_manager.fetch(
                "SELECT url FROM militaria WHERE site = %s AND available = TRUE;", (source_name,)
            )
            db_urls_flat = [row[0] for row in db_urls]
            unseen_urls = list(set(db_urls_flat) - set(p["url"] for p in all_product_tiles if p.get("url")) - already_marked_sold_urls)
            marked_sold_count = len(unseen_urls)
        except Exception as e:
            logging.error(f"AVAIL TRACKER: Error calculating unseen URLs for {source_name}: {e}")
            unseen_urls = []
            marked_sold_count = 0

        # 🔍 Summary
        logging.info(self.log_print.create_log_header("AVAILABILITY SUMMARY"))
        logging.info(f"{source_name:>60}")
        logging.info("📊 AVAILABILITY SUMMARY:")
        logging.info(f"- Products seen this run        : {total_seen}")
        logging.info(f"  ↳ Available                   : {len([p for p in all_product_tiles if p.get('available')])}")
        logging.info(f"  ↳ Sold                        : {len([p for p in all_product_tiles if not p.get('available')])}")
        logging.info(f"- Products in DB for this site  : {total_in_db}")
        logging.info(f"  ↳ Available in DB             : {db_available}")
        logging.info(f"  ↳ Sold in DB                  : {db_sold}")
        logging.info(f"- Newly marked sold this run    : {marked_sold_count}")

        expected_marked_sold = db_available - len([p for p in all_product_tiles if p.get('available')])
        self._log_discrepant_urls(
            unseen_urls=unseen_urls,
            expected_count=expected_marked_sold,
            actual_count=marked_sold_count,
            site_name=source_name
        )

        if unseen_urls:
            logging.info(f"AVAIL TRACKER: Marking {len(unseen_urls)} unseen URLs as sold (not found in this scrape).")
            self.rds_manager.mark_urls_as_sold(unseen_urls)
        else:
            logging.info("AVAIL TRACKER: No unseen products to mark as sold.")

        return set(p["url"] for p in all_product_tiles if p.get("url"))



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
        logging.warning(f"AVAIL TRACKER: Last seen mode is not implemented for {site_profile.get('source_name')}.")
        return set()

