# This is going to the program used to track the availability of products. How it is checked is dependant on the site.

import logging
from datetime import datetime, timezone
from product_tile_processor import TileUrlCollector  # make sure this import exists

class AvailabilityTracker:
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
    def avail_check_main(self, site_profile):
        """
        Routes availability processing based on the site's configured mode.
        """
        mode = site_profile.get("bulk_availability_mode", "tile").lower()

        if mode == "tile":
            return self._process_tile_mode(site_profile)

        elif mode == "last_seen":
            return self._process_last_seen_mode(site_profile)

        else:
            raise ValueError(f"Unknown availability mode: {mode}")


    # Process the last seen mode: Use Case: Sites where sold items are archived or removed after being sold.
    def _process_tile_mode(self, site_profile):
        try:
            seen_urls = set()
            self.counter.reset_current_page_count()
            self.counter.set_continue_state_true()

            # ✅ Use TileUrlCollector instead of full TileProcessor
            tile_url_collector = TileUrlCollector(site_profile)

            while self.counter.get_current_continue_state():
                page_path = site_profile['access_config']['products_page_path']
                base_url = site_profile['access_config']['base_url']
                page_number = self.counter.get_current_page_count()
                url = f"{base_url}{page_path.format(page=page_number)}"

                self.log_print.create_log_header(f"AVAILABILITY: Fetching page {url}")
                soup = self.html_manager.parse_html(url)

                if not soup:
                    self.counter.set_continue_state_false()
                    break

                try:
                    page_urls = tile_url_collector.extract_all_tile_urls(soup)
                    logging.debug(f"AVAIL TRACKER: Extracted {len(page_urls)} tile URLs")

                    if not page_urls:
                        logging.info("AVAIL TRACKER: No valid product URLs on current page. Ending availability check.")
                        self.log_print.terminating(
                            source=site_profile["source_name"],
                            consecutiveMatches=self.counter.get_current_page_count(),
                            targetMatch=self.counter.get_current_page_count(),  # same value as fallback
                            runCycle=site_profile.get("run_availability_check", False),
                            productsProcessed=self.counter.get_total_count()
                        )
                        self.counter.set_continue_state_false()
                        break

                    seen_urls.update(page_urls)

                except Exception as e:
                    logging.error(f"AVAIL TRACKER: Error extracting tile URLs: {e}")
                    self.counter.set_continue_state_false()
                    break


                self.counter.add_current_page_count(
                    site_profile.get("access_config", {}).get("page_increment_step", 1)
                )

            self._mark_unseen_products_unavailable(site_profile['source_name'], seen_urls)

        except Exception as e:
            logging.error(f"AVAIL TRACKER: Error in tile mode processing → {e}")



    # This just uses rds / postgresql to mark all products not seen as unavailable.
    def _mark_unseen_products_unavailable(self, source_name, seen_urls):
        """
        Marks all products from this site that were NOT seen in the current scrape as unavailable.
        The 'date_sold' field will be updated by a database trigger when 'available' becomes FALSE.
        """
        try:
            if not seen_urls:
                logging.warning("AVAIL TRACKER: No seen URLs — skipping availability update.")
                return

            unseen_urls_placeholder = ','.join(['%s'] * len(seen_urls))
            params = [source_name] + list(seen_urls)

            query = f"""
                UPDATE militaria
                SET available = FALSE
                WHERE site = %s AND url NOT IN ({unseen_urls_placeholder}) AND available = TRUE;
            """

            self.rds_manager.execute(query, params)
            logging.info(f"AVAIL TRACKER: Marked unseen products unavailable for site: {source_name}")

        except Exception as e:
            logging.error(f"AVAIL TRACKER: Error marking unseen products unavailable: {e}")


    def _construct_products_tile_list(self, soup):
        """
        Extracts valid product tiles from the soup.
        Applies URL and title validation, and removes duplicates.
        """
        try:
            tile_selectors = self.tile_processor.site_profile.get("product_tile_selectors", {})
            tiles_config = tile_selectors.get("tiles", {})
            method = tiles_config.get("method", "find_all")
            args = tiles_config.get("args", [])
            kwargs = tiles_config.get("kwargs", {})

            product_tiles = getattr(soup, method)(*args, **kwargs)
            logging.debug(f"AVAIL TRACKER: Raw tiles extracted from soup: {len(product_tiles)}")

            seen_urls = set()
            valid_tiles = []

            for idx, tile in enumerate(product_tiles):
                raw_url = self.tile_processor.extract_tile_url(tile)
                if not raw_url:
                    logging.debug(f"AVAIL TRACKER: Tile {idx} skipped — no URL found")
                    continue

                if raw_url in seen_urls:
                    logging.debug(f"AVAIL TRACKER: Tile {idx} skipped — duplicate URL → {raw_url}")
                    continue

                title_ok = self.tile_processor.extract_tile_title(tile)
                if title_ok:
                    seen_urls.add(raw_url)
                    valid_tiles.append(tile)
                    logging.debug(f"AVAIL TRACKER: Tile {idx} accepted — URL: {raw_url}")
                else:
                    logging.debug(f"AVAIL TRACKER: Tile {idx} skipped — no valid title for URL: {raw_url}")

            logging.debug(f"AVAIL TRACKER: Total tiles found: {len(product_tiles)}, Valid tiles: {len(valid_tiles)}")
            return valid_tiles

        except Exception as e:
            logging.error(f"AVAIL TRACKER: Error in construct_products_tile_list: {e}")
            return []



    def _expire_old_last_seen(self, site_name, timestamp_now):
        try:
            query = """
                UPDATE militaria
                SET available = FALSE
                WHERE site = %s AND (last_seen IS NULL OR last_seen < %s) AND available = TRUE;
            """
            self.rds_manager.execute(query, (site_name, timestamp_now))
            logging.info(f"AVAIL TRACKER: Marked expired products unavailable for site: {site_name}")
        except Exception as e:
            logging.error(f"AVAIL TRACKER: Error expiring old products for {site_name}: {e}")


    def _process_last_seen_mode(self, site_profile):
        try:
            self.counter.reset_current_page_count()
            self.counter.set_continue_state_true()

            self.tile_processor = TileProcessor(site_profile)  # ✅ Reuse this instance
            now_timestamp = datetime.now(timezone.utc)

            while self.counter.get_current_continue_state():
                page_path = site_profile['access_config']['products_page_path']
                base_url = site_profile['access_config']['base_url']
                page_number = self.counter.get_current_page_count()
                url = f"{base_url}{page_path.format(page=page_number)}"

                logging.info(f"AVAIL TRACKER: Fetching page: {url}")
                soup = self.html_manager.parse_html(url)

                if not soup:
                    self.counter.set_continue_state_false()
                    break

                try:
                    tiles = self._construct_products_tile_list(soup)  # ✅ No site_profile param
                    logging.debug(f"AVAIL TRACKER: Found {len(tiles)} tiles")

                    if not tiles:
                        logging.info("AVAIL TRACKER: No valid tiles found on current page. Ending last_seen pass.")
                        self.log_print.terminating(
                            source=site_profile["source_name"],
                            consecutiveMatches=self.counter.get_current_page_count(),
                            targetMatch=self.counter.get_current_page_count(),
                            runCycle=site_profile.get("run_availability_check", False),
                            productsProcessed=self.counter.get_total_count()
                        )
                        self.counter.set_continue_state_false()
                        break

                    tile_data = self.tile_processor.tile_process_main(tiles)

                    for product in tile_data:
                        url = product["url"]
                        update_query = """
                            UPDATE militaria
                            SET last_seen = %s
                            WHERE url = %s AND site = %s;
                        """
                        self.rds_manager.execute(update_query, (now_timestamp, url, site_profile["source_name"]))

                    self.counter.add_current_page_count(
                        site_profile.get("access_config", {}).get("page_increment_step", 1)
                    )

                except Exception as e:
                    logging.error(f"AVAIL TRACKER: Error processing tiles: {e}")
                    self.counter.set_continue_state_false()
                    break

            self._expire_old_last_seen(site_profile["source_name"], now_timestamp)

        except Exception as e:
            logging.error(f"AVAIL TRACKER: Error in last_seen mode: {e}")

