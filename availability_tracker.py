# This is going to the program used to track the availability of products. How it is checked is dependant on the site.

from product_tile_processor import TileProcessor
import logging

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

            tile_processor = TileProcessor(site_profile)

            while self.counter.get_current_continue_state():
                # Build current page URL
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
                    tiles = self._construct_products_tile_list(soup, site_profile)
                    logging.debug(f"AVAIL TRACKER: Tile count → {len(tiles)}")

                    if len(tiles) == 0:
                        logging.info("AVAIL TRACKER: No products found on current page. Ending scraping.")
                        self.log_print.terminating(
                            source=site_profile["source_name"],
                            consecutiveMatches=self.counter.get_current_page_count(),
                            targetMatch=self.counter.get_current_page_count(),  # reuse count as target here
                            runCycle=site_profile.get("run_availability_check", False),
                            productsProcessed=self.counter.get_total_count()
                        )
                        self.counter.set_continue_state_false()
                        break

                except Exception as e:
                    logging.error(f"AVAIL TRACKER: Error in construct_products_tile_list: {e}")
                    self.counter.set_continue_state_false()
                    break

                tile_data = tile_processor.tile_process_main(tiles)
                for product in tile_data:
                    seen_urls.add(product["url"])

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
        """
        try:
            query = """
                UPDATE militaria
                SET available = FALSE
                WHERE site = %s AND url NOT IN %s AND available = TRUE;
            """
            self.rds_manager.execute(query, (source_name, tuple(seen_urls)))
            logging.info(f"AVAIL TRACKER: Marked unseen products as unavailable for site: {source_name}")
        except Exception as e:
            logging.error(f"AVAIL TRACKER: Error marking unseen products unavailable: {e}")

    def _construct_products_tile_list(self, soup, site_profile):
        try:
            product_selectors = site_profile.get("product_selectors", {})
            tile_selectors = site_profile.get("product_tile_selectors", {})

            tiles_config = product_selectors.get("tiles", {})
            method = tiles_config.get("method", "find_all")
            args = tiles_config.get("args", [])
            kwargs = tiles_config.get("kwargs", {})

            product_tiles = getattr(soup, method)(*args, **kwargs)

            tile_processor = self.tile_processor or TileProcessor(site_profile)
            seen_urls = set()
            valid_tiles = []

            for tile in product_tiles:
                raw_url = tile_processor.extract_tile_url(tile)
                if not raw_url:
                    continue
                if raw_url in seen_urls:
                    continue
                if tile_processor.extract_tile_title(tile):
                    seen_urls.add(raw_url)
                    valid_tiles.append(tile)

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
