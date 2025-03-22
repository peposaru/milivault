import json
import logging
from bs4 import BeautifulSoup
from logging_manager import initialize_logging


class JsonTester:
    def __init__(self, managers):
        self.managers = managers
        self.rds_manager = managers.get("rdsManager")
        self.s3_manager = managers.get("s3_manager")
        self.log_print = managers.get("log_print")
        self.webScrapeManager = managers.get('webScrapeManager')
        self.jsonManager = managers.get('jsonManager')
        self.counter = managers.get('counter')
        self.html_manager = managers.get('html_manager')

    def load_site_profile(self, json_file):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"JSON TESTER: Error loading JSON file {json_file}: {e}")
            return None

    def fetch_webpage(self, url):
        try:
            response = self.html_manager.fetch_url(url)
            if response:
                logging.info(f"Successfully fetched HTML from: {url}")
                return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            logging.error(f"JSON TESTER: Failed to fetch {url}: {e}")
        return None

    def extract_data(self, soup, selector_config):
        try:
            method = selector_config.get("method", "find")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            element = getattr(soup, method)(*args, **kwargs)
            if element:
                return element.get(attribute, "").strip() if attribute else element.get_text(strip=True)
            return None
        except Exception as e:
            logging.error(f"JSON TESTER: Error extracting data: {e}")
            return None

    def test_site_profile(self, products_page_url, product_details_url, site_profile):
        if not site_profile:
            logging.error("JSON TESTER: Invalid site profile received. Skipping.")
            return

        logging.info(f"🔍 Testing JSON Profile: {site_profile.get('source_name')}")
        logging.info(f"📄 Products Page URL: {products_page_url}")
        logging.info(f"📄 Product Details URL: {product_details_url}")

        # ========== TILE SECTION ==========
        products_soup = self.fetch_webpage(products_page_url)
        if not products_soup:
            logging.error("❌ Could not parse products page HTML.")
            return

        tile_selectors = site_profile.get("product_tile_selectors", {})
        tile_tag = tile_selectors.get("tiles", {}).get("args", [])[0] if tile_selectors.get("tiles") else None
        tiles = products_soup.select(tile_tag) if tile_tag else []

        logging.info(f"🧱 Tiles Found: {len(tiles)} using selector: {tile_tag}")
        extracted_tiles = []

        for tile in tiles:
            tile_data = {
                "url": self.extract_data(tile, tile_selectors.get("details_url", {})),
                "title": self.extract_data(tile, tile_selectors.get("tile_title", {})),
                "price": self.extract_data(tile, tile_selectors.get("tile_price", {})),
                "available": self.extract_data(tile, tile_selectors.get("tile_availability", {}))
            }

            if tile_data["url"] and tile_data["title"]:
                extracted_tiles.append(tile_data)
                logging.info(f"""
                ===== TILE EXTRACTED =====
                URL         : {tile_data['url']}
                Title       : {tile_data['title']}
                Price       : {tile_data['price']}
                Availability: {tile_data['available']}
                ==========================
                """)

        if not extracted_tiles:
            logging.warning("⚠️ No valid tiles extracted (missing URL/title).")

        # ========== DETAILS SECTION ==========
        product_soup = self.fetch_webpage(product_details_url)
        if not product_soup:
            logging.error("❌ Could not parse product details page HTML.")
            return

        details = site_profile.get("product_details_selectors", {})
        details_data = {"url": product_details_url}

        for key, selector in details.items():
            try:
                value = self.extract_data(product_soup, selector)
                details_data[key] = value
                if not value:
                    logging.warning(f"🔸 Field '{key}' returned empty.")
            except Exception as e:
                logging.error(f"Error extracting '{key}': {e}")

        logging.info(f"""
        ====== PRODUCT DETAILS EXTRACTED ======
        URL              : {details_data.get('url')}
        Title            : {details_data.get('details_title')}
        Description      : {details_data.get('details_description')}
        Price            : {details_data.get('details_price')}
        Availability     : {details_data.get('details_availability')}
        ID               : {details_data.get('details_extracted_id')}
        Item Type        : {details_data.get('details_item_type')}
        Image URLs       : {str(details_data.get('details_image_url'))[:250]}...
        =======================================
        """)

    def main(self, selected_sites):
        initialize_logging()

        if not selected_sites:
            logging.error("JSON TESTER: No sites selected. Exiting.")
            return

        for site in selected_sites:
            try:
                site_profile = self.jsonManager.json_unpacker(site)
                logging.debug(f"JSON TESTER: Loaded profile for {site['source_name']}")
            except Exception as e:
                logging.error(f"JSON TESTER: Failed to load profile: {e}")
                continue

            products_page_url = 'https://militariaplaza.nl/new'
            product_details_url = 'https://militariaplaza.nl/new/large-wehrmacht-slide-projector-detail'
            self.test_site_profile(products_page_url, product_details_url, site_profile)
