import json
import os
import shutil
import logging
from bs4 import BeautifulSoup
from html_manager import HtmlManager
from logging_manager import initialize_logging

class JsonTester:
    """Class to test JSON profiles by extracting data from manually entered URLs."""
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
        """Loads a JSON profile from file."""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"JSON TESTER: Error loading JSON file {json_file}: {e}")
            return None

    def fetch_webpage(self, url):
        """Fetches and parses HTML content from the given URL."""
        response = self.html_manager.fetch_url(url)
        if response:
            return BeautifulSoup(response.text, 'html.parser')
        logging.error(f"JSON TESTER: Failed to fetch {url}")
        return None

    def extract_data(self, soup, selector_config):
        """Extracts data based on method, args, kwargs, and post-processing."""
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
        """Tests the selected JSON profile by extracting data from manually entered URLs."""
        if not site_profile:
            logging.error("JSON TESTER: Invalid site profile received. Skipping.")
            return

        logging.info(f"JSON TESTER: Using Products Page URL: {products_page_url}")
        logging.info(f"JSON TESTER: Using Product Details URL: {product_details_url}")

        # Fetch and parse products page
        products_soup = self.fetch_webpage(products_page_url)
        if not products_soup:
            return

        # Extract tile data
        tile_selectors = site_profile.get("product_tile_selectors", {})
        extracted_tiles = []
        
        for tile in products_soup.select(tile_selectors.get("tiles", {}).get("args", [])[0]):
            tile_data = {
                "url": self.extract_data(tile, tile_selectors.get("details_url", {})),
                "title": self.extract_data(tile, tile_selectors.get("tile_title", {})),
                "price": self.extract_data(tile, tile_selectors.get("tile_price", {})),
                "available": self.extract_data(tile, tile_selectors.get("tile_availability", {}))
            }

            # **Only add & log tiles if URL and Title are present (not None)**
            if tile_data["url"] and tile_data["title"]:
                extracted_tiles.append(tile_data)

                # **🔹 Structured Logging for Tile Extraction**
                logging.info(f"""
                ====== TILE EXTRACTION SUMMARY ======
                Extracted URL        : {tile_data.get('url')}
                Extracted Title      : {tile_data.get('title')}
                Extracted Price      : {tile_data.get('price')}
                Extracted Availability : {tile_data.get('available')}
                ====================================
                """)

        # Fetch and parse product details page
        product_soup = self.fetch_webpage(product_details_url)
        if not product_soup:
            return

        logging.info("JSON TESTER: Extracted Data from Product Details Page:")
        details = site_profile.get("product_details_selectors", {})

        details_data = {
            "url": product_details_url,
            "title": self.extract_data(product_soup, details.get("details_title", {})),
            "description": self.extract_data(product_soup, details.get("details_description", {})),
            "price": self.extract_data(product_soup, details.get("details_price", {})),
            "availability": self.extract_data(product_soup, details.get("details_availability", {})),
            "extracted_id": self.extract_data(product_soup, details.get("details_extracted_id", {})),
            "item_type": self.extract_data(product_soup, details.get("details_item_type", {})),
            "image_urls": self.extract_data(product_soup, details.get("details_image_url", {}))
        }

        # **🔹 Structured Logging for Product Details Extraction**
        logging.info(f"""
        ====== PRODUCT DETAILS EXTRACTION SUMMARY ======
        Extracted URL              : {details_data.get('url')}
        Extracted Title            : {details_data.get('title')}
        Extracted Description      : {details_data.get('description')}
        Extracted Price            : {details_data.get('price')}
        Extracted Availability     : {details_data.get('availability')}
        Extracted ID               : {details_data.get('extracted_id')}
        Extracted Item Type        : {details_data.get('item_type')}
        Extracted Image URLs       : {details_data.get('image_urls')}
        ===============================================
        """)



    def main(self, selected_sites):
        """Runs the JSON tester for each selected site with manual input for URLs."""
        initialize_logging()
        
        if not selected_sites:
            logging.error("JSON TESTER: No sites selected. Exiting JSON tester.")
            return
        
        for site in selected_sites:
            try:
                site_profile = self.jsonManager.json_unpacker(site)
                logging.debug(f"JSON TESTER: Source: {site['source_name']} site_profile loaded.")
            except Exception as e:
                logging.error(f"JSON TESTER: Failed to load site_profile: {e}")
                continue

            #json_file = site.get("json_file")
            if site_profile:
                # products_page_url = input("Enter the products page URL: ").strip()
                products_page_url = 'https://fjm44.com/product-category/sold-items/'
                # product_details_url = input("Enter a product details URL: ").strip()
                product_details_url = 'https://fjm44.com/product/kriegsmarine-nco-visor-by-erel-sonderklasse/'
                logging.info(f"JSON TESTER: Selected JSON Profile: {site_profile.get('source_name')}")
                self.test_site_profile(products_page_url, product_details_url, site_profile)
            else:
                logging.warning(f"JSON TESTER: No JSON profile found for site {site.get('source_name', 'Unknown')}. Skipping.")
