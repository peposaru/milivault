# This module is for handling html / css code

import requests, logging, time, re
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import RequestException, Timeout, ChunkedEncodingError
from time import sleep
from image_extractor import fetch_images

class HtmlManager:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0"
        }
        self.session = requests.Session()


    # This is used to create the list of all product urls on the products list page
    def construct_products_page_list_soup(self, products_list_page):
        try:
            response = self.session.get(products_list_page, headers=self.headers)
            response.raise_for_status()
        except Timeout:
            logging.info(f"Timeout occurred while accessing {products_list_page}.")
            return None
        except RequestException as e:
            logging.error(f"Error occurred while accessing {products_list_page} - {e}")
            return None
        return BeautifulSoup(response.content, 'html.parser')