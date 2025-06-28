import requests
import logging
import time
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, Timeout


class HtmlManager:
    def __init__(self, user_agent=None, retries=3, backoff_factor=2, timeout=20, cookies=None):
        self.headers = {
            "User-Agent": user_agent or (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Referer": "https://www.google.com/"
        }

        self.session = requests.Session()

        # ✅ Pool fix (Add this)
        from requests.adapters import HTTPAdapter
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.retries = retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout

        if cookies:
            for name, value in cookies.items():
                self.session.cookies.set(name, value)


    def fetch_url(self, url):
        for attempt in range(self.retries):
            try:
                response = self.session.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                # Set encoding
                #if not response.encoding or response.encoding.lower() == 'utf-8':
                 #   response.encoding = response.apparent_encoding
                return response
            except Timeout:
                logging.warning(f"HTML MGR: Timeout occurred while accessing {url}. Retrying...")
            except RequestException as e:
                logging.error(f"HTML MGR: Error occurred while accessing {url} (Attempt {attempt + 1}/{self.retries}): {e}")
            time.sleep(self.backoff_factor ** attempt)
        logging.error(f"HTML MGR: Failed to fetch {url} after {self.retries} attempts.")
        return None

    def fetch_with_final_url(self, url):
        # Fetch the page and return the BeautifulSoup object along with the final URL after redirection.
        try:
            response = self.session.get(url, headers=self.headers, timeout=self.timeout, allow_redirects=True)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser'), response.url
        except RequestException as e:
            logging.error(f"HTML MGR: Error fetching URL with redirection {url}: {e}")
            return None, url

    def fetch_with_retries(self, fetch_function, *args, max_retries=3, backoff_factor=2, **kwargs):
        #Retry logic for fetch operations.
        retries = 0
        while retries < max_retries:
            try:
                return fetch_function(*args, **kwargs)
            except Exception as e:
                retries += 1
                wait_time = backoff_factor ** retries
                logging.warning(f"HTML MGR: Retry {retries}/{max_retries}: {e}. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
        logging.error(f"Failed to fetch after {max_retries} retries.")
        return None

    def fetch_streaming_page(self, url, chunk_size=1024):
        # Fetch a page using streaming to handle large content responses.
        try:
            response = self.session.get(url, headers=self.headers, stream=True, timeout=self.timeout)
            response.raise_for_status()
            content = b"".join(chunk for chunk in response.iter_content(chunk_size=chunk_size) if chunk)
            return BeautifulSoup(content, 'html.parser')
        except RequestException as e:
            logging.error(f"HTML MGR: Error fetching streaming page {url}: {e}")
            return None

    def extract_data(self, soup, selector_config):
        # Extract data from a BeautifulSoup object dynamically based on configuration.
        try:
            method = selector_config.get("method", "find")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            element = getattr(soup, method)(*args, **kwargs)
            if attribute:
                return element.get(attribute).strip() if element and element.get(attribute) else None
            return element.get_text(strip=True) if element else None
        except Exception as e:
            logging.error(f"HTML MGR: Error extracting data with config {selector_config}: {e}")
            return None

    def parse_html(self, url, parser="html.parser"):
        # Parse a URL into a BeautifulSoup object.
        response = self.fetch_url(url)
        if not response:
            return None
        
        try:
            return BeautifulSoup(response.content, parser)
        except Exception as e:
            logging.error(f"HTML MGR: Error occurred while parsing the page {url}: {e}")
            return None
