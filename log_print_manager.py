from datetime import datetime
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import os
import subprocess

class log_print:
    def create_log_header(self, message, width=60):
        # Helper method to create a formatted log header.
        border = '-' * width
        return f"\n{border}\n{message.center(width)}\n{border}"

    def newInstance(self, source, productsPage, runCycle, productsProcessed):
        # Log the start of a new scraping instance.
        current_datetime = datetime.now()
        logging.info(self.create_log_header("NEW INSTANCE STARTED"))
        logging.info(f"MILITARIA SITE      : {source}")
        logging.info(f"PRODUCTS URL        : {productsPage}")
        logging.info(f"CYCLES RUN          : {runCycle}")
        logging.info(f"PRODUCTS PROCESSED  : {productsProcessed}")
        logging.info(f"TIMESTAMP           : {current_datetime}")

    def terminating(self, source, consecutiveMatches, targetMatch, runCycle, productsProcessed):
        # Log the termination of a scraping instance.
        current_datetime = datetime.now()
        logging.info(self.create_log_header("INSTANCE TERMINATED"))
        logging.info(f"MILITARIA SITE      : {source}")
        logging.info(f"CONSECUTIVE MATCHES : {consecutiveMatches} / {targetMatch}")
        logging.info(f"CYCLES RUN          : {runCycle}")
        logging.info(f"PRODUCTS PROCESSED  : {productsProcessed}")
        logging.info(f"TIMESTAMP           : {current_datetime}")

    def sysUpdate(self, page, urlCount, consecutiveMatches, targetMatch, productUrl, updated):
        # Log product update status conditionally.
        current_datetime = datetime.now()
        if updated:
            logging.info(self.create_log_header("PRODUCT UPDATED"))
        else:
            logging.info(self.create_log_header("NO PRODUCT UPDATE"))
        logging.info(f"CURRENT PAGE        : {page}")
        logging.info(f"PRODUCTS PROCESSED  : {urlCount}")
        logging.info(f"CONSECUTIVE MATCHES : {consecutiveMatches} / {targetMatch}")
        logging.info(f"PRODUCT URL         : {productUrl}")
        logging.info(f"TIMESTAMP           : {current_datetime}")

    def newProduct(self, page, urlCount, title, productUrl, description, price, available):
        # Log details of a newly scraped product.
        current_datetime = datetime.now()
        logging.info(self.create_log_header("NEW PRODUCT FOUND"))
        logging.info(f"CURRENT PAGE        : {page}")
        logging.info(f"PRODUCTS PROCESSED  : {urlCount}")
        logging.info(f"TITLE               : {title}")
        logging.info(f"PRODUCT URL         : {productUrl}")
        logging.info(f"DESCRIPTION         : {description}")
        logging.info(f"PRICE               : {price}")
        logging.info(f"AVAILABLE           : {available}")
        logging.info(f"TIMESTAMP           : {current_datetime}")

    def standby(self):
        # Log when the scraper enters a standby period between cycles.
        current_datetime = datetime.now()
        logging.info(self.create_log_header("CYCLE COMPLETED"))
        logging.info(f"PROCESS COMPLETED AT: {current_datetime}")
        logging.info("STANDING BY FOR NEXT CYCLE...")

class WatchdogHandler(FileSystemEventHandler):
    def on_modified(self, event):
        # Restart the program if a crash is detected
        if "crash.log" in event.src_path:
            logging.info("Crash detected. Restarting program...")
            subprocess.Popen(["python", "/home/ec2-user/projects/AWS-Militaria-Scraper/AWS_MILITARIA_SCRAPER_JSON.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

if __name__ == "__main__":
    logging.basicConfig(filename="scraper.log", level=logging.INFO)
    event_handler = WatchdogHandler()
    observer = Observer()
    observer.schedule(event_handler, path=".", recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
