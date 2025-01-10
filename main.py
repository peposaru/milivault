# Standard Library Modules
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import json
import logging
import time
from datetime import datetime
from time import sleep
from typing import Optional

# These are modules made for this program specifically.
from aws_rds_manager import PostgreSQLProcessor
from web_scraper import ProductScraper
from json_manager import JsonManager
from log_print_manager import log_print
from settings_manager import site_choice, setup_user_path, load_user_settings, setup_object_managers
from site_product_processor import process_site
from aws_s3_manager import S3Manager
from availability_check import run_availability_check_loop
from logging_manager import initialize_logging





def main():
    initialize_logging()

    # Getting user settings
    user_settings = load_user_settings()
    if not user_settings:
        logging.error(f'Error retrieving user settings.')
        return
    
    # Changing to user info location.
    setup_user_path(user_settings)

    # Setting up the object managers
    managers = setup_object_managers(user_settings)
    if not managers:
        logging.error(f'Error setting up user managers.')
        return

    # Run Availability Check if selected
    if user_settings["run_availability_check"]:
        run_availability_check_loop(managers, user_settings)

    # Load JSON selectors
    json_manager = managers.get('jsonManager')
    if not json_manager:
        logging.error("JsonManager is not initialized in managers.")
        return
    try:
        jsonData = json_manager.load_and_validate_selectors(user_settings["selectorJson"])
    except Exception as e:
        logging.error(f"Failed to load JSON selectors: {e}")
        return

    # Which sites to process
    selected_sites = site_choice(jsonData)

    # How many times has the program gone through all the sites?
    runCycle          = 0
    # How many products total has the program gone through?
    productsProcessed = 0

    # This is the main loop which keeps everything going.
    while True:
        for site in selected_sites:
            try:
                process_site(
                    managers.get('webScrapeManager'),
                    managers.get('dataManager'),
                    managers.get('jsonManager'),
                    managers.get('prints'),
                    site,
                    user_settings["targetMatch"],
                    runCycle,
                    productsProcessed,
                    managers.get('s3_manager')
                )
                logging.warning(f"Successfully processed site: {site['source']}")
            except Exception as e:
                logging.error(f"Error processing site {site['source']}: {e}")

        # Pause between cycles
        sleeptime = user_settings["sleeptime"]
        if sleeptime > 0:
            logging.warning(f"Pausing for {sleeptime} seconds before starting the next cycle...")
            try:
                sleep(sleeptime)
            except Exception as e:
                logging.error(f"Error during sleep: {e}")
        else:
            logging.info("No pause configured (sleeptime = 0). Starting the next cycle immediately.")

if __name__ == "__main__":
    main()