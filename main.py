# Standard Library Modules
import logging
from datetime import datetime
from time import sleep
import os, sys

# These are modules made for this program specifically.
from settings_manager import site_choice, setup_user_path, load_user_settings, setup_object_managers
from site_processor import SiteProcessor
from logging_manager import initialize_logging
from json_tester import JsonTester
def restart_program():
    """Restarts the program from the beginning."""
    logging.info("Restarting script...")
    python = sys.executable
    os.execv(python, [python] + sys.argv)  # Restart the script fully

def main():
    initialize_logging()

    # Get user settings before starting
    user_settings = load_user_settings()
    if not user_settings:
        logging.error("Error retrieving user settings.")
        return

    # Change to user info location
    setup_user_path(user_settings)

    # Set up the object managers
    managers = setup_object_managers(user_settings)
    if not managers:
        logging.error("Error setting up user managers.")
        return

    # Load JSON selectors
    json_manager = managers.get('jsonManager')
    if not json_manager:
        logging.error("JsonManager is not initialized in managers.")
        return
    try:
        jsonData = json_manager.compile_json_profiles(user_settings["selectorJsonFolder"])
    except Exception as e:
        logging.error(f"Failed to load JSON selectors: {e}")
        return

    # Ensure site selection before processing
    selected_sites = site_choice(jsonData)
    if not selected_sites:
        logging.error("No sites selected. Exiting program.")
        return

    print(f"Sites selected: {[site['source_name'] for site in selected_sites]}")

    # If json test is selected, run the json tester
    if user_settings.get("jsonTest"):
        logging.info("Running JSON Tester.")
        json_tester = JsonTester(managers)
        json_tester.main(selected_sites)
    
    # Create the URL list to compare new URLs to
    try:
        comparison_list = managers['rdsManager'].create_comparison_list()
    except Exception as e:
        logging.error(f"Error constructing url_comparison_list: {e}")
        return

    # Main processing loop
    for selected_site in selected_sites:
        print(f"Processing site: {selected_site['source_name']}")
        logging.info(f"Processing site: {selected_site['source_name']}")

        try:
            managers['siteprocessor'].site_processor_main(comparison_list, selected_site)
            logging.info(f"Successfully processed site: {selected_site['source_name']}")
        except Exception as e:
            logging.error(f"Error processing site {selected_site['source_name']}: {e}")

    logging.info("Processing completed.")

if __name__ == "__main__":
    main()
