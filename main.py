# Standard Library Modules
import logging
from datetime import datetime
from time import sleep

# These are modules made for this program specifically.
from settings_manager import site_choice, setup_user_path, load_user_settings, setup_object_managers
from site_processor import SiteProcessor
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
        jsonData = json_manager.compile_json_profiles(user_settings["selectorJsonFolder"])
    except Exception as e:
        logging.error(f"Failed to load JSON selectors: {e}")
        return

    # Which sites to process
    selected_sites = site_choice(jsonData)

    # This is the main loop which keeps everything going.
    while True:
        for selected_site in selected_sites:
            try:
                managers['siteprocessor'].site_processor_main(
                    selected_site
                )
                logging.info(f"Successfully processed site: {selected_site['source_name']}")
            except Exception as e:
                logging.error(f"Error processing site {selected_site['source_name']}: {e}")

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