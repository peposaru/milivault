# Standard Library Modules
import logging
from time import sleep

# These are modules made for this program specifically.
from settings_manager import site_choice, setup_user_path, load_user_settings, setup_object_managers
from logging_manager import initialize_logging
from json_tester import JsonTester


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

    # Logging and counter managers
    log_print = managers.get("log_print")
    counter = managers.get("counter")

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
    
    # How many empty pages to tolerate before stopping
    targetMatch = user_settings.get("targetMatch")
    
    # Create the URL list to compare new URLs to
    try:
        selected_sources = [site['source_name'] for site in selected_sites]
        comparison_list = managers['rdsManager'].create_comparison_list(selected_sources)
    except Exception as e:
        logging.error(f"Error constructing url_comparison_list: {e}")
        return

    # Main processing loop
    while True:
        for selected_site in selected_sites:
            logging.info(f"Processing site: {selected_site['source_name']}")

            try:
                managers['siteprocessor'].site_processor_main(comparison_list, selected_site, targetMatch)
                logging.info(f"Successfully processed site: {selected_site['source_name']}")
            except Exception as e:
                logging.error(f"Error processing site {selected_site['source_name']}: {e}")

        log_print.final_summary(selected_sites, counter)
        logging.info("Processing completed.")
        sleep_duration = user_settings.get("sleeptime")
        if sleep_duration:
            logging.info(f"Sleeping for {sleep_duration} seconds before next cycle...")
            sleep(sleep_duration)
        else:
            logging.info("No sleep time configured. Exiting loop.")
            log_print.final_summary(selected_sites, counter)
            break        

    
if __name__ == "__main__":
    main()
