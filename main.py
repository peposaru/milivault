# Standard Library Modules
import logging
from time import sleep

# These are modules made for this program specifically.
from settings_manager import site_choice, setup_user_path, load_user_settings, setup_object_managers
from logging_manager import initialize_logging
from availability_tracker import AvailabilityTracker

def main():
    initialize_logging()

    # Get user settings before starting
    result = load_user_settings()
    if not result:
        logging.error("Error retrieving user settings.")
        return

    pages_to_check, sleeptime, user_settings, run_availability_check, use_comparison_row = result

    # ✅ Set targetMatch high if we're in availability-only mode
    if run_availability_check:
        pages_to_check = 9999  # ensures full pagination is attempted

    user_settings.update({
        "targetMatch": pages_to_check,
        "sleeptime": sleeptime,
        "run_availability_check": run_availability_check,
        "use_comparison_row": use_comparison_row
    })

    setup_user_path(user_settings)

    # Initializes the various necessary managers / modules for the program to run.
    managers = setup_object_managers(user_settings)
    if not managers:
        logging.error("Error setting up user managers.")
        return

    log_print    = managers.get("log_print")
    counter      = managers.get("counter")
    json_manager = managers.get('jsonManager')
    if not json_manager:
        logging.error("JsonManager is not initialized in managers.")
        return

    # Loading the JSON profiles from the specified folder
    try:
        jsonData = json_manager.compile_json_profiles(user_settings["selectorJsonFolder"])
    except Exception as e:
        logging.error(f"Failed to load JSON selectors: {e}")
        return

    # This is where the user selects which sites to scrape from the JSON profiles.
    selected_sites = site_choice(jsonData)
    if not selected_sites:
        logging.error("No sites selected. Exiting program.")
        return

    print(f"Sites selected: {[site['source_name'] for site in selected_sites]}")

    # ✅ ADDED: Create an instance of AvailabilityTracker
    availability_tracker = AvailabilityTracker(managers)

    comparison_list = {}
    if not use_comparison_row:
        try:
            selected_sources = [site['source_name'] for site in selected_sites]
            comparison_list = managers['rdsManager'].create_comparison_list(selected_sources)
        except Exception as e:
            logging.error(f"Error constructing url_comparison_list: {e}")
            return

    while True:
        for selected_site in selected_sites:
            logging.info(f"Processing site: {selected_site['source_name']}")

            # ✅ NEW: If the JSON profile has "run_availability_check": true, run tile-only availability mode
            if selected_site.get("run_availability_check", False):
                try:
                    logging.info(f"Running availability check for {selected_site['source_name']}")
                    availability_tracker.avail_check_main(selected_site)  # ✅ DELEGATE to AvailabilityTracker
                    continue  # ✅ Skip full scrape
                except Exception as e:
                    logging.error(f"Availability tracker failed for {selected_site['source_name']}: {e}")
                    continue

            # 🔁 Otherwise, run the full scraper
            try:
                managers['siteprocessor'].site_processor_main(
                    comparison_list, 
                    selected_site, 
                    user_settings["targetMatch"], 
                    user_settings["use_comparison_row"]
                )
                logging.info(f"Successfully processed site: {selected_site['source_name']}")
            except Exception as e:
                logging.error(f"Error processing site {selected_site['source_name']}: {e}")

        log_print.final_summary(selected_sites, counter)
        logging.info("Processing completed.")

        if sleeptime:
            logging.info(f"Sleeping for {sleeptime} seconds before next cycle...")
            sleep(sleeptime)
        else:
            logging.info("No sleep time configured. Exiting loop.")
            log_print.final_summary(selected_sites, counter)
            break


if __name__ == "__main__":
    main()
