# Standard Library Modules
import logging
from time import sleep

# These are modules made for this program specifically.
from settings_manager import site_choice, setup_user_path, load_user_settings, setup_object_managers
from logging_manager import initialize_logging
from availability_tracker import SiteAvailabilityTracker

def main():
    initialize_logging()

    # Get user settings before starting
    result = load_user_settings()
    if not result:
        logging.error("Error retrieving user settings.")
        return

    pages_to_check, sleeptime, user_settings, run_availability_check, use_comparison_row = result

    # ✅ Set high page limit if doing availability check
    if run_availability_check:
        pages_to_check = 9999

    user_settings.update({
        "targetMatch": pages_to_check,
        "sleeptime": sleeptime,
        "run_availability_check": run_availability_check,
        "use_comparison_row": use_comparison_row
    })

    setup_user_path(user_settings)

    managers = setup_object_managers(user_settings)
    if not managers:
        logging.error("Error setting up user managers.")
        return

    log_print    = managers.get("log_print")
    counter      = managers.get("counter")
    json_manager = managers.get("jsonManager")
    if not json_manager:
        logging.error("JsonManager is not initialized in managers.")
        return

    try:
        jsonData = json_manager.compile_json_profiles(user_settings["selectorJsonFolder"])
    except Exception as e:
        logging.error(f"Failed to load JSON selectors: {e}")
        return

    selected_sites = site_choice(jsonData, run_availability_check)
    if not selected_sites:
        logging.error("No sites selected. Exiting program.")
        return

    print(f"Sites selected: {[site['source_name'] for site in selected_sites]}")

    try:
        availability_tracker = SiteAvailabilityTracker(managers)
    except Exception as e:
        logging.error(f"Error initializing SiteAvailabilityTracker: {e}")
        return
    
    comparison_list = {}
    if not use_comparison_row:
        try:
            selected_sources = [site['source_name'] for site in selected_sites]
            comparison_list = managers['rdsManager'].create_comparison_list(selected_sources)
        except Exception as e:
            logging.error(f"Error constructing url_comparison_list: {e}")
            return

    while True:
        if run_availability_check:
            try:
                logging.info("Running availability check for all selected sites...")
                availability_tracker.avail_track_main(selected_sites)
            except Exception as e:
                logging.error(f"Availability tracker failed: {e}")

            logging.info("Availability check completed.")

            if sleeptime:
                logging.info(f"Sleeping for {sleeptime} seconds before next availability check cycle...")
                sleep(sleeptime)
                continue
            else:
                logging.info("No sleep time configured. Exiting loop.")
                break

        for selected_site in selected_sites:
            logging.info(f"Processing site: {selected_site['source_name']}")

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

        # ✅ Availability mode skips final_summary
        if not run_availability_check:
            log_print.final_summary(selected_sites, counter)

        logging.info("Processing completed.")

        if sleeptime:
            logging.info(f"Sleeping for {sleeptime} seconds before next full scrape cycle...")
            sleep(sleeptime)
        else:
            logging.info("No sleep time configured. Exiting loop.")
            break



if __name__ == "__main__":
    main()
