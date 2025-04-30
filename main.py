# Standard Library Modules
import logging, time
from collections import defaultdict
from time import sleep
from datetime import datetime

# These are modules made for this program specifically.
from settings_manager import site_choice, setup_user_path, load_user_settings, setup_object_managers
from logging_manager import initialize_logging
from availability_tracker import SiteAvailabilityTracker

def main():
    # Initialize logging
    initialize_logging()

    # Get user settings
    user_settings = load_user_settings()
    if not user_settings:
        logging.error("Error retrieving user settings.")
        exit()

    run_mode = user_settings.get("run_mode", "both")
    pages_to_check = user_settings.get("pages_to_check", 1)
    use_comparison_row = user_settings.get("use_comparison_row", True)
    availability_sleeptime = user_settings.get("availability_sleeptime", 900)
    scrape_sleeptime = user_settings.get("scrape_sleeptime", 3600)

    now = time.time()
    user_settings.update({
        "targetMatch": pages_to_check,
        "last_avail_run": now - availability_sleeptime,
        "last_scrape_run": now - scrape_sleeptime
    })


    # Prepare environment
    setup_user_path(user_settings)
    managers = setup_object_managers(user_settings)
    if not managers:
        logging.error("Error setting up object managers.")
        exit()

    log_print    = managers.get("log_print")
    counter      = managers.get("counter")
    json_manager = managers.get("jsonManager")

    if not json_manager:
        logging.error("JsonManager is not initialized in managers.")
        exit()

    try:
        jsonData = json_manager.compile_json_profiles(user_settings["selectorJsonFolder"])
    except Exception as e:
        logging.error(f"Failed to load JSON selectors: {e}")
        exit()

    selected_sites = site_choice(jsonData, run_mode == "availability")
    if not selected_sites:
        logging.error("No sites selected. Exiting program.")
        exit()

    # Split into availability_sites and scrape_sites
    availability_sites = []
    scrape_sites = []

    for site in selected_sites:
        if isinstance(site, list):  # availability mode returns grouped lists
            for s in site:
                if s.get("is_sold_archive", False):
                    scrape_sites.append(s)
                else:
                    availability_sites.append(s)
        else:  # scrape mode returns flat list
            if site.get("is_sold_archive", False):
                scrape_sites.append(site)
            else:
                availability_sites.append(site)

    # Final scrape list is both
    all_scrape_sites = availability_sites + scrape_sites

    print(f"Sites selected for availability: {[site['source_name'] for site in availability_sites]}")
    print(f"Sites selected for scraping    : {[site['source_name'] for site in all_scrape_sites]}")

    try:
        availability_tracker = SiteAvailabilityTracker(managers)
    except Exception as e:
        logging.error(f"Error initializing SiteAvailabilityTracker: {e}")
        exit()

    comparison_list = {}
    if not use_comparison_row:
        try:
            selected_sources = [site['source_name'] for site in all_scrape_sites]
            comparison_list = managers['rdsManager'].create_comparison_list(selected_sources)
        except Exception as e:
            logging.error(f"Error constructing url_comparison_list: {e}")
            exit()

    # MAIN LOOP
    while True:
        now = time.time()

        # AVAILABILITY MODE
        if run_mode in ("availability", "both") and now - user_settings["last_avail_run"] >= availability_sleeptime:
            logging.info("Running availability check for all selected sites...")
            grouped_by_source = defaultdict(list)
            for site in availability_sites:
                grouped_by_source[site['source_name']].append(site)

            for site_group in grouped_by_source.values():
                try:
                    availability_tracker.avail_track_main(site_group)
                except Exception as e:
                    logging.error(f"Availability tracker failed for group: {e}")

            user_settings["last_avail_run"] = now
            logging.info("Availability check attempt completed.")

        # SCRAPE MODE
        if run_mode in ("scrape", "both") and now - user_settings["last_scrape_run"] >= scrape_sleeptime:
            for selected_site in all_scrape_sites:
                logging.info(f"Processing site: {selected_site['source_name']}")
                try:
                    managers['siteprocessor'].site_processor_main(
                        comparison_list,
                        selected_site,
                        user_settings["targetMatch"],
                        use_comparison_row
                    )
                    logging.info(f"Successfully processed site: {selected_site['source_name']}")
                except Exception as e:
                    logging.error(f"Error processing site {selected_site['source_name']}: {e}")
            user_settings["last_scrape_run"] = now
            log_print.final_summary(all_scrape_sites, counter)

        # Calculate next check timing
        next_avail_due = user_settings["last_avail_run"] + availability_sleeptime
        next_scrape_due = user_settings["last_scrape_run"] + scrape_sleeptime
        next_due = min(next_avail_due, next_scrape_due)

        sleep_seconds = next_due - time.time()
        if sleep_seconds < 10:
            logging.warning(f"Calculated sleep was {sleep_seconds:.2f} seconds. Forcing minimum 10 seconds sleep.")
            sleep_seconds = 10

        sleep_seconds = int(sleep_seconds)
        next_wakeup_time = datetime.fromtimestamp(time.time() + sleep_seconds).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Sleeping {sleep_seconds} seconds... Next check at {next_wakeup_time}")
        sleep(sleep_seconds)



if __name__ == "__main__":
    main()
