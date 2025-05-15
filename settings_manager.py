import os
import logging
import json
import subprocess
from collections import defaultdict


from aws_rds_manager import AwsRdsManager
from json_manager import JsonManager
from log_print_manager import log_print
from aws_s3_manager import S3Manager
from products_counter import ProductsCounter
from site_processor import SiteProcessor
from html_manager import HtmlManager
from logging_manager import adjust_logging_level
from openai_api_manager import OpenAIManager

# Default Settings
DEFAULT_RDS_SETTINGS = {
    "infoLocation"       : "/home/ec2-user/milivault/",
    "pgAdminCred"        : "/home/ec2-user/milivault/credentials/pgadmin_credentials.json",
    "selectorJsonFolder" : "/home/ec2-user/milivault/site-json/",
    "s3Cred"             : "/home/ec2-user/milivault/credentials/s3_credentials.json",
    "openaiCred"         : "/home/ec2-user/milivault/credentials/chatgpt_api_key.json",
    "militariaCategories": "/home/ec2-user/milivault/categorization/militaria-categories.json"
    }

DEFAULT_PC_SETTINGS = {
    "infoLocation"         : r'C:/Users/keena/Desktop/Milivault/scraper',
    "pgAdminCred"          : r'C:/Users/keena/Desktop/Milivault/credentials/pgadmin_credentials.json',
    "selectorJsonFolder"   : r'C:/Users/keena/Desktop/Milivault/site-json/',
    "s3Cred"               : r'C:/Users/keena/Desktop/Milivault/credentials/s3_credentials.json',
    "openaiCred"           : r'C:/Users/keena/Desktop/Milivault/credentials/chatgpt_api_key.json',
    "militariaCategories"  : r'C:/Users/keena/Desktop/Milivault/categorization/militaria-categories.json'

}

LAST_SETTINGS_FILE = "last_user_settings.json"


def save_last_settings(settings):
    """Save settings to disk for reuse."""
    try:
        with open(LAST_SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=4)
        logging.debug("Saved last user settings.")
    except Exception as e:
        logging.error(f"Error saving last settings: {e}")

def load_last_settings():
    """Load last settings from disk if available."""
    if not os.path.exists(LAST_SETTINGS_FILE):
        return None
    try:
        with open(LAST_SETTINGS_FILE, "r", encoding="utf-8") as f:
            settings = json.load(f)
        logging.debug("Loaded last user settings.")
        return settings
    except Exception as e:
        logging.error(f"Error loading last settings: {e}")
        return None

def load_user_settings():
    try:
        settings = get_user_settings()

        if not isinstance(settings, dict):
            logging.error("SETTINGS MANAGER: get_user_settings() did not return a dictionary.")
            return None

        # Normalize key aliases if needed
        settings["targetMatch"] = settings.get("pages_to_check")
        settings["sleeptime"] = settings.get("scrape_sleeptime")  # optional fallback for backward compatibility
        settings["run_availability_check"] = settings.get("run_mode") == "availability"  # optional legacy flag

        return settings

    except KeyError as e:
        logging.error(f"SETTINGS MANAGER: Error accessing user settings: {e}")
        return None




def setup_user_path(user_settings):
    # Switching to designated info / credentials location.
    try:
        os.chdir(user_settings["infoLocation"])
        logging.info(f"SETTINGS MANAGER: Changed directory to {user_settings['infoLocation']}")
    except Exception as e:
        logging.error(f"SETTINGS MANAGER: Error changing directory: {e}")
        raise

def setup_object_managers(user_settings):
    """
    Initialize and set up object managers required for the program.

    Args:
        user_settings (dict): User-defined settings with credentials and configurations.

    Returns:
        dict: A dictionary of initialized managers.
    """
    try:
        # Initialize independent managers
        rds_manager    = AwsRdsManager(credentials_file=user_settings["pgAdminCred"])
        s3_manager     = S3Manager(user_settings["s3Cred"])
        openai_manager = OpenAIManager(
            openai_cred_path=user_settings["openaiCred"],
            categories_path=user_settings["militariaCategories"]
        )
        json_manager   = JsonManager()
        log_printer    = log_print()
        counter        = ProductsCounter()
        html_manager   = HtmlManager()

        # Initialize dependent managers
        site_processor = SiteProcessor({
            "rdsManager"    : rds_manager,
            "s3_manager"    : s3_manager,
            "openai_manager": openai_manager,
            "jsonManager"   : json_manager,
            "log_print"     : log_printer,
            "counter"       : counter,
            "html_manager"  : html_manager,
        })

        # Return all managers as a dictionary
        return {
            "rdsManager": rds_manager,
            "s3_manager": s3_manager,
            "openai_manager": openai_manager,
            "jsonManager": json_manager,
            "log_print": log_printer,
            "counter": counter,
            "siteprocessor": site_processor,
            "html_manager": html_manager
        }

    except Exception as e:
        logging.error(f"SETTINGS MANAGER: Error setting up object managers: {e}")
        raise


def get_user_settings():
    # Try loading previous settings
    previous_settings = load_last_settings()

    if previous_settings:
        print("\nFound previous settings.")
        reuse = input("Press Enter to reuse previous settings, or type 'n' to configure new: ").strip().lower()
        if reuse == "":
            print("Reusing previous settings...")
            return previous_settings

    while True:
        print("""
Choose your settings:
1. Amazon RDS Settings
2. Personal Computer Settings
3. Custom Settings
4. Run Tests Only
5. Run Coverage Report Only
6. Run Tests + Coverage
        """)
        choice = input("Enter the number corresponding to your choice (1–6): ").strip()

        settings = {}

        if choice == '1':
            print("Using Amazon RDS Settings...")
            settings.update(DEFAULT_RDS_SETTINGS)
            settings["environment"] = "aws" 
            adjust_logging_level("aws")

        elif choice == '2':
            print("Using Personal Computer Settings...")
            settings.update(DEFAULT_PC_SETTINGS)
            settings["environment"] = "local"
            adjust_logging_level("local")

        elif choice == '3':
            print("Custom settings selected.")
            settings["infoLocation"] = input("Enter the directory path for configuration files: ").strip()
            settings["pgAdminCred"] = input("Enter the name of the pgAdmin credentials file: ").strip()
            settings["selectorJsonFolder"] = input("Enter the name of the JSON selector file: ").strip()
            settings["s3Cred"] = input("Enter the name of the S3 credentials file: ").strip()
            settings["openaiCred"] = input("Enter the name of the OpenAI credentials file: ").strip()

        elif choice == '4':
            print("Running tests only...")
            subprocess.call(r"C:\Users\keena\Desktop\Milivault\tests_scraper\test_scraper.bat", shell=True)
            continue

        elif choice == '5':
            print("Running coverage report only...")
            subprocess.call(r"C:\Users\keena\Desktop\Milivault\tests_scraper\coverage_report.bat", shell=True)
            continue

        elif choice == '6':
            print("Running tests with coverage...")
            subprocess.call(r"C:\Users\keena\Desktop\Milivault\tests_scraper\test_and_coverage.bat", shell=True)
            continue

        else:
            print("Invalid choice. Please enter a number from 1 to 6.")
            continue

        # Inventory behavior config
        print("""
Choose run mode:
1. Availability Tracker - Check to see if any items in site have been sold.
2. New Item / Full Product Check - Does a detail check of every product in given page range.
3. Both Availability and Scrape
        """)
        run_mode_choice = input("Enter your choice (1/2/3): ").strip()
        run_mode = "both"  # default

        if run_mode_choice == "1":
            run_mode = "availability"
        elif run_mode_choice == "2":
            run_mode = "scrape"
        elif run_mode_choice == "3":
            run_mode = "both"
        else:
            print("Invalid input. Defaulting to 'both'.")
            logging.warning("Invalid run_mode input. Defaulted to 'both'.")

        # Set defaults
        pages_to_check = 1
        use_comparison_row = True

        # Only ask for pages_to_check if scrape is involved
        if run_mode in ("scrape", "both"):
            print("""
Choose the type of inventory check:
1. New Inventory Check (pages_to_check = 1)
2. Custom Check (Enter your own pages_to_check and comparison type)
            """)
            check_choice = input("Enter your choice (1 or 2): ").strip()

            try:
                if check_choice == "2":
                    pages_to_check = int(input("Enter your desired pages_to_check value: ").strip())

                    use_comparison_row = True


            except ValueError:
                print("Invalid input. Defaulting to New Inventory Check with pages_to_check = 1.")
                logging.warning("Defaulting to pages_to_check = 1 due to input error.")

        # Sleep settings
        availability_sleeptime = 15 * 60
        scrape_sleeptime = 60 * 60

        if run_mode in ("availability", "both"):
            try:
                availability_sleeptime = int(input("Enter availability check sleeptime (in seconds): ").strip())
            except ValueError:
                print("Invalid input. Defaulting availability sleeptime to 15 minutes.")
                logging.warning("Defaulting availability_sleeptime to 900 seconds.")

        if run_mode in ("scrape", "both"):
            try:
                scrape_sleeptime = int(input("Enter scrape cycle sleeptime (in seconds): ").strip())
            except ValueError:
                print("Invalid input. Defaulting scrape sleeptime to 60 minutes.")
                logging.warning("Defaulting scrape_sleeptime to 3600 seconds.")

        # Build final settings dict
        settings.update({
            "pages_to_check": pages_to_check,
            "run_mode": run_mode,
            "use_comparison_row": use_comparison_row,
            "availability_sleeptime": availability_sleeptime,
            "scrape_sleeptime": scrape_sleeptime
        })

        return settings




def site_choice(all_sites, run_availability_check=False):
    """Display eligible sites, archive-only sites, and broken ones. 
       Groups by source_name automatically if run_availability_check=True.
    """
    from collections import defaultdict
    import shutil

    working_sites = [s for s in all_sites if s.get("is_working", False)]
    broken_sites = [s for s in all_sites if not s.get("is_working", False)]

    if run_availability_check:
        eligible_sites = [s for s in working_sites if not s.get("is_sold_archive", False)]
        archive_sites  = [s for s in working_sites if s.get("is_sold_archive", False)]
        all_display_sites = eligible_sites + archive_sites + broken_sites
    else:
        eligible_sites = working_sites
        archive_sites = []
        all_display_sites = working_sites + broken_sites

    term_width = shutil.get_terminal_size((80, 20)).columns
    max_name_length = max(len(site['json_desc']) for site in all_display_sites)
    max_notes_length = term_width - 4

    def display_sites(sites, start_index, label):
        print(f"\n{label}")
        for i, site in enumerate(sites, start=start_index):
            name = f"{i:>3}. {site['json_desc']:<{max_name_length}}"
            note = site.get("notes", "").strip()
            print(f"{name}\n     ↳ {note[:max_notes_length]}" if note else name)
        return start_index + len(sites)

    index = 1
    if run_availability_check:
        index = display_sites(eligible_sites, index, "✅ AVAILABLE FOR TRACKING")
        index = display_sites(archive_sites, index, "🔒 SOLD-ONLY ARCHIVES (Skipping)")
    else:
        index = display_sites(working_sites, index, "✅ WORKING SITES")

    display_sites(broken_sites, index, "❌ NOT WORKING SITES")

    while True:
        try:
            choice = input("\nSelect sites to scrape (e.g., '1,3-5,7'): ").strip()
            if not choice:
                print("Please enter a valid selection.")
                continue

            selected_indices = set()
            for part in choice.split(','):
                if '-' in part:
                    start, end = map(int, part.split('-'))
                    if start > end:
                        raise ValueError(f"Invalid range: {start}-{end}")
                    selected_indices.update(range(start - 1, end))
                else:
                    selected_indices.add(int(part) - 1)

            if any(idx < 0 or idx >= len(all_display_sites) for idx in selected_indices):
                raise ValueError("One or more indices are out of range.")

            selected_sites = [all_display_sites[idx] for idx in sorted(selected_indices)]

            if run_availability_check:
                sold_only = [s for s in selected_sites if s.get("is_sold_archive", False)]
                if sold_only:
                    names = ", ".join(s["json_desc"] for s in sold_only)
                    print(f"⚠️  The following are sold-only archives and cannot be used in availability mode:\n → {names}")
                    continue

                # 📦 Group selected sites by source_name
                grouped_sites = defaultdict(list)
                for site in selected_sites:
                    grouped_sites[site['source_name']].append(site)

                # Return a list of grouped site profiles
                return list(grouped_sites.values())

            else:
                # Normal scrape mode — no grouping needed
                return selected_sites

        except ValueError as e:
            print(f"SETTINGS MANAGER: Invalid selection: {e}. Please try again.")

