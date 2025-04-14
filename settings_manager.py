import os
import logging
import shutil
import subprocess

from aws_rds_manager import AwsRdsManager
from json_manager import JsonManager
from log_print_manager import log_print
from aws_s3_manager import S3Manager
from products_counter import ProductsCounter
from site_processor import SiteProcessor
from html_manager import HtmlManager
from logging_manager import adjust_logging_level

# Default Settings
DEFAULT_RDS_SETTINGS = {
    "infoLocation"       : "/home/ec2-user/milivault/",
    "pgAdminCred"        : "/home/ec2-user/milivault/credentials/pgadmin_credentials.json",
    "selectorJsonFolder" : "/home/ec2-user/milivault/site-json/",
    "s3Cred"             : "/home/ec2-user/milivault/credentials/s3_credentials.json"
    }

DEFAULT_PC_SETTINGS = {
    "infoLocation"       : r'C:/Users/keena/Desktop/Milivault/scraper',
    "pgAdminCred"        : r'C:/Users/keena/Desktop/Milivault/credentials/pgadmin_credentials.json',
    "selectorJsonFolder" : r'C:/Users/keena/Desktop/Milivault/site-json/',
    "s3Cred"             : r'C:/Users/keena/Desktop/Milivault/credentials/s3_credentials.json'
}

def load_user_settings():
    try:
        result = get_user_settings()
        if not isinstance(result, tuple) or len(result) != 5:
            logging.error("SETTINGS MANAGER: Unexpected return value from get_user_settings(). Expected 5 elements.")
            return None

        pages_to_check, sleeptime, user_settings, run_availability_check, use_comparison_row = result

        if not isinstance(user_settings, dict):
            logging.error("SETTINGS MANAGER: user_settings is not a dictionary. Check get_user_settings() implementation.")
            return None

        user_settings.update({
            "targetMatch": pages_to_check,
            "sleeptime": sleeptime,
            "run_availability_check": run_availability_check,
            "use_comparison_row": use_comparison_row
        })

        return pages_to_check, sleeptime, user_settings, run_availability_check, use_comparison_row

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
        rds_manager  = AwsRdsManager(credentials_file=user_settings["pgAdminCred"])
        s3_manager   = S3Manager(user_settings["s3Cred"])
        json_manager = JsonManager()
        log_printer  = log_print()
        counter      = ProductsCounter()
        html_manager = HtmlManager()

        # Initialize dependent managers
        site_processor = SiteProcessor({
            "rdsManager": rds_manager,
            "s3_manager": s3_manager,
            "jsonManager": json_manager,
            "log_print": log_printer,
            "counter": counter,
            "html_manager": html_manager,
        })

        # Return all managers as a dictionary
        return {
            "rdsManager": rds_manager,
            "s3_manager": s3_manager,
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
    """
    Prompt user to select settings for infoLocation, pgAdmin credentials, and selector JSON file.
    Returns:
        - settings (dict): A dictionary with keys 'infoLocation', 'pgAdminCred', 'selectorJsonFolder'.
    """
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
            settings = DEFAULT_RDS_SETTINGS
            settings["environment"] = "aws" 
            adjust_logging_level("aws")

        elif choice == '2':
            print("Using Personal Computer Settings...")
            settings = DEFAULT_PC_SETTINGS
            settings["environment"] = "local"
            adjust_logging_level("local")

        elif choice == '3':
            print("Custom settings selected.")
            settings["infoLocation"]  = input("Enter the directory path for configuration files: ").strip()
            settings["pgAdminCred"]   = input("Enter the name of the pgAdmin credentials file: ").strip()
            settings["selectorJsonFolder"]  = input("Enter the name of the JSON selector file: ").strip()
            settings["s3Cred"]        = input("Enter the name of the S3 credentials file: ").strip()

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


        # Second question: Select check type
        print("""
        Choose the type of inventory check:
        1. New Inventory Check (pages_to_check = 1, sleeptime = 15 minutes)
        2. Run Availability Check (Check and update product availability)
        3. Custom Check (Enter your own pages_to_check, comparison type and sleeptime)
        """)
        check_choice = input("Enter your choice (1/2/3): ").strip()

        run_availability_check = False
        use_comparison_row = True  # Default comparison mode.

        try:
            if check_choice == '1':
                pages_to_check = 1
                sleeptime = 15 * 60  # 15 minutes in seconds

            elif check_choice == '2':
                pages_to_check = None
                sleeptime = None
                run_availability_check = True

            elif check_choice == '3':
                pages_to_check = int(input("Enter your desired pages_to_check value: ").strip())

                print("""
        Choose comparison strategy:
        1. Use preloaded comparison list (faster, more memory)
        2. Query database for each product (slower, scalable)
        """)
                comp_choice = input("Enter 1 or 2: ").strip()
                use_comparison_row = comp_choice == '2'

                sleeptime = int(input("Enter your desired sleeptime value (in seconds): ").strip())

            else:
                raise ValueError

        except ValueError:
            print("Invalid input. Defaulting to New Inventory Check.")
            logging.warning("Invalid input for inventory check. Defaulting to targetMatch=25, sleeptime=15 minutes.")
            pages_to_check = 1
            sleeptime = 15 * 60

        return pages_to_check, sleeptime, settings, run_availability_check, use_comparison_row


def site_choice(all_sites):
    """Display working and non-working sites with notes, and handle user selection."""
    
    # Split sites by working status
    working_sites = [s for s in all_sites if s.get("is_working", False)]
    broken_sites = [s for s in all_sites if not s.get("is_working", False)]
    all_display_sites = working_sites + broken_sites  # Combined for index tracking

    # Determine terminal width and formatting
    term_width = shutil.get_terminal_size((80, 20)).columns
    max_name_length = max(len(site['json_desc']) for site in all_display_sites)
    col_width = max_name_length + 5
    max_notes_length = term_width - 4

    # Display sites by category
    def display_sites(sites, start_index):
        for i, site in enumerate(sites, start=start_index):
            name = f"{i:>3}. {site['json_desc']:<{max_name_length}}"
            note = site.get("notes", "").strip()
            if note:
                print(f"{name}\n     ↳ {note[:max_notes_length]}")
            else:
                print(f"{name}")
        return start_index + len(sites)

    print("\n✅ WORKING SITES")
    next_index = display_sites(working_sites, 1)

    print("\n❌ NOT WORKING SITES")
    display_sites(broken_sites, next_index)

    # User selection loop
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
                    selected_indices.update(range(start - 1, end))  # 0-based
                else:
                    selected_indices.add(int(part) - 1)

            if any(idx < 0 or idx >= len(all_display_sites) for idx in selected_indices):
                raise ValueError("One or more indices are out of range.")

            selected_sites = [all_display_sites[idx] for idx in sorted(selected_indices)]
            return selected_sites

        except ValueError as e:
            print(f"SETTINGS MANAGER: Invalid selection: {e}. Please try again.")