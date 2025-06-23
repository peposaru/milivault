import os
import logging
import json
import subprocess
from collections import defaultdict
import requests
import socket, shutil

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
        openai_manager = OpenAIManager(
            openai_cred_path=user_settings["openaiCred"],
            categories_path=user_settings["militariaCategories"]
        )
        rds_manager = AwsRdsManager(
            credentials_file=user_settings["pgAdminCred"],
            openai_manager=openai_manager
        )
        s3_manager     = S3Manager(user_settings["s3Cred"])
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
    # Auto-detect environment
    settings = {}

    try:
        if is_running_on_ec2():
            print("✅ Environment detected: AWS EC2")
            settings.update(DEFAULT_RDS_SETTINGS)
            settings["environment"] = "aws"
            adjust_logging_level("aws")
        else:
            print("✅ Environment detected: Local PC")
            settings.update(DEFAULT_PC_SETTINGS)
            settings["environment"] = "local"
            adjust_logging_level("local")
    except Exception as e:
        print(f"⚠️ Failed to detect environment automatically: {e}")
        print("Defaulting to Local PC settings.")
        settings.update(DEFAULT_PC_SETTINGS)
        settings["environment"] = "local"
        adjust_logging_level("local")

    while True:
        print("""
What do you want to do?

1. Run Scraper (Check Availability, New Items, or Both)
2. Run Data Integrity Tools (Recover Images / Clean Data)
3. Run Tests Only
4. Run Coverage Report Only
5. Run Tests + Coverage
6. Enter Custom Configuration
        """)
        choice = input("Enter your choice (1–6): ").strip()

        if choice == '1':
            print("""
Select scraper mode:
1. Availability Tracker (detect sold items)
2. New Item / Full Product Check
3. Both Availability + Scrape
""")
            mode = input("Choose run mode (1/2/3): ").strip()
            run_mode = {"1": "availability", "2": "scrape", "3": "both"}.get(mode, "both")

            # Scraper config
            pages_to_check = 1
            use_comparison_row = True

            if run_mode in ("scrape", "both"):
                print("""
Inventory check type:
1. New Inventory (check only 1 page)
2. Custom (specify page count)
""")
                custom_check = input("Enter your choice (1 or 2): ").strip()
                if custom_check == "2":
                    try:
                        pages_to_check = int(input("How many pages should be checked?: ").strip())
                    except ValueError:
                        print("Invalid input. Defaulting to 1 page.")
                        pages_to_check = 1

            # Sleeptimes
            availability_sleeptime = 15 * 60
            scrape_sleeptime = 60 * 60

            if run_mode in ("availability", "both"):
                try:
                    availability_sleeptime = int(input("Availability check delay (seconds): ").strip())
                except ValueError:
                    print("Invalid input. Defaulting to 900 seconds.")

            if run_mode in ("scrape", "both"):
                try:
                    scrape_sleeptime = int(input("Scrape cycle delay (seconds): ").strip())
                except ValueError:
                    print("Invalid input. Defaulting to 3600 seconds.")

            settings.update({
                "run_mode": run_mode,
                "pages_to_check": pages_to_check,
                "use_comparison_row": use_comparison_row,
                "availability_sleeptime": availability_sleeptime,
                "scrape_sleeptime": scrape_sleeptime
            })

            return settings

        elif choice == '2':
            print("Running Data Integrity Tools...")
            settings["run_mode"] = "data_integrity"
            return settings

        elif choice == '3':
            print("Running tests only...")
            subprocess.call(r"C:\Users\keena\Desktop\Milivault\tests_scraper\test_scraper.bat", shell=True)

        elif choice == '4':
            print("Running coverage report only...")
            subprocess.call(r"C:\Users\keena\Desktop\Milivault\tests_scraper\coverage_report.bat", shell=True)

        elif choice == '5':
            print("Running tests with coverage...")
            subprocess.call(r"C:\Users\keena\Desktop\Milivault\tests_scraper\test_and_coverage.bat", shell=True)

        elif choice == '6':
            print("Custom configuration selected.")
            settings["infoLocation"] = input("Enter config directory path: ").strip()
            settings["pgAdminCred"] = input("Enter pgAdmin credentials filename: ").strip()
            settings["selectorJsonFolder"] = input("Enter selector JSON folder name: ").strip()
            settings["s3Cred"] = input("Enter S3 credentials filename: ").strip()
            settings["openaiCred"] = input("Enter OpenAI credentials filename: ").strip()
            return settings

        else:
            print("Invalid choice. Please enter a number from 1 to 6.")





from collections import defaultdict
import math

def site_choice(site_profiles, availability_mode=False):
    """
    Interactive site selector with support for search, fraction, range, and group view.
    """
    def group_sites(profiles):
        working = []
        not_working = []
        for i, site in enumerate(profiles, start=1):
            if site.get("is_working", True):
                working.append((i, site))
            else:
                not_working.append((i, site))
        return working, not_working


    def print_grouped_sites(working, not_working):
        def print_columns(items, label):
            if not items:
                return

            print(f"\n{label}")
            terminal_width = shutil.get_terminal_size((100, 20)).columns
            col_width = 35
            num_cols = max(1, terminal_width // col_width)

            rows = math.ceil(len(items) / num_cols)
            padded = items + [("", {"source_name": ""})] * (rows * num_cols - len(items))

            for row_idx in range(rows):
                line = ""
                for col_idx in range(num_cols):
                    idx = row_idx + col_idx * rows
                    i, site = padded[idx]
                    if i != "":
                        entry = f"{i:>3}: {site['source_name']}"
                        line += entry.ljust(col_width)
                print(line)
            print()

        print_columns(working, "🟢 WORKING SITES:")
        print_columns(not_working, "🔴 NOT WORKING SITES:")

    def search_sites(query):
        query = query.lower()
        return [(i + 1, site) for i, site in enumerate(site_profiles)
                if query in site.get("source_name", "").lower()]

    def parse_fractional_input(input_text, working_sites):
        try:
            num, denom = map(int, input_text.split("/"))
            total = len(working_sites)
            chunk_size = math.ceil(total / denom)
            start = (num - 1) * chunk_size
            end = min(start + chunk_size, total)
            print(f"\n📦 Selected working chunk {num}/{denom} → rows {start + 1} to {end}")
            return [site for _, site in working_sites[start:end]]
        except Exception:
            return None

    # Initial grouping and display
    working_sites, not_working_sites = group_sites(site_profiles)
    print_grouped_sites(working_sites, not_working_sites)

    while True:
        user_input = input("🔎 Type site name, fraction (e.g. 2/3), or press ENTER to list all: ").strip()

        # Fractional shortcut
        if "/" in user_input:
            chunk = parse_fractional_input(user_input, working_sites)
            if chunk:
                if availability_mode:
                    grouped = defaultdict(list)
                    for s in chunk:
                        grouped[s['source_name']].append(s)
                    return list(grouped.values())
                return chunk
            else:
                print("⚠️ Invalid format or out of range. Try again.")
                continue

        # Keyword search
        if user_input == "":
            matching = list(enumerate(site_profiles, 1))
        else:
            matching = search_sites(user_input)

        if not matching:
            print("No matches found. Try again.")
            continue

        match_working = [(i, s) for i, s in matching if s.get("is_working", True)]
        match_not_working = [(i, s) for i, s in matching if not s.get("is_working", True)]
        print_grouped_sites(match_working, match_not_working)

        selection = input("👉 Enter numbers (e.g. 1,3-5), '999' for all working, or 'all': ").strip().lower()
        if selection == "999":
            return [s for _, s in working_sites]
        if selection == "all":
            return site_profiles

        selected_indexes = []
        for part in selection.split(","):
            if "-" in part:
                try:
                    start, end = map(int, part.split("-"))
                    selected_indexes.extend(range(start, end + 1))
                except:
                    continue
            else:
                try:
                    selected_indexes.append(int(part))
                except:
                    continue

        all_indexed_sites = working_sites + not_working_sites
        selected_profiles = [site for i, site in all_indexed_sites if i in selected_indexes]

        if not selected_profiles:
            print("⚠️ No valid selections. Try again.")
            continue

        if availability_mode:
            grouped = defaultdict(list)
            for s in selected_profiles:
                grouped[s['source_name']].append(s)
            return list(grouped.values())

        return selected_profiles




def is_running_on_ec2():
    """Detects if running on EC2 by checking hostname prefix."""
    hostname = socket.gethostname()
    return hostname.startswith("ip-")