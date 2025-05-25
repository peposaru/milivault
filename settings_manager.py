import os
import logging
import json
import subprocess
from collections import defaultdict
import requests
import socket

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





def site_choice(all_sites, run_availability_check=False):
    """Display eligible sites and allow user to select or choose all working sites."""
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

    print(f"\n999. All Working Sites")

    # 🧭 Prompt user for input
    while True:
        try:
            choice = input(
                "\n🔢 Select site(s) to scrape (e.g. '1,3-5,7' or '999' for all working): "
            ).strip()

            if not choice:
                print("⚠️  No input received. Please enter at least one number.")
                continue

            if choice == "999":
                selected_sites = eligible_sites if run_availability_check else working_sites
            else:
                selected_indices = set()
                for part in choice.split(','):
                    part = part.strip()
                    if not part:
                        continue
                    if '-' in part:
                        try:
                            start, end = map(int, part.split('-'))
                            if start > end:
                                raise ValueError(f"Invalid range: {part}")
                            selected_indices.update(range(start - 1, end))
                        except:
                            raise ValueError(f"Invalid range format: '{part}'")
                    else:
                        idx = int(part) - 1
                        selected_indices.add(idx)

                if any(idx < 0 or idx >= len(all_display_sites) for idx in selected_indices):
                    raise ValueError("One or more indices are out of range.")

                selected_sites = [all_display_sites[idx] for idx in sorted(selected_indices)]

            if run_availability_check:
                sold_only = [s for s in selected_sites if s.get("is_sold_archive", False)]
                if sold_only:
                    names = ", ".join(s["json_desc"] for s in sold_only)
                    print(f"⚠️  The following are sold-only archives and cannot be used in availability mode:\n → {names}")
                    continue

                grouped_sites = defaultdict(list)
                for site in selected_sites:
                    grouped_sites[site['source_name']].append(site)
                return list(grouped_sites.values())

            else:
                return selected_sites

        except ValueError as e:
            print(f"❌ Invalid selection: {e}. Please try again.")


def is_running_on_ec2():
    """Detects if running on EC2 by checking hostname prefix."""
    hostname = socket.gethostname()
    return hostname.startswith("ip-")