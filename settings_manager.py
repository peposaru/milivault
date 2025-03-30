import os
import logging
import shutil

from aws_rds_manager import AwsRdsManager
from json_manager import JsonManager
from log_print_manager import log_print
from aws_s3_manager import S3Manager
from products_counter import ProductsCounter
from site_processor import SiteProcessor
from html_manager import HtmlManager

# Default Settings
DEFAULT_RDS_SETTINGS = {
      "infoLocation"  : "/home/ec2-user/projects/milivault/",
      "pgAdminCred"   : "/home/ec2-user/projects/milivault/credentials/pgadminCredentials.json",
      "selectorJson"  : "/home/ec2-user/projects/milivault/site-json/",
      "s3Cred"        : "/home/ec2-user/projects/milivault/credentials/s3_credentials.json"
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
            logging.error("SETTINGS MANAGER: Unexpected return value from get_user_settings(). Expected a tuple with four elements.")
            return None

        targetMatch, sleeptime, user_settings, run_availability_check, test_json = result

        if not isinstance(user_settings, dict):
            logging.error("SETTINGS MANAGER: user_settings is not a dictionary. Check get_user_settings() implementation.")
            return None

        user_settings.update({
            "targetMatch": targetMatch,
            "sleeptime": sleeptime,
            "run_availability_check": run_availability_check,
            "jsonTest": test_json
        })
        return user_settings
    
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
""")
        choice = input("Enter the number corresponding to your choice (1/2/3): ").strip()

        settings = {}

        if choice == '1':
            print("Using Amazon RDS Settings...")
            settings = DEFAULT_RDS_SETTINGS

        elif choice == '2':
            print("Using Personal Computer Settings...")
            settings = DEFAULT_PC_SETTINGS

        elif choice == '3':
            print("Custom settings selected.")
            # Prompt user for custom settings
            settings["infoLocation"]  = input("Enter the directory path for configuration files (e.g., /path/to/config/): ").strip()
            settings["pgAdminCred"]   = input("Enter the name of the pgAdmin credentials file (e.g., pgadminCredentials.json): ").strip()
            settings["selectorJsonFolder"]  = input("Enter the name of the JSON selector file (e.g., AWS_MILITARIA_SELECTORS.json): ").strip()
            settings["s3Cred"]        = input("Enter the name of the S3 credentials file (e.g., s3_credentials.json): ").strip()

            # Validate the directory exists
            if not os.path.exists(settings["infoLocation"]):
                print(f"Error: The directory {settings['infoLocation']} does not exist.")
                logging.error(f"SETTINGS MANAGER: Invalid directory entered: {settings['infoLocation']}")
                exit()

            return settings

        else:
            print("Invalid choice. Please enter 1, 2, or 3.")


        # Second question: Select check type
        print("""
Choose the type of inventory check:
1. New Inventory Check (pages_to_check = 1, sleeptime = 15 minutes)
2. Run Availability Check (Check and update product availability)
3. Custom Check (Enter your own pages_to_check and sleeptime)
4. Test JSON Profile
    """)
        check_choice = input("Enter your choice (1/2/3/4): ").strip()

        run_availability_check = False
        test_json = False
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
                sleeptime = int(input("Enter your desired sleeptime value (in seconds): ").strip())
            # Test JSON profile
            elif check_choice == '4':
                pages_to_check = None
                test_json = True
                sleeptime = None
            else:
                raise ValueError
        except ValueError:
            print("Invalid input. Defaulting to New Inventory Check.")
            logging.warning("Invalid input for inventory check. Defaulting to targetMatch=25, sleeptime=15 minutes.")
            targetMatch = 25
            sleeptime = 15 * 60

        return pages_to_check, sleeptime, settings, run_availability_check, test_json

# Below uses the source name to display the sites to the user.

# # Which sites does the user want to process?
# def site_choice(jsonData):
#     """Displays site choices in a column format and handles user selection."""
    
#     # Determine terminal width and calculate formatting
#     term_width = shutil.get_terminal_size((80, 20)).columns  # Default to 80 if size can't be determined
#     max_name_length = max(len(site['json_desc']) for site in jsonData)
#     padding = 5
#     col_width = max_name_length + padding
#     num_columns = max(1, term_width // col_width)  # Ensure at least 1 column
#     num_rows = (len(jsonData) + num_columns - 1) // num_columns  # Round up for uneven rows

#     # Display available sites in column format
#     print("\nAvailable sites:")
#     for row in range(num_rows):
#         row_sites = []
#         for col in range(num_columns):
#             idx = row + col * num_rows
#             if idx < len(jsonData):
#                 row_sites.append(f"{idx + 1:>3}. {jsonData[idx]['json_desc']:<{max_name_length}}")
#         print(" | ".join(row_sites))

#     # User selection loop
#     while True:
#         try:
#             choice = input("\nSelect sites to scrape (e.g., '1,3-5,7'): ").strip()
            
#             if not choice:
#                 print("Please enter a valid selection.")
#                 continue

#             selected_indices = set()
#             for part in choice.split(','):
#                 if '-' in part:
#                     start, end = map(int, part.split('-'))
#                     if start > end:
#                         raise ValueError(f"Invalid range: {start}-{end}")
#                     selected_indices.update(range(start - 1, end))  # Convert to 0-based index
#                 else:
#                     selected_indices.add(int(part) - 1)

#             # Ensure all selected indices are within valid range
#             if any(idx < 0 or idx >= len(jsonData) for idx in selected_indices):
#                 raise ValueError("One or more indices are out of range.")

#             selected_sites = [jsonData[idx] for idx in sorted(selected_indices)]
#             return selected_sites

#         except ValueError as e:
#             print(f"SETTINGS MANAGER: Invalid selection: {e}. Please try again.")


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