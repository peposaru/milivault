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
    "infoLocation"        : r'/home/ec2-user/projects/AWS-Militaria-Scraper/',
    "pgAdminCred"         : 'pgadminCredentials.json',
    "selectorJsonFolder"  : 'AWS_MILITARIA_SELECTORS.json',
    "s3Cred"              : 's3_credentials.json'
}

DEFAULT_PC_SETTINGS = {
    "infoLocation"       : r'C:/Users/keena/Desktop/Milivault/scraper',
    "pgAdminCred"        : r'C:/Users/keena/Desktop/Milivault/credentials/pgadmin_credentials.json',
    "selectorJsonFolder" : r'C:/Users/keena/Desktop/Milivault/site-json/',
    "s3Cred"             : r'C:/Users/keena/Desktop/Milivault/credentials/s3_credentials.json'
}

def load_user_settings():
    try:
        targetMatch, sleeptime, user_settings, run_availability_check = get_user_settings()
        user_settings.update({
            "targetMatch": targetMatch,
            "sleeptime": sleeptime,
            "run_availability_check": run_availability_check
        })
        return user_settings
    except KeyError as e:
        logging.error(f"Error accessing user settings: {e}")
        return None

def setup_user_path(user_settings):
    # Switching to designated info / credentials location.
    try:
        os.chdir(user_settings["infoLocation"])
        logging.info(f"Changed directory to {user_settings['infoLocation']}")
    except Exception as e:
        logging.error(f"Error changing directory: {e}")
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
        logging.error(f"Error setting up object managers: {e}")
        raise


def get_user_settings():
    """
    Prompt user to select settings for infoLocation, pgAdmin credentials, and selector JSON file.
    Returns:
        - sleeptime (int): The sleep time between cycles (in seconds).
        - settings (dict): A dictionary with keys 'infoLocation', 'pgAdminCred', 'selectorJsonFolder'.
        - run_availability_check (bool): Indicates whether the user wants to run the availability check.
    """
    # First question: Choose settings
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
        settings["s3Cred"]        = input("Enter the name of the s3 credentials file (e.g., s3_credentials.json): ").strip()

        # Validate the directory exists
        if not os.path.exists(settings["infoLocation"]):
            print(f"Error: The directory {settings['infoLocation']} does not exist.")
            logging.error(f"Invalid directory entered: {settings['infoLocation']}")
            exit()

    else:
        print("Invalid choice. Exiting program.")
        exit()

    # Second question: Select check type
    print("""
Choose the type of inventory check:
1. New Inventory Check (pages_to_check = 1, sleeptime = 15 minutes)
2. Run Availability Check (Check and update product availability)
3. Custom Check (Enter your own pages_to_check and sleeptime)
""")
    check_choice = input("Enter your choice (1/2/3): ").strip()

    run_availability_check = False
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
        else:
            raise ValueError
    except ValueError:
        print("Invalid input. Defaulting to New Inventory Check.")
        logging.warning("Invalid input for inventory check. Defaulting to targetMatch=25, sleeptime=15 minutes.")
        targetMatch = 25
        sleeptime = 15 * 60

    return pages_to_check, sleeptime, settings, run_availability_check


# Which sites does the user want to process?
def site_choice(jsonData):
    # Determine terminal width and maximum site name length
    term_width = shutil.get_terminal_size().columns
    max_name_length = max(len(site['source_name']) for site in jsonData)
    padding = 5  # Padding for spacing
    col_width = max_name_length + padding
    num_columns = term_width // col_width  # Fit as many columns as the terminal width allows
    num_rows = (len(jsonData) + num_columns - 1) // num_columns

    print("Available sites:")
    for row in range(num_rows):
        row_sites = []
        for col in range(num_columns):
            idx = row + col * num_rows
            if idx < len(jsonData):
                # Format each column with uniform width
                row_sites.append(f"{idx + 1:>3}. {jsonData[idx]['source_name']:<{max_name_length}}")
        print(" | ".join(row_sites))  # Join columns with a separator

    try:
        choice = input("Select sites to scrape (e.g., '1,3-5,7'): ")
        selected_indices = set()
        for part in choice.split(','):
            if '-' in part:
                start, end = map(int, part.split('-'))
                selected_indices.update(range(start - 1, end))
            else:
                selected_indices.add(int(part) - 1)

        selected_indices = sorted(selected_indices)
        if any(idx < 0 or idx >= len(jsonData) for idx in selected_indices):
            raise ValueError("One or more indices are out of range.")

        selected_sites = [jsonData[idx] for idx in selected_indices]
        return selected_sites
    except ValueError as e:
        print(f"Invalid selection: {e}")
        return