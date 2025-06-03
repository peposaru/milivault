import logging
import json
import os

class JsonManager:
    def compile_json_profiles(self, base_directory):
        """
        Read all JSON profiles from alphabetical folders and compile them into a list.
    
        Args:
            base_directory (str): The base directory containing JSON profile files.

        Returns:
            list: A list of compiled JSON profiles.
        """
        json_profiles = []
        try:
            for root, _, files in os.walk(base_directory):
                for file in files:
                    if file.endswith(".json"):
                        file_path = os.path.join(root, file)
                        with open(file_path, "r") as f:
                            try:
                                json_data = json.load(f)
                                json_profiles.append(json_data)
                            except json.JSONDecodeError as e:
                                logging.error(f"Error decoding JSON file {file_path}: {e}")
        except Exception as e:
            logging.error(f"An error occurred while compiling JSON profiles: {e}")

        return json_profiles

    def json_unpacker(self, selected_site):
        """
        Dynamically load a site profile from a JSON structure.

        Args:
            selected_site (dict): The JSON structure for the selected site.

        Returns:
            dict: The unpacked site profile.
        """
        try:
            site_profile = {
                "source_name": selected_site.get("source_name"),
                "access_config": {
                    "base_url": selected_site.get("access_config", {}).get("base_url"),
                    "products_page_path": selected_site.get("access_config", {}).get("products_page_path"),
                    "currency_code": selected_site.get("access_config", {}).get("currency_code"),
                    "page_increment_step": selected_site.get("access_config", {}).get("page_increment_step"),
                },
                "product_tile_selectors": selected_site.get("product_tile_selectors", {}),
                "product_details_selectors": selected_site.get("product_details_selectors", {}),
                "metadata_selectors": selected_site.get("metadata_selectors", {}),
                "additional_properties": selected_site.get("additional_properties", {})
            }
            return site_profile
        except KeyError as e:
            logging.error(f"Missing key in JSON profile: {e}")
            raise
        except Exception as e:
            logging.error(f"Error loading site profile: {e}")
            raise

    def compile_working_site_profiles(self, directory_path):
        profiles = []
        for filename in os.listdir(directory_path):
            if not filename.endswith(".json"):
                continue

            filepath = os.path.join(directory_path, filename)

            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                logging.error(f"Error decoding JSON file {filepath}: {e}")
                continue

            if not data.get("is_working", True):
                logging.info(f"⏩ Skipping {filename}: marked as not working")
                continue

            if "source_name" not in data:
                logging.warning(f"⛔ Skipping {filename}: missing source_name")
                continue

            profiles.append(data)

        logging.info(f"✅ Loaded {len(profiles)} working site profiles from {directory_path}")
        return profiles
