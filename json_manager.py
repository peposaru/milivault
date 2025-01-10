import logging, json

class JsonManager:
    def load_json_selectors(self, selectorJson):
        """
        Load and validate the JSON selectors file.
        """
        try:
            logging.info(f"Attempting to load JSON selectors from: {selectorJson}")
            with open(selectorJson, 'r') as userFile:
                jsonData = json.load(userFile)
            logging.info(f"Successfully loaded JSON selectors from: {selectorJson}")
            return jsonData
        except FileNotFoundError:
            logging.error(f"JSON selector file not found: {selectorJson}")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON selector file: {e}")
            raise


    def jsonSelectors(self, militariaSite):
        """Safely unpack JSON site profile into expected fields, ignoring unwanted keys."""
        try:
            base_url            = militariaSite['base_url']
            source              = militariaSite['source']
            pageIncrement       = militariaSite['page_increment']
            currency            = militariaSite['currency']
            products            = militariaSite['products']
            productUrlElement   = militariaSite['product_url_element']
            titleElement        = militariaSite['title_element']
            descElement         = militariaSite['desc_element']
            priceElement        = militariaSite['price_element']
            availableElement    = militariaSite['available_element']
            conflict            = militariaSite['conflict_element']
            nation              = militariaSite['nation_element']
            item_type           = militariaSite['item_type_element']
            grade               = militariaSite['grade_element']
            productsPageUrl     = militariaSite['productsPageUrl']
            
            # Handle image_element: Treat empty string or placeholder as None
            imageElement = militariaSite.get('image_element', None)
            if imageElement in ["", "skip", "none"]:  # Add any placeholders here
                imageElement = None

            # Return only the required fields
            return (
                conflict, nation, item_type, grade, source, pageIncrement, currency, products,
                productUrlElement, titleElement, descElement, priceElement, availableElement,
                productsPageUrl, base_url, imageElement
            )
        except KeyError as e:
            logging.error(f"Missing key in JSON selectors: {e}")
            raise
        except Exception as e:
            logging.error(f"Error unpacking JSON selectors: {e}")
            raise

    # This makes sure that the json profile has the required elements.
    def validate_json_profile(self,militariaSite):
        """Validate required keys in JSON profile."""
        all_keys = {
            "source",
            "product_url_element",
            "productsPageUrl",
            "base_url",
            "page_increment",
            "currency",
            "products",
            "title_element",
            "desc_element",
            "price_element",
            "available_element",
            "image_element",
            "conflict_element",
            "nation_element",
            "item_type_element",
            "grade_element"
        }

        optional_keys = {"conflict_element", "nation_element", "item_type_element", "grade_element"}
        required_keys = all_keys - optional_keys

        # Determine missing keys by comparing the JSON keys to the required keys
        missing_keys = required_keys - set(militariaSite.keys())
        if missing_keys:
            logging.error(f"Missing required keys: {missing_keys}")
            raise ValueError(f"Missing required keys in JSON profile: {missing_keys}")

    def load_and_validate_selectors(self, selector_path):
        """
        Load and validate JSON selectors from the given file path.

        Args:
            selector_path (str): The file path to the JSON selector file.

        Returns:
            dict: The loaded JSON data.

        Raises:
            Exception: For unexpected errors during JSON loading.
        """
        try:
            jsonData = self.load_json_selectors(selector_path)
            return jsonData
        except Exception as e:
            logging.error(f"Error loading or validating JSON selectors: {e}")
            raise

