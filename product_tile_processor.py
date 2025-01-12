class TileProcessor:
    def __init__(self, site_profile):
        self.site_profile = site_profile
        self.site_profile_tile_selectors = site_profile.get("product_tile_selectors", {})

    def construct_categorized_product_urls(self, products_tile_list):
        categorized_urls = {"available": set(), "unavailable": set()}  # Use sets to ensure uniqueness

        for product_tile in products_tile_list:
            try:
                # Extract product URL
                details_url_config = self.parse_tile_config("details_url")
                product_url = self.extract_data_from_tile(product_tile, *details_url_config)

                if not product_url:
                    continue

                # Check availability and unavailability
                if self.is_product_available(product_tile, self.site_profile_tile_selectors):
                    categorized_urls["available"].add(product_url)
                elif self.is_product_unavailable(product_tile, self.site_profile_tile_selectors):
                    categorized_urls["unavailable"].add(product_url)
            except Exception as e:
                print(f"Error processing tile: {e}, Tile: {product_tile}")

        # Convert sets to lists for the final output
        return {
            "available": list(categorized_urls["available"]),
            "unavailable": list(categorized_urls["unavailable"]),
        }

    def parse_tile_config(self, selector_key):
        try:
            product_tile_selectors = self.site_profile_tile_selectors.get(selector_key, {})
            return (
                product_tile_selectors.get("method", "find"),
                product_tile_selectors.get("args", []),
                product_tile_selectors.get("kwargs", {}),
                product_tile_selectors.get("attribute")
            )
        except Exception as e:
            raise ValueError(f"Error parsing configuration for {selector_key}: {e}")

    def extract_data_from_tile(self, product_tile, method, args, kwargs, attribute):
        try:
            element = getattr(product_tile, method)(*args, **kwargs)
            if attribute:
                return element.get(attribute) if element else None
            return element
        except Exception as e:
            raise ValueError(f"Error extracting data from tile: {e}")
        
    def is_product_available(self, product_tile, site_profile_tile_selectors):
        try:
            # List of availability keys to check
            availability_keys = ["tile_availability"]

            for key in availability_keys:
                # Skip if the key is not in the selectors
                if key not in site_profile_tile_selectors:
                    continue

                config = site_profile_tile_selectors.get(key, {})
                method = config.get("method", "find")
                args = config.get("args", [])
                kwargs = config.get("kwargs", {})
                exists = config.get("exists", False)
                value = config.get("value", None)

                # Handle the 'has_attr' method
                if method == "has_attr" and "class" in args:
                    # Check if the class contains the specified value
                    attribute_value = product_tile.get("class", [])
                    if value in attribute_value:
                        return True
                    continue

                # Extract element based on other methods
                element = getattr(product_tile, method)(*args, **kwargs)
                if exists and element is not None:
                    return True  # Available if element exists

            return False  # No availability conditions matched
        except Exception as e:
            print(f"Error checking availability: {e}")
            return False
    
    def is_product_unavailable(self, product_tile, site_profile_tile_selectors):
        try:
            unavailability_keys = ["tile_unavailability_reserved", "tile_unavailability_sold"]

            for key in unavailability_keys:
                if key not in site_profile_tile_selectors:
                    continue

                config = site_profile_tile_selectors.get(key, {})
                method = config.get("method", "find")
                args = config.get("args", [])
                kwargs = config.get("kwargs", {})
                exists = config.get("exists", False)

                element = getattr(product_tile, method)(*args, **kwargs)
                if exists and element is not None:
                    return True

            return False
        except Exception as e:
            print(f"Error checking unavailability: {e}")
            return False



