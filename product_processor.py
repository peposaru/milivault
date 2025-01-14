import logging

# This will handle the dictionary of data extracted from the tile on the products page tile.
class ProductTileDictProcessor:
    def __init__(self,site_profile,comparison_list):
        self.site_profile        = site_profile
        self.comparison_list = comparison_list

        
    def product_processor_main(self, tile_product_data_list):
        # Categorize products into old and new
        try:
            processing_required_list, availability_update_list = self.compare_tile_url_to_rds(tile_product_data_list)  # Remove extra self
        except Exception as e:
            logging.error(f"compare_tile_url_to_rds: {e}")
        return processing_required_list, availability_update_list

        

    # If in rds, compare availability status, title, price and update if needed
    # If not in rds, create
    def compare_tile_url_to_rds(self, tile_product_data_list):
        processing_required_list = []  # Products needing full processing
        availability_update_list = []  # Products needing only availability updates

        for tile_product_dict in tile_product_data_list:
            url       = tile_product_dict['url']
            title     = tile_product_dict['title']
            price     = tile_product_dict['price']
            available = tile_product_dict['available']

            # Check if the URL exists in the comparison dictionary
            if url in self.comparison_list:
                try:
                    # Unpack database values
                    db_title, db_price, db_available = self.comparison_list[url]
                except ValueError as e:
                    logging.error(f"Error unpacking comparison data for URL {url}: {e}")
                    continue

                # Check for exact matches of title, price, and availability
                if title == db_title and price == db_price:
                    if available != db_available:
                        # Only availability differs, add to availability update list
                        availability_update_list.append({
                            "url": url,
                            "available": available
                        })
                    continue  # Skip exact matches if nothing else differs
                else:
                    # Add non-matching products to the full processing list
                    processing_required_list.append(tile_product_dict)
            else:
                # Add new products not in the database to the full processing list
                processing_required_list.append(tile_product_dict)

        logging.info(f'Products needing further processing: {len(processing_required_list)}')
        logging.info(f'Products needing availability updates: {len(availability_update_list)}')
        return processing_required_list, availability_update_list



    def new_product_processor():
        return
    
    def old_product_processor():
        return

    def source_name ():
        return

    def base_url ():
        return

    def tile_title_scrape ():
        
        return

    def tile_description_scrape ():
        return



#class ProductDetailsProcessor:
    


