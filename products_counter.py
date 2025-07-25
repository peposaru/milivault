import logging

class ProductsCounter:
    def __init__(self):
        self.reset_all_counts()
        # empty_page_tolerance has been replaced by targetMatch but need to double check it first
        # self.empty_page_tolerance = 5
        self.continue_state = None

    def reset_all_counts(self):
        self.total_products_count = 0
        self.old_products_count = 0
        self.new_products_count = 0
        self.sites_processed_count = 0
        self.availability_update_count = 0
        self.processing_required_count = 0
        self.price_update_count = 0            # newly added counter
        self.current_page_count = 0
        self.empty_page_count = 0

    # --- Continue State (unchanged) ---
    def get_current_continue_state(self):
        return self.continue_state

    def set_continue_state_false(self):
        self.continue_state = False

    def set_continue_state_true(self):
        self.continue_state = True

    # --- Total Count Logic (unchanged externally) ---
    def get_total_count(self):
        return self.total_products_count

    def update_total_products_count(self):
        self.total_products_count = self.old_products_count + self.new_products_count

    def increment_total_products_count(self, count=1):
        self.total_products_count += count

    def reset_total_products_count(self):
        self.total_products_count = 0

    # --- New Products ---
    def get_new_products_count(self):
        return self.new_products_count

    def add_new_product_count(self, count=1):
        self.new_products_count += count
        self.update_total_products_count()

    def reset_new_products_count(self):
        self.new_products_count = 0

    # --- Old Products ---
    def get_old_products_count(self):
        return self.old_products_count

    def add_old_product_count(self, count=1):
        self.old_products_count += count
        self.update_total_products_count()

    def reset_old_products_count(self):
        self.old_products_count = 0

    # --- Sites ---
    def get_sites_processed_count(self):
        return self.sites_processed_count

    def add_sites_processed_count(self, count=1):
        self.sites_processed_count += count

    def reset_sites_processed_count(self):
        self.sites_processed_count = 0

    # --- Pages ---
    def get_current_page_count(self):
        return self.current_page_count

    def add_current_page_count(self, count=1):
        self.current_page_count += count
        logging.debug(f"**********************Current page count updated: {self.current_page_count}**********************")

    def reset_current_page_count(self):
        self.current_page_count = 0

    def set_empty_page_tolerance(self, count=2):
        self.empty_page_tolerance = count

    def add_empty_page_count(self, count=1):
        self.empty_page_count += count

    def reset_empty_page_count(self):
        self.empty_page_count = 0

    def get_empty_page_count(self):
        return self.empty_page_count

    # --- Availability & Processing ---
    def get_availability_update_count(self):
        return self.availability_update_count

    def add_availability_update_count(self, count=1):
        self.availability_update_count += count

    def get_processing_required_count(self):
        return self.processing_required_count

    def add_processing_required_count(self, count=1):
        self.processing_required_count += count

    # --- Price Updates (new) ---
    def get_price_update_count(self):
        return self.price_update_count

    def add_price_update_count(self, count=1):
        self.price_update_count += count

    def add_skipped_sold_item(self):
        if not hasattr(self, 'skipped_sold'):
            self.skipped_sold = 0
        self.skipped_sold += 1