# Product Counting Module
class ProductsCounter:
    def __init__(self):
        self.total_products_count      = 0  
        self.old_products_count        = 0   
        self.new_products_count        = 0   
        self.sites_processed_count     = 0
        self.availability_update_count = 0
        self.processing_required_count = 0
        self.current_page_count        = 0
        self.empty_page_count          = 0
        self.empty_page_tolerance      = 5
        self.continue_state            = None

    def get_current_continue_state(self):
        return self.continue_state
    
    def set_continue_state_false(self):
        self.continue_state = False

    def set_continue_state_true(self):
        self.continue_state = True

    # Total Products Count
    def get_total_count(self):
        """Retrieve the total products count."""
        return self.total_products_count

    def update_total_products_count(self):
        """Update the total products count based on old and new products."""
        self.total_products_count = self.old_products_count + self.new_products_count

    def increment_total_products_count(self, count=1):
        """Increment the total products count directly (rare use case)."""
        self.total_products_count += count

    def reset_total_products_count(self):
        """Reset the total products count to zero."""
        self.total_products_count = 0

    # New Products Count
    def get_new_products_count(self):
        """Retrieve the new products count."""
        return self.new_products_count

    def add_new_product_count(self, count=1):
        """Add to the count of new products."""
        self.new_products_count += count
        self.update_total_products_count()

    def reset_new_products_count(self):
        """Reset the new products count to zero."""
        self.new_products_count = 0

    # Old Products Count
    def get_old_products_count(self):
        """Retrieve the old products count."""
        return self.old_products_count

    def add_old_product_count(self, count=1):
        """Add to the count of old products."""
        self.old_products_count += count
        self.update_total_products_count()

    def reset_old_products_count(self):
        """Reset the old products count to zero."""
        self.old_products_count = 0

    # Sites Processed Count
    def get_sites_processed_count(self):
        """Retrieve the sites processed count."""
        return self.sites_processed_count

    def add_sites_processed_count(self, count=1):
        """Add to the count of sites processed."""
        self.sites_processed_count += count

    def reset_sites_processed_count(self):
        """Reset the sites processed count to zero."""
        self.sites_processed_count = 0

    # Current Page Count
    def get_current_page_count(self):
        """Retrieve the current page count."""
        return self.current_page_count

    def add_current_page_count(self, count=1):
        """Add to the current page count."""
        self.current_page_count += count

    def reset_current_page_count(self):
        """Reset the current page count to zero."""
        self.current_page_count = 0

    # How many empty pages until skipping to the next site
    def set_empty_page_tolerance(self, count=2):
        self.empty_page_tolerance = count

    def add_empty_page_count(self, count=1):
        self.empty_page_count += count

    def reset_empty_page_count(self):
        self.empty_page_count = 0

    def get_empty_page_count(self):
        return self.empty_page_count
    
    def get_empty_page_tolerance(self):
        return self.empty_page_tolerance
    
    def check_empty_page_tolerance(self):
        if self.empty_page_count == self.get_empty_page_tolerance():
            self.set_continue_state_false()

    # Update availability_update_count
    def get_availability_update_count(self):
        """Retrieve the current page count."""
        return self.availability_update_count

    def add_availability_update_count(self, count=1):
        """Add to the current page count."""
        self.availability_update_count += count

    # Update processing_required_count
    def get_processing_required_count(self):
        """Retrieve the current page count."""
        return self.processing_required_count

    def add_processing_required_count(self, count=1):
        """Add to the current page count."""
        self.processing_required_count += count
        


