# Product Counting Module
class ProductsCounter:
    def __init__(self):
        self.total_products_count  = 0  
        self.old_products_count    = 0   
        self.new_products_count    = 0   
        self.sites_processed_count = 0
        self.current_page_count    = 0

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


