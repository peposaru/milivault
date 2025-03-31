from datetime import datetime
import logging

class log_print:
    def create_log_header(self, message, width=60):
        border = '-' * width
        return f"\n{border}\n{message.center(width)}\n{border}"

    def newInstance(self, source, productsPage, runCycle, productsProcessed):
        current_datetime = datetime.now()
        self._log_header("NEW INSTANCE STARTED")
        self._log_lines([
            ("MILITARIA SITE", source),
            ("PRODUCTS URL", productsPage),
            ("CYCLES RUN", runCycle),
            ("PRODUCTS PROCESSED", productsProcessed),
            ("TIMESTAMP", current_datetime),
        ])

    def terminating(self, source, consecutiveMatches, targetMatch, runCycle, productsProcessed):
        current_datetime = datetime.now()
        self._log_header("INSTANCE TERMINATED")
        self._log_lines([
            ("MILITARIA SITE", source),
            ("CONSECUTIVE MATCHES", f"{consecutiveMatches} / {targetMatch}"),
            ("CYCLES RUN", runCycle),
            ("PRODUCTS PROCESSED", productsProcessed),
            ("TIMESTAMP", current_datetime),
        ])

    def sysUpdate(self, page, urlCount, consecutiveMatches, targetMatch, productUrl, updated):
        current_datetime = datetime.now()
        header = "PRODUCT UPDATED" if updated else "NO PRODUCT UPDATE"
        self._log_header(header)
        self._log_lines([
            ("CURRENT PAGE", page),
            ("PRODUCTS PROCESSED", urlCount),
            ("CONSECUTIVE MATCHES", f"{consecutiveMatches} / {targetMatch}"),
            ("PRODUCT URL", productUrl),
            ("TIMESTAMP", current_datetime),
        ])

    def newProduct(self, page, urlCount, title, productUrl, description, price, available):
        current_datetime = datetime.now()
        self._log_header("NEW PRODUCT FOUND")
        self._log_lines([
            ("CURRENT PAGE", page),
            ("PRODUCTS PROCESSED", urlCount),
            ("TITLE", title),
            ("PRODUCT URL", productUrl),
            ("DESCRIPTION", description),
            ("PRICE", price),
            ("AVAILABLE", available),
            ("TIMESTAMP", current_datetime),
        ])

    def standby(self):
        current_datetime = datetime.now()
        self._log_header("CYCLE COMPLETED")
        self._log_lines([
            ("PROCESS COMPLETED AT", current_datetime),
            ("STATUS", "STANDING BY FOR NEXT CYCLE..."),
        ])

    def final_summary(self, processed_sites, counter):
        current_datetime = datetime.now()
        self._log_header("FINAL PROCESSING SUMMARY")
        
        self._log_lines([
            ("TIMESTAMP", current_datetime),
            ("SITES PROCESSED", ", ".join(processed_sites)),
            ("TOTAL PRODUCTS", counter.get_total_count()),
            ("NEW PRODUCTS", counter.get_new_products_count()),
            ("OLD PRODUCTS", counter.get_old_products_count()),
            ("SITES COUNT", counter.get_sites_processed_count()),
            ("AVAILABILITY UPDATES", counter.get_availability_update_count()),
            ("PROCESSING REQUIRED", counter.get_processing_required_count()),
        ])

    # --- Internal Helper Methods ---
    def _log_header(self, message):
        logging.info(self.create_log_header(message))

    def _log_lines(self, kv_pairs):
        for label, value in kv_pairs:
            logging.info(f"{label:<22}: {value}")
