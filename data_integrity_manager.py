# This will be for checking various data points within the militaria database. If the S3 image is missing, download it, etc.
 
import logging
import post_processors as post_processors
from product_tile_processor import TileProcessor
from site_processor import SiteProcessor

import random
import time
import json
import requests
from collections import defaultdict

class DataIntegrityManager:
    def __init__(self, db, s3_client):
        self.db = db
        self.s3_client = s3_client
        self.logger = logging.getLogger(__name__)

    def check_data_integrity(self):
        """
        Download missing S3 images in batches.
        This process avoids server overload by controlling batch size and delay.
        """

        # USER-CONTROLLED SETTINGS 👇👇👇
        batch_size = 10                # 💡 Number of products to process per batch
        delay_between_products = 2   # 💡 Delay in seconds between each product's image downloads
        max_batches = None             # 💡 Optional: set to a number like 5 to stop after X batches (None = no limit)
        # 👆👆👆 CHANGE THESE VALUES TO CONTROL DOWNLOAD SPEED

        start_time = time.time()
        total_products = 0
        total_images = 0
        sites_touched = set()
        images_per_product = defaultdict(int)

        offset = 0
        batch_number = 0

        while True:
            if max_batches is not None and batch_number >= max_batches:
                self.logger.info("Reached max_batches limit. Stopping.")
                break

            self.logger.info(f"📦 Fetching batch #{batch_number + 1} (offset={offset})")
            try:
                rows = self.db.get_missing_s3_images(batch_size=batch_size, offset=offset)
            except Exception as e:
                self.logger.warning(f"DB fetch failed at offset {offset}, attempting reconnect: {e}")
                if hasattr(self.db, "reconnect"):
                    self.db.reconnect()
                    rows = self.db.get_missing_s3_images(batch_size=batch_size, offset=offset)
                else:
                    raise


            if not rows:
                self.logger.info("✅ No more rows to process. Done.")
                break

            for row in rows:
                product_id, product_url, site, original_image_json = row

                if not original_image_json:
                    self.logger.warning(f"Skipping row {product_id}: original_image_urls is null or empty")
                    continue

                try:
                    image_urls = json.loads(original_image_json)
                except json.JSONDecodeError:
                    try:
                        # Safe fallback if wrapped in quotes or escaped
                        image_urls = json.loads(json.loads(original_image_json))
                    except Exception:
                        self.logger.warning(f"Skipping row {product_id}: invalid JSON in original_image_urls")
                        continue

                if not image_urls:
                    self.logger.info(f"Skipping row {product_id}: no original image URLs")
                    continue

                self.logger.info(f"📌 Product URL: {product_url}")
                self.logger.info(f"🖼️ Downloading {len(image_urls)} images")
                self.logger.info(f"➡️ First image: {image_urls[0]}")

                self.download_and_upload_images(product_id, site, image_urls)

                # 👇 Track stats
                total_products += 1
                total_images += len(image_urls)
                images_per_product[product_id] = len(image_urls)
                sites_touched.add(site)

                # USER-CONTROLLED DELAY
                self.logger.info(f"⏳ Waiting {delay_between_products} seconds before next product...")
                time.sleep(delay_between_products)

            elapsed = round(time.time() - start_time, 2)
            estimated_remaining = "unknown"
            eta_text = "n/a"

            # Estimate based on total product count
            try:
                estimated_total_rows = self.db.count_missing_s3_image_products()
                remaining = estimated_total_rows - total_products
                rate = elapsed / max(total_products, 1)  # seconds per product

                if remaining > 0:
                    eta_seconds = remaining * rate
                    minutes, sec = divmod(int(eta_seconds), 60)
                    hours, minutes = divmod(minutes, 60)
                    days, hours = divmod(hours, 24)
                    eta_parts = []
                    if days:
                        eta_parts.append(f"{days}d")
                    if hours:
                        eta_parts.append(f"{hours}h")
                    if minutes:
                        eta_parts.append(f"{minutes}m")
                    eta_text = " ".join(eta_parts)
                    estimated_remaining = f"{remaining} products"
                else:
                    estimated_remaining = "0"
                    eta_text = "0m"
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to calculate ETA: {e}")

            # Final summary
            self.logger.warning("📊 INTEGRITY SUMMARY")
            self.logger.warning(f"✔️ Products processed       : {total_products}")
            self.logger.warning(f"🖼️ Total images uploaded    : {total_images}")
            self.logger.warning(f"🏷️ Sites touched            : {len(sites_touched)} ({', '.join(sorted(sites_touched))})")
            self.logger.warning(f"⏱️ Elapsed time             : {elapsed} seconds")
            self.logger.warning(f"📦 Estimated remaining      : {estimated_remaining}")
            self.logger.warning(f"📅 Estimated time remaining : {eta_text}")

            offset += batch_size
            batch_number += 1



    def download_and_upload_images(self, product_id, site, image_urls):
        """
        Uploads all images for a product using S3Manager, and updates the database with resulting S3 URLs.
        Includes graceful DB reconnection if needed.
        """
        try:
            # Upload images using centralized logic in S3Manager
            s3_urls = self.s3_client.upload_images_for_product(
                product_id=product_id,
                image_urls=image_urls,
                site_name=site,
                product_url=None
            )

            # Only update DB if upload succeeded
            if s3_urls:
                update_query = """
                    UPDATE militaria
                    SET s3_image_urls = %s
                    WHERE id = %s;
                """
                try:
                    self.db.execute(update_query, (json.dumps(s3_urls), product_id))
                    self.logger.info(f"🗃️ Updated DB for product {product_id} with {len(s3_urls)} image(s)")
                except Exception as db_err:
                    self.logger.warning(f"⚠️ DB update failed for product {product_id}, retrying after reconnect: {db_err}")
                    if hasattr(self.db, "reconnect"):
                        self.db.reconnect()
                        self.db.execute(update_query, (json.dumps(s3_urls), product_id))
                        self.logger.info(f"🔁 Retried DB update for product {product_id} after reconnect.")
                    else:
                        raise

            else:
                self.logger.warning(f"⚠️ No images uploaded for product {product_id}")

        except Exception as e:
            self.logger.error(f"❌ Failed image upload or DB update for product {product_id}: {e}")


    def format_duration(seconds):
        minutes, sec = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)
        if days:
            return f"{days}d {hours}h {minutes}m"
        elif hours:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"

    def safe_db_execute(self, query, params=None):
        try:
            self.db.execute(query, params)
        except Exception as e:
            self.logger.warning(f"DB execute failed, trying reconnect: {e}")
            self.db.reconnect()
            self.db.execute(query, params)

    def safe_db_fetch(self, query, params=None):
        try:
            return self.db.fetch(query, params)
        except Exception as e:
            self.logger.warning(f"DB fetch failed, trying reconnect: {e}")
            self.db.reconnect()
            return self.db.fetch(query, params)



class ImageRecoveryProcessor:
    def __init__(self, managers):
        self.rds = managers["rdsManager"]
        self.s3 = managers["s3_manager"]
        self.html = managers["html_manager"]
        self.json = managers["jsonManager"]
        self.sleep_between = 5  # adjustable delay in seconds

    def recover_images(self, batch_size=10):
        # 1. Get up to N products with missing images
        query = """
        SELECT url, site
        FROM militaria
        WHERE (
            original_image_urls IS NULL OR original_image_urls = '[]'
        ) AND (
            s3_image_urls IS NULL OR s3_image_urls = '[]'
        )
        LIMIT 100;
        """
        records = self.rds.fetch(query)

        # 2. Track which sites we've already touched in this round
        used_sites = set()

        for url, site in records:
            if site in used_sites:
                continue  # skip if already used this site in this round

            # 3. Download and upload logic here...
            # - get soup
            # - extract image URLs
            # - upload with `self.s3.upload_images_for_product(...)`

            used_sites.add(site)

            # Reset after batch
            if len(used_sites) >= batch_size:
                used_sites.clear()
                logging.info(f"Sleeping {self.sleep_between} sec before next image batch...")
                time.sleep(self.sleep_between)
