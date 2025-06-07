# This will be for checking various data points within the militaria database. If the S3 image is missing, download it, etc.
 
import logging
import post_processors as post_processors
from product_tile_processor import TileProcessor
from site_processor import SiteProcessor
import image_extractor

import random
import time
import json
import requests
from collections import defaultdict

class DataIntegrityManager:
    def __init__(self, managers):
        self.managers     = managers
        self.rds_manager = managers.get("rdsManager")
        self.s3_manager   = managers.get("s3_manager")
        selector_path = self.managers['user_settings']['selectorJsonFolder']
        self.db = self.rds_manager
        self.logger       = logging.getLogger(__name__)

    def run_submenu(self):
        print("""
    DATA INTEGRITY MENU
    1. Recover missing images
    2. Generate thumbnails from first S3 image
    3. Recover datapoints with URL
    (Press Enter to exit)
        """)
        choice = input("Select an option: ").strip()

        if choice == "1":
            tool = ImageRecoveryProcessor(self.managers)
            tool.recover_images()
        elif choice == "2":
            tool = ThumbnailGenerator(self.db, self.s3_manager)
            tool.generate()
        elif choice == "3":
            tool = DataPointRecoverer(self.db, self.s3_manager)
            tool.recover()
        else:
            print("Exited integrity submenu.")

    def check_data_integrity(self):
        """
        Download missing S3 images in batches.
        This process avoids server overload by controlling batch size and delay.
        """

        # USER-CONTROLLED SETTINGS 👇👇👇
        print("\n🛠️ Data Integrity Configuration")
        try:
            batch_size = int(input("Batch size (default = 10): ").strip() or 10)
        except ValueError:
            batch_size = 10

        try:
            delay_between_products = int(input("Delay between products in seconds (default = 2): ").strip() or 2)
        except ValueError:
            delay_between_products = 2

        max_batches_input = input("Max batches to run (press Enter for unlimited): ").strip()
        max_batches = int(max_batches_input) if max_batches_input.isdigit() else None

        self.logger.info(f"Running data integrity with: batch_size={batch_size}, delay={delay_between_products}, max_batches={max_batches or '∞'}")

        successful_products = 0
        failed_products = 0

        start_time = time.time()
        total_products = 0
        total_images = 0
        sites_touched = set()
        images_per_product = defaultdict(int)

        offset = 0
        batch_number = 0

        while True:
    # 🩺 Check DB connection health before each batch
            try:
                self.db.fetch("SELECT 1;")
            except Exception as e:
                self.logger.warning(f"DB ping failed, attempting reconnect: {e}")
                if hasattr(self.db, "reconnect"):
                    self.db.reconnect()
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

                # 👇 Track success/failure
                try:
                    result = self.db.fetch("SELECT s3_image_urls FROM militaria WHERE id = %s;", (product_id,))
                    if result and result[0][0] and len(result[0][0]) > 0:
                        successful_products += 1
                    else:
                        failed_products += 1
                except Exception as e:
                    self.logger.warning(f"⚠️ Unable to verify upload for product {product_id}: {e}")
                    failed_products += 1


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
            self.logger.warning(f"✅ Products with successful uploads: {successful_products}")
            self.logger.warning(f"❌ Products that failed to upload   : {failed_products}")
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
            s3_urls = self.s3_manager.upload_images_for_product(
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
                try:
                    self.db.execute(
                        "UPDATE militaria SET image_download_failed = TRUE WHERE id = %s;",
                        (product_id,)
                    )
                    self.logger.info(f"📛 Marked product {product_id} as failed to download images")
                except Exception as e:
                    self.logger.error(f"❌ Failed to update image_download_failed for product {product_id}: {e}")


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
        self.managers = managers
        self.rds_manager = managers.get('rdsManager')
        self.s3 = managers.get("s3_manager")
        self.html = managers.get('html_manager')
        self.json = managers.get("jsonManager")
        self.sleep_between = 5
        self.selector_path = self.managers["user_settings"]["selectorJsonFolder"]

    def recover_images(self, batch_size=10):
        site_profiles = self.json.compile_working_site_profiles(self.selector_path)
        working_sites = [p["source_name"] for p in site_profiles]
        site_filter = "('" + "', '".join(working_sites) + "')"

        while True:
            # Count remaining products needing image recovery
            count_query = f"""
            SELECT COUNT(*)
            FROM militaria
            WHERE (
                original_image_urls IS NULL OR original_image_urls = '[]'
            ) AND (
                s3_image_urls IS NULL OR s3_image_urls = '[]'
            )
            AND image_download_failed IS false
            AND site IN {site_filter};
            """
            remaining = self.rds_manager.fetch(count_query)[0][0]
            if remaining == 0:
                logging.info("🎉 All image recovery complete.")
                break

            estimated_minutes = (remaining / batch_size) * self.sleep_between / 60
            logging.info(f"📦 {remaining} products still need image recovery.")
            logging.info(f"⏳ Estimated time to complete: {estimated_minutes:.1f} minutes assuming full batches.")

            query = f"""
            SELECT url, site, id
            FROM militaria
            WHERE (
                original_image_urls IS NULL OR original_image_urls = '[]'
            ) AND (
                s3_image_urls IS NULL OR s3_image_urls = '[]'
            )
            AND image_download_failed IS false
            AND site IN {site_filter}
            LIMIT 100;
            """
            records = self.rds_manager.fetch(query)
            logging.info(f"🛠️ Attempting image recovery for {len(records)} records...")

            used_sites = set()

            for url, site, product_id in records:
                if site in used_sites:
                    logging.info(f"⚠️ Skipping {site}: already processed in this batch.")
                    continue

                logging.info(f"\n🔍 Processing product URL: {url} (site: {site})")
                site_profile = next((p for p in site_profiles if p.get("source_name") == site), None)

                if not site_profile:
                    logging.warning(f"❌ No site profile found for {site}. Skipping.")
                    continue

                extractor_func_name = site_profile.get("product_details_selectors", {}).get("details_image_url", {}).get("function")
                if not extractor_func_name:
                    logging.warning(f"❌ No image extractor function defined for {site}.")
                    continue

                if not hasattr(self.s3, "upload_images_for_product"):
                    logging.error("❌ Missing method 'upload_images_for_product' in S3 manager.")
                    break

                try:
                    soup = self.html.parse_html(url)
                    if not soup:
                        logging.warning(f"❌ Could not fetch or parse HTML for: {url}")
                        continue
                except Exception as e:
                    logging.warning(f"❌ Exception while fetching/parsing HTML for {url}: {e}")
                    continue

                try:
                    image_func = getattr(image_extractor, extractor_func_name)
                    image_urls = image_func(soup)
                    if not image_urls:
                        logging.warning(f"🚫 No images extracted for {url} using {extractor_func_name}")
                        continue

                    logging.info(f"🖼️ Extracted {len(image_urls)} image(s) for {url}. Uploading...")

                    s3_urls = self.s3.upload_images_for_product(product_id, image_urls, site, url, self.rds_manager)

                    if s3_urls:
                        update_query = """
                            UPDATE militaria
                            SET original_image_urls = %s, s3_image_urls = %s
                            WHERE url = %s;
                        """
                        self.rds_manager.execute(update_query, (
                            json.dumps(image_urls),
                            json.dumps(s3_urls),
                            url
                        ))
                        logging.info(f"✅ Successfully uploaded and updated DB for {url}")
                    else:
                        logging.warning(f"⚠️ Upload failed for {url} — no S3 URLs returned.")
                        self.mark_image_failed(url)

                except Exception as e:
                    logging.error(f"❌ Exception during image extraction/upload for {url}: {e}")
                    if image_urls:
                        self.mark_image_failed(url)

                used_sites.add(site)

                if len(used_sites) >= batch_size:
                    used_sites.clear()
                    logging.info(f"⏸️ Sleeping {self.sleep_between} sec before next batch...")
                    time.sleep(self.sleep_between)

            logging.info("🎉 Image recovery loop completed.")


    def mark_image_failed(self, url):
        try:
            self.rds_manager.execute(
                "UPDATE militaria SET image_download_failed = TRUE WHERE url = %s;",
                (url,)
            )
            logging.info(f"🛑 Marked {url} as failed in database.")
        except Exception as e:
            logging.error(f"❌ Could not mark {url} as failed: {e}")



class ThumbnailGenerator:
    def __init__(self, db, s3_manager):
        self.db = db
        self.s3 = s3_manager
        self.logger = logging.getLogger(__name__)

    def generate(self, limit=100000):
        self.logger.info("📸 Starting thumbnail generation")
        success = 0
        fail = 0
        processed_ids = []

        query = f"""
        SELECT id, site, s3_image_urls
        FROM militaria
        WHERE s3_image_urls IS NOT NULL AND s3_image_urls <> '[]'
          AND (s3_first_image_thumbnail IS NULL OR s3_first_image_thumbnail = '')
        ORDER BY id DESC
        LIMIT {limit};
        """

        try:
            rows = self.db.fetch(query)
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch products for thumbnail generation: {e}")
            return

        if not rows:
            self.logger.info("✅ No eligible products found for thumbnail generation.")
            return

        for product_id, site, s3_urls_json in rows:
            processed_ids.append(product_id)

            try:
                # Parse image URL list
                urls = s3_urls_json if isinstance(s3_urls_json, list) else json.loads(s3_urls_json)
                if not urls:
                    self.logger.warning(f"🚫 No images in s3_image_urls for product {product_id}")
                    fail += 1
                    continue

                first_url = urls[0]
                object_name = f"{site}/{product_id}/{product_id}-thumb.jpg"

                # Attempt thumbnail creation
                self.logger.info(f"🖼️ Processing product {product_id}: {first_url}")
                thumbnail_url = self.s3.generate_thumbnail_from_s3_url(
                    image_url=first_url,
                    object_name=object_name
                )

                if thumbnail_url:
                    # Update DB
                    self.db.execute(
                        "UPDATE militaria SET s3_first_image_thumbnail = %s WHERE id = %s;",
                        (thumbnail_url, product_id)
                    )
                    self.logger.info(f"✅ Thumbnail uploaded and linked: {thumbnail_url}")
                    success += 1
                else:
                    self.logger.warning(f"❌ Thumbnail creation failed for product {product_id}")
                    fail += 1

            except Exception as e:
                self.logger.error(f"❌ Exception while processing product {product_id}: {e}")
                fail += 1

        # Final summary
        self.logger.warning("📊 THUMBNAIL GENERATION SUMMARY")
        self.logger.warning(f"🔢 Total processed  : {len(processed_ids)}")
        self.logger.warning(f"✅ Successes        : {success}")
        self.logger.warning(f"❌ Failures         : {fail}")
        self.logger.warning(f"🧾 Product IDs      : {processed_ids}")

class DataPointRecoverer:
    pass