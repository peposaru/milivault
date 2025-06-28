# This will be for checking various data points within the militaria database. If the S3 image is missing, download it, etc.
 
import logging
import post_processors as post_processors
from product_tile_processor import TileProcessor
from site_processor import SiteProcessor
import image_extractor
from time import sleep
from openai_api_manager import OpenAIManager
import signal
from multiprocessing import Pool, cpu_count
from functools import partial
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from urllib.parse import urlparse

import os
import re
import random
import time
import json
import requests
from collections import defaultdict

class DataIntegrityManager:
    def __init__(self, managers):
        self.managers     = managers
        self.rds_manager  = managers.get("rdsManager")
        self.s3_manager   = managers.get("s3_manager")
        self.db           = self.rds_manager
        self.logger       = logging.getLogger(__name__)

        # Initialize OpenAIManager
        self.openai_manager = managers.get("openai_manager")

        # Vector generator is now fully wired
        self.vector_generator = VectorEmbeddingGenerator(
            rds_manager=self.rds_manager,
            openai_manager=self.openai_manager
        )

    def run_submenu(self):
        print("""
        DATA INTEGRITY MENU
        1. Recover missing images
        2. Generate thumbnails from first S3 image
        3. Recover datapoints with URL
        4. Generate OpenAI vector embeddings
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
            print("\n🔁 Re-running classification for all incomplete products (3→2→1)...")
            fixer = MissingClassificationFixer(self.rds_manager, self.openai_manager)
            for missing_required in [3, 2, 1]:
                fixer.rerun(required_missing=missing_required)
            print("✅ Classification re-run complete.")
        elif choice == "4":
            use_parallel = input("Run in parallel? [y/N]: ").strip().lower() == "y"
            tool = VectorEmbeddingGenerator(self.db, self.openai_manager)
            if use_parallel:
                tool.run_all_parallel()
            else:
                tool.run_all()
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

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import time, random, json
import image_extractor

class ImageRecoveryProcessor:
    def __init__(self, managers):
        self.managers = managers
        self.rds_manager = managers.get('rdsManager')
        self.s3 = managers.get("s3_manager")
        self.html = managers.get('html_manager')
        self.json = managers.get("jsonManager")
        self.selector_path = self.managers["user_settings"]["selectorJsonFolder"]

        self.site_limits = {}
        self.default_limit = 100000
        self.batch_size_per_site = 3

        # 🔧 Initialize the bad image URL tracking
        self.bad_image_file = "bad_image_urls.txt"
        self.bad_url_set = set()
        if os.path.exists(self.bad_image_file):
            with open(self.bad_image_file, "r") as f:
                self.bad_url_set = set(line.strip() for line in f if line.strip())


    def recover_images(self):
        site_profiles = self.json.compile_working_site_profiles(self.selector_path)
        all_sites = [p["source_name"] for p in site_profiles]
        site_profiles_map = {p["source_name"]: p for p in site_profiles}

        print("Choose recovery mode:")
        print("1. Run for all sites")
        print("2. Choose specific sites")
        choice = input("Enter 1 or 2: ").strip()

        if choice == "2":
            print("\nAvailable sites:")
            for i, site in enumerate(all_sites):
                print(f"{i + 1}. {site}")
            indexes = input("Enter comma-separated site numbers (e.g., 1,3,5): ")
            try:
                selected_indexes = [int(i.strip()) - 1 for i in indexes.split(",")]
                working_sites = [all_sites[i] for i in selected_indexes if 0 <= i < len(all_sites)]
            except Exception:
                print("Invalid selection. Aborting.")
                return
        else:
            working_sites = all_sites

        print(f"\n🟢 Processing {len(working_sites)} sites...\n")
        limits = {site: self.site_limits.get(site, self.default_limit) for site in working_sites}

        while True:
            site_batches = {}
            for site in working_sites:
                limit = limits[site]
                print(f"🔍 Querying {site} (limit {limit})...")
                query = """
                    SELECT url, site, id
                    FROM militaria
                    WHERE (
                        (original_image_urls IS NULL OR original_image_urls = '[]')
                        OR (s3_image_urls IS NULL OR s3_image_urls = '[]')
                    )
                    AND image_download_failed IS FALSE
                    AND (requires_attention IS FALSE OR requires_attention IS NULL)
                    AND site = %s
                    ORDER BY id DESC
                    LIMIT %s;
                """
                records = self.rds_manager.fetch(query, (site, limit))
                print(f"📦 Found {len(records)} pending images in {site}")
                site_batches[site] = records

            total = sum(len(batch) for batch in site_batches.values())
            print(f"\n📊 Total images pending: {total}\n")
            if total == 0:
                print("✅ All done.")
                break

            for site in working_sites:
                batch = site_batches.get(site, [])
                if not batch:
                    continue

                max_workers = 1 if site in ["WORLDWAR_COLLECTIBLES", "QMS_MILITARIA"] else 2 if site in self.site_limits else 4
                sleep_range = (3, 7) if max_workers == 1 else (2, 5) if max_workers == 2 else (1, 2.5)
                site_profile = site_profiles_map.get(site)

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(
                            self._process_single_product,
                            product=row,
                            site=site,
                            site_profile=site_profile,
                            sleep_range=sleep_range
                        ): row[0] for row in batch
                    }

                    for future in as_completed(futures):
                        url, success = future.result()
                        status = "✅" if success else "❌"
                        print(f"{status} {url}")
                        if not success:
                            self.mark_image_failed(url)


    def _process_single_product(self, product, site, site_profile, sleep_range):
        url, site_name, product_id = product
        logging.info(f"🔄 START: Processing [{site}] {url}")

        try:
            # --- Validate extractor ---
            extractor_func_name = site_profile.get("product_details_selectors", {}).get("details_image_url", {}).get("function")
            if not extractor_func_name:
                logging.warning(f"❌ SKIP: No image extractor defined for site [{site}]")
                return url, False

            # --- Fetch HTML ---
            soup = None
            for attempt in range(3):
                try:
                    soup = self.html.parse_html(url)
                    if soup:
                        logging.debug(f"✅ HTML loaded (attempt {attempt + 1}) for {url}")
                        break
                except Exception as e:
                    logging.warning(f"⚠️ HTML fetch error (attempt {attempt + 1}) for {url}: {e}")
                    time.sleep(2 ** attempt + random.uniform(0, 1))
            if not soup:
                logging.error(f"❌ FAIL: Could not load HTML after retries → {url}")
                return url, False

            # --- Page validity check ---
            parsed = urlparse(getattr(soup, "original_url", url))
            path = parsed.path or ""
            title = soup.title.string.strip().lower() if soup.title and soup.title.string else ""

            # Only fail if it's clearly a broken page or redirect loop
            too_generic = any(t in title for t in ["not found", "404"]) and path in ["", "/"]

            logging.debug(f"🔎 PAGE CHECK: path={path} | title={title!r}")

            if too_generic:
                logging.warning(f"🚫 BROKEN PAGE: Detected dead page (path/title) → {url}")
                self.mark_requires_attention(url)
                return url, False


            # --- Image extraction ---
            time.sleep(random.uniform(*sleep_range))
            image_func = getattr(image_extractor, extractor_func_name)
            image_urls = image_func(soup)

            if not image_urls:
                logging.warning(f"❌ NO IMAGES: {url}")
                self.mark_requires_attention(url)
                return url, False

            # --- Check for bad image reuse ---
            first_image = image_urls[0]
            if first_image in self.bad_url_set:
                logging.warning(f"🚫 BAD IMAGE URL DETECTED (already seen): {first_image}")
                self.mark_requires_attention(url)
                return url, False

            logging.info(f"📸 FOUND {len(image_urls)} image(s). Uploading to S3...")

            # --- Upload to S3 ---
            s3_result = self.s3.upload_images_for_product(
                product_id, image_urls, site, url, self.rds_manager, max_workers=4
            )
            s3_urls = s3_result["uploaded_image_urls"]

            if s3_urls:
                self.rds_manager.execute(
                    """
                    UPDATE militaria
                    SET original_image_urls = %s, s3_image_urls = %s
                    WHERE url = %s;
                    """,
                    (json.dumps(image_urls), json.dumps(s3_urls), url)
                )
                logging.info(f"✅ DONE: Uploaded {len(s3_urls)} image(s) → {url}")
                time.sleep(random.uniform(*sleep_range))
                return url, True
            else:
                logging.error(f"❌ UPLOAD FAILED: Could not upload for {url}")
                self.flag_bad_image_url(first_image)
                return url, False

        except Exception as e:
            logging.exception(f"❌ EXCEPTION during {url}: {e}")
            return url, False

    def init_bad_image_set(self):
        self.bad_image_file = "bad_image_urls.txt"
        self.bad_url_set = set()
        if os.path.exists(self.bad_image_file):
            with open(self.bad_image_file, "r") as f:
                self.bad_url_set = set(line.strip() for line in f if line.strip())

    def flag_bad_image_url(self, image_url):
        if image_url not in self.bad_url_set:
            with open(self.bad_image_file, "a") as f:
                f.write(image_url + "\n")
            self.bad_url_set.add(image_url)

    def mark_requires_attention(self, url):
        try:
            self.rds_manager.execute(
                "UPDATE militaria SET requires_attention = TRUE WHERE url = %s;",
                (url,)
            )
        except Exception:
            pass

    def mark_image_failed(self, url):
        try:
            self.rds_manager.execute(
                "UPDATE militaria SET image_download_failed = TRUE WHERE url = %s;",
                (url,)
            )
        except Exception:
            pass


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

class VectorEmbeddingGenerator:
    def __init__(self, rds_manager, openai_manager, batch_size=100, num_workers=4):
        self.rds_manager = rds_manager
        self.openai_manager = openai_manager
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.stop_requested = False
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        def handler(signum, frame):
            if self.stop_requested:
                logging.warning("Force exiting immediately...")
                exit(1)
            else:
                self.stop_requested = True
                logging.warning("Graceful shutdown requested. Finishing current batch... (Press Ctrl+C again to force quit)")
        signal.signal(signal.SIGINT, handler)

    def generate_embedding(self, text):
        try:
            response = self.openai_manager.client.embeddings.create(
                input=[text],
                model="text-embedding-3-small"
            )
            return response.data[0].embedding
        except Exception as e:
            logging.error(f"Embedding failed: {e}")
            return None

    def update_vector(self, db_id, vector):
        try:
            query = """
                UPDATE militaria
                SET openai_vector = %s, date_modified = CURRENT_TIMESTAMP
                WHERE id = %s;
            """
            self.rds_manager.execute(query, (vector, db_id))
            logging.info(f"✅ Updated vector for DB ID {db_id}")
        except Exception as e:
            logging.error(f"Failed to update vector for DB ID {db_id}: {e}")

    def process_row(self, row):
        db_id, title, description = row
        text = f"{title or ''} {description or ''}".strip()
        if not text:
            logging.warning(f"⚠️ Empty input for DB ID {db_id} — skipping.")
            return

        vector = self.generate_embedding(text)
        if vector:
            self.update_vector(db_id, vector)
        time.sleep(1.2)  # stay under 60 RPM per worker

    def run_all(self):
        count_query = "SELECT COUNT(*) FROM militaria WHERE openai_vector IS NULL;"
        total = self.rds_manager.fetch(count_query)[0][0]
        logging.info(f"🔢 Total rows needing vectors: {total}")
        estimated_total_time = (total * 1.5) / 60  # approx in minutes
        logging.info(f"⏳ Estimated total time: ~{estimated_total_time:.1f} minutes")

        start_time = time.time()
        processed = 0

        for offset in range(0, total, self.batch_size):
            if self.stop_requested:
                break
            self.process_batch(offset)
            processed += self.batch_size
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            remaining = max(0, total - processed)
            eta_sec = remaining / rate if rate > 0 else 0
            eta_min = eta_sec / 60
            logging.info(f"📊 Progress: {processed}/{total} ({(processed/total)*100:.1f}%) — ETA: ~{eta_min:.1f} min")

        logging.info("✅ Embedding process complete (or interrupted).")

    def process_batch(self, offset):
        query = """
            SELECT id, title, description
            FROM militaria
            WHERE openai_vector IS NULL
            ORDER BY id DESC
            LIMIT %s OFFSET %s;
        """
        rows = self.rds_manager.fetch(query, (self.batch_size, offset))
        logging.info(f"🔄 Processing batch at offset {offset} — {len(rows)} rows")

        for row in rows:
            if self.stop_requested:
                logging.warning("⏹️ Stop requested. Ending after this batch.")
                break
            self.process_row(row)

    def run_all_parallel(self):
        query = """
            SELECT id, title, description
            FROM militaria
            WHERE openai_vector IS NULL
            ORDER BY id DESC;
        """
        rows = self.rds_manager.fetch(query)
        total = len(rows)
        logging.info(f"🚀 Starting parallel processing with {self.num_workers} workers — Total: {total}")

        # Extract OpenAI API key and DB creds
        openai_api_key = self.openai_manager.api_key
        db_credentials = self.rds_manager.db_config  # assumes this exists as a dict

        from functools import partial
        worker_func = partial(process_row_parallel, openai_api_key=openai_api_key, db_credentials=db_credentials)

        from multiprocessing import Pool
        with Pool(processes=self.num_workers) as pool:
            try:
                pool.map(worker_func, rows, chunksize=10)
            except KeyboardInterrupt:
                logging.warning("❌ Ctrl+C detected — terminating pool.")
                pool.terminate()
                pool.join()

        logging.info("✅ Parallel embedding complete.")

class MissingClassificationFixer:
    def __init__(self, rds_manager, classifier):
        self.rds = rds_manager
        self.classifier = classifier

    def rerun(self, required_missing=3):
        missing_conditions = {
            3: """
                WHERE (conflict_ai_generated IS NULL OR conflict_ai_generated = '')
                AND (nation_ai_generated IS NULL OR nation_ai_generated = '')
                AND (item_type_ai_generated IS NULL OR item_type_ai_generated = '')
            """,
            2: """
                WHERE (
                    (conflict_ai_generated IS NULL OR conflict_ai_generated = '')::int +
                    (nation_ai_generated IS NULL OR nation_ai_generated = '')::int +
                    (item_type_ai_generated IS NULL OR item_type_ai_generated = '')::int
                ) = 2
            """,
            1: """
                WHERE (
                    (conflict_ai_generated IS NULL OR conflict_ai_generated = '')::int +
                    (nation_ai_generated IS NULL OR nation_ai_generated = '')::int +
                    (item_type_ai_generated IS NULL OR item_type_ai_generated = '')::int
                ) = 1
            """
        }

        condition = missing_conditions.get(required_missing)
        if not condition:
            logging.warning(f"Unsupported missing count: {required_missing}")
            return

        query = f"""
            SELECT id, title, description, s3_first_image_thumbnail
            FROM militaria
            {condition}
            LIMIT 1000;
        """

        total_fixed = 0
        total_skipped = 0
        total_failed = 0
        batch_num = 1

        while True:
            rows = self.rds.fetch(query)
            if not rows:
                logging.info("🎉 No more matching rows found. Finished rerunning classification.")
                break

            logging.info(f"\n🔁 Batch {batch_num}: Found {len(rows)} items with ≥ {required_missing} missing fields...")

            for row in rows:
                product_id, title, description, image_url = row

                try:
                    title = title if isinstance(title, str) else ""
                    description = description if isinstance(description, str) else ""
                    image_url = image_url if isinstance(image_url, str) else ""

                    result = self.classifier.classify_single_product(
                        title=title,
                        description=description,
                        image_url=image_url
                    )

                    if all(v is None for v in result.values()):
                        total_skipped += 1
                        logging.warning(f"Skipping update — classification returned nothing for ID {product_id}")
                        continue

                    update_query = """
                        UPDATE militaria
                        SET conflict_ai_generated = %s,
                            nation_ai_generated = %s,
                            item_type_ai_generated = %s
                        WHERE id = %s;
                    """
                    self.rds.execute(update_query, (
                        result.get("conflict_ai_generated"),
                        result.get("nation_ai_generated"),
                        result.get("item_type_ai_generated"),
                        product_id
                    ))

                    total_fixed += 1
                    print(f"\n✅ {title}\n📝 {description[:200]}...\n📦 {result.get('item_type_ai_generated')}\n")

                except Exception as e:
                    total_failed += 1
                    logging.error(f"❌ Failed to classify ID {product_id}: {e}")

            logging.info(f"📦 Batch {batch_num} stats — Fixed: {total_fixed} | Skipped: {total_skipped} | Failed: {total_failed}")
            batch_num += 1

        logging.info(f"\n✅ Finished rerun — Total Fixed: {total_fixed}, Skipped: {total_skipped}, Failed: {total_failed}")


    def _fetch_rows(self, required_missing):
        query = """
        SELECT url, title, description, s3_first_image_thumbnail,
               conflict_ai_generated, nation_ai_generated, item_type_ai_generated
        FROM militaria
        WHERE (
            (conflict_ai_generated IS NULL OR conflict_ai_generated ILIKE ANY (ARRAY['', 'UNKNOWN', 'OTHER', 'MISC', 'NONE']))
            ::int +
            (nation_ai_generated IS NULL OR nation_ai_generated ILIKE ANY (ARRAY['', 'UNKNOWN', 'OTHER', 'MISC', 'NONE']))
            ::int +
            (item_type_ai_generated IS NULL OR item_type_ai_generated ILIKE ANY (ARRAY['', 'UNKNOWN', 'OTHER', 'MISC', 'NONE']))
            ::int
        ) >= %s
        """
        return self.rds.fetch(query, (required_missing,))

    def _process_row(self, row):
        url, title, description, thumbnail, *_ = row

        result = self.classifier.classify_single_product(
            title=title,
            description=description,
            image_url=thumbnail
        )

        update_query = """
        UPDATE militaria
        SET conflict_ai_generated = %s,
            nation_ai_generated = %s,
            item_type_ai_generated = %s
        WHERE url = %s
        """
        self.rds.execute(update_query, (
            result.get("conflict"),
            result.get("nation"),
            result.get("item_type"),
            url
        ))

        logging.info(f"Updated classification for: {url}")


def process_row_parallel(row, openai_api_key, db_credentials):
    from openai import OpenAI
    import psycopg2

    db_id, title, description = row
    text = f"{title or ''} {description or ''}".strip()
    if not text:
        return

    try:
        # Re-init OpenAI client
        client = OpenAI(api_key=openai_api_key)
        response = client.embeddings.create(
            input=[text],
            model="text-embedding-3-small"
        )
        vector = response.data[0].embedding
    except Exception as e:
        logging.error(f"Embedding failed: {e}")
        return

    try:
        conn = psycopg2.connect(**db_credentials)
        cur = conn.cursor()
        cur.execute(
            "UPDATE militaria SET openai_vector = %s, date_modified = CURRENT_TIMESTAMP WHERE id = %s;",
            (vector, db_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"✅ Updated vector for DB ID {db_id}")
    except Exception as e:
        logging.error(f"DB update failed for {db_id}: {e}")
