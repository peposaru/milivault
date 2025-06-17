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
            tool = DataPointRecoverer(self.db, self.s3_manager)
            tool.recover()
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


import random
import time
import logging
import json

class ImageRecoveryProcessor:
    def __init__(self, managers):
        self.managers = managers
        self.rds_manager = managers.get('rdsManager')
        self.s3 = managers.get("s3_manager")
        self.html = managers.get('html_manager')
        self.json = managers.get("jsonManager")
        self.selector_path = self.managers["user_settings"]["selectorJsonFolder"]

        # Per-site daily limits (tweak as needed for your use case)
        self.site_limits = {
            "BEVO_MILITARIA": 700,
            "DEAD_SPARTAN_MILITARIA": 620,
            "HISCOLL_MILITARY_ANTIQUES": 375,
            "WORLDWAR_COLLECTIBLES": 310,
            "QMS_MILITARIA": 220,
            # ...add others...
        }
        self.default_limit = 150
        self.batch_size_per_site = 3  # Number of products processed per site per round

    def recover_images(self):
        # 1. Compile working site profiles
        site_profiles = self.json.compile_working_site_profiles(self.selector_path)
        working_sites = [p["source_name"] for p in site_profiles]
        limits = {site: self.site_limits.get(site, self.default_limit) for site in working_sites}

        # 2. Fetch batches per site (only those needing image uploads)
        site_batches = {}
        for site in working_sites:
            limit = limits[site]
            query = """
                SELECT url, site, id
                FROM militaria
                WHERE (
                    original_image_urls IS NOT NULL AND original_image_urls <> '[]'
                ) AND (
                    s3_image_urls IS NULL OR s3_image_urls = '[]'
                )
                AND image_download_failed IS FALSE
                AND site = %s
                ORDER BY id DESC
                LIMIT %s;
            """
            records = self.rds_manager.fetch(query, (site, limit))
            site_batches[site] = records

        # 3. Setup round-robin state
        cursors = {site: 0 for site in working_sites}
        done_sites = set()
        total = sum(len(batch) for batch in site_batches.values())
        logging.info(f"🟢 Starting adaptive mini-batch image recovery: {total} total records across {len(site_batches)} sites")

        # 4. Main processing loop
        while len(done_sites) < len(working_sites):
            for site in working_sites:
                if site in done_sites:
                    continue
                batch = site_batches[site]
                cursor = cursors[site]
                remaining = len(batch) - cursor
                to_do = min(self.batch_size_per_site, remaining)
                if to_do <= 0:
                    done_sites.add(site)
                    continue

                max_workers = 2 if site in self.site_limits else 4
                sleep_range = (2, 5) if site in self.site_limits else (1, 2.5)

                for _ in range(to_do):
                    url, site_name, product_id = batch[cursor]
                    logging.info(f"[{site}] [{cursor+1}/{len(batch)}] Processing product URL: {url} (id: {product_id})")

                    site_profile = next((p for p in site_profiles if p.get("source_name") == site), None)
                    if not site_profile:
                        logging.warning(f"❌ No site profile found for {site}. Skipping.")
                        cursors[site] += 1
                        continue

                    extractor_func_name = site_profile.get("product_details_selectors", {}).get("details_image_url", {}).get("function")
                    if not extractor_func_name:
                        logging.warning(f"❌ No image extractor function defined for {site}.")
                        cursors[site] += 1
                        continue

                    if not hasattr(self.s3, "upload_images_for_product"):
                        logging.error("❌ Missing method 'upload_images_for_product' in S3 manager.")
                        done_sites.add(site)
                        break

                    try:
                        soup = self.html.parse_html(url)
                        if not soup:
                            logging.warning(f"❌ Could not fetch or parse HTML for: {url}")
                            self.mark_image_failed(url)
                            cursors[site] += 1
                            break
                    except Exception as e:
                        logging.warning(f"❌ Exception while fetching/parsing HTML for {url}: {e}")
                        self.mark_image_failed(url)
                        cursors[site] += 1
                        break

                    try:
                        image_func = getattr(image_extractor, extractor_func_name)
                        image_urls = image_func(soup)
                        if not image_urls:
                            logging.warning(f"🚫 No images extracted for {url} using {extractor_func_name}")
                            self.mark_image_failed(url)
                            cursors[site] += 1
                            break

                        logging.info(f"🖼️ Extracted {len(image_urls)} image(s) for {url}. Uploading...")
                        s3_urls = self.s3.upload_images_for_product(
                            product_id, image_urls, site, url, self.rds_manager, max_workers=max_workers
                        )

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
                            break
                    except Exception as e:
                        logging.error(f"❌ Exception during image extraction/upload for {url}: {e}")
                        self.mark_image_failed(url)
                        break

                    sleep_time = random.uniform(*sleep_range)
                    logging.info(f"Sleeping {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                    cursors[site] += 1

                if cursors[site] >= len(batch):
                    done_sites.add(site)

        logging.info("🎉 All adaptive mini-batch image recovery for today complete.")

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
