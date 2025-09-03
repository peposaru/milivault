import logging, json, pprint, re

from exceptiongroup import catch
from clean_data import CleanData
import image_extractor
from datetime import datetime,timezone
from decimal import Decimal
from post_processors import normalize_input, apply_post_processors
from typing import Any

# This will handle the dictionary of data extracted from the tile on the products page tile.
class ProductTileDictProcessor:
    def __init__(self, site_profile, managers, use_comparison_row=True):
        self.site_profile       = site_profile
        self.use_comparison_list = not use_comparison_row
        self.managers           = managers
        self.counter            = managers.get('counter')
        self.rds_manager        = managers.get('rdsManager')
        self.log_print          = managers.get('logPrint')
        self.use_comparison_row = use_comparison_row
        
    def product_tile_dict_processor_main(self, tiles: list) -> tuple[list[dict], list[dict]]:
        """
        Decide what to do with each tile:
        • price-only  → batch price update
        • availability-only → batch availability update
        • anything else (incl. title diff) → full detail
        Returns (processing_required, availability_updates) to preserve old API.
        """
        try:
            processing_required, availability_updates, price_updates = self.compare_tile_url_to_rds(tiles)
            # counters
            self.counter.add_processing_required_count(len(processing_required))
            self.counter.add_availability_update_count(len(availability_updates))
            self.counter.add_price_update_count(len(price_updates))
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: compare_tile_url_to_rds failed: {e}")
            return [], []

        # Push availability updates first
        try:
            self.process_availability_update_list(availability_updates)
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: process_availability_update_list failed: {e}")

        # Then prices
        try:
            self.process_price_update_list(price_updates)
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: process_price_update_list failed: {e}")

        return processing_required, availability_updates


    # If in rds, compare availability status, title, price and update if needed
    # If not in rds, create
    def compare_tile_url_to_rds(
        self,
        tiles: list[dict]
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """
        Returns three lists:
        - processing_required (full detail)
        - availability_updates
        - price_updates
        """
        processing_required  = []
        availability_updates = []
        price_updates        = []
        cleaner              = CleanData()

        for tile in tiles:
            url       = tile.get("url")
            title     = tile.get("title")
            raw_price = tile.get("price")
            available = tile.get("available")

            # --- Sanity ---------------------------------------------------------
            if url is None or title is None or available is None:
                logging.error(f"TILE DEDUP: missing url/title/available, skipping → {tile}")
                continue

            db_row = self.find_existing_db_row(tile, self.site_profile, self.rds_manager)
            if not db_row:
                logging.info(f"NEW PRODUCT → full detail → {url}")
                processing_required.append(tile)
                continue

            db_url, db_title, db_price, db_available = db_row

            # --- Clean inputs ---------------------------------------------------
            try:
                tile_price_clean = cleaner.clean_price(str(raw_price)) if raw_price is not None else None
            except Exception:
                tile_price_clean = None

            try:
                tile_title_clean = cleaner.clean_title(title)
            except Exception:
                tile_title_clean = title

            # --- Diff flags -----------------------------------------------------
            avail_changed = bool(available) != bool(db_available)
            title_changed = tile_title_clean != db_title
            price_changed = self._meaningful_price_change(db_price, tile_price_clean)

            # --- Debug logging --------------------------------------------------
            if title_changed or price_changed or avail_changed:
                logging.debug("------------------------------------------------")
                logging.debug(f"TILE COMPARE: url={url!r}")
                if title_changed:
                    logging.debug("↪️ TITLE CHANGED:")
                    logging.debug(f"  INCOMING: {tile_title_clean}")
                    logging.debug(f"  DB      : {db_title}")
                if price_changed:
                    logging.debug("↪️ PRICE CHANGED:")
                    logging.debug(f"  INCOMING: {tile_price_clean}")
                    logging.debug(f"  DB      : {db_price}")
                if avail_changed:
                    logging.debug("↪️ AVAILABILITY CHANGED:")
                    logging.debug(f"  INCOMING: {available}")
                    logging.debug(f"  DB      : {db_available}")
            else:
                logging.debug("------------------------------------------------")
                logging.debug(f"TILE COMPARE: url={url!r}, title_changed=False, price_changed=False, avail_changed=False")
                logging.debug("NO CHANGE → skipping")

            # --- Routing --------------------------------------------------------
            if not (title_changed or price_changed or avail_changed):
                continue

            # 1) price‑only
            if price_changed and not title_changed and not avail_changed:
                if (
                    isinstance(db_price, (int, float)) and
                    isinstance(tile_price_clean, (int, float)) and
                    self._meaningful_price_change(db_price, tile_price_clean)
                ):
                    price_updates.append({
                        "url": db_url,
                        "old": float(db_price),
                        "new": float(tile_price_clean),
                    })
                else:
                    logging.debug(
                        f"PRICE GUARD: ignoring price diff for {url} "
                        f"(old={db_price}, new={tile_price_clean})"
                    )
                continue

            # 2) availability‑only
            if avail_changed and not title_changed and not price_changed:
                logging.info(f"AVAILABILITY CHANGE → {url} (to available={available})")
                availability_updates.append({"url": db_url, "available": bool(available)})
                continue

            # 3) anything else → full detail
            logging.info(
                f"REQUIRE FULL DETAIL → {url} "
                f"(title_changed={title_changed}, price_changed={price_changed}, avail_changed={avail_changed})"
            )
            processing_required.append(tile)

        return processing_required, availability_updates, price_updates


    def process_price_update_list(self, updates: list[dict]) -> None:
        """
        Batch‐update prices and append to price_history.
        Each dict: {'url': str, 'old': float, 'new': float}
        """
        logging.debug(f"PROCESS_PRICE_UPDATE_LIST: received {len(updates)} updates")
        if not updates:
            logging.debug("PROCESS_PRICE_UPDATE_LIST: no updates to process")
            return

        now = datetime.now(timezone.utc).isoformat()

        for upd in updates:
            url = upd.get("url")
            old = upd.get("old")
            new = upd.get("new")

            # Basic shape check
            if not isinstance(url, str):
                logging.error(f"PRICE UPDATE: invalid url in entry → {upd!r}")
                continue

            if not self._meaningful_price_change(old, new):
                logging.debug(f"PRICE GUARD: skipping update for {url} (old={old}, new={new})")
                continue

            try:
                history_json = json.dumps([{"price": float(old), "date": now}])
                query = """
                    UPDATE militaria
                    SET price = %s,
                        price_history = coalesce(price_history, '[]'::jsonb) || %s::jsonb,
                        date_modified = %s,
                        last_seen = %s
                    WHERE url = %s;
                """
                params = (float(new), history_json, now, now, url)

                # If update_record doesn't return rowcount, fall back to execute and assume success.
                try:
                    rows_updated = self.rds_manager.update_record(query, params)
                except TypeError:
                    # old signature
                    self.rds_manager.update_record(query, params)
                    rows_updated = 1

                if rows_updated:
                    logging.info(f"PRICE UPDATE: {url} → {old} ⇒ {new}")
                else:
                    logging.warning(f"PRICE UPDATE: no rows updated for {url}")
            except Exception as e:
                logging.error(f"PRICE UPDATE: failed for {url}: {e}")



    # Check if the price is empty or zero
    def is_empty_price(self, value) -> bool:
        """
        Return True if the supplied price is blank or numerically zero.
        Handles:
        - None
        - Empty or all-whitespace strings
        - Strings with currency symbols (e.g. "$0.00", "€0")
        - Any numeric type (int, float, Decimal) equal to zero
        """
        if value is None:
            return True

        text = str(value).strip()
        if not text:
            return True

        # Drop everything except digits and dot
        cleaned = re.sub(r"[^\d\.]", "", text)
        if not cleaned:
            # e.g. original value was "$" or "—"
            return True

        try:
            return float(cleaned) == 0.0
        except (TypeError, ValueError):
            return False


    def process_availability_update_list(self, updates: list[dict]) -> None:
        """
        Batch‑update availability flags and timestamps for each product in `updates`.
        Each dict must include 'url' (str) and 'available' (bool).
        """
        if not updates:
            return

        now = datetime.now(timezone.utc).isoformat()
        sold_query = (
            "UPDATE militaria "
            "SET available = %s, date_sold = %s, date_modified = %s, last_seen = %s "
            "WHERE url = %s;"
        )
        avail_query = (
            "UPDATE militaria "
            "SET available = %s, date_sold = NULL, date_modified = %s, last_seen = %s "
            "WHERE url = %s;"
        )

        for item in updates:
            url = item.get("url")
            if not url:
                logging.error("PRODUCT PROCESSOR: Missing 'url' in availability update item, skipping.")
                continue

            available = bool(item.get("available"))
            try:
                if available:
                    params = (True, now, now, url)
                    query = avail_query
                else:
                    params = (False, now, now, now, url)
                    query = sold_query

                self.rds_manager.update_record(query, params)
                logging.info(f"PRODUCT PROCESSOR: Updated availability for {url} → available={available}")
            except Exception as e:
                logging.error(f"PRODUCT PROCESSOR: Failed to update availability for {url}: {e}")

    def find_existing_db_row(
        self,
        product: dict,
        site_profile: dict,
        rds_manager
    ) -> tuple[str, str, float, bool] | None:
        """
        Tile‑level dedup:
        1. Exact URL match (with/without trailing slash, scheme‑insensitive)
        2. site + title fallback
        """
        raw = product.get("url") or ""
        try:
            clean_url = CleanData.clean_url(raw)
        except ValueError:
            return None

        # Build slash/no‑slash variants
        if clean_url.endswith("/"):
            alt_url = clean_url[:-1]
        else:
            alt_url = clean_url + "/"

        # Strip scheme for matching
        strip1 = re.sub(r"^https?://", "", clean_url, flags=re.IGNORECASE)
        strip2 = re.sub(r"^https?://", "", alt_url,   flags=re.IGNORECASE)

        # 1) Exact URL match (either variant, either scheme)
        try:
            rows = rds_manager.fetch(
                """
                SELECT url, title, price, available
                FROM militaria
                WHERE url = %s
                    OR url = %s
                    OR REPLACE(REPLACE(url,'http://',''),'https://','') = %s
                    OR REPLACE(REPLACE(url,'http://',''),'https://','') = %s
                LIMIT 1
                """,
                (clean_url, alt_url, strip1, strip2)
            )
            if rows:
                return rows[0]
        except Exception as e:
            logging.error(f"TILE DEDUP: exact-URL lookup failed for {raw!r}: {e}")

        # 2) site + title fallback
        site = site_profile.get("source_name")
        title = CleanData.clean_title(product.get("title") or "")
        if site and title:
            try:
                clean_title = CleanData.clean_title(title)
                rows = rds_manager.fetch(
                    """
                    SELECT url, title, price, available
                    FROM militaria
                    WHERE site = %s
                    AND title = %s
                ORDER BY COALESCE(date_modified, last_seen, date_sold) DESC
                    LIMIT 1
                    """,
                    (site, clean_title)
                )
                if rows:
                    return rows[0]
            except Exception as e:
                logging.error(f"TILE DEDUP: site+title lookup failed for {site!r}, {title!r}: {e}")

        return None
    
    def _meaningful_price_change(self, old_price, new_price) -> bool:
        """
        Consider a price change meaningful ONLY when:
        - new_price parses to a real number > 0
        - and it differs from the stored price

        Rules:
        • Never let 0 / None overwrite a positive DB price.
        • Allow setting a price if DB was 0 / None and new is > 0.
        • Ignore any transition where new is None or 0.0.

        Returns:
            bool: True if we should treat this as a real price change.
        """
        def to_float(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        old_val = to_float(old_price)  # may be None
        new_val = to_float(new_price)  # may be None

        # New value missing or zero → never meaningful (we don't downgrade prices here)
        if new_val is None or new_val == 0.0:
            return False

        # Old missing/zero, new positive → yes, we want to set it
        if old_val is None or old_val == 0.0:
            return True

        # Both positive numbers → meaningful if different
        return new_val != old_val





class ProductDetailsProcessor:
    def __init__(self, site_profile, managers, use_comparison_row=True):
        self.site_profile       = site_profile
        self.managers           = managers
        self.counter            = managers.get('counter')
        self.rds_manager        = managers.get('rdsManager')
        self.s3_manager         = managers.get("s3_manager")
        self.html_manager       = managers.get('html_manager')
        self.details_selectors  = site_profile.get("product_details_selectors", {})
        self.use_comparison_row = use_comparison_row

    def product_details_processor_main(self, processing_required: list[dict]) -> None:
        """
        Process detailed pages for each product needing a full details refresh.
        Logs page-level headers, skips, and records only real updates.
        """
        count = len(processing_required)
        logging.info(f"DETAIL PROCESSOR: {count} products to handle")
        if count == 0:
            return

        for prod in processing_required:
            url = prod.get("url")
            logging.info(f"\n****************** Processing details for {url} ******************")

            # ------------------ STEP 1: DB snapshot ------------------
            try:
                result = self.rds_manager.fetch(
                    """
                    SELECT id, title, description, price, available, original_image_urls
                    FROM militaria
                    WHERE url = %s
                    LIMIT 1
                    """,
                    (url,)
                )
                db_present = bool(result)
                if db_present:
                    (db_id, db_title, db_description, db_price,
                    db_available, db_image_urls) = result[0]
                else:
                    db_id = db_title = db_description = db_price = db_available = db_image_urls = None
            except Exception as e:
                logging.error(f"DETAIL PROCESSOR: DB lookup failed for {url}: {e}")
                db_present = False
                db_title = db_description = db_price = db_available = db_image_urls = None

            # ------------------ STEP 2: Normalize TILE fields ------------------
            try:
                new_title = CleanData.clean_title(prod.get("title", ""), allow_empty=True)
                if not new_title:
                    new_title = CleanData.clean_title(db_title or "", allow_empty=True)
            except Exception as e:
                logging.error(f"DETAIL PROCESSOR: title clean error for {url}: {e}")
                new_title = db_title or ""

            try:
                new_price = CleanData.clean_price(str(prod.get("price", "")))
            except Exception:
                new_price = None

            tile_available = bool(prod.get("available", True))

            # ------------------ STEP 3: Normalize DB fields ------------------
            db_title_clean = CleanData.clean_title(db_title or "", allow_empty=True)
            db_description_clean = CleanData.clean_description(db_description or "", allow_empty=True)
            try:
                db_price_float = float(db_price)
            except Exception:
                db_price_float = None

            # ------------------ STEP 4: Quick diff ------------------
            title_changed = new_title != db_title_clean
            price_changed = self._meaningful_price_change(db_price_float, new_price)
            avail_changed = (tile_available != (db_available if db_available is not None else True))

            logging.debug("DETAIL COMPARE (tile vs DB):")
            if title_changed:
                logging.debug(f"→ TITLE CHANGED:\nDB   : {db_title_clean}\nNEW  : {new_title}")
            if price_changed:
                logging.debug(f"→ PRICE CHANGED:\nDB   : {db_price_float}\nNEW  : {new_price}")
            if avail_changed:
                logging.debug(f"→ AVAIL CHANGED:\nDB   : {db_available}\nNEW  : {tile_available}")

            # ------------------ STEP 5: Fetch & parse HTML ------------------
            try:
                soup = self.html_manager.parse_html(url)
            except Exception as e:
                logging.error(f"DETAIL PROCESSOR: HTML parse failed for {url}: {e}")
                continue

            # ------------------ STEP 6: Extract & clean ------------------
            try:
                raw_details = self.construct_details_data(url, soup)
                clean_details = self.construct_clean_details_data(raw_details)

                if not clean_details.get("title"):
                    logging.debug("DETAIL FALLBACK: empty detail title → using tile/DB title")
                    clean_details["title"] = new_title or db_title_clean

                if not clean_details.get("description"):
                    logging.debug("DETAIL FALLBACK: empty detail description → using DB or placeholder")
                    clean_details["description"] = db_description_clean or "No description available."

            except Exception as e:
                logging.error(f"DETAIL PROCESSOR: Data extraction/cleaning failed for {url}: {e}")
                continue

            # ------------------ STEP 7: Dedup & upsert ------------------
            try:
                matched_id, matched_url = find_existing_db_row_details(
                    clean_details, self.site_profile, self.rds_manager
                )
                incoming_url = clean_details.get("url")

                if matched_id:
                    if matched_url != incoming_url:
                        logging.info(f"DETAIL PROCESSOR: Replacing old DB URL with new one → {matched_url} → {incoming_url}")
                        try:
                            self.rds_manager.update_record(
                                "UPDATE militaria SET url = %s, date_modified = %s WHERE id = %s;",
                                (incoming_url, datetime.now(timezone.utc).isoformat(), matched_id)
                            )
                            logging.info(f"DETAIL PROCESSOR: DB URL updated for id={matched_id}")
                        except Exception as e:
                            logging.error(f"DETAIL PROCESSOR: Failed to update URL for id={matched_id}: {e}")

                    clean_details['url'] = incoming_url
                    old_price = db_price_float
                    new_price = clean_details.get("price")
                    is_sold = clean_details.get("available") is False

                    if is_sold and not self._meaningful_price_change(old_price, new_price):
                        logging.info(f"DETAIL PROCESSOR: Skipping unchanged sold item: {incoming_url}")
                        self.counter.add_skipped_sold_item()
                        continue

                    logging.info(f"DETAIL PROCESSOR: Found existing record (id={matched_id}) for {incoming_url}")
                    self.counter.add_old_product_count()
                    did_update = self.old_product_processor(clean_details, matched_id)
                    if did_update:
                        logging.info(f"DETAIL PROCESSOR: Updated old product id={matched_id}")
                    else:
                        logging.info(f"DETAIL PROCESSOR: No detail changes for id={matched_id}")

                else:
                    logging.info(f"DETAIL PROCESSOR: New product detected for {incoming_url}")
                    self.counter.add_new_product_count()
                    try:
                        self.new_product_processor(clean_details, raw_details)
                        logging.info(f"DETAIL PROCESSOR: Inserted new product for {incoming_url}")
                    except Exception as e:
                        logging.error(f"DETAIL PROCESSOR: new_product_processor failed for {incoming_url}: {e}")

            except Exception as e:
                final_url = clean_details.get("url")
                logging.error(f"DETAIL PROCESSOR: final insert/update step failed for {final_url}: {e}")

        logging.info("DETAIL PROCESSOR: Finished processing all products")


    def new_product_processor(self, clean_details_data: dict, raw_details_data: dict) -> None:
        """
        Insert a new product, upload its images to S3, then classify with local ML first,
        falling back to OpenAI automatically when a model is disabled or low-confidence.
        """
        url = clean_details_data.get("url")
        thumb = None

        # 1) Insert new record
        try:
            self.rds_manager.new_product_input(clean_details_data)
            logging.info(f"NEW PRODUCT: Inserted {url}")
        except Exception as e:
            logging.error(f"NEW PRODUCT: Failed to insert {url}: {e}")
            return

        # 2) Retrieve its database ID
        try:
            db_id = self.rds_manager.get_record_id(
                "SELECT id FROM militaria WHERE url = %s AND site = %s;",
                (url, clean_details_data.get("site"))
            )
            if not db_id:
                logging.error(f"NEW PRODUCT: Couldn't fetch ID for {url}")
                return
        except Exception as e:
            logging.error(f"NEW PRODUCT: ID lookup failed for {url}: {e}")
            return

        # 3) Upload images
        image_urls = clean_details_data.get("original_image_urls", [])
        if image_urls:
            try:
                result = self.s3_manager.upload_images_for_product(
                    db_id, image_urls, clean_details_data.get("site"), url, self.rds_manager
                )
                s3_urls = result.get("uploaded_image_urls", [])
                thumb = result.get("thumbnail_url")
                if s3_urls:
                    self.rds_manager.execute(
                        "UPDATE militaria SET s3_image_urls = %s WHERE id = %s;",
                        (json.dumps(s3_urls), db_id)
                    )
                    logging.info(f"NEW PRODUCT: Uploaded {len(s3_urls)} images for {url}")
                clean_details_data["s3_image_urls"] = s3_urls
            except Exception as e:
                logging.error(f"NEW PRODUCT: Image upload failed for {url}: {e}")
        else:
            logging.info(f"NEW PRODUCT: No images to upload for {url}")

        # 4) Unified classification (ML first per-label, then OpenAI fallback per-label)
        title = (clean_details_data.get("title") or "")
        description = (clean_details_data.get("description") or "")

        updates = {}
        try:
            labels = self._predict_labels(title=title, description=description, image_url=thumb)

            # Map normalized outputs to DB columns:
            # - If ML accepted → write *_ml_designated
            # - Else if OpenAI provided → write *_ai_generated
            def _apply(label_key: str, ml_col: str, ai_col: str):
                info = labels.get(label_key) or {}
                val = info.get("value")
                source = info.get("source")
                accepted = info.get("accepted")
                if not val:
                    return
                if source == "ml" and accepted:
                    updates[ml_col] = str(val).upper()
                    logging.info(f"LABEL {label_key} -> ML ACCEPTED value={val} conf={info.get('conf')} τ={info.get('threshold')}")
                elif source == "openai":
                    updates[ai_col] = str(val).upper()
                    logging.info(f"LABEL {label_key} -> FALLBACK OPENAI value={val}")
                else:
                    logging.info(f"LABEL {label_key} -> NO DECISION")

            _apply("item_type", "item_type_ml_designated", "item_type_ai_generated")
            _apply("conflict",  "conflict_ml_designated",  "conflict_ai_generated")
            _apply("nation",    "nation_ml_designated",    "nation_ai_generated")

            # Supergroup (aux)
            sg = (labels.get("supergroup") or {}).get("value")
            if sg:
                updates["supergroup_ai_generated"] = sg

        except Exception as e:
            logging.error(f"NEW PRODUCT: Classification step failed for {url}: {e}")

        # 5) Persist whatever we got
        if updates:
            try:
                set_clause = ", ".join(f"{k} = %s" for k in updates.keys())
                params = list(updates.values()) + [db_id]
                self.rds_manager.execute(f"UPDATE militaria SET {set_clause} WHERE id = %s;", tuple(params))
                logging.info(f"NEW PRODUCT: Classification fields updated for {url} ({', '.join(updates.keys())})")
            except Exception as e:
                logging.error(f"NEW PRODUCT: Failed to store classification for {url}: {e}")



    def old_product_processor(self, clean: dict, record_id: int) -> bool:
        """
        Update an existing product’s record if any key details have changed.
        Guards against overwriting a real price with 0/None.
        Returns True if an UPDATE was executed, False otherwise.
        """
        now = datetime.now(timezone.utc).isoformat()

        try:
            row = self.rds_manager.fetch(
                """
                SELECT title, price, available, description,
                    price_history, original_image_urls, extracted_id
                FROM militaria
                WHERE id = %s
                """,
                (record_id,)
            )
            if not row:
                logging.warning(f"OLD PRODUCT: no record for id={record_id}, skipping")
                return False

            (db_title, db_price, db_avail, db_desc,
            db_history, db_images, db_extracted_id) = row[0]

            updates = {}

            # ---------- TITLE & DESCRIPTION ----------
            raw_input_title = clean.get("title") or ""
            new_title = CleanData.clean_title(raw_input_title)
            logging.debug("TITLE COMPARISON:\nDB   : %r\nCLEAN: %r", db_title, new_title)
            if new_title and new_title != db_title:
                logging.debug("TITLE UPDATE CANDIDATE (id=%s)\nDB : %r\nNEW: %r",
                            record_id, db_title, new_title)

                # If previous_title column exists, update both via dedicated RDS method
                if "previous_title" in self.rds_manager.get_column_names("militaria"):
                    try:
                        self.rds_manager.update_title_and_previous_title(record_id, new_title, db_title)
                        logging.info(f"OLD PRODUCT: title updated directly for id={record_id}")
                        db_title = new_title  # So future comparisons use the new title
                    except Exception as e:
                        logging.error(f"OLD PRODUCT: Failed to update title for id={record_id}: {e}")
                else:
                    updates["title"] = new_title
                    logging.info(f"OLD PRODUCT: title changed (fallback path) for id={record_id}")


            new_desc = clean.get("description")
            if new_desc and new_desc != db_desc:
                updates["description"] = new_desc
                logging.info(f"OLD PRODUCT: description changed for id={record_id}")

            # ---------- PRICE & HISTORY ----------
            new_price_raw = clean.get("price")

            def to_float(v):
                try:
                    return float(v) if v is not None else None
                except (TypeError, ValueError):
                    return None

            db_price_val = to_float(db_price)
            new_price_val = to_float(new_price_raw)

            price_changed = self._meaningful_price_change(db_price_val, new_price_val)
            logging.debug(
                "OLD PRODUCT DEBUG: id=%s, db_price=%r (%s), new_price=%r (%s)",
                record_id, db_price, type(db_price), new_price_raw, type(new_price_raw)
            )

            if price_changed:
                try:
                    history = json.loads(db_history) if isinstance(db_history, str) else (db_history or [])
                except Exception:
                    history = []

                if db_price_val is not None:
                    if not history or history[-1].get("price") != db_price_val:
                        history.append({"price": db_price_val, "date": now})

                updates["price"] = new_price_val
                updates["price_history"] = json.dumps(history)
                logging.info(f"OLD PRODUCT: price changed for id={record_id} (from {db_price_val} to {new_price_val})")
            else:
                logging.debug("PRICE GUARD: skipping price update for id=%s (db_price=%r, new_price=%r)",
                            record_id, db_price_val, new_price_val)

            # ---------- AVAILABILITY ----------
            new_avail = clean.get("available")
            if new_avail is not None and new_avail != db_avail:
                updates["available"] = new_avail
                updates["last_seen"] = now
                updates["date_sold"] = None if new_avail else now
                logging.info(f"OLD PRODUCT: availability changed for id={record_id}")

            # ---------- IMAGES ----------
            new_images = clean.get("original_image_urls") or []
            try:
                old_images = json.loads(db_images) if isinstance(db_images, str) else (db_images or [])
            except Exception:
                old_images = []
            if new_images and new_images != old_images:
                updates["original_image_urls"] = json.dumps(new_images)
                logging.info(f"OLD PRODUCT: images updated for id={record_id}")

            # ---------- OTHER METADATA ----------
            for field in [
                "nation_site_designated", "conflict_site_designated",
                "item_type_site_designated", "grade", "categories_site_designated"
            ]:
                val = clean.get(field)
                if val:
                    updates[field] = json.dumps(val) if isinstance(val, list) else val
                    logging.info(f"OLD PRODUCT: {field} updated for id={record_id}")

            # ---------- NOTHING CHANGED ----------
            if not updates:
                logging.info(f"OLD PRODUCT: no changes for id={record_id}")
                return False

            # Always stamp modification
            updates["date_modified"] = now
            updates.setdefault("last_seen", now)

            set_clause = ", ".join(f"{k} = %s" for k in updates)
            params = list(updates.values()) + [record_id]
            query = f"UPDATE militaria SET {set_clause} WHERE id = %s"

            # Prefer rowcount if available
            try:
                rows = self.rds_manager.update_record(query, params)
            except TypeError:
                self.rds_manager.update_record(query, params)
                rows = 1

            if rows:
                logging.info(f"OLD PRODUCT: record id={record_id} updated successfully ({rows} row)")
                return True
            else:
                logging.warning(f"OLD PRODUCT: UPDATE touched 0 rows for id={record_id} (possible mismatch?)")
                return False

        except Exception as e:
            logging.error(f"OLD PRODUCT: failed to process id={record_id}: {e}")
            return False

    def extract_data(
        self,
        soup,
        method: str,
        args: list = None,
        kwargs: dict = None,
        attribute: str = None,
        config: dict = None
    ) -> str | None:
        """
        Extract a value from a BeautifulSoup `soup` object.

        Supports:
        - method=="has_attr": returns the raw attribute value.
        - tag methods (find, find_all, select, etc.).
        - config-driven extraction:
            * config["extract"] == "text" → element.get_text()
            * config["extract"] == "html" → str(element)
        - attribute extraction via `attribute`.
        - defaults to first found element’s text.

        Returns the stripped string or None if nothing was found or on error.
        """
        args = args or []
        kwargs = kwargs or {}
        try:
            # Special “has_attr” shortcut
            if method == "has_attr" and args:
                val = soup.get(args[0], "")
                if isinstance(val, list):
                    val = " ".join(val)
                return normalize_input(val)

            # Locate the extraction method on soup
            extractor = getattr(soup, method, None)
            if not extractor:
                logging.debug(f"EXTRACT DATA: no such method '{method}' on soup")
                return None

            element = extractor(*args, **kwargs)
            if not element:
                return None

            # If we got a list, take its first item
            if isinstance(element, list):
                element = element[0] if element else None
                if element is None:
                    return None

            # HTML extraction requested
            if config and config.get("extract") == "html":
                return str(element).strip()

            # Text extraction requested, or no attribute specified
            if config and config.get("extract") == "text" or not attribute:
                if hasattr(element, "get_text"):
                    return element.get_text(strip=True)
                return normalize_input(str(element))

            # Attribute extraction
            attr_val = element.get(attribute)
            return attr_val.strip() if attr_val else None

        except Exception as e:
            logging.error(f"EXTRACT DATA: unexpected error in {method} → {e}")
            return None

    def extract_details_title(self, soup) -> str | None:
        """
        Extract and post-process the product title from the details page.
        Returns a cleaned title string or None if not found.
        """
        # 1) Grab the selector config; bail out if none defined
        config = self.site_profile.get("product_details_selectors", {}).get("details_title")
        if not config:
            return None

        # 2) Fetch method, args, kwargs, and attribute via reusable helper
        method, args, kwargs, attribute, _ = self.parse_details_config("details_title")

        # 3) Extract raw value
        raw = self.extract_data(soup, method, args, kwargs, attribute, config)
        if not raw:
            return None

        # 4) Normalize
        title = normalize_input(raw)

        # 5) Post-process if requested
        if title and config.get("post_process"):
            try:
                title = apply_post_processors(title, config["post_process"])
            except Exception as e:
                logging.error(f"DETAILS TITLE: post-process failed → {e}")

        return title or None


    def extract_details_description(self, soup) -> str | None:
        """
        Extract and post-process the product description from the details page.
        Returns a cleaned description string or None if not found.
        """
        # 1) Load selector config or bail
        config = self.details_selectors.get("details_description")
        if not config:
            return None

        # 2) Parse method, args, kwargs, attribute
        method, args, kwargs, attribute, _ = self.parse_details_config("details_description")

        try:
            # 3) Locate the base element
            element = getattr(soup, method, lambda *a, **k: None)(*args or [], **kwargs or {})
            if not element:
                return None

            # 4) Handle optional nested submethod
            sub = config.get("submethod")
            if sub:
                element = getattr(element, sub.get("method", "find"))(
                    *sub.get("args", []), **sub.get("kwargs", {})
                ) or None
                attribute = sub.get("attribute", attribute)
                if not element:
                    return None

            # 5) Extract raw text or attribute
            if attribute and element.get(attribute) is not None:
                raw = element.get(attribute).strip()
            else:
                raw = element.get_text(strip=True)

            # 6) Normalize
            desc = normalize_input(raw)

            # 7) Post-process if configured
            if desc and config.get("post_process"):
                try:
                    desc = apply_post_processors(desc, config["post_process"], soup=soup)
                except Exception as e:
                    logging.error(f"DETAILS DESC: post-process failed → {e}")

            return desc or None

        except Exception as e:
            logging.error(f"DETAILS DESC: extraction failed → {e}")
            return None

    def extract_details_price(self, soup, product_url) -> str:
        """
        Extract and clean the product price from the details page.
        Returns a normalized price string (e.g. "0" if missing).
        """
        # 1) Load selector config or bail
        config = self.details_selectors.get("details_price")
        if not config:
            return "0"

        # 2) Parse method, args, kwargs, attribute
        method, args, kwargs, attribute, _ = self.parse_details_config("details_price")

        try:
            # 3) Extract raw text via generic helper
            raw = self.extract_data(soup, method, args or [], kwargs or {}, attribute, config)
            # 4) Normalize the extracted value (e.g. strip whitespace, handle None)
            norm = normalize_input(raw)
            # 5) Apply any post-processors, injecting context if needed
            if norm and config.get("post_process"):
                # Ensure each post-processor has soup & url contexts
                for proc in config["post_process"].values():
                    if isinstance(proc, dict):
                        proc.setdefault("soup", soup)
                        proc.setdefault("url", product_url)
                norm = apply_post_processors(norm, config["post_process"], soup=soup)

            # 6) Always return a string; default to "0" when empty
            return str(norm) if norm else "0"

        except Exception as e:
            logging.error(f"DETAILS PRICE: extraction failed for {product_url}: {e}")
            return "0"

    def _meaningful_price_change(self, old_price, new_price) -> bool:
        """
        Consider a price change meaningful ONLY when:
        - new_price parses to a real number > 0
        - and it differs from the stored price

        Rules:
        • Never let 0 / None overwrite a positive DB price.
        • Allow setting a price if DB was 0 / None and new is > 0.
        • Ignore any transition where new is None or 0.0.

        Returns:
            bool: True if we should treat this as a real price change.
        """
        def to_float(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        old_val = to_float(old_price)  # may be None
        new_val = to_float(new_price)  # may be None

        # New value missing or zero → never meaningful (we don't downgrade prices here)
        if new_val is None or new_val == 0.0:
            return False

        # Old missing/zero, new positive → yes, we want to set it
        if old_val is None or old_val == 0.0:
            return True

        # Both positive numbers → meaningful if different
        return new_val != old_val


    def extract_details_availability(self, soup) -> bool:
        """
        Extract and post-process availability from the details page.
        Supports:
        - Static booleans (True/False) in the JSON config
        - Static string flags ("true"/"false")
        - Selector‑based extraction with optional post-processing
        Returns True if the item is available, False otherwise.
        """
        # 1) Load selector config or default to False
        config = self.details_selectors.get("details_availability")
        if config is None:
            return False

        # 2) Handle static configs
        if config is True:
            return True
        if config is False:
            return False
        if isinstance(config, str):
            val = config.strip().lower()
            return val in ("true", "yes", "available", "in stock", "add to cart")

        # 3) Dynamic extraction
        method, args, kwargs, attribute, _ = self.parse_details_config("details_availability")
        raw = self.extract_data(soup, method, args or [], kwargs or {}, attribute, config)
        val = normalize_input(raw)

        # 4) Post-process if needed
        if val and config.get("post_process"):
            try:
                val = apply_post_processors(val, config["post_process"], soup=soup)
            except Exception as e:
                logging.error(f"DETAILS AVAILABILITY: post-process failed → {e}")

        # 5) Interpret final value
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.lower() in ("true", "yes", "available", "in stock", "add to cart")

        return False


    def extract_details_image_url(self, soup) -> list[str]:
        """
        Extract image URLs via a configured function. Returns [] if no function is set
        or if extraction fails.
        """
        cfg = self.details_selectors.get("details_image_url")
        if not cfg:
            return []

        fn = cfg.get("function")
        # explicit “skip” or missing → no images
        if not isinstance(fn, str) or fn.lower() == "skip":
            return []

        extractor = getattr(image_extractor, fn, None)
        if not extractor:
            logging.error(f"IMAGE URL: extractor '{fn}' not found")  # :contentReference[oaicite:0]{index=0}
            return []

        try:
            urls = extractor(soup)
            if not isinstance(urls, list):
                logging.error(f"IMAGE URL: extractor '{fn}' returned non-list")  # :contentReference[oaicite:1]{index=1}
                return []
            return urls
        except Exception as e:
            logging.error(f"IMAGE URL: extractor '{fn}' failed → {e}")  # :contentReference[oaicite:2]{index=2}
            return []


    def extract_details_nation(self, soup) -> str | None:
        """
        Extract country/nation. Falls back to metadata_selectors if no JSON selector.
        """
        cfg = self.details_selectors.get("details_nation")
        # hard‑coded string
        if isinstance(cfg, str):
            return cfg  # :contentReference[oaicite:3]{index=3}
        # no selector → fallback
        if cfg is None:
            return self.site_profile.get("metadata_selectors", {}).get("nation")  # :contentReference[oaicite:4]{index=4}

        # dict selector
        method, args, kwargs, attr, _ = self.parse_details_config("details_nation")
        raw = self.extract_data(soup, method, args or [], kwargs or {}, attr, cfg)
        val = normalize_input(raw)

        if val and cfg.get("post_process"):
            try:
                val = apply_post_processors(val, cfg["post_process"], soup=soup)
            except Exception as e:
                logging.error(f"NATION: post-process failed → {e}")

        return val or None


    def extract_details_conflict(self, soup) -> str | None:
        """
        Extract conflict designation. Falls back to metadata_selectors if no JSON selector.
        """
        cfg = self.details_selectors.get("details_conflict")
        if isinstance(cfg, str):
            return cfg  # :contentReference[oaicite:5]{index=5}
        if cfg is None:
            return self.site_profile.get("metadata_selectors", {}).get("conflict")  # :contentReference[oaicite:6]{index=6}

        method, args, kwargs, attr, _ = self.parse_details_config("details_conflict")
        try:
            raw = self.extract_data(soup, method, args or [], kwargs or {}, attr, cfg)
            val = normalize_input(raw)

            if val and cfg.get("post_process"):
                try:
                    val = apply_post_processors(val, cfg["post_process"], soup=soup)
                except Exception as e:
                    logging.error(f"CONFLICT: post-process failed → {e}")

            return val or None
        except Exception as e:
            logging.error(f"CONFLICT: extraction failed → {e}")
            return None  # :contentReference[oaicite:7]{index=7}

    def extract_details_item_type(self, soup) -> str | None:
        """
        Extract and post-process the product’s item type.
        Falls back to metadata_selectors if no JSON config is provided.
        """
        cfg = self.details_selectors.get("details_item_type")
        if isinstance(cfg, str):
            return cfg
        if cfg is None:
            return self.site_profile.get("metadata_selectors", {}).get("item_type")

        method, args, kwargs, attr, _ = self.parse_details_config("details_item_type")
        try:
            raw = self.extract_data(soup, method, args or [], kwargs or {}, attr, cfg)
            val = normalize_input(raw)
            if val and cfg.get("post_process"):
                try:
                    val = apply_post_processors(val, cfg["post_process"], soup=soup)
                except Exception as e:
                    logging.error(f"ITEM_TYPE: post-process failed → {e}")
            return val or None
        except Exception as e:
            logging.error(f"ITEM_TYPE: extraction failed → {e}")
            return None


    def extract_details_extracted_id(self, soup) -> str | None:
        """
        Extract and post-process the product’s extracted ID (SKU).
        """
        cfg = self.details_selectors.get("details_extracted_id")
        if isinstance(cfg, str):
            return cfg
        if not cfg:
            return None

        method, args, kwargs, attr, _ = self.parse_details_config("details_extracted_id")
        try:
            raw = self.extract_data(soup, method, args or [], kwargs or {}, attr, cfg)
            val = normalize_input(raw)
            if val and cfg.get("post_process"):
                try:
                    val = apply_post_processors(val, cfg["post_process"], soup=soup)
                except Exception as e:
                    logging.error(f"EXTRACTED_ID: post-process failed → {e}")
            return val or None
        except Exception as e:
            logging.error(f"EXTRACTED_ID: extraction failed → {e}")
            return None


    def extract_details_grade(self, soup) -> str | None:
        """
        Extract and post-process the product’s grade.
        """
        cfg = self.details_selectors.get("details_grade")
        if isinstance(cfg, str):
            return cfg
        if not cfg:
            return None

        method, args, kwargs, attr, _ = self.parse_details_config("details_grade")
        try:
            raw = self.extract_data(soup, method, args or [], kwargs or {}, attr, cfg)
            val = normalize_input(raw)
            if val and cfg.get("post_process"):
                try:
                    val = apply_post_processors(val, cfg["post_process"], soup=soup)
                except Exception as e:
                    logging.error(f"GRADE: post-process failed → {e}")
            return val or None
        except Exception as e:
            logging.error(f"GRADE: extraction failed → {e}")
            return None


        
    def extract_details_site_categories(self, soup):
        """
        Extract site-specific product categories and apply post-processing if defined.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_site_categories", {})
            method = selector_config.get("method", "find_all")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            categories = self.extract_data(soup, method, args, kwargs, attribute, selector_config)

            categories = normalize_input(categories)

            if categories and "post_process" in selector_config:
                categories = apply_post_processors(categories, selector_config["post_process"])

            return categories if categories else []

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting site categories: {e}")
            return []


        
    def parse_details_config(self, selector_key):
        """
        Extracts and returns detailed selector configuration.

        Args:
            selector_key (str): The key for the selector in the JSON profile (e.g., "details_title")

        Returns:
            tuple: (method, args, kwargs, attribute, full_config)
        """
        try:
            config = self.details_selectors.get(selector_key)
            if config is None:
                logging.debug(f"PRODUCT PROCESSOR: No selector config found for key: {selector_key}")
                return None, None, None, None, {}

            return (
                config.get("method", "find"),
                config.get("args", []),
                config.get("kwargs", {}),
                config.get("attribute"),
                config
            )
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error parsing configuration for {selector_key}: {e}")
            return None, None, None, None, {}



        
    def construct_details_data(self, product_url, product_url_soup):
        # Shortcut to avoid repeating self.details_selectors.get(...)
        sel = self.details_selectors  
        try:
            data = {
                "url": product_url,
                "title": self.extract_details_title(product_url_soup) if sel.get("details_title") else None,
                "description": self.extract_details_description(product_url_soup) if sel.get("details_description") else None,
                "price": self.extract_details_price(product_url_soup, product_url) if sel.get("details_price") else "0",
                "available": self.extract_details_availability(product_url_soup) if sel.get("details_availability") else None,
                "original_image_urls": self.extract_details_image_url(product_url_soup) if sel.get("details_image_url") else [],
                "nation_site_designated": self._static_value_or_extracted("details_nation", self.extract_details_nation, product_url_soup),
                "conflict_site_designated": self._static_value_or_extracted("details_conflict", self.extract_details_conflict, product_url_soup),
                "item_type_site_designated": self._static_value_or_extracted("details_item_type", self.extract_details_item_type, product_url_soup),
                "extracted_id": self.extract_details_extracted_id(product_url_soup) if sel.get("details_extracted_id") else None,
                "grade": self.extract_details_grade(product_url_soup) if sel.get("details_grade") else None,
                "categories_site_designated": self.extract_details_site_categories(product_url_soup) if sel.get("details_site_categories") else [],
            }

            logging.debug(f"CONSTRUCT DETAILS DATA: Extracted fields →\n{pprint.pformat(data)}")

            return data

        except Exception as e:
            logging.error(f"CONSTRUCT DETAILS DATA: Error while constructing data for {product_url} → {e}")
            return {
                "url": product_url,
                "title": None,
                "description": None,
                "price": "0",
                "available": False,
                "original_image_urls": [],
                "nation_site_designated": None,
                "conflict_site_designated": None,
                "item_type_site_designated": None,
                "extracted_id": None,
                "grade": None,
                "categories_site_designated": []
            }

    
    def construct_clean_details_data(self, details_data):
        """
        Process and clean details data dynamically.

        Args:
            details_data (dict): Raw details data.

        Returns:
            dict: Cleaned details data.
        """
        clean_data = CleanData()

        # Map keys to their corresponding cleaning functions
        cleaning_functions = {
            "url"                       : clean_data.clean_url,
            "title"                     : lambda v: clean_data.clean_title(v, allow_empty=True),
            "description"               : lambda v: clean_data.clean_description(v, allow_empty=True),
            "price"                     : clean_data.clean_price,
            "available"                 : clean_data.clean_available,
            "original_image_urls"       : clean_data.clean_url_list,
            "nation_site_designated"    : clean_data.clean_nation,
            "conflict_site_designated"  : clean_data.clean_conflict,
            "item_type_site_designated" : clean_data.clean_item_type,
            "extracted_id"              : clean_data.clean_extracted_id,
            "grade"                     : clean_data.clean_grade,
            "categories_site_designated": clean_data.clean_categories,
        }

        cleaned_data = {}
        for key, value in details_data.items():
            if key in cleaning_functions:
                if key == "available" and isinstance(value, bool):
                    cleaned_value = value  # Avoid double-cleaning
                else:
                    cleaned_value = cleaning_functions[key](value)
                cleaned_data[key] = cleaned_value
            else:
                cleaned_data[key] = value  # Preserve unrecognized fields

        # Add consistent metadata
        cleaned_data.update({
            "site": self.site_profile.get('source_name', 'unknown'),
            "currency": self.site_profile.get("access_config", {}).get("currency_code", "usd"),
        })

        return cleaned_data

    
    def convert_decimal_to_float(self,data):
        """
        Recursively convert Decimal objects in a nested structure to float.
        
        Args:
            data: The data structure (dict, list, or scalar) to process.
        
        Returns:
            The data structure with all Decimal objects converted to float.
        """
        if isinstance(data, dict):
            return {key: self.convert_decimal_to_float(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.convert_decimal_to_float(item) for item in data]
        elif isinstance(data, Decimal):
            return float(data)
        else:
            return data
        
    
    def cast(self, value: Any, config: str) -> Any:
        """
        Cast `value` according to `config`:
        - "float": strip non-digits (except “.”) and convert to float
        - "int"  : strip non-digits and convert to int
        - other  : return original
        If conversion fails, returns the original `value`.
        """
        text = normalize_input(value) or ""
        if config == "float":
            cleaned = re.sub(r"[^\d\.]", "", text)
            try:
                return float(cleaned)
            except ValueError:
                logging.warning(f"POST PROCESS: cast to float failed for {value!r}")
                return value
        elif config == "int":
            cleaned = re.sub(r"[^\d]", "", text)
            try:
                return int(cleaned)
            except ValueError:
                logging.warning(f"POST PROCESS: cast to int failed for {value!r}")
                return value
        return value

    def _static_value_or_extracted(self, key, extractor_func, soup):
        config = self.details_selectors.get(key)
        if isinstance(config, str):
            return config
        elif isinstance(config, dict):
            return extractor_func(soup)
        return None

    # --------------ML vs AI Determination Functions--------------

    def _predict_labels(self, title: str, description: str, image_url: str | None):
        """
        Unified facade:
        1) Try local ML per label (if available). If it returns raw scores, apply thresholds here.
        2) Fall back to OpenAI per label when ML is disabled/low-confidence/unavailable.
        Returns a normalized dict with per-label decisions.
        """
        us = (self.managers.get("user_settings") or {})
        thresholds = {
            "item_type": float(us.get("itemTypeTau", us.get("itemTypeTau".lower(), 0.85)) or 0.85),
            "conflict":  float(us.get("conflictTau", 0.85)),
            "nation":    float(us.get("nationTau", 0.85)),
        }

        def _norm_empty():
            return {"value": None, "source": "none", "accepted": False, "conf": None, "threshold": None}

        out = {
            "item_type": _norm_empty(),
            "conflict":  _norm_empty(),
            "nation":    _norm_empty(),
            "supergroup": {"value": None, "source": "none"},
        }

        mlm = self.managers.get("ml_manager")
        ai  = self.managers.get("openai_manager")

        # ---------- 1) Try ML if available ----------
        ml_raw = None
        if mlm:
            try:
                predict_fn = None
                if callable(getattr(mlm, "predict", None)):
                    predict_fn = mlm.predict
                elif callable(getattr(mlm, "classify", None)):
                    predict_fn = mlm.classify

                if predict_fn:
                    ml_raw = predict_fn(title=title, description=description, image_url=image_url)
                else:
                    logging.info("NEW PRODUCT: ML manager exposes neither 'predict' nor 'classify'; skipping ML.")
            except Exception as e:
                logging.error(f"NEW PRODUCT: ML inference failed: {e}")
                ml_raw = None


        def _extract_ml(label_key: str):
            """
            Normalizes potential ML manager outputs for one label.
            Accepts:
            - dict with keys value/conf/threshold/accepted
            - tuple (value, conf)
            - plain string (value only; treated as conf=None)
            """
            ml_val = None
            ml_conf = None
            ml_tau = thresholds[label_key]

            if not ml_raw:
                return None, None, ml_tau, False

            cand = ml_raw.get(label_key)
            if cand is None:
                return None, None, ml_tau, False

            if isinstance(cand, dict):
                ml_val = cand.get("value")
                ml_conf = cand.get("conf")
                ml_tau  = float(cand.get("threshold", ml_tau) or ml_tau)
                accepted = bool(cand.get("accepted")) if cand.get("accepted") is not None else (
                    (ml_conf is not None) and (ml_conf >= ml_tau)
                )
                return ml_val, ml_conf, ml_tau, accepted

            if isinstance(cand, (list, tuple)) and len(cand) >= 1:
                ml_val = cand[0]
                ml_conf = cand[1] if len(cand) > 1 else None
                accepted = (ml_conf is not None) and (ml_conf >= ml_tau)
                return ml_val, ml_conf, ml_tau, accepted

            # string fallback
            ml_val = str(cand)
            ml_conf = None
            accepted = False
            return ml_val, ml_conf, ml_tau, accepted

        for key in ("item_type", "conflict", "nation"):
            ml_val, ml_conf, ml_tau, accepted = _extract_ml(key)
            if ml_val and accepted:
                out[key] = {"value": ml_val, "source": "ml", "accepted": True, "conf": ml_conf, "threshold": ml_tau}

        # ---------- 2) Per-label fallback to OpenAI ----------
        need_ai = {k: (out[k]["source"] != "ml" or not out[k]["accepted"]) for k in ("item_type", "conflict", "nation")}
        ai_result = None
        if ai and any(need_ai.values()):
            try:
                ai_result = ai.classify_single_product(title=title, description=description, image_url=image_url) or {}
                # ai_result example keys: conflict_ai_generated, nation_ai_generated, item_type_ai_generated, supergroup_ai_generated
            except Exception:
                ai_result = None

        def _fill_ai(label_key: str, ai_key: str):
            if not need_ai[label_key]:
                return
            if not ai_result:
                return
            val = ai_result.get(ai_key)
            if val:
                out[label_key] = {"value": val, "source": "openai", "accepted": True, "conf": None, "threshold": thresholds[label_key]}

        _fill_ai("item_type", "item_type_ai_generated")
        _fill_ai("conflict",  "conflict_ai_generated")
        _fill_ai("nation",    "nation_ai_generated")

        # supergroup is purely auxiliary; if OpenAI produced it, include it
        if ai_result and ai_result.get("supergroup_ai_generated"):
            out["supergroup"] = {"value": ai_result["supergroup_ai_generated"], "source": "openai"}

        return out

# Product Details Deduplication Logic
def find_existing_db_row_details(product, site_profile, rds_manager):
    """
    Deduplication logic:
    1. Exact URL match
    2. site + any matching image URL
    Else: treat as new product

    Returns:
        (matched_id, matched_url) or (None, None)
    """
    url = product.get("url")
    url = CleanData.clean_url(url) if url else None

    site = site_profile.get("source_name")
    image_urls = product.get("original_image_urls") or []

    logging.debug(f"DETAIL DEDUP START: site={site!r}, url={url!r}, num_imgs={len(image_urls)}")

    # 1) Exact URL match
    if url:
        rows = rds_manager.fetch(
            "SELECT id, url FROM militaria WHERE url = %s LIMIT 1",
            (url,)
        )
        if rows:
            db_id, db_url = rows[0]
            logging.info("🟢 DEDUP MATCH: [Exact URL]")
            logging.info(f"    Incoming URL : {url}")
            logging.info(f"    Matched URL  : {db_url}")
            logging.info(f"    Matched ID   : {db_id}")
            return db_id, db_url

    # 2) Match by site + any image URL
    if site and image_urls:
        for img in image_urls:
            if not img or "placeholder" in img.lower():
                continue
            rows = rds_manager.fetch(
                "SELECT id, url FROM militaria WHERE site = %s AND original_image_urls ? %s LIMIT 1",
                (site, img)
            )
            if rows:
                db_id, db_url = rows[0]
                logging.info("🟡 DEDUP MATCH: [Site + Image]")
                logging.info(f"    Incoming URL : {url}")
                logging.info(f"    Matched URL  : {db_url}")
                logging.info(f"    Matched ID   : {db_id}")
                logging.info(f"    Matched Image: {img}")
                return db_id, db_url

    # No match → new product
    logging.info("🔵 DEDUP NEW: No match found")
    logging.info(f"    Incoming URL : {url}")
    return None, None

