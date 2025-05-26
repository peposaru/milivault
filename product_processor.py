import logging, json, pprint, re
from clean_data import CleanData
import image_extractor
from datetime import datetime,timezone
from decimal import Decimal
import post_processors as post_processors
from post_processors import normalize_input, apply_post_processors

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
        
    def product_tile_dict_processor_main(self, tile_product_data_list):
        processing_required_list = []
        availability_update_list = []

        # Categorize products into old and new
        try:
            logging.info(f"PRODUCT PROCESSOR: Categorizing products between old and new...")
            processing_required_list, availability_update_list = self.compare_tile_url_to_rds(tile_product_data_list)
            self.counter.add_availability_update_count(count=len(availability_update_list))
            self.counter.add_processing_required_count(count=len(processing_required_list))
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: compare_tile_url_to_rds: {e}")

        # Update availability list
        try:
            self.process_availability_update_list(availability_update_list)
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: process_availability_update_list: {e}")

        return processing_required_list, availability_update_list


    # If in rds, compare availability status, title, price and update if needed
    # If not in rds, create
    def compare_tile_url_to_rds(self, tile_product_data_list):
        processing_required_list = []  # Products needing full processing
        availability_update_list = []  # Products needing only availability updates
        ignored_update_count = 0       # Count products with no updates required

        for tile_product_dict in tile_product_data_list:
            try:
                url       = tile_product_dict['url']            
                title     = tile_product_dict['title']
                price     = tile_product_dict['price']
                available = tile_product_dict['available']
            except Exception as e:
                logging.error(f'PRODUCT PROCESSOR: Error retrieving tile_product_dict values: {e}')
                continue

            product_category = "Processing Required"
            reason = "New product or mismatched details"

            db_row = None
            if self.use_comparison_row:
                try:
                    result = self.rds_manager.fetch(
                        "SELECT title, price, available, description, price_history FROM militaria WHERE url = %s",
                        (url,)
                    )
                    if result:
                        db_row = (*result[0], True)
                except Exception as e:
                    logging.error(f"PRODUCT PROCESSOR: DB lookup failed for URL {url}: {e}")
                    continue

            if db_row:
                try:
                    db_title, db_price, db_available, db_description, db_price_history, db_present = db_row
                except ValueError as e:
                    logging.error(f"PRODUCT PROCESSOR: Error unpacking db_row for URL {url}: {e}")
                    continue

                # Determine price match logic
                if db_available == False and available == False and self.is_empty_price(price):
                    price_match = True
                    logging.debug(f"PRODUCT PROCESSOR: [MATCH] Sold item with no price: DB={db_price}, TILE={price}")
                elif self.is_empty_price(db_price) and not self.is_empty_price(price):
                    price_match = False
                    tile_product_dict["force_details_process"] = True
                    logging.debug(f"PRODUCT PROCESSOR: [FORCE PROCESS] DB has 0 price, tile has value → DB={db_price}, TILE={price}")
                elif not self.is_empty_price(db_price) and self.is_empty_price(price):
                    price_match = True
                    logging.debug(f"PRODUCT PROCESSOR: [IGNORE] Tile price is 0 but DB still has value → DB={db_price}, TILE={price}")
                else:
                    try:
                        price_match = float(db_price) == float(price)
                        if not price_match:
                            logging.debug(f"PRODUCT PROCESSOR: [MISMATCH] Price changed → DB={db_price}, TILE={price}")
                    except (TypeError, ValueError):
                        price_match = False
                        logging.debug(f"PRODUCT PROCESSOR: [MISMATCH] Could not compare prices → DB={db_price}, TILE={price}")

                # Compare cleaned titles
                title_cleaned = CleanData.clean_title(title)
                db_title_cleaned = CleanData.clean_title(db_title)

                if title_cleaned == db_title_cleaned and price_match:
                    if available != db_available:
                        logging.info(f"AVAIL CHANGE: Availability changed → URL: {url}, DB: {db_available}, Tile: {available}")
                        availability_update_list.append({"url": url, "available": available})
                        product_category = "Availability Update"
                        reason = "Availability status changed"
                        self.counter.add_availability_update_count(1)
                    else:
                        logging.info(f"SKIP: No changes → URL: {url}")
                        ignored_update_count += 1
                        continue
                else:
                    logging.info(
                        f"""PROCESSING REQUIRED: Title or price mismatch → URL: {url}
    DB Title     : {db_title_cleaned}
    Tile Title   : {title_cleaned}
    DB Price     : {db_price}
    Tile Price   : {price}"""
                    )
                    processing_required_list.append(tile_product_dict)
                    reason = "Mismatch in title or price"
                    self.counter.add_processing_required_count(1)
            else:
                logging.info(f"NEW PRODUCT: Not found in DB or comparison list → URL: {url}")
                processing_required_list.append(tile_product_dict)
                reason = "New product"
                self.counter.add_processing_required_count(1)

            logging.info(
                f"""
    ======== Product Summary ========
    URL                  : {url}
    DB Title             : {db_title if 'db_title' in locals() else 'N/A'}
    Tile Title           : {tile_product_dict['title']}
    DB Price             : {db_price if 'db_price' in locals() else 'N/A'}
    Tile Price           : {tile_product_dict['price']}
    DB Availability      : {db_available if 'db_available' in locals() else 'N/A'}
    Tile Availability    : {tile_product_dict['available']}
    ==================================
    Product Category     : {product_category}
    Reason               : {reason}
    ==================================
                """
            )

        logging.info(f'PRODUCT PROCESSOR: Products needing full processing  : {len(processing_required_list)}')
        logging.info(f'PRODUCT PROCESSOR: Products needing availability updates: {len(availability_update_list)}')
        logging.info(f'PRODUCT PROCESSOR: Products ignored (no updates needed): {ignored_update_count}')

        return processing_required_list, availability_update_list




    def is_empty_price(self, value):
        """Check if a price is empty (None, 0, 0.0, or an empty string)."""
        return value is None or value == 0 or value == 0.0 or value == ""

    def process_availability_update_list(self, availability_update_list):
        for product in availability_update_list:
            try:
                url = product['url']
                available = product['available']
                now_utc = datetime.now(timezone.utc).isoformat()

                logging.debug(f"PRODUCT PROCESSOR: Preparing to update: URL={url}, Available={available}")

                if available is False:
                    update_query = """
                    UPDATE militaria
                    SET available = %s,
                        date_sold = %s,
                        date_modified = %s,
                        last_seen = %s
                    WHERE url = %s;
                    """
                    params = (available, now_utc, now_utc, now_utc, url)
                else:
                    update_query = """
                    UPDATE militaria
                    SET available = %s,
                        date_sold = NULL,
                        date_modified = %s,
                        last_seen = %s
                    WHERE url = %s;
                    """
                    params = (available, now_utc, now_utc, url)

                self.rds_manager.update_record(update_query, params)
                logging.info(f"PRODUCT PROCESSOR: Successfully updated availability and last_seen for URL: {url}")

            except Exception as e:
                logging.error(f"PRODUCT PROCESSOR: Failed to update record for URL: {url}. Error: {e}")
        return
    


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


    def product_details_processor_main(self, processing_required_list):
        """
        Process product details for new and old products, adding debugging information.

        Args:
            processing_required_list (list): List of products needing processing.
        """
        logging.debug(f"PRODUCT PROCESSOR: Processing required list count: {len(processing_required_list)}")

        for product in processing_required_list:
            product_url = product.get('url')
            logging.debug(f"""PRODUCT PROCESSOR: 
    **************************************************              
    ******************PRODUCT CHANGE******************
    **************************************************""")
            logging.debug(f"PRODUCT PROCESSOR: Processing product URL: {product_url}")

            # Step 1: Query availability and price in one go
            try:
                result = self.rds_manager.fetch(
                    "SELECT available, price FROM militaria WHERE url = %s LIMIT 1", (product_url,)
                )
                db_present = bool(result)
                db_available, db_price = result[0] if result else (None, None)
            except Exception as e:
                logging.error(f"PRODUCT PROCESSOR: DB check for availability/price failed: {e}")
                db_present = False
                db_available = db_price = None

            # ✅ Skip early if product is sold in DB and likely unchanged
            if db_present and db_available is False:
                logging.debug(f"PRODUCT PROCESSOR: Fast check — item is sold in DB, validating if still sold on site...")
                
                # Use tile-level availability check to confirm it is *still* sold
                site_thinks_sold = not product.get("available", True)

                if site_thinks_sold:
                    logging.info(f"PRODUCT PROCESSOR: Skipping sold product (still sold): {product_url}")
                    continue

            # ✅ Step 2: Load product HTML
            try:
                product_url_soup = self.html_manager.parse_html(product_url)
                logging.debug("PRODUCT PROCESSOR: Successfully parsed product URL into BeautifulSoup.")
            except Exception as e:
                logging.error(f"PRODUCT PROCESSOR: Error parsing product URL {product_url}: {e}")
                continue

            # ✅ Step 3: Extract raw details from HTML
            try:
                details_data = self.construct_details_data(product_url, product_url_soup)
                tile_price = product.get("price")
                cleaned_details_price = CleanData.clean_price(details_data.get("price"))

                if (
                    db_present and
                    self.is_empty_price(cleaned_details_price) and
                    self.is_empty_price(db_price) and
                    not self.is_empty_price(tile_price)
                ):
                    logging.warning(
                        f"PRODUCT PROCESSOR: Overriding missing cleaned details/DB price with tile price → {tile_price}"
                    )
                    details_data["price"] = tile_price

                preview = pprint.pformat({k: str(v)[:200] for k, v in details_data.items()})
                logging.debug(f"PRODUCT PROCESSOR: Constructed details data (preview) for {product_url}: {preview}")
            except Exception as e:
                logging.error(f"PRODUCT PROCESSOR: Error constructing details data for {product_url}: {e}")
                continue

            # ✅ Step 4: Clean extracted data
            try:
                clean_details_data = self.construct_clean_details_data(details_data)
                logging.debug(f"PRODUCT PROCESSOR: Constructed clean details data for {product_url}: {clean_details_data}")
            except Exception as e:
                preview = pprint.pformat({k: str(v)[:200] for k, v in details_data.items()})
                logging.debug(f"PRODUCT PROCESSOR: Constructed details data (preview) for {product_url}: {preview}")
                continue

            # ✅ Step 5: Final availability check — skip if sold & still sold
            if db_available is False and clean_details_data.get("available") is False:
                logging.info(f"PRODUCT PROCESSOR: Skipping reprocessing of unchanged sold product: {product_url}")
                continue

            # ✅ Step 6: Choose new vs old handling
            if not db_present or product.get("force_new_upload"):
                logging.debug(f"PRODUCT PROCESSOR: Product URL {product_url} identified as new.")
                self.counter.add_new_product_count()
                try:
                    self.new_product_processor(clean_details_data, details_data)
                    logging.info(f"PRODUCT PROCESSOR: New product processed successfully: {product_url}")
                except Exception as e:
                    logging.error(f"PRODUCT PROCESSOR: Error processing new product {product_url}: {e}")
            else:
                logging.debug(f"PRODUCT PROCESSOR: Product URL {product_url} identified as old.")
                self.counter.add_old_product_count()
                try:
                    self.old_product_processor(clean_details_data)
                    logging.info(f"PRODUCT PROCESSOR: Old product processed successfully: {product_url}")
                except Exception as e:
                    logging.error(f"PRODUCT PROCESSOR: Error processing old product {product_url}: {e}")

        logging.debug(f"PRODUCT PROCESSOR: Processing completed for {len(processing_required_list)} products.")


    def new_product_processor(self, clean_details_data, details_data):
        """
        Insert new product data into the database.
        Uploads images to S3 using the database ID and updates DB with s3_image_urls.
        """
        try:
            logging.info('PRODUCT PROCESS: Starting data cleaning process...')
            logging.info(
                f"""
    ====== Data Cleaning Summary ======
    Pre-clean URL                : {details_data.get('url')}
    Post-clean URL               : {clean_details_data.get('url')}
    Pre-clean Title              : {details_data.get('title')}
    Post-clean Title             : {clean_details_data.get('title')}
    Pre-clean Description        : {details_data.get('description')}
    Post-clean Description       : {clean_details_data.get('description')}
    Pre-clean Price              : {details_data.get('price')}
    Post-clean Price             : {clean_details_data.get('price')}
    Pre-clean Availability       : {details_data.get('available')}
    Post-clean Availability      : {clean_details_data.get('available')}
    Pre-clean Image URLs         : {details_data.get('original_image_urls')}
    Post-clean Image URLs        : {clean_details_data.get('original_image_urls')}
    Pre-clean Nation             : {details_data.get('nation_site_designated')}
    Post-clean Nation            : {clean_details_data.get('nation_site_designated')}
    Pre-clean Conflict           : {details_data.get('conflict_site_designated')}
    Post-clean Conflict          : {clean_details_data.get('conflict_site_designated')}
    Pre-clean Item Type          : {details_data.get('item_type_site_designated')}
    Post-clean Item Type         : {clean_details_data.get('item_type_site_designated')}
    Pre-clean Extracted ID       : {details_data.get('extracted_id')}
    Post-clean Extracted ID      : {clean_details_data.get('extracted_id')}
    Pre-clean Grade              : {details_data.get('grade')}
    Post-clean Grade             : {clean_details_data.get('grade')}
    Pre-clean Site Categories    : {details_data.get('categories_site_designated')}
    Post-clean Site Categories   : {clean_details_data.get('categories_site_designated')}
    ===================================
                """
            )
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error logging cleaning summary: {e}")

        # STEP 0 — Generate AI classification
        try:
            ai_classifier = self.managers.get("openai_manager")
            if ai_classifier:
                ai_result = ai_classifier.classify_single_product(
                    title=clean_details_data.get("title", ""),
                    description=clean_details_data.get("description", "")
                )
                clean_details_data.update(ai_result)
                logging.info(f"PRODUCT PROCESSOR: AI classification added → {ai_result}")
            else:
                logging.warning("PRODUCT PROCESSOR: OpenAI manager not available — skipping AI classification.")
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: AI classification failed: {e}")

        # STEP 0.5 — Subcategory
        main_type = clean_details_data.get("item_type_ai_generated")
        if main_type and ai_classifier:
            try:
                sub_type = ai_classifier.classify_sub_item_type(
                    main_type,
                    clean_details_data.get("title", ""),
                    clean_details_data.get("description", "")
                )
                clean_details_data["sub_item_type_ai_generated"] = sub_type
                logging.info(f"PRODUCT PROCESSOR: AI subcategory added → {sub_type}")
            except Exception as e:
                logging.warning(f"PRODUCT PROCESSOR: Sub-item classification failed: {e}")

        # STEP 1 — Insert product
        try:
            self.rds_manager.new_product_input(clean_details_data)
            logging.info(f"PRODUCT PROCESSOR: Inserted new product {clean_details_data.get('url')} into database.")
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Failed to insert new product: {e}")
            return

        # STEP 2 — Get DB ID
        try:
            fetch_id_query = "SELECT id FROM militaria WHERE url = %s AND site = %s;"
            db_id = self.rds_manager.get_record_id(fetch_id_query, (clean_details_data["url"], clean_details_data["site"]))
            if db_id is None:
                logging.error(f"PRODUCT PROCESSOR: Could not retrieve DB ID for {clean_details_data['url']}")
                return
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Failed to fetch DB ID: {e}")
            return

        # STEP 3 — Upload images to S3 if image URLs exist
        try:
            site_name = clean_details_data.get("site")
            image_urls = clean_details_data.get("original_image_urls")
            product_url = clean_details_data.get("url")

            if not image_urls:
                logging.warning(f"PRODUCT PROCESSOR: No image URLs found for {product_url} — skipping S3 upload")
                s3_urls = []
            else:
                s3_urls = self.s3_manager.upload_images_for_product(db_id, image_urls, site_name, product_url,self.rds_manager)
                clean_details_data["s3_image_urls"] = s3_urls
                logging.info(f"PRODUCT PROCESSOR: Uploaded images to S3 for DB ID {db_id}")
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Failed to upload images for DB ID {db_id}: {e}")
            return


        # STEP 4 — Update DB with s3_image_urls if any were uploaded
        try:
            if s3_urls:
                update_query = """
                    UPDATE militaria
                    SET s3_image_urls = %s
                    WHERE id = %s;
                """
                self.rds_manager.execute(update_query, (json.dumps(s3_urls), db_id))
                logging.info(f"PRODUCT PROCESSOR: Updated s3_image_urls in DB for product ID {db_id}")
            else:
                logging.info(f"PRODUCT PROCESSOR: Skipping DB update for s3_image_urls — no images uploaded.")
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Failed to update s3_image_urls for product ID {db_id}: {e}")
            return



    def old_product_processor(self, clean_details_data):
        """
        Process existing products to compare and update changes in the database.

        Args:
            clean_details_data (dict): The cleaned product details to compare and update.
        """
        url = clean_details_data.get('url')
        now = datetime.now().isoformat()
        now_utc = datetime.now(timezone.utc).isoformat()

        try:
            # Fetch existing DB values (you may want to include original_image_urls here too)
            result = self.rds_manager.fetch(
                "SELECT title, price, available, description, price_history, original_image_urls FROM militaria WHERE url = %s",
                (url,)
            )
            if not result:
                logging.warning(f"PRODUCT PROCESSOR: Old product not found in DB, skipping → {url}")
                return

            db_title, db_price, db_available, db_description, db_price_history, db_image_urls = result[0]
            updates = {}
            only_availability_changed = True

            # Update title
            if clean_details_data.get('title') and clean_details_data['title'] != db_title:
                updates['title'] = clean_details_data['title']
                only_availability_changed = False

            # Update description
            if clean_details_data.get('description') and clean_details_data['description'] != db_description:
                updates['description'] = clean_details_data['description']
                only_availability_changed = False

            # Update price
            if clean_details_data.get('price') is not None and clean_details_data['price'] != db_price:
                if clean_details_data['price'] == 0.0 and db_price not in (None, 0.0):
                    logging.info(f"PRODUCT PROCESSOR: Skipping price update for {url}, keeping {db_price}")
                else:
                    updates['price'] = clean_details_data['price']
                    try:
                        price_history = json.loads(db_price_history) if isinstance(db_price_history, str) else db_price_history or []
                    except:
                        price_history = []

                    price_history.append({
                        "price": float(db_price) if db_price is not None else None,
                        "date": now
                    })
                    updates['price_history'] = json.dumps(price_history)

                    only_availability_changed = False

            # Update availability
            if clean_details_data.get('available') != db_available:
                new_availability = clean_details_data.get('available')
                updates['available'] = new_availability
                updates['last_seen'] = now_utc
                updates['date_sold'] = None if new_availability else now_utc

                if only_availability_changed:
                    # Fast-path: availability is the only thing that changed
                    update_query = """
                    UPDATE militaria
                    SET available = %s,
                        date_sold = %s,
                        date_modified = %s,
                        last_seen = %s
                    WHERE url = %s;
                    """
                    self.rds_manager.execute(update_query, (
                        new_availability, updates['date_sold'], now_utc, now_utc, url
                    ))
                    logging.info(f"PRODUCT PROCESSOR: Availability-only update completed for {url}")
                    return

            # Update original image URLs — only if changed
            new_images = clean_details_data.get('original_image_urls')
            if new_images:
                try:
                    current_images = json.loads(db_image_urls) if isinstance(db_image_urls, str) else db_image_urls or []
                except:
                    current_images = []

                if new_images != current_images:
                    updates['original_image_urls'] = json.dumps(new_images)
                    only_availability_changed = False
                    logging.info(f"PRODUCT PROCESSOR: Image URLs changed for {url}")

            # Update remaining fields if different
            for field in [
                'nation_site_designated', 'conflict_site_designated',
                'item_type_site_designated', 'extracted_id', 'grade',
                'categories_site_designated'
            ]:
                value = clean_details_data.get(field)
                if value:
                    updates[field] = json.dumps(value) if isinstance(value, list) else value
                    only_availability_changed = False

            # ✅ Skip unnecessary DB update if nothing changed
            if not updates:
                logging.info(f"PRODUCT PROCESSOR: No changes detected — skipping update for {url}")
                return

            # Finalize update
            updates['date_modified'] = now_utc
            updates['last_seen'] = now_utc
            set_clause = ', '.join(f"{key} = %s" for key in updates)
            query = f"UPDATE militaria SET {set_clause} WHERE url = %s"
            self.rds_manager.execute(query, list(updates.values()) + [url])
            logging.info(f"PRODUCT PROCESSOR: Successfully updated old product {url}")

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error processing old product {url}: {e}")



    def extract_data(self, soup, method, args, kwargs, attribute, config=None):
        from post_processors import normalize_input

        try:
            logging.debug(f"PRODUCT PROCESSOR: EXTRACT DATA: method={method}, args={args}, kwargs={kwargs}, attribute={attribute}")

            # Special case: check if attribute exists
            if method == "has_attr" and args:
                attr_name = args[0]
                attr_value = soup.get(attr_name)
                result = " ".join(attr_value) if isinstance(attr_value, list) else attr_value or ""
                result = normalize_input(result)
                logging.debug(f"PRODUCT PROCESSOR: EXTRACT DATA: has_attr result → {result}")
                return result

            # Perform the extraction (e.g., soup.find(...))
            element = getattr(soup, method)(*args, **kwargs)
            if not element:
                logging.debug("PRODUCT PROCESSOR: EXTRACT DATA: Element not found.")
                return None

            # If result is a list (e.g., from find_all), extract text from the first element
            if isinstance(element, list):
                if not element:
                    logging.debug("PRODUCT PROCESSOR: EXTRACT DATA: Empty list from find_all.")
                    return None
                element = element[0]  # fallback to first element

            # --- Text or attribute extraction ---

            # Config says extract text explicitly
            if config and config.get("extract") == "text":
                result = element.get_text(strip=True)
                logging.debug(f"PRODUCT PROCESSOR: EXTRACT DATA: Extracted text → {result}")
                return result

            # Config says extract raw HTML
            if config and config.get("extract") == "html":
                result = str(element)
                logging.debug(f"PRODUCT PROCESSOR: EXTRACT DATA: Extracted HTML → {result[:100]}...")
                return result

            # Extract specific attribute
            if attribute:
                attr_val = element.get(attribute, "").strip()
                logging.debug(f"PRODUCT PROCESSOR: EXTRACT DATA: Extracted attribute '{attribute}' → {attr_val}")
                return attr_val

            # Default: get text from tag
            if hasattr(element, "get_text"):
                result = element.get_text(strip=True)
                logging.debug(f"PRODUCT PROCESSOR: EXTRACT DATA: Default tag text → {result}")
                return result

            # Fallback to string conversion
            result = str(element).strip()
            logging.debug(f"PRODUCT PROCESSOR: EXTRACT DATA: Fallback str() → {result}")
            return result

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting data: {e}")
            return None



    def extract_details_title(self, soup):
        """
        Extract and post-process the product title from the details page using the configured selectors.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_title", {})
            method = selector_config.get("method", "find")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            # Extract raw value
            title = self.extract_data(soup, method, args, kwargs, attribute, selector_config)

            title = normalize_input(title)

            # Apply post-processing using central dispatcher
            if title and "post_process" in selector_config:
                title = apply_post_processors(title, selector_config["post_process"])

            logging.debug(f"EXTRACT DETAILS TITLE: Final value → {title}")
            return title if title else None

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting title: {e}")
            return None




    def extract_details_description(self, soup):
        """
        Extract and optionally post-process the product description.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_description", {})
            method = selector_config.get("method", "find")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            # Submethod logic stays as-is
            submethod = selector_config.get("submethod")
            element = getattr(soup, method)(*args, **kwargs)

            if submethod and element:
                sub_element = getattr(element, submethod.get("method", "find"))(
                    *submethod.get("args", []),
                    **submethod.get("kwargs", {})
                )
                if sub_element:
                    attribute = submethod.get("attribute", attribute)
                    description = sub_element.get(attribute).strip() if attribute else sub_element.get_text(strip=True)
                else:
                    description = None
            else:
                description = (
                    element.get(attribute).strip() if attribute and element and element.get(attribute)
                    else element.get_text(strip=True) if element else None
                )

            # Normalize value
            description = normalize_input(description)

            # Apply post-processing using dispatcher
            if description and "post_process" in selector_config:
                description = apply_post_processors(description, selector_config["post_process"])

            return description if description else None

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting description: {e}")
            return None



    def extract_details_price(self, soup, product_url):
        """
        Extract and optionally post-process the product price from the details page.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_price", {})
            method = selector_config.get("method", "find")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            # Extract raw value
            raw_price = self.extract_data(soup, method, args, kwargs, attribute)

            # Normalize input
            raw_price = normalize_input(raw_price)

            # Apply post-processing if defined
            if raw_price and "post_process" in selector_config:
                # Inject soup and URL if the function needs context
                for key, arg in selector_config["post_process"].items():
                    if isinstance(arg, dict):
                        arg.setdefault("soup", soup)
                        arg.setdefault("url", product_url)

                raw_price = apply_post_processors(raw_price, selector_config["post_process"], soup=soup)

            return str(raw_price) if raw_price else "0"

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting price: {e}")
            return "0"



    def process_price_updates(clean_details_data, db_price, db_price_history, now):
        """
        Process price updates and manage the price history.

        Args:
            clean_details_data (dict): The new product details data.
            db_price (Decimal): The existing price in the database.
            db_price_history (list|str): The existing price history, as list or JSON string.
            now (str): The current timestamp.

        Returns:
            dict: A dictionary of updates for price and price history.
        """
        updates = {}

        new_price = clean_details_data.get('price')

        # Normalize db_price_history if it's a string
        if isinstance(db_price_history, str):
            try:
                db_price_history = json.loads(db_price_history)
            except Exception:
                db_price_history = []

        if new_price is not None:
            try:
                if float(new_price) != float(db_price):
                    # Update price
                    updates['price'] = new_price

                    # Avoid storing invalid old prices
                    if db_price not in (None, 0.0):
                        updated_price_history = db_price_history or []
                        updated_price_history.append({
                            'price': float(db_price),
                            'date': now
                        })
                        updates['price_history'] = json.dumps(updated_price_history)

            except Exception as e:
                logging.warning(f"PRODUCT PROCESSOR: Failed to compare or update price → {e}")

        return updates



    def extract_details_availability(self, soup):
        """
        Extract availability and apply post-processing if defined.
        Supports static values and post-processing logic.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_availability", {})

            # Static hardcoded config support
            if selector_config is True:
                logging.debug("PRODUCT PROCESSOR: Availability hardcoded as True in JSON.")
                return True
            elif selector_config is False:
                logging.debug("PRODUCT PROCESSOR: Availability hardcoded as False in JSON.")
                return False
            elif isinstance(selector_config, str):
                val = selector_config.strip().lower()
                if val == "true":
                    logging.debug("PRODUCT PROCESSOR: Availability string 'true' treated as True.")
                    return True
                elif val == "false":
                    logging.debug("PRODUCT PROCESSOR: Availability string 'false' treated as False.")
                    return False

            # Selector-based extraction
            method = selector_config.get("method", "find")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            raw_value = self.extract_data(soup, method, args, kwargs, attribute, selector_config)
            logging.debug(f"PRODUCT PROCESSOR: Extracted availability raw: {raw_value}")

            # Normalize before processing
            raw_value = normalize_input(raw_value)

            # Apply post-processing if defined
            if raw_value and "post_process" in selector_config:
                raw_value = apply_post_processors(raw_value, selector_config["post_process"])

            # Final interpretation
            if isinstance(raw_value, bool):
                return raw_value
            if isinstance(raw_value, str):
                return raw_value.lower() in ("true", "yes", "available", "in stock", "add to cart")

            # Fallback default
            return False

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting availability: {e}")
            return False




    def extract_details_image_url(self, soup):
        """
        Extract image URLs using the specified function in the JSON profile.
        
        Args:
            soup (BeautifulSoup): The BeautifulSoup object for the product page.
            
        Returns:
            list: A list of extracted image URLs or an empty list if the function is not defined or fails.
        """
        try:
            # Fetch the function name from the JSON profile
            image_extractor_function_version = self.site_profile.get("product_details_selectors", {}).get("details_image_url", {}).get("function")

            # Ensure it's a string before calling .lower()
            if isinstance(image_extractor_function_version, str) and image_extractor_function_version.lower() == "skip":
                return []

            logging.debug(f"PRODUCT PROCESSOR: Function name retrieved: {image_extractor_function_version}")

            if not image_extractor_function_version:
                raise ValueError("PRODUCT PROCESSOR: Image extraction function name not specified in the JSON profile.")
            
            # Ensure the function exists in the image_extractor module
            if not hasattr(image_extractor, image_extractor_function_version):
                raise AttributeError(f"PRODUCT PROCESSOR: Function '{image_extractor_function_version}' not found in image_extractor.")
            
            # Dynamically call the function from image_extractor
            details_image_urls = getattr(image_extractor, image_extractor_function_version)(soup)
            if not isinstance(details_image_urls, list):
                raise TypeError(f"PRODUCT PROCESSOR: Function '{image_extractor_function_version}' did not return a list of URLs.")
        
            return details_image_urls

        except AttributeError as ae:
            logging.error(f"PRODUCT PROCESSOR: AttributeError: {ae}")
            return []

        except ValueError as ve:
            logging.error(f"PRODUCT PROCESSOR: ValueError: {ve}")
            return []

        except TypeError as te:
            logging.error(f"PRODUCT PROCESSOR: TypeError: {te}")
            return []

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Unexpected error in extract_details_image_url: {e}")
            return []

    def extract_details_nation(self, soup):
        config = self.site_profile.get("product_details_selectors", {}).get("details_nation")
        if isinstance(config, str):
            return config
        if isinstance(config, dict):
            method = config.get("method", "find")
            args = config.get("args", [])
            kwargs = config.get("kwargs", {})
            attribute = config.get("attribute")
            value = self.extract_data(soup, method, args, kwargs, attribute, config)
            value = normalize_input(value)
            return apply_post_processors(value, config.get("post_process", {})) if value else None
        return self.site_profile.get("metadata_selectors", {}).get("nation")

    def extract_details_conflict(self, soup):
        try:
            config = self.site_profile.get("product_details_selectors", {}).get("details_conflict")
            if isinstance(config, str):
                return config
            if isinstance(config, dict):
                method = config.get("method", "find")
                args = config.get("args", [])
                kwargs = config.get("kwargs", {})
                attribute = config.get("attribute")
                value = self.extract_data(soup, method, args, kwargs, attribute, config)
                value = normalize_input(value)
                if value and "post_process" in config:
                    value = apply_post_processors(value, config["post_process"])
                return value if value else None
            return self.site_profile.get("metadata_selectors", {}).get("conflict")
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting conflict: {e}")
            return None


    def extract_details_item_type(self, soup):
        try:
            config = self.site_profile.get("product_details_selectors", {}).get("details_item_type")
            if isinstance(config, str):
                return config
            if isinstance(config, dict):
                method = config.get("method", "find")
                args = config.get("args", [])
                kwargs = config.get("kwargs", {})
                attribute = config.get("attribute")
                value = self.extract_data(soup, method, args, kwargs, attribute, config)
                value = normalize_input(value)
                if value and "post_process" in config:
                    value = apply_post_processors(value, config["post_process"])
                return value if value else None
            return self.site_profile.get("metadata_selectors", {}).get("item_type")
        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting item type: {e}")
            return None


    def extract_details_extracted_id(self, soup):
        """
        Extract the product ID or SKU and apply post-processing if defined.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_extracted_id", {})
            method = selector_config.get("method", "find")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            extracted_id = self.extract_data(soup, method, args, kwargs, attribute, selector_config)

            extracted_id = normalize_input(extracted_id)

            if extracted_id and "post_process" in selector_config:
                extracted_id = apply_post_processors(extracted_id, selector_config["post_process"])

            return extracted_id if extracted_id else None

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting extracted_id: {e}")
            return None


    # This is just a placeholder
    def extract_details_grade(self, soup):
        """
        Extract the grade information from the product page using selectors and post-processing.
        """
        try:
            selector_config = self.site_profile.get("product_details_selectors", {}).get("details_grade", {})
            if not selector_config:
                return None

            method = selector_config.get("method", "find")
            args = selector_config.get("args", [])
            kwargs = selector_config.get("kwargs", {})
            attribute = selector_config.get("attribute")

            grade = self.extract_data(soup, method, args, kwargs, attribute, selector_config)

            grade = normalize_input(grade)

            if grade and "post_process" in selector_config:
                grade = apply_post_processors(grade, selector_config["post_process"])

            return grade if grade else None

        except Exception as e:
            logging.error(f"PRODUCT PROCESSOR: Error extracting grade: {e}")
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
            "title"                     : clean_data.clean_title,
            "description"               : clean_data.clean_description,
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
        
    def is_empty_price(self, value):
        """Check if a price is empty (None, 0, 0.0, or an empty string)."""
        return value is None or value == 0 or value == 0.0 or value == ""
    
    def cast(value, config):
        value = normalize_input(value)
        if config == "float":
            try:
                return float(re.sub(r"[^\d.]", "", value))
            except Exception as e:
                logging.warning(f"POST PROCESS: Failed cast to float: {e}")
                return value
        return value
    

    def strip_list(value, config=None):
        if isinstance(value, list):
            return [str(v).strip() for v in value]
        return value

    def _static_value_or_extracted(self, key, extractor_func, soup):
        config = self.details_selectors.get(key)
        if isinstance(config, str):
            return config
        elif isinstance(config, dict):
            return extractor_func(soup)
        return None
