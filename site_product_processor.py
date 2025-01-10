from datetime import datetime, date
import logging, json
import time
from urllib.parse import urlparse

#Process individual site.
def process_site(webScrapeManager, dataManager, jsonManager, prints, site, targetMatch, runCycle, productsProcessed, s3_manager):
    """
    Processes a single site based on the JSON selector configuration, scraping and updating/inserting products.
    """
    (
        conflict, nation, item_type, grade, source, pageIncrement, currency, products,
        productUrlElement, titleElement, descElement, priceElement, availableElement,
        productsPageUrl, base_url, imageElement
    ) = jsonManager.jsonSelectors(site)

    urlCount           = 0
    consecutiveMatches = 0
    page               = 0

    # While the target number of consecutive matches have no been met, keep the program running.
    while True:
        if consecutiveMatches == targetMatch:
            # Ensure termination when the targetMatch is reached
            logging.warning(f"Target match count ({targetMatch}) reached. Exiting the processing loop.")
            break

        # Generate link for the sites products page. Typically the new items page.
        productsPage = base_url + productsPageUrl.format(page=page)
        logging.debug(f"Navigating to products page: {productsPage}")

        # Create a list of urls extracted from products page. Typically the new items page.
        product_list = fetch_products_from_page(webScrapeManager, productsPage, products)
        if not product_list:
            logging.warning(f"No products found on page: {productsPage}")
            break

        prints.newInstance(source, productsPage, runCycle, productsProcessed)

        for product in product_list:
            if not product:
                logging.error("Empty product element found, skipping.")
                continue

            # Process each product and track updates
            urlCount, consecutiveMatches = process_product(
                webScrapeManager, dataManager, prints, product, source, base_url, currency,
                conflict, nation, item_type, page, urlCount, consecutiveMatches, targetMatch,
                productUrlElement, titleElement, descElement, priceElement, availableElement,
                imageElement, s3_manager 
            )

            # Stop if the target match count is reached
            if consecutiveMatches == targetMatch:
                logging.warning(f"Target match count ({targetMatch}) reached. Terminating site processing.")
                prints.terminating(source, consecutiveMatches, targetMatch, runCycle, productsProcessed)
                return

        # Increment to the next page
        page += int(pageIncrement)

    logging.info(f"Finished processing site: {source}")



# Process a single product
def process_product(
    webScrapeManager, dataManager, prints, product, source, base_url, currency,
    conflict, nation, item_type, page, urlCount, consecutiveMatches, targetMatch,
    productUrlElement, titleElement, descElement, priceElement, availableElement,
    imageElement, s3_manager
):
    """
    Process a single product by constructing its URL, scraping details, and updating/inserting into the database.
    """
    try:
        # Increment the total number of products processed
        urlCount += 1

        # Check if the target match count has been reached
        if consecutiveMatches is None or targetMatch is None:
            logging.error(
                f"One of the values is None before comparison. consecutiveMatches={consecutiveMatches}, targetMatch={targetMatch}"
            )
        else:
            logging.debug(
                f"Checking target match in regular processing. consecutiveMatches={consecutiveMatches}, targetMatch={targetMatch}"
            )

        if consecutiveMatches is not None and targetMatch is not None and consecutiveMatches >= targetMatch:
            logging.warning(
                f"Target match count ({targetMatch}) reached. Halting product processing."
            )
            return urlCount, consecutiveMatches

        # Create the product URL and if it doesn't work, stop.
        productUrl = construct_product_url(productUrlElement, base_url, product)
        if not productUrl:
            logging.warning("Product URL is invalid. Skipping this product.")
            return urlCount, consecutiveMatches

        # Check if the product's images have already been uploaded
        if dataManager.should_skip_image_upload(productUrl):
            consecutiveMatches += 1  # Increment because the product is already known
            logging.info(f"Skipping image upload for product: {productUrl} as images are already uploaded.")
            return urlCount, consecutiveMatches
        else:
            # Reset consecutiveMatches because this product is new
            consecutiveMatches = 0

        # Fetch and scrape the product details
        try:
            title, description, price, available, original_image_urls, uploaded_image_urls = fetch_and_scrape_product(
                webScrapeManager, productUrl, titleElement, descElement, priceElement,
                availableElement, imageElement, currency, source, s3_manager, dataManager
            )
        except Exception as e:
            logging.warning(f"Error during fetch and scrape for product: {productUrl}, Error: {e}")
            return urlCount, consecutiveMatches

        if not title:
            logging.warning(f"Failed to fetch or scrape product details for {productUrl}. Skipping.")
            return urlCount, consecutiveMatches

        # Handle cases where image extraction was skipped
        if imageElement is None:
            logging.debug(f"Image extraction skipped for product: {productUrl}")
            original_image_urls = []
            uploaded_image_urls = []

        # Update or insert product in the database
        urlCount, consecutiveMatches, updated = update_or_insert_product(
            dataManager, prints, productUrl, title, description, price, available,
            source, currency, conflict, nation, item_type, page, urlCount, consecutiveMatches,
            targetMatch, original_image_urls, uploaded_image_urls, s3_manager
        )

        # Stop processing immediately if the target match count is reached after updating
        if consecutiveMatches is None or targetMatch is None:
            logging.error(
                f"One of the values is None before comparison. consecutiveMatches={consecutiveMatches}, targetMatch={targetMatch}"
            )
        else:
            logging.debug(
                f"Checking target match after update. consecutiveMatches={consecutiveMatches}, targetMatch={targetMatch}"
            )

        if consecutiveMatches is not None and targetMatch is not None and consecutiveMatches >= targetMatch:
            logging.info(
                f"Target match count ({targetMatch}) reached after processing product: {productUrl}."
            )
            return urlCount, consecutiveMatches

        # Log based on whether the product was updated or not
        if updated:
            logging.info(f"Product '{productUrl}' was updated or inserted successfully.")
        else:
            logging.info(f"No changes made for product '{productUrl}'.")

    except Exception as e:
        logging.error(f"Error processing product: {e}")
    return urlCount, consecutiveMatches

# Get the list of products from the products page.
def fetch_products_from_page(webScrapeManager, productsPage, productsSelector):
    soup = webScrapeManager.readProductPage(productsPage)
    if soup is None:
        logging.warning(f"Failed to load products page: {productsPage}")
        return None

    try:
        product_list = eval(productsSelector)
        return product_list
    except Exception as e:
        logging.error(f"Error parsing products on page {productsPage}: {e}")
        return None

# Construct product URL
def construct_product_url(productUrlElement, base_url, product):
    """Construct the product URL using the provided product selector and base URL."""
    try:
        productUrl = eval(productUrlElement)
        if not productUrl.startswith("http"):
            productUrl = base_url + productUrl
        logging.debug(f"Product URL constructed: {productUrl}")
        return productUrl
    except Exception as e:
        logging.error(f"Error constructing product URL: {e}")
        return None

# Fetch and scrape product details
def fetch_and_scrape_product(
    webScrapeManager, productUrl, titleElement, descElement, priceElement,
    availableElement, imageElement, currency, source, s3_manager, dataManager
):
    """
    Fetch the product page and scrape its details, including uploading images to S3.
    """
    try:
        # Fetch the product page
        productSoup = webScrapeManager.fetch_with_retries(
            webScrapeManager.scrapePage, productUrl, max_retries=3, backoff_factor=2
        )
        if not productSoup:
            logging.warning(f"Failed to fetch product page: {productUrl}")
            return None, None, None, None, [], []

        # Scrape product details
        title, description, price, available, image_urls = webScrapeManager.scrapeData(
            productSoup, titleElement, descElement, priceElement, availableElement,
            None if not imageElement else imageElement, currency, source
        )
        logging.debug(f"Scraped data: Title={title}, Price={price}, Available={available}")

        # Skip image processing if imageElement is None or "skip"
        if not imageElement or imageElement.lower() == "skip":
            logging.debug("Image extraction skipped due to missing or placeholder imageElement.")
            return title, description, price, available, [], []

        # Validate extracted image URLs
        if not all(isinstance(url, str) for url in image_urls):
            logging.error(f"Invalid image URLs detected: {image_urls}")
            return title, description, price, available, image_urls, []

        # Get product ID from the database
        product_id = dataManager.get_product_id(productUrl)
        if product_id is None:
            logging.error(f"Failed to find product ID for URL: {productUrl}")
            return title, description, price, available, image_urls, []

        # Upload images to S3
        uploaded_image_urls = []
        for idx, url in enumerate(image_urls, start=1):
            try:
                # Construct the S3 object name
                parsed_url = urlparse(url)
                extension = parsed_url.path.split('.')[-1] or "jpg"
                object_name = f"{source}/{product_id}/{product_id}-{idx}.{extension}"

                # Upload image to S3
                s3_manager.upload_image(url, object_name)  # Pass only two arguments
                uploaded_image_urls.append(f"s3://{s3_manager.bucket_name}/{object_name}")
            except Exception as e:
                logging.warning(f"Error uploading image {url}: {e}")

        return title, description, price, available, image_urls, uploaded_image_urls
    except Exception as e:
        logging.error(f"Error fetching and scraping product: {e}")
        return None, None, None, None, [], []

# Update or insert product in database
def update_or_insert_product(
    dataManager, prints, productUrl, title, description, price, available, source, 
    currency, conflict, nation, item_type, page, urlCount, consecutiveMatches, 
    targetMatch, original_image_urls, uploaded_image_urls, s3_manager
):
    """
    Update or insert product details into the database, including handling image URLs.
    """
    try:
        searchQuery = "SELECT url, available, date_sold, original_image_urls, s3_image_urls FROM militaria WHERE url = %s"
        existingProducts = dataManager.sqlFetch(searchQuery, (productUrl,))

        updated = False
        s3_image_urls_json = json.dumps(uploaded_image_urls)
        original_image_urls_json = json.dumps(original_image_urls)

        if existingProducts:
            existingProduct = existingProducts[0]
            original_available, original_date_sold, db_original_image_urls, db_s3_image_urls = existingProduct[1:]

            # Update available status
            if available != original_available:
                updateQuery = "UPDATE militaria SET available = %s WHERE url = %s;"
                dataManager.sqlExecute(updateQuery, (available, productUrl))
                updated = True

            # Update sold date
            if not available and original_date_sold is None:
                todayDate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                updateSoldQuery = "UPDATE militaria SET date_sold = %s WHERE url = %s;"
                dataManager.sqlExecute(updateSoldQuery, (todayDate, productUrl))
                updated = True

            # Update image URLs
            if s3_image_urls_json != db_s3_image_urls or original_image_urls_json != db_original_image_urls:
                updateImageUrlsQuery = """
                    UPDATE militaria SET s3_image_urls = %s, original_image_urls = %s WHERE url = %s;
                """
                dataManager.sqlExecute(updateImageUrlsQuery, (s3_image_urls_json, original_image_urls_json, productUrl))
                updated = True
        else:
            todayDate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            insertQuery = """
                INSERT INTO militaria (url, title, description, price, available, date, 
                site, currency, conflict, nation, item_type, s3_image_urls, original_image_urls)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            dataManager.sqlExecute(insertQuery, (
                productUrl, title, description, price, available, todayDate,
                source, currency, conflict, nation, item_type, s3_image_urls_json, original_image_urls_json
            ))
            updated = True

        consecutiveMatches = consecutiveMatches + 1 if updated else consecutiveMatches
    except Exception as e:
        logging.error(f"Error updating or inserting product: {e}")
    return urlCount + 1, consecutiveMatches, updated