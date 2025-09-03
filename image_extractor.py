import logging, time
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import sys

def woo_commerce(product_soup):
    """
    Extracts high-quality images from WooCommerce product pages.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: List of URLs for the largest images.
    """
    try:
        # Extract `data-large_image` directly if available
        large_image_urls = [
            tag['data-large_image']
            for tag in product_soup.select("div.woocommerce-product-gallery__image")
            if 'data-large_image' in tag.attrs
        ]
        
        # If `data-large_image` is missing, fallback to the <a href>
        if not large_image_urls:
            large_image_urls = [
                a_tag['href']
                for a_tag in product_soup.select("div.woocommerce-product-gallery__image a")
                if 'href' in a_tag.attrs
            ]

        return large_image_urls
    except Exception as e:
        logging.error(f"Error in woo_commerce: {e}")
        return []

def woo_commerce2(product_soup):
    """
    Extracts high-quality images from WooCommerce-like vertical gallery product pages.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: List of URLs for the largest images.
    """
    try:
        # Extract URLs from `data-zoom` attribute
        large_image_urls = [
            div['data-zoom']
            for div in product_soup.select("div.product.item-image.imgzoom")
            if 'data-zoom' in div.attrs
        ]

        # Fallback to <a href> if `data-zoom` is not present
        if not large_image_urls:
            large_image_urls = [
                a_tag['href']
                for a_tag in product_soup.select("div.product.item-image.imgzoom a")
                if 'href' in a_tag.attrs
            ]

        # Validate URLs
        valid_image_urls = [url for url in large_image_urls if isinstance(url, str) and url.startswith("http")]
        if not valid_image_urls:
            logging.warning("No valid image URLs found in woo_commerce2.")
            return []

        # Add a delay to prevent rate limiting or server overload
        for url in valid_image_urls:
            time.sleep(1)  # 1-second delay between processing each image
            logging.debug(f"Processing valid URL: {url}")

        return valid_image_urls
    except Exception as e:
        logging.error(f"Error in woo_commerce2: {e}")
        return []

def concept500(product_soup):
    """
    Extracts high-quality image URLs from HTML structured with 'content-part block-image' divs.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: List of absolute URLs for the largest images.
    """
    try:
        image_urls = [
            tag['href']
            for tag in product_soup.select("div.content-part.block-image a")
            if 'href' in tag.attrs
        ]

        # If image URLs are relative, prepend the inferred base URL
        if image_urls and not image_urls[0].startswith("http"):
            base_tag = product_soup.find('base')
            inferred_base_url = (
                base_tag['href'].rstrip('/')
                if base_tag and 'href' in base_tag.attrs
                else product_soup.select_one("link[rel='canonical']")['href'].rstrip('/')
            )

            image_urls = [
                inferred_base_url + '/' + url.lstrip('/') if not url.startswith("http") else url
                for url in image_urls
            ]

        return image_urls
    except Exception as e:
        logging.error(f"Error in concept500: {e}")
        return []

def fetch_images(product_soup, function_name):
    """
    Fetch images dynamically based on function name.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.
        function_name (str): Name of the function to use for image extraction.

    Returns:
        list: List of image URLs.
    """
    try:
        # Dynamically fetch the function
        func = globals()[function_name]
        return func(product_soup)
    except KeyError:
        logging.error(f"Function {function_name} not found.")
        return []
    except Exception as e:
        logging.error(f"Error fetching images: {e}")
        return []

def ea_militaria(product_soup):
    """
    Extracts the largest image URLs from EA Militaria product pages.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: List of URLs for the largest images.
    """
    try:
        # Find all elements with 'data-zoom' attribute (largest images)
        image_elements = product_soup.select('div.product.item-image.imgzoom')
        large_image_urls = [
            img['data-zoom']
            for img in image_elements
            if 'data-zoom' in img.attrs
        ]
        return large_image_urls
    except Exception as e:
        logging.error(f"Error in ea_militaria: {e}")
        return []

def rg_militaria(product_soup):
    """
    Extracts the URLs of the largest images from the gallery on the product page.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: List of URLs for the largest images.
    """
    try:
        # Select all the gallery items with high-resolution images
        high_res_images = [
            a_tag['href']
            for a_tag in product_soup.select("a.image-gallery__slide-item")
            if 'href' in a_tag.attrs
        ]
        return high_res_images
    except Exception as e:
        print(f"Error extracting images: {e}")
        return []
    
def militaria_plaza(product_soup):
    """
    Extracts high-resolution images from Militaria Plaza product pages.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: List of URLs for the largest images.
    """
    try:
        # Collect high-resolution image URLs from the `href` attribute of links with `rel="vm-additional-images"`
        image_urls = [
            tag['href']
            for tag in product_soup.select("a[rel='vm-additional-images']")
            if 'href' in tag.attrs
        ]
        return image_urls
    except Exception as e:
        logging.error(f"Error in militaria_plaza: {e}")
        return []

def circa1941(product_soup):
    """
    Extracts high-resolution image URLs from Circa1941 product pages.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: High-resolution image URLs.
    """
    try:
        # Select all image containers
        image_containers = product_soup.select('[data-hook="main-media-image-wrapper"] div.media-wrapper-hook')
        
        # Extract image URLs
        image_urls = []
        for container in image_containers:
            image = container.get('href')  # The `href` attribute contains the high-resolution image URL
            if image:
                image_urls.append(image)
        
        return image_urls
    except Exception as e:
        logging.error(f"Error extracting images: {e}")
        return []
    
def frontkampfer45(product_soup):
    """
    Extracts high-resolution image URLs from Frontkampfer45 product pages.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: High-resolution image URLs.
    """
    try:
        # Select all containers with image data
        image_containers = product_soup.select('[data-hook="main-media-image-wrapper"] div.media-wrapper-hook')
        
        # Extract image URLs from the href attribute
        image_urls = []
        for container in image_containers:
            high_res_image = container.get('href')
            if high_res_image:
                image_urls.append(high_res_image)

        return image_urls
    except Exception as e:
        logging.error(f"Error extracting images: {e}")
        return []
    
def wars_end_shop(product_soup):
    """
    Extracts high-resolution image URLs from Wars End Shop product pages.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: High-resolution image URLs.
    """
    try:
        # Find all image containers within the product-photo-container
        image_containers = product_soup.select('#product-photo-container a.gallery')
        
        # Extract high-resolution image URLs
        image_urls = []
        for container in image_containers:
            high_res_image = container.get('href')
            if high_res_image:
                # Ensure full URL is formed
                if high_res_image.startswith("//"):
                    high_res_image = "https:" + high_res_image
                image_urls.append(high_res_image)

        return image_urls
    except Exception as e:
        logging.error(f"Error extracting images: {e}")
        return []  
    
def the_war_front(product_soup):
    """
    Extracts high-resolution image URLs from The War Front product pages.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: High-resolution image URLs.
    """
    try:
        # Find all image containers within the main media section
        image_containers = product_soup.select('[data-hook="main-media-image-wrapper"] .media-wrapper-hook')
        
        # Extract high-resolution image URLs
        image_urls = []
        for container in image_containers:
            high_res_image = container.get("href")
            if high_res_image:
                # Ensure full URL is formed
                if high_res_image.startswith("//"):
                    high_res_image = "https:" + high_res_image
                image_urls.append(high_res_image)

        return image_urls
    except Exception as e:
        logging.error(f"Error extracting images: {e}")
        return []
    
def the_ruptured_duck(product_soup):
    """
    Extracts high-resolution image URLs from The Ruptured Duck product pages.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: High-resolution image URLs.
    """
    try:
        # Select all primary and thumbnail images
        image_containers = product_soup.select(".product-single__thumbnail-item a")
        
        # Extract high-resolution image URLs from the href attribute
        image_urls = []
        for container in image_containers:
            high_res_image = container.get("href")
            if high_res_image:
                # Ensure full URL is formed
                if high_res_image.startswith("//"):
                    high_res_image = "https:" + high_res_image
                image_urls.append(high_res_image)

        return image_urls
    except Exception as e:
        logging.error(f"Error extracting images from The Ruptured Duck: {e}")
        return []

def virtual_grenadier(product_soup):
    """
    Extracts high-resolution image URLs from Virtual Grenadier product pages.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: High-resolution image URLs.
    """
    try:
        # Extract main image
        main_image_tag = product_soup.find('a', class_='album-main')
        main_image_url = main_image_tag['href'] if main_image_tag and 'href' in main_image_tag.attrs else None

        # Extract detail images
        detail_image_tags = product_soup.find_all('a', class_='album')
        detail_image_urls = [tag['href'] for tag in detail_image_tags if 'href' in tag.attrs]

        # Combine all images
        all_images = []
        if main_image_url:
            all_images.append(main_image_url)
        all_images.extend(detail_image_urls)

        # Normalize image URLs (add base URL if needed)
        if all_images and not all_images[0].startswith("http"):
            inferred_base_url = "https://www.virtualgrenadier.com/"
            all_images = [inferred_base_url + url.lstrip('/') for url in all_images]

        return all_images
    except Exception as e:
        logging.error(f"Error extracting images from Virtual Grenadier: {e}")
        return []


def concept500_2(product_soup):
    """
    Extracts high-quality image URLs from HTML within the 'content-part block-image' div.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: List of absolute URLs for the largest images.
    """
    try:
        # Collect URLs from 'content-part block-image'
        image_urls = [
            tag['href']
            for tag in product_soup.select("div.content-part.block-image a")
            if 'href' in tag.attrs
        ]

        # If image URLs are relative, prepend the inferred base URL
        if image_urls and not image_urls[0].startswith("http"):
            base_tag = product_soup.find('base')
            inferred_base_url = (
                base_tag['href'].rstrip('/')
                if base_tag and 'href' in base_tag.attrs
                else product_soup.select_one("link[rel='canonical']")['href'].rstrip('/')
            )

            image_urls = [
                inferred_base_url + '/' + url.lstrip('/') if not url.startswith("http") else url
                for url in image_urls
            ]

        return image_urls
    except Exception as e:
        logging.error(f"Error in concept500_2: {e}")
        return []


def concept500_basmilitaria(product_soup):
    """
    Extracts image URLs from the product carousel on BASMILITARIA.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list[str]: List of full image URLs.
    """
    try:
        image_tags = product_soup.select("div.carousel-inner img")
        image_urls = [
            tag['src'].strip()
            for tag in image_tags
            if tag.get('src', '').startswith("http")
        ]
        return image_urls
    except Exception as e:
        logging.error(f"Error in concept500_basmilitaria: {e}")
        return []


def tarnmilitaria(product_soup):
    """
    Extracts high-resolution image URLs from Tarn Militaria product pages.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: List of full image URLs.
    """
    try:
        # Find all gallery images in the detailed photo section
        image_urls = [
            a['href']
            for a in product_soup.select("div.gallery-thumb a")
            if a.has_attr('href')
        ]

        # Normalize to full URL if needed
        return [
            "https://www.tarnmilitaria.com" + url if url.startswith("/") else url
            for url in image_urls
        ]
    except Exception as e:
        logging.error(f"Error extracting Tarn Militaria images: {e}")
        return []


def eagle_relics_gallery(soup):
    """
    Extracts full-size image URLs from the Eagle Relics details page gallery.
    Looks for all <a> tags under #product-slides .item-slide and returns their hrefs.
    """
    try:
        gallery_divs = soup.select("div#product-slides div.item-slide a")
        image_urls = [a["href"] for a in gallery_divs if a.get("href")]
        return image_urls if image_urls else None
    except Exception as e:
        return None


def stewarts_militaria(product_soup):
    """
    Extracts original product image URLs from Stewarts Military Antiques product pages,
    skipping thumbnails and placeholder icons.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: List of valid image URLs (excluding thumbnails and icons).
    """
    try:
        image_urls = [
            img['src'].strip()
            for img in product_soup.select("img[src^='https://stewartsmilitaryantiques.com/img/']")
            if "thumb" not in img['src']
            and "thumbnail" not in img['src']
            and "small" not in img['src']
            and "icons/help.png" not in img['src']
        ]
        return image_urls
    except Exception as e:
        logging.error(f"Error in stewarts_militaria: {e}")
        return []



def tarnmilitaria(product_soup):
    """
    Extracts all full-size image URLs from Tarn Militaria product pages.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: List of full image URLs (strings).
    """
    try:
        image_urls = []
        gallery_divs = product_soup.select("div.gallery-thumb a")

        for a_tag in gallery_divs:
            href = a_tag.get("href", "").strip()
            if href and href.startswith("/uploads/"):
                image_urls.append("https://tarnmilitaria.com" + href)

        return image_urls
    except Exception as e:
        logging.error(f"Error in tarnmilitaria image extraction: {e}")
        return []


def militaria_1944(product_soup):
    """
    Extracts high-resolution image URLs from 1944militaria.com product pages.

    Args:
        product_soup (BeautifulSoup): Parsed HTML of the product page.

    Returns:
        list: List of high-res image URLs extracted from schema JSON.
    """
    import json
    try:
        # Look for <script type="application/ld+json">
        script_tag = product_soup.find("script", type="application/ld+json")
        if not script_tag or not script_tag.string:
            return []

        json_data = json.loads(script_tag.string.strip())

        # If "image" is a dict with numeric keys: extract and sort by key
        if isinstance(json_data.get("image"), dict):
            return [url for _, url in sorted(json_data["image"].items(), key=lambda x: int(x[0]))]

        # If "image" is a list
        elif isinstance(json_data.get("image"), list):
            return json_data["image"]

        # If "image" is a single URL
        elif isinstance(json_data.get("image"), str):
            return [json_data["image"]]

        return []
    except Exception as e:
        import logging
        logging.error(f"Error extracting images from 1944militaria: {e}")
        return []

def ss_steel_inc(soup):
    try:
        seen = set()
        image_urls = []

        for img in soup.select("img"):
            src = img.get("src")
            if not src:
                continue
            if "/uploads/" not in src:
                continue

            base_url = src.split("?")[0]
            # Remove known thumbnail suffixes like -100x100, -150x150
            clean_url = re.sub(r"-\d+x\d+(?=\.(jpg|jpeg|png|webp))", "", base_url, flags=re.IGNORECASE)

            if clean_url.endswith((".jpg", ".jpeg", ".png", ".webp")) and clean_url not in seen:
                seen.add(clean_url)
                image_urls.append(clean_url)

        return image_urls  # order preserved, duplicates removed
    except Exception:
        return []

# def bunker_militaria(product_soup):
#     """
#     Extracts all full-size product images from Bunker Militaria.
#     Preserves order and removes duplicates.
#     """
#     try:
#         base_url = "https://www.bunkermilitaria.com/"
#         image_urls = []
#         seen = set()

#         # 1. Get main image from <meta itemprop="image">
#         meta_img = product_soup.select_one('meta[itemprop="image"]')
#         if meta_img and meta_img.get("content"):
#             main_image = urljoin(base_url, meta_img["content"].strip())
#             if main_image not in seen:
#                 image_urls.append(main_image)
#                 seen.add(main_image)
#         else:
#             logging.warning("[bunker_militaria] No <meta itemprop='image'> found.")

#         # 2. Extract from thumbnail filmstrip
#         thumbnails = product_soup.select("img.x-filmstrip__image")
#         for thumb in thumbnails:
#             thumb_src = thumb.get("src")
#             if not thumb_src:
#                 continue

#             # Remove resolution suffix like _64x48, _435x580, etc.
#             full_img = re.sub(r"_\d+x\d+(?=\.jpg$)", "", thumb_src)
#             full_img_url = urljoin(base_url, full_img.strip())

#             if full_img_url not in seen:
#                 image_urls.append(full_img_url)
#                 seen.add(full_img_url)

#         # 3. Final check
#         if not image_urls:
#             logging.debug("SOUP DUMP:\n" + product_soup.prettify())
#             logging.error("[bunker_militaria] ❌ No valid full-size images found. Exiting.")
#             sys.exit(1)

#         logging.debug(f"[bunker_militaria] ✅ Found {len(image_urls)} full-size image(s): {image_urls}")
#         return image_urls

#     except Exception as e:
#         logging.exception(f"[bunker_militaria] ❌ Unexpected error during image extraction: {e}")
#         sys.exit(1)


def bunker_militaria(product_soup):


    base_url = "https://www.bunkermilitaria.com/Merchant2/"
    final_images = []
    seen_bases = set()

    try:
        if not product_soup:
            logging.warning("[bunker_militaria] No product_soup provided.")
            return []

        script_tags = product_soup.find_all("script")
        for script in script_tags:
            script_text = script.get_text()
            if "image_data" not in script_text:
                continue

            matches = re.findall(r'"graphics\\\/[^"]+\.jpg"', script_text)
            for match in matches:
                cleaned = match.strip('"').replace('\\/', '/')

                # Skip thumbnails
                if "_64x48" in cleaned or "_48x64" in cleaned:
                    continue

                # Get the base image name without resolution suffix
                base_key = re.sub(r'(_\d+x\d+)?(?=\.jpg)', '', cleaned)

                # Only keep the first encountered version of each base
                if base_key not in seen_bases:
                    seen_bases.add(base_key)
                    full_url = urljoin(base_url, cleaned)
                    final_images.append(full_url)

        logging.info(f"[bunker_militaria] ✅ Extracted {len(final_images)} ordered, unique images.")
        return final_images

    except Exception as e:
        logging.exception(f"[bunker_militaria] ❌ Failed to extract image URLs: {e}")
        return []
    

def collectors_guild_images(soup, **kwargs):
    """
    Extracts all relevant product images from GermanMilitaria.com Heer detail pages.
    Prepends full path to relative image sources.
    """
    base_url = "https://www.germanmilitaria.com/Heer/photos/"
    image_tags = soup.find_all("img")
    images = []

    for img in image_tags:
        src = img.get("src", "")
        if src.lower().endswith(".jpg") and not src.startswith("http"):
            full_url = base_url + src
            images.append(full_url)

    return images

def axis_militaria(product_soup):
    """
    Extracts all product image URLs from axis-militaria.com product detail page.
    Terminates the program if no valid images are found.
    """
    try:
        image_tags = product_soup.select("div.woocommerce-product-gallery img")
        image_urls = []

        for tag in image_tags:
            src = tag.get("src")
            if src and "placeholder" not in src.lower():
                image_urls.append(src)

        seen = set()
        image_urls = [x for x in image_urls if not (x in seen or seen.add(x))]

        if not image_urls:
            logging.error("[AXIS_MILITARIA] No images found — terminating.")
            sys.exit("[AXIS_MILITARIA] No images extracted. Exiting program.")

        return image_urls

    except Exception as e:
        logging.error(f"[AXIS_MILITARIA] Exception during image extraction: {e}")
        sys.exit(f"[AXIS_MILITARIA] Image extraction failed due to error: {e}")
