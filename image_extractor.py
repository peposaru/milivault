import logging, time
from bs4 import BeautifulSoup

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


