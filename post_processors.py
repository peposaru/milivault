
import re
import logging
import json
from bs4 import BeautifulSoup
from html_manager import HtmlManager

def prepend(value, prefix):
    if not value:
        return value
    return prefix + value.strip()

def append(value, suffix):
    if not value:
        return value
    return value.strip() + suffix

def replace_all(value, replacements):
    if not isinstance(value, str):
        return value
    for pair in replacements:
        old = pair.get("old", "")
        new = pair.get("new", "")
        value = value.replace(old, new)
    return value

def remove_prefix(value, prefix):
    if value and isinstance(value, str) and value.startswith(prefix):
        return value[len(prefix):].strip()
    return value

def remove_suffix(value, suffix):
    if value and isinstance(value, str) and value.endswith(suffix):
        return value[:-len(suffix)].strip()
    return value

def split(value, config):
    delimiter = config.get("delimiter", "-")
    take = config.get("take", "first")
    parts = value.split(delimiter) if isinstance(value, str) else []
    if take == "first":
        return parts[0].strip() if parts else value
    elif take == "last":
        return parts[-1].strip() if parts else value
    return value

def find_text_contains(value, config):
    try:
        if not config or not isinstance(config, dict):
            return False

        needle = config.get("value", "")
        if not isinstance(needle, str):
            return config.get("if_false", False)

        # If value is a tag (BeautifulSoup), extract text
        if hasattr(value, 'get_text'):
            value = value.get_text(strip=True)

        # If value is a list, join into a string
        if isinstance(value, list):
            value = " ".join(map(str, value))

        if not isinstance(value, str):
            return config.get("if_false", False)

        case_insensitive = config.get("case_insensitive", True)
        haystack = value.lower() if case_insensitive else value
        needle = needle.lower() if case_insensitive else needle

        return config.get("if_true", True) if needle in haystack else config.get("if_false", False)

    except Exception as e:
        import logging
        logging.error(f"Error in find_text_contains: {e}")
        return config.get("if_false", False)


def submethod_exists(parent, config):
    try:
        from bs4.element import Tag
        if not isinstance(parent, Tag):
            print("submethod_exists: Parent is not a BeautifulSoup Tag.")
            return False

        method_name = config.get("method", "find")
        args = config.get("args", [])
        raw_kwargs = config.get("kwargs", {})
        expect = config.get("expect", True)

        # Remove post-processing metadata
        bs4_safe_kwargs = {k: v for k, v in raw_kwargs.items() if k not in {"expect", "exists"}}

        if not hasattr(parent, method_name):
            print(f"submethod_exists: Parent tag has no method '{method_name}'")
            return False

        result = getattr(parent, method_name)(*args, **bs4_safe_kwargs)
        exists = result is not None

        return exists == expect
    except Exception as e:
        print(f"POST PROCESSOR: Error in submethod_exists: {e}")
        return False

# def find_text_contains(value, config):
#     """
#     Checks if a substring exists in a given string (case-insensitive).
#     Returns True/False based on config.
#     """
#     if not isinstance(value, str):
#         value = str(value)

#     text = value.lower()
#     target = config.get("value", "").lower()

#     if target in text:
#         return config.get("if_true", True)
#     else:
#         return config.get("if_false", False)


def validate_startswith(value, prefix):
    if isinstance(value, str) and value.startswith(prefix):
        return value
    return None

def smart_prepend(value, prefix):
    if isinstance(value, str) and not value.startswith("http"):
        return prefix + value
    return value

def strip_html_tags(value, arg=None):
    """
    Removes all HTML tags from a string.
    Example: '<a href="#">US</a>' → 'US'
    """
    if isinstance(value, str):
        return re.sub(r'<[^>]+>', '', value).strip()
    return value

def strip(value, config=None):
    """
    Strip leading and trailing whitespace from a string.

    Args:
        value (Any): The input value to strip.
        config (dict): Unused, but included for compatibility.

    Returns:
        str: Stripped string if input is string, otherwise original value.
    """
    try:
        if isinstance(value, str):
            return value.strip()
        return value
    except Exception as e:
        logging.warning(f"Error applying post-processing 'strip': {e}")
        return value

def regex(value, config):
    try:
        pattern = config.get("pattern")
        if not pattern or not isinstance(value, str):
            return None
        match = re.search(pattern, value)
        return match.group(1) if match else None
    except Exception as e:
        logging.error(f"Regex post-process error: {e}")
        return None

def set(_value, arg):
    """
    Always return the value specified in `arg`, ignoring input.
    Example: If arg=True, this will always return True.
    """
    return arg

def from_url(original_text, arg=None):
    """Returns the product URL for further post-processing."""
    return arg if isinstance(arg, str) else ""

def rg_militaria_hidden_price(value, config):
    """Post-process function to extract hidden price from rg-militaria"""
    try:
        logging.info("POST PROCESS: [rg_militaria_hidden_price] Start fallback price check")

        # 1. Check if current price is valid
        try:
            cleaned = re.sub(r"[^\d\.]", "", str(value))
            if cleaned and float(cleaned) > 0:
                logging.info(f"POST PROCESS: [rg_militaria_hidden_price] Existing price is valid: {cleaned}")
                return cleaned
        except Exception:
            logging.warning("POST PROCESS: [rg_militaria_hidden_price] Could not validate current price format")

        # 2. Check if fallback is enabled
        if not config.get("fallback", False):
            logging.info("POST PROCESS: [rg_militaria_hidden_price] Fallback not enabled in config")
            return value

        # 3. Validate URL
        url = config.get("url")
        if not url:
            logging.warning("POST PROCESS: [rg_militaria_hidden_price] No URL provided in config for fallback fetch")
            return value

        logging.info(f"POST PROCESS: [rg_militaria_hidden_price] Fetching HTML from {url}")
        html = HtmlManager().parse_html(url)
        if not html:
            logging.warning("POST PROCESS: [rg_militaria_hidden_price] Failed to fetch HTML; returning original value")
            return value

        soup = BeautifulSoup(html, "html.parser")

        # 4. Try meta price first
        meta_price = soup.find("meta", attrs={"itemprop": "price"})
        if meta_price and meta_price.get("content"):
            extracted = meta_price["content"].strip()
            logging.info(f"POST PROCESS: [rg_militaria_hidden_price] Found hidden price in meta tag: {extracted}")
            return extracted

        # 5. Optionally check visible span if meta fails
        span_price = soup.find("span", class_="product__price__price")
        if span_price:
            extracted = span_price.get_text(strip=True)
            logging.info(f"POST PROCESS: [rg_militaria_hidden_price] Found hidden price in span: {extracted}")
            return extracted

        # 6. Final fallback
        logging.info("POST PROCESS: [rg_militaria_hidden_price] No hidden price found; returning original value")
        return value

    except Exception as e:
        logging.error(f"POST PROCESS: [rg_militaria_hidden_price] Error: {e}")
        return value
