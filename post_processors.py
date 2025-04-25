
import re
import logging
import json
from bs4 import BeautifulSoup
from html_manager import HtmlManager

"""
apply_post_processors(value, post_process_config, soup=None)

This function takes a raw extracted value (e.g., from a product page) and applies one or more post-processing operations
defined in the post_process_config dictionary. It is designed to be universal and compatible with flexible JSON-style
scraper configurations.

USE CASES:
- Strip whitespace
- Apply regex patterns to extract parts of a string (e.g., price)
- Convert to lowercase, replace text, or call custom functions
- Fallback to HTML-based price extraction if standard method fails

=======================
✨ BASIC USAGE EXAMPLES
=======================

EXAMPLE 1: Simple strip
-----------------------
{
  "strip": true
}
→ Removes leading/trailing spaces → " €325 " → "€325"

EXAMPLE 2: Regex to extract numbers
-----------------------------------
{
  "type": "regex",
  "pattern": "([\\d.,]+)"
}
→ From "€325.00", extracts "325.00"

EXAMPLE 3: Regex with fallback function (site-specific hidden price)
---------------------------------------------------------------------
{
  "type": "regex_fallback",
  "pattern": "([\\d.,]+)",
  "giels_hidden_price": {
    "fallback": true
  }
}
→ First tries regex.
→ If value is empty or 'SOLD', it calls: giels_hidden_price(soup)

EXAMPLE 4: Direct custom function
---------------------------------
{
  "function": "my_custom_extractor"
}
→ Calls `my_custom_extractor(soup)` and uses the return value.

=======================
🔧 HANDLED INTERNALLY:
=======================
- Keys like "strip", "replace", "lower" will trigger functions of the same name
- If "type" is given:
    - "regex" → calls regex()
    - "contains" → calls find_text_contains()
    - "regex_fallback" → tries regex, then fallback function
- If "function" is specified, it is immediately run and overrides other logic
- Keys like "pattern", "fallback", "if_true", etc. are passed into their respective processors

=======================
🔁 FLEXIBLE & EXTENDABLE
=======================
You can add your own post-processing functions (e.g., def giels_hidden_price(soup): ...)
and reference them in the config like so:
{
  "giels_hidden_price": { "fallback": true }
}

OR

{
  "function": "giels_hidden_price"
}

"""
def apply_post_processors(value, post_process_config, soup=None):
    """
    Apply a series of post-processing operations to a value.

    Args:
        value (any): The value to process.
        post_process_config (dict): The config dict with instructions.
        soup (BeautifulSoup, optional): The full HTML for function fallback access.

    Returns:
        any: The final processed value.
    """
    if not isinstance(post_process_config, dict):
        return value

    for func_name, arg in post_process_config.items():
        # 🔁 Function delegation mode: {"function": "gielsmilitaria_hidden_price"}
        if func_name == "function":
            try:
                func = globals().get(arg)
                if callable(func):
                    value = func(soup) if soup is not None else func()
                else:
                    logging.warning(f"POST PROCESSOR: Function '{arg}' not found.")
            except Exception as e:
                logging.warning(f"POST PROCESSOR: Error calling function '{arg}': {e}")
            continue

        # 🔎 Type-specific logic like regex or contains
        if func_name == "type":
            if arg == "contains":
                result = find_text_contains(value, post_process_config)
                if isinstance(result, dict) and "function" in result:
                    try:
                        fallback_func = globals().get(result["function"])
                        if callable(fallback_func):
                            return fallback_func(soup) if soup else fallback_func()
                    except Exception as e:
                        logging.warning(f"POST PROCESSOR: Error in fallback function '{result['function']}': {e}")
                return result

            elif arg == "regex":
                try:
                    result = regex(value, post_process_config)
                    # Fallback if result is empty string or "0"
                    if result in (None, "", "0") and "fallback" in post_process_config:
                        for fallback_func_name, opts in post_process_config.items():
                            if isinstance(opts, dict) and opts.get("fallback"):
                                func = globals().get(fallback_func_name)
                                if callable(func):
                                    logging.info(f"POST PROCESSOR: Falling back to function '{fallback_func_name}'")
                                    return func(soup) if soup else func()
                    return result
                except Exception as e:
                    logging.warning(f"POST PROCESSOR: Regex failed with error: {e}")
                    if "fallback" in post_process_config:
                        for fallback_func_name, opts in post_process_config.items():
                            if isinstance(opts, dict) and opts.get("fallback"):
                                func = globals().get(fallback_func_name)
                                if callable(func):
                                    logging.info(f"POST PROCESSOR: Falling back to function '{fallback_func_name}'")
                                    return func(soup) if soup else func()
                return value

            continue

        # 🚫 Skip keys that are meant for internal processor use
        if func_name in {"value", "if_true", "if_false", "case_insensitive", "pattern", "fallback", "fallback_price", "url"}:
            continue

        # 🛠️ Simple processors like "strip": true
        func = globals().get(func_name)
        if callable(func):
            try:
                value = func(value, arg) if arg is not True else func(value)
            except Exception as e:
                logging.warning(f"POST PROCESSOR: Failed {func_name} → {e}")
        else:
            logging.warning(f"POST PROCESSOR: Function '{func_name}' not found.")

    return value







# Having a hard time since my post processors were disjointed.
def normalize_input(value):
    if hasattr(value, 'get_text'):
        return value.get_text(strip=True)
    elif isinstance(value, list):
        return " ".join(str(v) for v in value)
    elif value is None:
        return ""
    return str(value).strip()

def prepend(value, prefix):
    value = normalize_input(value)
    if not value:
        return value
    return prefix + value.strip()

def append(value, suffix):
    value = normalize_input(value)
    if not value:
        return value
    return value.strip() + suffix

def replace_all(value, replacements):
    value = normalize_input(value)
    if not isinstance(value, str):
        return value
    for pair in replacements:
        old = pair.get("old", "")
        new = pair.get("new", "")
        value = value.replace(old, new)
    return value

def remove_prefix(value, prefix):
    value = normalize_input(value)
    if value and isinstance(value, str) and value.startswith(prefix):
        return value[len(prefix):].strip()
    return value

def remove_suffix(value, suffix):
    value = normalize_input(value)
    if value and isinstance(value, str) and value.endswith(suffix):
        return value[:-len(suffix)].strip()
    return value

def split(value, config):
    value = normalize_input(value)
    delimiter = config.get("delimiter", "-")
    take = config.get("take", "first")
    parts = value.split(delimiter) if isinstance(value, str) else []
    if take == "first":
        return parts[0].strip() if parts else value
    elif take == "last":
        return parts[-1].strip() if parts else value
    return value

def find_text_contains(value, config):
    value = normalize_input(value)
    needle = config.get("value", "")
    if not isinstance(needle, str):
        return config.get("if_false", False)

    case_insensitive = config.get("case_insensitive", True)
    haystack = value.lower() if case_insensitive else value
    needle = needle.lower() if case_insensitive else needle

    found = needle in haystack
    return config.get("if_true", True) if found else config.get("if_false", False)

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
    value = normalize_input(value)
    if isinstance(value, str) and value.startswith(prefix):
        return value
    return None

def smart_prepend(value, prefix):
    value = normalize_input(value)
    if isinstance(value, str) and not value.startswith("http"):
        return prefix + value
    return value

def strip_html_tags(value, arg=None):
    value = normalize_input(value)
    """
    Removes all HTML tags from a string.
    Example: '<a href="#">US</a>' → 'US'
    """
    if isinstance(value, str):
        return re.sub(r'<[^>]+>', '', value).strip()
    return value

def strip(value, config=None):
    value = normalize_input(value)
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
    value = normalize_input(value)
    try:
        pattern = config.get("pattern")
        if not pattern or not isinstance(value, str):
            return None
        match = re.search(pattern, value)
        return match.group(1) if match else None
    except Exception as e:
        logging.error(f"Regex post-process error: {e}")
        return None

def set(value, arg):
    value = normalize_input(value)
    """
    Always return the value specified in `arg`, ignoring input.
    Example: If arg=True, this will always return True.
    """
    return arg

def from_url(value, arg=None):
    """
    Pass-through that returns the full product URL for further post-processing.
    Assumes 'value' is ignored and URL comes from config["url"].
    """
    return arg if isinstance(arg, str) else ""


def rg_militaria_hidden_price(value, config):
    value = normalize_input(value)
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
    
def gielsmilitaria_hidden_price(value, **kwargs):
    import logging
    logging.info("[GIELSMILITARIA HIDDEN PRICE] Function activated. Initial value: %s", value)

    if value and isinstance(value, str) and "SOLD" in value.upper():
        soup = kwargs.get("soup")
        if soup:
            price_div = soup.find("div", attrs={"data-pp-amount": True})
            if price_div:
                extracted = price_div.get("data-pp-amount", value)
                logging.info("[GIELSMILITARIA HIDDEN PRICE] Fallback price extracted: %s", extracted)
                return extracted
            else:
                logging.warning("[GIELSMILITARIA HIDDEN PRICE] data-pp-amount div not found")
        else:
            logging.warning("[GIELSMILITARIA HIDDEN PRICE] BeautifulSoup object not available")

    return value


