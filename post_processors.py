
import re
import logging

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
