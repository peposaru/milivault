
import re

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
    if not isinstance(value, str):
        return config.get("if_false", False)
    needle = config.get("value", "").lower()
    case_insensitive = config.get("case_insensitive", True)
    haystack = value.lower() if case_insensitive else value
    return config.get("if_true") if needle in haystack else config.get("if_false")
