"""Audited v7 rules surrounding the WWII German helmet branch model."""

from __future__ import annotations

import re

from classification_pipeline.core import Decision


PLUGIN_VERSION = "1"

LABELS = {
    "army": "Heer",
    "air_force": "Luftwaffe",
    "navy": "Kriegsmarine",
    "paramilitary": "Waffen-SS",
    "police": "Polizei",
    "civil_defense": "Luftschutz",
    "red_cross": "Deutsches Rotes Kreuz (DRK)",
    "labor_service": "Reichsarbeitsdienst (RAD)",
    "transportation_logistics": "NSKK",
    "wehrmacht_or_waffen_ss": "Wehrmacht / Waffen-SS",
    "service_unspecified": "German service / organization unresolved",
}

EXPLICIT_PATTERNS = {
    "air_force": (
        r"\bluftwaffe\b", r"\blutwaffe\b", r"\bgerman air force\b",
        r"\bfallschirmj(?:ä|a)ger\b", r"\bfj (?:m3[578]|helmet)\b",
    ),
    "navy": (r"\bkriegsmarine\b", r"\bgerman navy\b", r"\bnaval\b", r"\bkm\b"),
    "paramilitary": (
        r"\bwaffen[ -]?ss\b", r"\bschutzstaffel\b", r"\bss pith\b",
        r"\bss (?:helmet|combat|runic|decal)\b", r"\bss\s+m(?:35|40|42)\b",
    ),
    "police": (
        r"\bpolizei\b", r"\bpolice\b", r"\bordnungspolizei\b",
        r"\bschutzpolizei\b", r"\bgendarmerie\b",
    ),
    "civil_defense": (
        r"\bluftschutz\b", r"\bcivil defen[cs]e\b",
        r"\bcivic\b.*\bbeaded\b", r"\bbeaded\b.*\bcivic\b",
    ),
    "red_cross": (
        r"\bdeutsches rotes kreuz\b", r"\brotes kreuz\b", r"\bred cross\b", r"\bdrk\b",
    ),
    "labor_service": (r"\breichsarbeitsdienst\b", r"\brad\b", r"\blabor service\b"),
    "transportation_logistics": (r"\bnskk\b", r"\bnational socialist motor corps\b"),
    "wehrmacht_or_waffen_ss": (r"\bwehrmacht\b", r"\bgerman wh\b", r"\bwh m(?:35|40|42)\b"),
    "army": (r"\bheer\b", r"\bheeres\b", r"\barmy\b", r"\bafrika ?korps\b", r"\bdak\b"),
}

UNCERTAIN_PATTERNS = (
    r"\bbelieved\b", r"\bpossibly\b", r"\bperhaps\b", r"\battributed to\b", r"\bintended for\b",
)
FOREIGN_REISSUE_PATTERNS = (
    r"\bfinnish issued\b", r"\bpost[ -]?war decorated\b", r"\breissued dutch\b", r"\bdutch reissue\b",
)


def _matches(text: str, patterns) -> bool:
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def pre_predict(product, _manifest):
    subtype = str(product.get("sub_item_type") or "").strip().casefold()
    text = str(product.get("title") or product.get("description") or "").casefold()
    matched = [
        label for label, patterns in EXPLICIT_PATTERNS.items() if _matches(text, patterns)
    ]
    if _matches(text, UNCERTAIN_PATTERNS) and matched:
        return Decision(
            "service_unspecified", 0.0, "hedged_listing_text",
            metadata={"matched_labels": matched},
        )
    non_armed = {
        "police", "civil_defense", "red_cross", "labor_service", "transportation_logistics",
    } & set(matched)
    if re.search(r"\bno[ -]?decal\b", text, flags=re.IGNORECASE) and not non_armed:
        return Decision(
            "wehrmacht_or_waffen_ss", 0.0, "no_decal_armed_service_fallback",
            metadata={"matched_labels": matched},
        )
    exact = [label for label in matched if label != "wehrmacht_or_waffen_ss"]
    non_army = [label for label in exact if label != "army"]
    if "army" in matched and "paramilitary" in matched:
        return Decision(
            "wehrmacht_or_waffen_ss", 0.0, "conflicting_listing_text",
            metadata={"matched_labels": matched},
        )
    if len(non_army) == 1:
        return Decision(non_army[0], 0.99, "explicit_listing_text")
    if not non_army and exact == ["army"]:
        return Decision("army", 0.99, "explicit_listing_text")
    if not exact and matched == ["wehrmacht_or_waffen_ss"]:
        return Decision("wehrmacht_or_waffen_ss", 0.99, "explicit_listing_text")
    if len(non_army) > 1:
        return None
    if subtype == "pith_helmet":
        return Decision("army", 0.51, "profile_default")
    return None


def post_predict(decision, product, _manifest):
    subtype = str(product.get("sub_item_type") or "").strip().casefold()
    title = str(product.get("title") or "").casefold()
    if subtype != "helmet":
        return decision
    if _matches(title, FOREIGN_REISSUE_PATTERNS):
        return Decision("service_unspecified", 0.0, "foreign_reissue_fallback")
    if decision.label in {"army", "air_force", "navy", "paramilitary"} and decision.confidence < 0.35:
        return Decision(
            "wehrmacht_or_waffen_ss",
            decision.confidence,
            "low_confidence_armed_service_fallback",
            confidence_margin=decision.confidence_margin,
            top_predictions=decision.top_predictions,
            evidence_image_url=decision.evidence_image_url,
            metadata=decision.metadata,
        )
    return decision
