import openai
import logging
import json

class OpenAIManager:
    def __init__(self, openai_cred_path, categories_path):
        self.openai_cred_path = openai_cred_path
        self.categories_path = categories_path
        self.api_key = self._load_api_key()
        self.client = openai.OpenAI(api_key=self.api_key)
        self.model = "gpt-4o"

    def _load_api_key(self):
        with open(self.openai_cred_path, "r") as file:
            data = json.load(file)
            return data["key"]


    def classify_single_product(self, title, description, image_url=None):
        try:
            with open(self.categories_path, "r", encoding="utf-8") as f:
                category_data = json.load(f)

            item_type_enum = sorted(list(category_data.keys())) 
            conflict_enum = [
                "PRE_19TH", "19TH_CENTURY", "PRE_WW1", "WW1", "PRE_WW2", "WW2",
                "COLD_WAR", "VIETNAM_WAR", "KOREAN_WAR", "CIVIL_WAR", "MODERN", "UNKNOWN"
            ]
            nation_enum = [     
                "GERMANY", "UNITED KINGDOM", "USA", "JAPAN", "FRANCE", "CANADA",
                "AUSTRALIA", "RUSSIA", "ITALY", "NETHERLANDS", "POLAND", "AUSTRIA",
                "BELGIUM", "CHINA", "VIETNAM", "SOUTH KOREA", "NORTH KOREA", "ISRAEL",
                "CZECHOSLOVAKIA", "HUNGARY", "SPAIN", "SWEDEN", "FINLAND", "INDIA",
                "UNKNOWN", "OTHER ALLIED FORCES", "OTHER AXIS FORCES", "OTHER EUROPEAN",
                "OTHER AMERICAN", "OTHER MIDDLE EAST", "OTHER AFRICAN", "OTHER OCEANIC",
                "OTHER ASIAN", "OTHER"
            ]

            tools = [
                {
                    "type": "function",
                    "function": {
                        "name": "classify_product",
                        "description": "Classify a militaria item based on title and description",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "conflict": {"type": "string", "enum": conflict_enum},
                                "nation": {"type": "string", "enum": nation_enum},
                                "item_type": {"type": "string", "enum": item_type_enum}
                            },
                            "required": ["conflict", "nation", "item_type"]
                        }
                    }
                }
            ]

            # Add thumbnail URL if available
            image_note = f'\nImage: {image_url}' if image_url else ''

            messages = [
                    {
                        "role": "system",
                        "content": """You are a military historian AI helping categorize military collectibles.

                    Use the enums exactly as provided. If unsure, use 'UNKNOWN'.

                    conflict enum meanings:
                    - WW1 = World War 1
                    - WW2 = World War 2 (1939–1945)
                    - PRE_WW2 = Interwar period (1919–1938)
                    - MODERN = Post-1990 items
                    - COLD_WAR = Items from 1945–1990 that don't fit other conflicts
                    - VIETNAM_WAR = U.S. vs Vietnam 1955–1975
                    - KOREAN_WAR = Korea conflict 1950–1953
                    - CIVIL_WAR = U.S. or other civil wars

                    📛 IMPORTANT CLASSIFICATION GUIDELINES:

                    1. If the product is **an insignia, badge, medal bar, patch, or rank emblem**, classify the item_type as "INSIGNIA & PATCHES", not the category it attaches to (like uniforms or headgear). You are classifying the object itself, not where it’s worn.

                    2. If the product is **a store announcement**, **price update**, **sale alert**, or **new listings message** (not an actual physical item), classify the item_type as "SITE ADVERTISEMENTS".

                    Examples:
                    - ✅ "Reduced Price Category Updated"
                    - 🛒 "We have added 20+ new items"
                    - 💬 "Upcoming auction notice"
                    These are not physical collectibles.

                    Clues like “D.R.P.”, “Zeiler”, or bakelite material may indicate German WW2 items.
                    """
                    },
                {
                    "role": "user",
                    "content": f"""Classify this item based on the following:

    Title: "{title}"
    Description: "{description}"{image_note}
    """
                }
            ]

            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=tools,
                tool_choice="auto",
                temperature=0.3,
            )

            # Check cost of the request
            usage = response.usage
            total_tokens = usage.total_tokens if usage else 0

            # Calculate cost (adjust rate if using gpt-4o or others)
            cost_per_1k = 0.005  # for GPT-4o input + output
            estimated_cost = (total_tokens / 1000) * cost_per_1k

            logging.debug(f"AI CLASSIFIER: Tokens used → {total_tokens} tokens (${estimated_cost:.5f})")

            args = response.choices[0].message.tool_calls[0].function.arguments
            result = json.loads(args)

            return {
                "conflict_ai_generated": result.get("conflict", "").upper(),
                "nation_ai_generated": result.get("nation", "").upper(),
                "item_type_ai_generated": result.get("item_type", "").upper()
            }

        except Exception as e:
            logging.error(f"AI CLASSIFIER: Structured output classification failed → {e}")
            return {
                "conflict_ai_generated": None,
                "nation_ai_generated": None,
                "item_type_ai_generated": None
            }

    def classify_sub_item_type(self, main_item_type, title, description):
        try:
            with open(self.categories_path, "r", encoding="utf-8") as f:
                category_data = json.load(f)

            subcategories = None
            for key, values in category_data.items():
                if key.strip().lower() == main_item_type.strip().lower():
                    subcategories = values
                    break

            if not subcategories:
                logging.warning(f"No subcategories found for main item type: {main_item_type}")
                return None

            tools = [
                {
                    "type": "function",
                    "function": {
                        "name": "classify_subcategory",
                        "description": f"Pick the best subcategory for a militaria item under {main_item_type}",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "subcategory": {"type": "string", "enum": subcategories}
                            },
                            "required": ["subcategory"]
                        }
                    }
                }
            ]

            messages = [
    {
        "role": "system",
        "content": f"You are a militaria classification assistant. Based on the title and description, choose the best subcategory for '{main_item_type}' from the provided enum."
    },
    {
        "role": "user",
        "content": f"""This item was classified as '{main_item_type}'.

Now choose the most appropriate subcategory from the list below:

Title: "{title}"
Description: "{description}"
"""
    }
]

            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=tools,
                tool_choice="auto",
                temperature=0
            )

            args = response.choices[0].message.tool_calls[0].function.arguments
            result = json.loads(args)
            return result.get("subcategory", "").upper()


        except Exception as e:
            logging.error(f"AI CLASSIFIER: Failed to classify sub-item type for {main_item_type} → {e}")
            return None


    def generate_vector_from_text(self, title, description):
        """Generate an OpenAI vector from title and description text."""
        try:
            combined = f"{title or ''} {description or ''}".strip()
            if not combined:
                return None
            response = self.client.embeddings.create(
                input=[combined],
                model="text-embedding-3-small"
            )
            return response.data[0].embedding
        except Exception as e:
            logging.error(f"OpenAIManager: Failed to generate vector: {e}")
            return None
