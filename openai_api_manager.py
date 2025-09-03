import json
import logging
import openai

class OpenAIManager:
    def __init__(self, settings):
        self.openai_cred_path = settings["openaiCred"]
        self.categories_path = settings["militariaCategories"]
        self.supergroups_path = settings["supergroupCategories"]
        self.model = settings.get("openaiModel", "gpt-5-mini")
        self.fallback_model = settings.get("openaiFallbackModel", "gpt-5")
        self.confidence_threshold = settings.get("openaiConfidenceThreshold", 0.9)

        self.api_key = self._load_api_key()
        self.client = openai.OpenAI(api_key=self.api_key)

        self._category_data_cache = None
        self._supergroup_data_cache = None

    def _load_api_key(self):
        with open(self.openai_cred_path, "r") as file:
            data = json.load(file)
            return data["key"]

    def get_category_data(self):
        if self._category_data_cache is None:
            with open(self.categories_path, "r", encoding="utf-8") as f:
                self._category_data_cache = json.load(f)
        return self._category_data_cache

    def get_supergroup_data(self):
        if self._supergroup_data_cache is None:
            with open(self.supergroups_path, "r", encoding="utf-8") as f:
                self._supergroup_data_cache = json.load(f)
        return self._supergroup_data_cache

    def classify_single_product(self, title, description, image_url=None):
        try:
            # Step 1: Get supergroup
            supergroup = self._classify_supergroup(title, description, image_url)
            if not supergroup:
                return self._empty_result()

            # Step 2: Use supergroup to restrict categories
            categories = self.get_category_data()
            valid_types = [c["label"] for c in categories if c["supergroup"] == supergroup]

            result = self._classify_main_fields(title, description, valid_types, image_url)
            result["supergroup_ai_generated"] = supergroup
            return result

        except Exception as e:
            logging.error(f"AI CLASSIFICATION ERROR: {e}")
            return self._empty_result()

    def _classify_supergroup(self, title, description, image_url):
        try:
            supergroup_data = self.get_supergroup_data()
            enum_options = [sg["key"] for sg in supergroup_data]

            messages = [
                {
                    "role": "system",
                    "content": """
                    You are a military historian AI.
                    Classify each item into one of the following broad supergroups based on its purpose and form.
                    Return only the enum key that best describes the overall group this item fits into.
                    """
                },
                {
                    "role": "user",
                    "content": f"""
                    Title: {title}
                    Description: {description}
                    {'Image: ' + image_url if image_url else ''}
                    """
                }
            ]

            tools = [
                {
                    "type": "function",
                    "function": {
                        "name": "classify_supergroup",
                        "description": "Classify the item into a supergroup",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "supergroup": {"type": "string", "enum": enum_options}
                            },
                            "required": ["supergroup"]
                        }
                    }
                }
            ]

            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=tools,
                tool_choice="auto",
                temperature=1
            )
            args = response.choices[0].message.tool_calls[0].function.arguments
            return json.loads(args).get("supergroup")

        except Exception as e:
            logging.error(f"Supergroup classification failed: {e}")
            return None

    def _classify_main_fields(self, title, description, item_type_enum, image_url=None):
        try:
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
                        "description": "Classify a militaria item",
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

            image_note = f"\nImage: {image_url}" if image_url else ""

            messages = [
                {
                    "role": "system",
                    "content": "You are a military historian AI helping classify collectibles. Use only the provided enums."
                },
                {
                    "role": "user",
                    "content": f"""
                        Title: {title}
                        Description: {description}{image_note}
                    """
                }
            ]

            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=tools,
                tool_choice="auto",
                temperature=1
            )

            args = response.choices[0].message.tool_calls[0].function.arguments
            result = json.loads(args)

            return {
                "conflict_ai_generated": result.get("conflict", "").upper(),
                "nation_ai_generated": result.get("nation", "").upper(),
                "item_type_ai_generated": result.get("item_type", "").upper()
            }

        except Exception as e:
            logging.error(f"Main field classification failed: {e}")
            return self._empty_result()

    def _empty_result(self):
        return {
            "conflict_ai_generated": None,
            "nation_ai_generated": None,
            "item_type_ai_generated": None,
            "supergroup_ai_generated": None
        }

    def generate_vector_from_text(self, title, description):
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
