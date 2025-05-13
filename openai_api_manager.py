from openai import OpenAI
import logging
import json

class OpenAIManager:
    def __init__(self, api_key, model="gpt-4o-mini"):
        self.client = OpenAI(api_key=api_key)
        self.model = model

    def classify_single_product(self, title, description):
        """
        Classify a single product using OpenAI.
        Returns a dict: conflict_ai_generated, nation_ai_generated, item_type_ai_generated.
        """
        system_message = """
You are an expert in historical militaria classification.

Your task is to label military antiques with 3 fields:
- Conflict (the era or war it comes from)
- Nation (the country of origin)
- Item Type (the category of object)

Only use the following allowed values:

Conflict:
PRE_19TH, 19TH_CENTURY, PRE_WW1, WW1, INTER_WAR, WW2, COLD_WAR, 
VIETNAM_WAR, KOREAN_WAR, CIVIL_WAR, MODERN, UNKNOWN

Item Type:
PAPER_ITEMS, FIELD_GEAR, UNIFORM, INSIGNIA, EDGED_WEAPONS, HELMET, 
MEDALS_AWARDS, FLAG, HEADGEAR, ART, TINNIE, BELTS_BUCKLES, POSTCARD, 
REPRODUCTION, FIREARMS, TOYS

Always format your response as:
Conflict: <value>
Nation: <value>
Item Type: <value>
"""

        user_message = f"""Classify this item:

Title: "{title}"
Description: "{description}"
"""

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": user_message}
                ]
            )
            raw = completion.choices[0].message.content.strip()
            logging.debug(f"AI RAW OUTPUT: {raw}")

            lines = raw.split("\n")
            return {
                "conflict_ai_generated": lines[0].split(":")[1].strip(),
                "nation_ai_generated": lines[1].split(":")[1].strip(),
                "item_type_ai_generated": lines[2].split(":")[1].strip(),
            }
        except Exception as e:
            logging.error(f"AI CLASSIFIER: Error during OpenAI classification: {e}")
            return {
                "conflict_ai_generated": None,
                "nation_ai_generated": None,
                "item_type_ai_generated": None,
            }

item_type_sub_categories = {
    "Toys": [
        "Scale Models",
        "Toy Soldiers",
        "Toy Vehicles",
        "Water Guns",
        "Toy Weapons",
        "Building Sets",
        "Action Figures"
    ],
    "Medals & Awards": [
        "Orders & Decorations",
        "Campaign Medals",
        "Gallantry & Merit Awards",
        "Wound Badges",
        "Qualification Badges",
        "Commemorative Medals",
        "Medal Ribbons",
        "Miniature Medals & Lapel Pins",
        "Award Documents & Cases"
    ],
    "Headwear": [
        "Visor & Service Caps",
        "Field & Patrol Caps",
        "Garrison & Side Caps",
        "Berets",
        "Traditional Headwear",
        "Winter Fur Caps",
        "Cap Covers & Rain Caps",
        "Cap Accessories (Sold Separately)"
    ],
    "Helmets & Accessories": [
        "Combat Helmets",
        "Flight Helmets",
        "Ceremonial Helmets",
        "Helmet Liners & Suspensions",
        "Helmet Covers & Camouflage",
        "Helmet Accessories",
        "Helmet Insignia"
    ],
    "Specialized Uniforms & Outerwear": [
        "Camouflage Uniforms",
        "Flight Suits & Jackets",
        "Cold Weather Gear",
        "Rain Gear",
        "Protective Suits",
        "Work & Utility Uniforms",
        "Sniper & Observation Suits",
        "Motorcycle & Tanker Clothing"
    ],
    "Uniforms, Jackets & Shirts": [
        "Dress Jackets & Tunics",
        "Field Jackets & Combat Shirts",
        "Service & Dress Shirts",
        "Mess & Formal Uniforms",
        "Uniform Sets",
        "Sweaters & Jerseys"
    ],
    "Trousers & Pants": [
        "Dress Trousers",
        "Field & Cargo Pants",
        "Breeches & Riding Pants",
        "Specialized Trousers",
        "Shorts"
    ],
    "Uniform Accessories": [
        "Lanyards & Aiguillettes",
        "Dress Belts & Sashes",
        "Armbands & ID Panels",
        "Collar & Lapel Insignia",
        "Shoulder Boards & Straps",
        "Military Buttons & Cufflinks",
        "Neckties & Scarves",
        "Whistles & Dress Cords"
    ],
    "Belts, Buckles & Support Gear": [
        "Dress Belts",
        "Field Equipment Belts",
        "Belt Buckles (Separate)",
        "Load Bearing Equipment (LBE)",
        "Suspenders & Braces",
        "Sword & Bayonet Hangers",
        "Ammo Pouches (Belt)"
    ],
    "Footwear": [
        "Combat Boots",
        "Dress Shoes & Boots",
        "Jump Boots",
        "Specialized Boots",
        "Gaiters & Leggings",
        "Military Socks",
        "Shoe Care"
    ],
    "Flags, Banners & Signs": [
        "National Flags",
        "Unit & Regimental Flags",
        "Banners & Guidons",
        "Pennants",
        "Vehicle Markings",
        "Field & Tactical Signs",
        "Flagpole Accessories"
    ],
    "Insignia & Patches": [
        "Shoulder Sleeve Insignia (SSI)",
        "Rank Insignia (Cloth)",
        "Branch & Corps Insignia (Cloth)",
        "Name & Service Tapes (Cloth)",
        "Collar Tabs & Lapel Insignia (Cloth)",
        "Squadron & Specialist Patches",
        "Qualification Badges (Cloth)"
    ],
    "Edged Weapons": [
        "Swords & Sabers",
        "Daggers & Combat Knives",
        "Bayonets",
        "Field & Utility Knives",
        "Machetes & Axes (Weapon)",
        "Sheaths & Accessories"
    ],
    "Vehicle Items & Manuals": [
        "Land Vehicle Parts & Accessories",
        "Aircraft Components (Non-Clothing)",
        "Naval Vessel Fittings",
        "Vehicle Manuals & Blueprints",
        "Vehicle Crew Gear",
        "Vehicle Tools"
    ],
    "Heavy Ordnance": [
        "Artillery",
        "Mortars",
        "Large Caliber Projectiles (Inert)",
        "Artillery & Mortar Sights",
        "Ordnance Handling Tools",
        "Deactivated Ordnance Parts"
    ],
    "Ammunition & Explosives": [
        "Small Arms Ammunition (Inert)",
        "Ammo Boxes & Packaging",
        "Grenades (Inert)",
        "Land Mines (Inert)",
        "Demolition Equipment (Inert)",
        "Fuzes & Primers (Inert)",
        "Pyrotechnics (Non-Guns)"
    ],
    "Crew-Served & Heavy Infantry Weapons": [
        "Anti-Tank Weapons",
        "Recoilless Rifles",
        "Heavy Machine Guns",
        "Automatic Grenade Launchers",
        "Flamethrowers",
        "Heavy Weapon Mounts",
        "Sights for Heavy Weapons"
    ],
    "Small Arms & Accessories": [
        "Bolt Action Rifles & Carbines",
        "Semi-Automatic Rifles",
        "Assault Rifles",
        "Sniper & Anti-Materiel Rifles",
        "Submachine Guns & PDWs",
        "Handguns (Pistols & Revolvers)",
        "Machine Pistols",
        "Magazines & Clips (Small Arms)",
        "Slings & Furniture (Small Arms)",
        "Holsters (Handguns)",
        "Suppressors (Display)",
        "Cleaning Kits (Small Arms)",
        "Signal Pistols"
    ],
    "Individual Field Gear": [
        "Canteens & Water Bottles",
        "Mess Kits & Field Stoves",
        "Entrenching Tools",
        "Haversacks & Small Packs",
        "Basic Utility Pouches",
        "Shelter & Sleeping Gear",
        "Field Hygiene Kits",
        "Field Repair & Utility Items"
    ],
    "Optical Sighting Equipment (Separate from Firearms)": [
        "Binoculars & Monoculars",
        "Telescopes & Spotting Scopes",
        "Rangefinders",
        "Weapon Sights (Separate)",
        "Periscopes & Aiming Devices",
        "Night Vision & Thermal Optics (Standalone)",
        "Surveying Optics",
        "Optical Maintenance Kits"
    ],
    "Medical, Dental & Veterinary Equipment": [
        "Surgical Instruments & Kits",
        "First Aid Kits",
        "Stretchers & Litters",
        "Medication (Display)",
        "Dental Equipment (Display)",
        "Veterinary Equipment (Display)",
        "Medical Bags & Pouches",
        "Field Sanitation Equipment"
    ],
    "Packs & Bags": [
        "Backpacks & Rucksacks",
        "Duffel & Sea Bags",
        "Officer & Document Bags",
        "Specialized Equipment Bags",
        "Pack Accessories & Components",
        "Cargo Nets & Straps"
    ],
    "Navigation & Surveying": [
        "Maps & Charts",
        "Compasses",
        "Map Cases & Tools",
        "GPS Units",
        "Sextants & Navigational Tools",
        "Altimeters & Surveying Markers",
        "Celestial Navigation Aids"
    ],
    "Communications Equipment": [
        "Field Radios",
        "Radio Accessories",
        "Field Telephones",
        "Telegraphy Equipment",
        "Signal Flags & Panels",
        "Signal Lamps & Devices",
        "Headsets & Crypto Gear",
        "Pigeon Post Equipment (Historical)"
    ],
    "Personal Effects & Kit Items": [
        "Toiletry Kits",
        "Smoking Accessories",
        "Writing Implements & Stationery",
        "Personal Mementos",
        "Sewing Kits",
        "Personal Eating Utensils",
        "Watches",
        "Games & Reading Material"
    ],
    "Identification & Personal Papers": [
        "ID Discs & Tags",
        "Paybooks & Service Records",
        "Military ID Cards & Passes",
        "Personal Diaries & Journals",
        "Letters & Field Post",
        "Licenses & Permits (Military Issued)",
        "Personal Certificates & Records",
        "Personal Maps & Notes"
    ],
    "Propaganda & PsyOps Material": [
        "Propaganda Leaflets",
        "Propaganda Posters",
        "News Sheets & Publications",
        "Audio & Video Propaganda",
        "Safe Conduct Passes",
        "Loyalty Oaths & Patriotic Material",
        "Counter-Propaganda (For Analysis)"
    ],
    "Books, Manuals & Periodicals": [
        "Official Field & Technical Manuals",
        "Training Manuals & Guides",
        "Historical Books",
        "Collector Guides (Militaria)",
        "Period Magazines & Newspapers",
        "Atlases & Gazetteers",
        "Song Books & Religious Texts"
    ],
    "Photographs & Albums": [
        "Loose Photographs",
        "Photo Albums & Scrapbooks",
        "Negatives & Slides",
        "Film Reels",
        "Stereoscope & 3D Images",
        "Photo Postcards",
        "Microfilm & Microfiche"
    ],
    "Miscellaneous Collectibles": [
        "Trench Art",
        "Battlefield Souvenirs",
        "Ephemera (Event Passes, Coupons, etc.)",
        "Models & Dioramas",
        "Commemorative Items (Non-Award)",
        "Musical Instruments (Personal/Band)",
        "Sporting Goods (Military Issue)",
        "Cigarette & Trade Cards (Military)",
        "Postcards"
    ],
    "Art & Artwork": [
        "Paintings",
        "Drawings",
        "Prints",
        "Sculptures",
        "Posters",
        "Trench Art",
        "Propaganda Art"
    ],
    "Tinnie": [
        "Political Badges",
        "Commemorative Pins",
        "Event Medals",
        "Occupation Souvenirs"
    ],
    "Reproduction": [
        "Replica Uniforms",
        "Replica Weapons",
        "Replica Headgear",
        "Replica Insignia",
        "Replica Documents",
        "Replica Equipment"
    ]
}







ATTRIBUTE_MAP = {
    "PAPER_ITEMS": [
        "document_type",
        "date",
        "recipient_name",
        "issuer",
        "location",
        "language"
    ],
    "FIELD_GEAR": [
        "item_function",
        "maker",
        "model",
        "material",
        "condition",
        "date"
    ],
    "UNIFORM": [
        "garment_type",  # jacket, trousers, smock, etc.
        "maker",
        "chest_size",
        "material",
        "insignia_type",
        "branch",
        "model"
    ],
    "INSIGNIA": [
        "insignia_type",  # collar tab, cuff title, etc.
        "rank",
        "branch",
        "material",
        "maker",
        "attachment_method"
    ],
    "EDGED_WEAPONS": [
        "weapon_type",  # bayonet, dagger, sword, etc.
        "blade_length",
        "handle_material",
        "scabbard_type",
        "serial_number",
        "maker",
        "model"
    ],
    "HELMET": [
        "model",         # M35, M40, M42, etc.
        "shell_size",
        "liner_size",
        "decal_type",
        "batch_number",
        "maker",
        "branch"
    ],
    "MEDALS_AWARDS": [
        "award_name",
        "ribbon_color",
        "material",
        "maker",
        "award_date",
        "serial_number"
    ],
    "FLAG": [
        "flag_type",     # battle flag, political flag, etc.
        "material",
        "dimensions",
        "branch",
        "condition",
        "inscription"
    ],
    "HEADGEAR": [
        "cap_type",      # visor, side cap, etc.
        "size",
        "material",
        "branch",
        "insignia_type",
        "maker"
    ],
    "ART": [
        "art_type",      # trench art, sculpture, etc.
        "medium",
        "dimensions",
        "artist",
        "date",
        "subject"
    ],
    "TINNIE": [
        "event",         # rally, day badge, etc.
        "material",
        "date",
        "maker",
        "attachment_type"
    ],
    "BELTS_BUCKLES": [
        "buckle_design",  # eagle, motto, etc.
        "maker",
        "material",
        "buckle_type",
        "size",
        "branch"
    ],
    "POSTCARD": [
        "postmark_date",
        "sender_name",
        "recipient_name",
        "location",
        "inscription",
        "image_subject"
    ],
    "REPRODUCTION": [
        "original_item_type",
        "reproduction_method",
        "maker",
        "intent"  # reenactment, display, deception
    ],
    "FIREARMS": [
        "firearm_type",  # rifle, pistol, etc.
        "model",
        "caliber",
        "serial_number",
        "maker",
        "deactivation_status",
        "date"
    ],
    "TOYS": [
        "toy_type",      # soldier, model tank, etc.
        "material",
        "maker",
        "scale",
        "condition",
        "date"
    ]
}
