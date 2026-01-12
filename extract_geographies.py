#!/usr/bin/env python3
"""
Extract geographic locations from MongoDB documents using NLP + census validation.
Stores results in a new MongoDB collection for the dashboard.
"""

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime

from pymongo import MongoClient
from tqdm import tqdm

# spaCy has compatibility issues with Python 3.14, use regex-based extraction
SPACY_AVAILABLE = False


class GeographyExtractor:
    """Extract and validate US geographic locations from text."""

    def __init__(self, data_dir='data'):
        self.data_dir = data_dir
        self.locations = {}
        self.city_states = {}
        self.states = {}
        self.nlp = None
        self.common_words = set()
        self.common_names = set()

        self._load_census_data()
        self._load_nlp_model()
        self._load_common_words()
        self._load_common_names()

    def _load_common_words(self):
        """Load common English words to filter out false positive city matches."""
        words_path = os.path.join(self.data_dir, 'common_words.txt')
        if os.path.exists(words_path):
            with open(words_path, 'r') as f:
                self.common_words = {line.strip().lower() for line in f if line.strip()}
            print(f"Loaded {len(self.common_words)} common English words for filtering")

    def _load_common_names(self):
        """Load common first and last names to filter out false positive matches."""
        first_names_path = os.path.join(self.data_dir, 'first_names.txt')
        last_names_path = os.path.join(self.data_dir, 'last_names.txt')

        if os.path.exists(first_names_path):
            with open(first_names_path, 'r') as f:
                for line in f:
                    name = line.strip().lower()
                    if name:
                        self.common_names.add(name)

        if os.path.exists(last_names_path):
            with open(last_names_path, 'r') as f:
                for line in f:
                    name = line.strip().lower()
                    if name:
                        self.common_names.add(name)

        print(f"Loaded {len(self.common_names)} common names for filtering")

    def _load_census_data(self):
        """Load pre-processed census data."""
        locations_path = os.path.join(self.data_dir, 'us_locations.json')
        city_states_path = os.path.join(self.data_dir, 'city_states_index.json')
        states_path = os.path.join(self.data_dir, 'states.json')
        counties_path = os.path.join(self.data_dir, 'us_counties.json')

        if not os.path.exists(locations_path):
            raise FileNotFoundError(
                f"Census data not found at {locations_path}. "
                "Run setup_census_data.py first."
            )

        with open(locations_path, 'r') as f:
            self.locations = json.load(f)

        with open(city_states_path, 'r') as f:
            self.city_states = json.load(f)

        with open(states_path, 'r') as f:
            self.states = json.load(f)

        # Load county data
        self.counties = {}
        if os.path.exists(counties_path):
            with open(counties_path, 'r') as f:
                self.counties = json.load(f)

        print(f"Loaded {len(self.locations)} locations, {len(self.city_states)} city names, {len(self.counties)} counties")

    def _load_nlp_model(self):
        """Load spaCy NLP model if available."""
        if SPACY_AVAILABLE:
            try:
                self.nlp = spacy.load('en_core_web_sm')
                print("Loaded spaCy model: en_core_web_sm")
            except OSError:
                print("spaCy model not found. Downloading en_core_web_sm...")
                os.system('python -m spacy download en_core_web_sm')
                self.nlp = spacy.load('en_core_web_sm')

    def extract_locations_spacy(self, text):
        """Extract locations using spaCy NER."""
        if not self.nlp or not text:
            return []

        # Limit text length for performance
        if len(text) > 100000:
            text = text[:100000]

        doc = self.nlp(text)

        locations = []
        for ent in doc.ents:
            if ent.label_ in ('GPE', 'LOC', 'FAC'):
                locations.append(ent.text)

        return locations

    # Old-style state abbreviations commonly found in historical documents
    OLD_STATE_ABBREVS = {
        'Ala.': 'AL', 'Ariz.': 'AZ', 'Ark.': 'AR', 'Calif.': 'CA', 'Colo.': 'CO',
        'Conn.': 'CT', 'Del.': 'DE', 'Fla.': 'FL', 'Ga.': 'GA', 'Ill.': 'IL',
        'Ind.': 'IN', 'Kans.': 'KS', 'Ky.': 'KY', 'La.': 'LA', 'Mass.': 'MA',
        'Md.': 'MD', 'Mich.': 'MI', 'Minn.': 'MN', 'Miss.': 'MS', 'Mo.': 'MO',
        'Mont.': 'MT', 'Nebr.': 'NE', 'Nev.': 'NV', 'N.H.': 'NH', 'N.J.': 'NJ',
        'N.M.': 'NM', 'N.Y.': 'NY', 'N.C.': 'NC', 'N.D.': 'ND', 'Okla.': 'OK',
        'Oreg.': 'OR', 'Pa.': 'PA', 'R.I.': 'RI', 'S.C.': 'SC', 'S.D.': 'SD',
        'Tenn.': 'TN', 'Tex.': 'TX', 'Vt.': 'VT', 'Va.': 'VA', 'Wash.': 'WA',
        'W.Va.': 'WV', 'Wis.': 'WI', 'Wyo.': 'WY', 'D.C.': 'DC',
    }

    def extract_locations_regex(self, text):
        """Extract locations using regex patterns."""
        if not text:
            return []

        locations = []

        # Pattern 1: "City, State" or "City, ST" (e.g., "Houston, TX", "St. Louis, Missouri")
        # Skip if city name is a common first/last name (e.g., "Stephen, MN" is likely a person)
        pattern1 = r'\b([A-Z][a-z]+(?:[\.\s]+[A-Z]?[a-z]+)*),\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*|[A-Z]{2})\b'

        for match in re.finditer(pattern1, text):
            city, state = match.groups()
            # Skip if city is a common name
            if city.lower() in self.common_names:
                continue
            state_upper = state.upper()
            state_title = state.title()
            if (state_upper in self.states.get('abbrev_to_full', {}) or
                state_title in self.states.get('full_to_abbrev', {})):
                locations.append(f"{city}, {state}")

        # Pattern 2: Old-style abbreviations like "Boston, Mass." or "Midland, Mich."
        pattern2 = r'\b([A-Z][a-z]+(?:[\.\s]+[A-Z]?[a-z]+)*),\s*([A-Z][a-z]+\.)'

        for match in re.finditer(pattern2, text):
            city, abbrev = match.groups()
            # Skip if city is a common name
            if city.lower() in self.common_names:
                continue
            if abbrev in self.OLD_STATE_ABBREVS:
                state_code = self.OLD_STATE_ABBREVS[abbrev]
                state_full = self.states['abbrev_to_full'].get(state_code, state_code)
                locations.append(f"{city}, {state_full}")

        # Pattern 3: "City, N.Y." or "City, N.J." style
        pattern3 = r'\b([A-Z][a-z]+(?:[\.\s]+[A-Z]?[a-z]+)*),\s*([A-Z]\.[A-Z]\.)'

        for match in re.finditer(pattern3, text):
            city, abbrev = match.groups()
            # Skip if city is a common name
            if city.lower() in self.common_names:
                continue
            if abbrev in self.OLD_STATE_ABBREVS:
                state_code = self.OLD_STATE_ABBREVS[abbrev]
                state_full = self.states['abbrev_to_full'].get(state_code, state_code)
                locations.append(f"{city}, {state_full}")

        # Pattern 4: County mentions like "Cook County" or "Los Angeles County"
        # Match "X County" where X is one or more capitalized words
        county_pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+County\b'

        for match in re.finditer(county_pattern, text):
            county_name = match.group(1)
            full_county = f"{county_name} County"
            locations.append(full_county)

        # Pattern 5: "X County, State" format
        county_state_pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+County,\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*|[A-Z]{2})\b'

        for match in re.finditer(county_state_pattern, text):
            county_name, state = match.groups()
            full_county = f"{county_name} County"
            state_upper = state.upper()
            state_title = state.title()
            if (state_upper in self.states.get('abbrev_to_full', {}) or
                state_title in self.states.get('full_to_abbrev', {})):
                # Normalize state name
                if state_upper in self.states.get('abbrev_to_full', {}):
                    state = self.states['abbrev_to_full'][state_upper]
                locations.append(f"{full_county}, {state}")

        # Also look for standalone state names
        for state_name in self.states.get('full_to_abbrev', {}).keys():
            if state_name in text:
                locations.append(state_name)

        # Pattern 6: DISABLED - Standalone city names caused too many false positives
        # Now we ONLY match cities that have explicit state context (Patterns 1-3)

        return locations

    def extract_locations(self, text):
        """Extract locations using best available method."""
        if SPACY_AVAILABLE and self.nlp:
            return self.extract_locations_spacy(text)
        else:
            return self.extract_locations_regex(text)

    def validate_and_geocode(self, location_str):
        """
        Validate a location string against census data and return coordinates.
        Returns None if not a valid US location.
        """
        loc_lower = location_str.lower().strip()

        # Direct lookup in places
        if loc_lower in self.locations:
            return self.locations[loc_lower]

        # Check county lookup
        if loc_lower in self.counties:
            return self.counties[loc_lower]

        # Try "County, State" format for counties
        if 'county' in loc_lower and ',' in loc_lower:
            parts = [p.strip() for p in loc_lower.split(',')]
            if len(parts) == 2:
                county_part, state = parts
                # Try with full state name
                if state.upper() in self.states.get('abbrev_to_full', {}):
                    full_state = self.states['abbrev_to_full'][state.upper()]
                    key = f"{county_part}, {full_state}".lower()
                    if key in self.counties:
                        return self.counties[key]
                # Try with state as-is
                key = f"{county_part}, {state}".lower()
                if key in self.counties:
                    return self.counties[key]

        # Try "City, State" format
        if ',' in loc_lower:
            parts = [p.strip() for p in loc_lower.split(',')]
            if len(parts) == 2:
                city, state = parts

                # Try with full state name if abbreviation
                if state.upper() in self.states['abbrev_to_full']:
                    full_state = self.states['abbrev_to_full'][state.upper()]
                    key = f"{city}, {full_state}".lower()
                    if key in self.locations:
                        return self.locations[key]

                # Try with abbreviation if full name
                if state.title() in self.states['full_to_abbrev']:
                    abbrev = self.states['full_to_abbrev'][state.title()]
                    key = f"{city}, {abbrev}".lower()
                    if key in self.locations:
                        return self.locations[key]
        else:
            # City name only - check if it's unambiguous or pick largest
            city_lower = loc_lower
            if city_lower in self.city_states:
                states_list = self.city_states[city_lower]
                if len(states_list) == 1:
                    # Unambiguous
                    s = states_list[0]
                    return {
                        'name': location_str,
                        'state': s['state'],
                        'state_abbrev': s['state_abbrev'],
                        'lat': s['lat'],
                        'lng': s['lng'],
                        'type': 'place',
                        'ambiguous': False
                    }
                # For ambiguous cities, return the first match but flag it
                # In production, you might want smarter disambiguation
                elif len(states_list) > 0:
                    s = states_list[0]
                    return {
                        'name': location_str,
                        'state': s['state'],
                        'state_abbrev': s['state_abbrev'],
                        'lat': s['lat'],
                        'lng': s['lng'],
                        'type': 'place',
                        'ambiguous': True,
                        'possible_states': [x['state'] for x in states_list]
                    }

        # Check if it's just a state name
        if loc_lower.title() in self.states['full_to_abbrev']:
            return {'name': loc_lower.title(), 'type': 'state'}
        if loc_lower.upper() in self.states['abbrev_to_full']:
            return {'name': self.states['abbrev_to_full'][loc_lower.upper()], 'type': 'state'}

        return None


def process_documents(mongo_uri='mongodb://localhost:27017', db_name='toxic_docs',
                      batch_size=1000, limit=None):
    """
    Process all documents and extract geography mentions.
    Stores aggregated results in MongoDB.
    """
    print(f"Connecting to MongoDB: {mongo_uri}")
    client = MongoClient(mongo_uri)
    db = client[db_name]

    # Initialize extractor
    extractor = GeographyExtractor()

    # Count documents
    total_docs = db.documents.count_documents({})
    if limit:
        total_docs = min(total_docs, limit)
    print(f"Processing {total_docs} documents...")

    # Aggregate geography counts
    geo_counts = defaultdict(lambda: {
        'count': 0,
        'doc_ids': [],
        'info': None
    })

    # Process in batches
    processed = 0
    cursor = db.documents.find({}, {'_id': 1, 'text': 1, 'title': 1})

    if limit:
        cursor = cursor.limit(limit)

    for doc in tqdm(cursor, total=total_docs, desc="Extracting geographies"):
        text = doc.get('text', '') or ''
        title = doc.get('title', '') or ''
        full_text = f"{title} {text}"

        # Extract locations
        raw_locations = extractor.extract_locations(full_text)

        # Validate and count
        seen_in_doc = set()
        for loc in raw_locations:
            validated = extractor.validate_and_geocode(loc)
            if validated and validated.get('lat'):
                # Create a canonical key - handle states differently
                if validated.get('type') == 'state':
                    key = validated['name'].lower()
                else:
                    key = f"{validated['name']}, {validated['state']}".lower()

                if key not in seen_in_doc:
                    seen_in_doc.add(key)
                    geo_counts[key]['count'] += 1
                    geo_counts[key]['info'] = validated
                    # Store doc_ids for small counts, skip for large
                    if geo_counts[key]['count'] <= 100:
                        geo_counts[key]['doc_ids'].append(str(doc['_id']))

        processed += 1

    print(f"\nProcessed {processed} documents")
    print(f"Found {len(geo_counts)} unique locations")

    # Store results in MongoDB
    print("\nStoring results in MongoDB...")
    db.geography_counts.drop()

    geo_docs = []
    for key, data in geo_counts.items():
        if data['info'] and data['count'] >= 1:
            loc_type = data['info'].get('type', 'place')

            # For states, don't duplicate state in the state field
            if loc_type == 'state':
                state_val = ''
                state_abbrev_val = ''
            else:
                state_val = data['info'].get('state', '')
                state_abbrev_val = data['info'].get('state_abbrev', '')

            geo_doc = {
                'location_key': key,
                'name': data['info'].get('name', key),
                'state': state_val,
                'state_abbrev': state_abbrev_val,
                'county': data['info'].get('county', ''),
                'lat': data['info'].get('lat'),
                'lng': data['info'].get('lng'),
                'count': data['count'],
                'type': loc_type,
                'sample_doc_ids': data['doc_ids'][:10],
                'updated_at': datetime.utcnow()
            }
            geo_docs.append(geo_doc)

    if geo_docs:
        db.geography_counts.insert_many(geo_docs)
        db.geography_counts.create_index('location_key')
        db.geography_counts.create_index('count')
        db.geography_counts.create_index('state')
        db.geography_counts.create_index('county')
        db.geography_counts.create_index('type')
        db.geography_counts.create_index([('lat', 1), ('lng', 1)])

    print(f"Stored {len(geo_docs)} location records")

    # Print top locations
    print("\nTop 20 locations by mention count:")
    top_20 = sorted(geo_docs, key=lambda x: x['count'], reverse=True)[:20]
    for i, loc in enumerate(top_20, 1):
        if loc.get('type') == 'state':
            display_name = loc['name']
        else:
            display_name = f"{loc['name']}, {loc['state']}"
        print(f"  {i}. {display_name}: {loc['count']} mentions")

    return geo_docs


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Extract geographies from documents')
    parser.add_argument('--mongo-uri', default='mongodb://localhost:27017',
                        help='MongoDB connection URI')
    parser.add_argument('--db', default='toxic_docs', help='Database name')
    parser.add_argument('--limit', type=int, help='Limit number of documents to process')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size')

    args = parser.parse_args()

    process_documents(
        mongo_uri=args.mongo_uri,
        db_name=args.db,
        batch_size=args.batch_size,
        limit=args.limit
    )
