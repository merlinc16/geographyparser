#!/usr/bin/env python3
"""
Extract international geographic locations from MongoDB documents.
Only matches cities that appear near country mentions (within ~200 characters).
Stores results in a separate MongoDB collection for the world dashboard.
"""

import json
import os
import re
from collections import defaultdict
from datetime import datetime

from pymongo import MongoClient
from tqdm import tqdm


class WorldGeographyExtractor:
    """Extract and validate international geographic locations from text."""

    # Proximity window: city must be within this many characters of country mention
    PROXIMITY_CHARS = 200

    def __init__(self, data_dir='data'):
        self.data_dir = data_dir
        self.locations = {}
        self.city_countries = {}
        self.countries = {}
        self.common_words = set()
        self.common_names = set()

        self._load_world_data()
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

    def _load_world_data(self):
        """Load pre-processed world data."""
        locations_path = os.path.join(self.data_dir, 'world_locations.json')
        city_countries_path = os.path.join(self.data_dir, 'city_countries_index.json')
        countries_path = os.path.join(self.data_dir, 'countries.json')

        if not os.path.exists(locations_path):
            raise FileNotFoundError(
                f"World data not found at {locations_path}. "
                "Run setup_world_data.py first."
            )

        with open(locations_path, 'r', encoding='utf-8') as f:
            self.locations = json.load(f)

        with open(city_countries_path, 'r', encoding='utf-8') as f:
            self.city_countries = json.load(f)

        with open(countries_path, 'r', encoding='utf-8') as f:
            self.countries = json.load(f)

        print(f"Loaded {len(self.locations)} world locations, {len(self.city_countries)} city names")

    def _build_country_pattern(self):
        """Build a compiled regex pattern for all country names (called once at init)."""
        if not hasattr(self, '_country_pattern'):
            # Get all country names sorted by length (longest first to avoid partial matches)
            names = sorted(self.countries.get('name_to_code', {}).keys(), key=len, reverse=True)
            # Escape and join with alternation
            escaped = [re.escape(name) for name in names if len(name) > 2]
            if escaped:
                pattern = r'\b(' + '|'.join(escaped) + r')\b'
                self._country_pattern = re.compile(pattern, re.IGNORECASE)
            else:
                self._country_pattern = None

            # Also build pattern for 2-letter codes (case-sensitive)
            codes = [code for code in self.countries.get('code_to_name', {}).keys() if len(code) == 2]
            if codes:
                code_pattern = r'\b(' + '|'.join(codes) + r')\b'
                self._code_pattern = re.compile(code_pattern)
            else:
                self._code_pattern = None

    def find_country_mentions(self, text):
        """
        Find all country name mentions in the text with their positions.
        Returns list of (start_pos, end_pos, country_code, country_name).
        """
        self._build_country_pattern()
        mentions = []

        # Find country names (case-insensitive)
        if self._country_pattern:
            for match in self._country_pattern.finditer(text):
                name_lower = match.group(1).lower()
                code = self.countries['name_to_code'].get(name_lower)
                if code:
                    country_name = self.countries['code_to_name'].get(code, match.group(1))
                    mentions.append((match.start(), match.end(), code, country_name))

        # Find 2-letter country codes (case-sensitive)
        if self._code_pattern:
            for match in self._code_pattern.finditer(text):
                code = match.group(1)
                name = self.countries['code_to_name'].get(code)
                if name:
                    mentions.append((match.start(), match.end(), code, name))

        return mentions

    def extract_locations(self, text):
        """
        Extract international locations using proximity-based matching.
        Requires both city AND country to appear close together in text.
        """
        if not text:
            return []

        # Limit text length for performance
        if len(text) > 50000:
            text = text[:50000]

        locations = []

        # First, find all country mentions
        country_mentions = self.find_country_mentions(text)
        if not country_mentions:
            return locations

        # Pattern 1: "City, Country" - direct adjacency (most reliable)
        pattern1 = r'\b([A-Z][a-z\u00C0-\u024F]+(?:[\s-][A-Z][a-z\u00C0-\u024F]+)*),\s*([A-Z][a-z\u00C0-\u024F]+(?:\s+[A-Z][a-z\u00C0-\u024F]+)*)\b'

        for match in re.finditer(pattern1, text):
            city, country = match.groups()
            city_lower = city.lower()
            country_lower = country.lower()

            country_code = self.countries.get('name_to_code', {}).get(country_lower)
            if country_code:
                country_name = self.countries['code_to_name'].get(country_code, country)
                key = f"{city_lower}, {country_name.lower()}"
                if key in self.locations:
                    locations.append((city, country_name, country_code))

        # Pattern 2: "City (Country)" format
        pattern2 = r'\b([A-Z][a-z\u00C0-\u024F]+(?:[\s-][A-Z][a-z\u00C0-\u024F]+)*)\s*\(([A-Z]{2,}|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\)'

        for match in re.finditer(pattern2, text):
            city, country = match.groups()
            city_lower = city.lower()

            country_code = self.countries.get('name_to_code', {}).get(country.lower())
            if not country_code and country.upper() in self.countries.get('code_to_name', {}):
                country_code = country.upper()

            if country_code:
                country_name = self.countries['code_to_name'].get(country_code, country)
                key = f"{city_lower}, {country_name.lower()}"
                if key in self.locations:
                    locations.append((city, country_name, country_code))

        # Pattern 3: "City, XX" country code format
        pattern3 = r'\b([A-Z][a-z\u00C0-\u024F]+(?:[\s-][A-Z][a-z\u00C0-\u024F]+)*),\s*([A-Z]{2})\b'
        us_states = {'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'}

        for match in re.finditer(pattern3, text):
            city, code = match.groups()
            if code in us_states:
                continue

            city_lower = city.lower()
            actual_code = 'GB' if code == 'UK' else code

            country_name = self.countries.get('code_to_name', {}).get(actual_code)
            if country_name:
                key = f"{city_lower}, {country_name.lower()}"
                if key in self.locations:
                    locations.append((city, country_name, actual_code))

        # Pattern 4: Proximity-based - city within 100 chars of country mention
        # Only for cities NOT already found above
        found_cities = {loc[0].lower() for loc in locations}

        for country_start, country_end, country_code, country_name in country_mentions:
            # Get text window around country mention
            window_start = max(0, country_start - 100)
            window_end = min(len(text), country_end + 100)
            window = text[window_start:window_end]

            # Look for capitalized words that could be cities
            city_pattern = r'\b([A-Z][a-z\u00C0-\u024F]+(?:[\s-][A-Z][a-z\u00C0-\u024F]+)?)\b'
            for city_match in re.finditer(city_pattern, window):
                city = city_match.group(1)
                city_lower = city.lower()

                # Skip if already found
                if city_lower in found_cities:
                    continue

                # Check if this city exists in this country
                key = f"{city_lower}, {country_name.lower()}"
                if key in self.locations:
                    locations.append((city, country_name, country_code))
                    found_cities.add(city_lower)

        return locations

    def validate_and_geocode(self, city, country_name, country_code):
        """
        Validate a city/country pair and return coordinates.
        Returns None if not a valid location.
        """
        key = f"{city}, {country_name}".lower()

        if key in self.locations:
            loc = self.locations[key]
            return {
                'name': loc['name'],
                'country': loc['country'],
                'country_code': loc['country_code'],
                'lat': loc['lat'],
                'lng': loc['lng'],
                'population': loc.get('population', 0),
                'type': 'city'
            }

        # Try with ASCII name
        key_ascii = f"{city.lower()}, {country_name.lower()}"
        if key_ascii in self.locations:
            loc = self.locations[key_ascii]
            return {
                'name': loc['name'],
                'country': loc['country'],
                'country_code': loc['country_code'],
                'lat': loc['lat'],
                'lng': loc['lng'],
                'population': loc.get('population', 0),
                'type': 'city'
            }

        return None


def process_documents(mongo_uri='mongodb://localhost:27017', db_name='toxic_docs',
                      batch_size=1000, limit=None):
    """
    Process all documents and extract international geography mentions.
    Stores aggregated results in MongoDB.
    """
    print(f"Connecting to MongoDB: {mongo_uri}")
    client = MongoClient(mongo_uri)
    db = client[db_name]

    # Initialize extractor
    extractor = WorldGeographyExtractor()

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

    # Process documents - skip very large docs that cause regex issues
    processed = 0
    # Use $expr with $strLenCP to filter by text length (skip docs > 50k chars)
    query = {
        '$expr': {
            '$lt': [{'$strLenCP': {'$ifNull': ['$text', '']}}, 50000]
        }
    }
    cursor = db.documents.find(query, {'_id': 1, 'text': 1, 'title': 1})

    if limit:
        cursor = cursor.limit(limit)

    for doc in tqdm(cursor, total=total_docs, desc="Extracting world geographies"):
        text = doc.get('text', '') or ''
        title = doc.get('title', '') or ''
        full_text = f"{title} {text}"

        # Extract locations
        raw_locations = extractor.extract_locations(full_text)

        # Validate and count
        seen_in_doc = set()
        for city, country_name, country_code in raw_locations:
            validated = extractor.validate_and_geocode(city, country_name, country_code)
            if validated and validated.get('lat'):
                # Create a canonical key
                key = f"{validated['name']}, {validated['country']}".lower()

                if key not in seen_in_doc:
                    seen_in_doc.add(key)
                    geo_counts[key]['count'] += 1
                    geo_counts[key]['info'] = validated
                    # Store doc_ids for small counts
                    if geo_counts[key]['count'] <= 100:
                        geo_counts[key]['doc_ids'].append(str(doc['_id']))

        processed += 1

    print(f"\nProcessed {processed} documents")
    print(f"Found {len(geo_counts)} unique world locations")

    # Store results in MongoDB
    print("\nStoring results in MongoDB...")
    db.world_geography_counts.drop()

    geo_docs = []
    for key, data in geo_counts.items():
        if data['info'] and data['count'] >= 1:
            geo_doc = {
                'location_key': key,
                'name': data['info'].get('name', key),
                'country': data['info'].get('country', ''),
                'country_code': data['info'].get('country_code', ''),
                'lat': data['info'].get('lat'),
                'lng': data['info'].get('lng'),
                'population': data['info'].get('population', 0),
                'count': data['count'],
                'type': 'city',
                'sample_doc_ids': data['doc_ids'][:10],
                'updated_at': datetime.utcnow()
            }
            geo_docs.append(geo_doc)

    if geo_docs:
        db.world_geography_counts.insert_many(geo_docs)
        db.world_geography_counts.create_index('location_key')
        db.world_geography_counts.create_index('count')
        db.world_geography_counts.create_index('country')
        db.world_geography_counts.create_index('country_code')
        db.world_geography_counts.create_index([('lat', 1), ('lng', 1)])

    print(f"Stored {len(geo_docs)} world location records")

    # Print top locations
    print("\nTop 20 world locations by mention count:")
    top_20 = sorted(geo_docs, key=lambda x: x['count'], reverse=True)[:20]
    for i, loc in enumerate(top_20, 1):
        print(f"  {i}. {loc['name']}, {loc['country']}: {loc['count']} mentions")

    return geo_docs


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Extract world geographies from documents')
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
