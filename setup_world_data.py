#!/usr/bin/env python3
"""
Download and prepare world city data for international geography validation.
Uses GeoNames database (https://www.geonames.org/).
"""

import csv
import json
import os
import urllib.request
import zipfile
from collections import defaultdict

# Data sources
CITIES_URL = 'https://download.geonames.org/export/dump/cities5000.zip'
COUNTRIES_URL = 'https://download.geonames.org/export/dump/countryInfo.txt'

# GeoNames column indices (tab-delimited)
GEONAME_COLS = {
    'geonameid': 0,
    'name': 1,
    'asciiname': 2,
    'alternatenames': 3,
    'latitude': 4,
    'longitude': 5,
    'feature_class': 6,
    'feature_code': 7,
    'country_code': 8,
    'cc2': 9,
    'admin1_code': 10,
    'admin2_code': 11,
    'admin3_code': 12,
    'admin4_code': 13,
    'population': 14,
    'elevation': 15,
    'dem': 16,
    'timezone': 17,
    'modification_date': 18
}


def download_file(url, dest):
    """Download a file with progress."""
    print(f"Downloading {url}...")
    urllib.request.urlretrieve(url, dest)
    print(f"Saved to {dest}")


def parse_country_info(filepath):
    """
    Parse countryInfo.txt to get country code -> name mappings.
    File has comments starting with # and tab-delimited data.
    """
    countries = {}
    code_to_name = {}
    name_to_code = {}

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            # Skip comments
            if line.startswith('#'):
                continue

            parts = line.strip().split('\t')
            if len(parts) < 5:
                continue

            iso_code = parts[0].strip()  # ISO 2-letter code
            iso3_code = parts[1].strip()  # ISO 3-letter code
            country_name = parts[4].strip()  # Country name

            if iso_code and country_name:
                code_to_name[iso_code] = country_name
                code_to_name[iso3_code] = country_name
                name_to_code[country_name.lower()] = iso_code

                # Also add common variations
                # e.g., "United Kingdom" -> also match "UK", "Britain"
                countries[iso_code] = {
                    'name': country_name,
                    'iso2': iso_code,
                    'iso3': iso3_code
                }

    # Add common aliases
    aliases = {
        'uk': 'GB',
        'britain': 'GB',
        'great britain': 'GB',
        'england': 'GB',
        'scotland': 'GB',
        'wales': 'GB',
        'usa': 'US',
        'america': 'US',
        'united states of america': 'US',
        'holland': 'NL',
        'the netherlands': 'NL',
        'czech republic': 'CZ',
        'czechia': 'CZ',
        'russia': 'RU',
        'south korea': 'KR',
        'korea': 'KR',
        'north korea': 'KP',
        'taiwan': 'TW',
        'ivory coast': 'CI',
        'uae': 'AE',
        'vatican': 'VA',
        'congo': 'CD',
    }

    for alias, code in aliases.items():
        if code in code_to_name:
            name_to_code[alias] = code

    return {
        'code_to_name': code_to_name,
        'name_to_code': name_to_code,
        'countries': countries
    }


def parse_cities(filepath, country_info):
    """
    Parse GeoNames cities file (tab-delimited).
    Returns dict of places and disambiguation index.
    """
    places = {}
    code_to_name = country_info['code_to_name']

    # Skip US cities - they're handled by the US geography system
    skip_countries = {'US'}

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) < 15:
                continue

            try:
                country_code = parts[GEONAME_COLS['country_code']].strip()

                # Skip US cities
                if country_code in skip_countries:
                    continue

                # Get country name
                country_name = code_to_name.get(country_code)
                if not country_name:
                    continue

                name = parts[GEONAME_COLS['name']].strip()
                ascii_name = parts[GEONAME_COLS['asciiname']].strip()
                lat = float(parts[GEONAME_COLS['latitude']])
                lng = float(parts[GEONAME_COLS['longitude']])
                population = int(parts[GEONAME_COLS['population']] or 0)

                place_data = {
                    'name': name,
                    'ascii_name': ascii_name,
                    'country': country_name,
                    'country_code': country_code,
                    'lat': lat,
                    'lng': lng,
                    'population': population,
                    'type': 'city'
                }

                # Store with multiple key formats
                keys = [
                    f"{name}, {country_name}".lower(),
                    f"{ascii_name}, {country_name}".lower(),
                    f"{name}, {country_code}".lower(),
                    f"{ascii_name}, {country_code}".lower(),
                ]

                for key in keys:
                    if key not in places:
                        places[key] = place_data

            except (ValueError, IndexError) as e:
                continue

    return places


def build_city_countries_index(places):
    """
    Build index of city names to possible countries (for disambiguation).
    """
    city_countries = defaultdict(list)

    seen = set()  # Avoid duplicates

    for key, data in places.items():
        city_name = data['name'].lower()
        country_info = {
            'country': data['country'],
            'country_code': data['country_code'],
            'lat': data['lat'],
            'lng': data['lng'],
            'population': data['population']
        }

        # Create unique key to avoid duplicates
        unique_key = f"{city_name}|{data['country_code']}"
        if unique_key not in seen:
            seen.add(unique_key)
            city_countries[city_name].append(country_info)

        # Also index ASCII name if different
        ascii_name = data['ascii_name'].lower()
        if ascii_name != city_name:
            unique_key = f"{ascii_name}|{data['country_code']}"
            if unique_key not in seen:
                seen.add(unique_key)
                city_countries[ascii_name].append(country_info)

    # Sort each city's countries by population (largest first)
    for city in city_countries:
        city_countries[city].sort(key=lambda x: x['population'], reverse=True)

    return dict(city_countries)


def main():
    os.makedirs('data', exist_ok=True)

    # Download country info
    countries_file = 'data/countryInfo.txt'
    if not os.path.exists(countries_file):
        download_file(COUNTRIES_URL, countries_file)
    else:
        print(f"{countries_file} already exists, skipping download")

    # Download cities data
    cities_zip = 'data/cities5000.zip'
    cities_file = 'data/cities5000.txt'

    if not os.path.exists(cities_file):
        if not os.path.exists(cities_zip):
            download_file(CITIES_URL, cities_zip)

        # Extract zip
        print("Extracting cities data...")
        with zipfile.ZipFile(cities_zip, 'r') as z:
            z.extractall('data')
        print(f"Extracted to {cities_file}")
    else:
        print(f"{cities_file} already exists, skipping download")

    # Parse country info
    print("\nParsing country information...")
    country_info = parse_country_info(countries_file)
    print(f"Loaded {len(country_info['countries'])} countries")

    # Parse cities
    print("\nParsing cities...")
    places = parse_cities(cities_file, country_info)
    print(f"Loaded {len(places)} place entries (excluding US)")

    # Build city disambiguation index
    city_countries = build_city_countries_index(places)

    # Save to JSON
    print("\nSaving processed data...")

    with open('data/world_locations.json', 'w', encoding='utf-8') as f:
        json.dump(places, f, ensure_ascii=False)
    print(f"Saved {len(places)} locations to data/world_locations.json")

    with open('data/city_countries_index.json', 'w', encoding='utf-8') as f:
        json.dump(city_countries, f, ensure_ascii=False)
    print(f"Saved disambiguation index for {len(city_countries)} city names")

    with open('data/countries.json', 'w', encoding='utf-8') as f:
        json.dump(country_info, f, ensure_ascii=False)
    print(f"Saved country mappings to data/countries.json")

    print("\nDone! World data is ready for geography extraction.")

    # Print some stats
    print(f"\nSample locations:")
    samples = list(places.items())[:5]
    for key, data in samples:
        print(f"  {key}: {data['country']} ({data['lat']}, {data['lng']})")

    # Show cities in multiple countries
    multi_country_cities = [(city, countries) for city, countries in city_countries.items()
                            if len(countries) > 3]
    multi_country_cities.sort(key=lambda x: len(x[1]), reverse=True)

    print(f"\nCities appearing in many countries (top 10):")
    for city, countries in multi_country_cities[:10]:
        country_names = [c['country'] for c in countries[:5]]
        print(f"  {city}: {len(countries)} countries - {', '.join(country_names)}...")


if __name__ == '__main__':
    main()
