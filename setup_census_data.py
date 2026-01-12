#!/usr/bin/env python3
"""
Download and prepare US city/county data for geography validation.
Uses the US-Cities-Database from GitHub.
"""

import csv
import json
import os
import urllib.request
from collections import defaultdict

# Data source
CITIES_URL = 'https://raw.githubusercontent.com/kelvins/US-Cities-Database/main/csv/us_cities.csv'

# State abbreviations to full names
STATE_ABBREV = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia',
    'PR': 'Puerto Rico', 'VI': 'Virgin Islands', 'GU': 'Guam',
}

STATE_TO_ABBREV = {v: k for k, v in STATE_ABBREV.items()}

# State centroids for state-level lookups
STATE_CENTROIDS = {
    'Alabama': (32.806671, -86.791130), 'Alaska': (61.370716, -152.404419),
    'Arizona': (33.729759, -111.431221), 'Arkansas': (34.969704, -92.373123),
    'California': (36.116203, -119.681564), 'Colorado': (39.059811, -105.311104),
    'Connecticut': (41.597782, -72.755371), 'Delaware': (39.318523, -75.507141),
    'Florida': (27.766279, -81.686783), 'Georgia': (33.040619, -83.643074),
    'Hawaii': (21.094318, -157.498337), 'Idaho': (44.240459, -114.478828),
    'Illinois': (40.349457, -88.986137), 'Indiana': (39.849426, -86.258278),
    'Iowa': (42.011539, -93.210526), 'Kansas': (38.526600, -96.726486),
    'Kentucky': (37.668140, -84.670067), 'Louisiana': (31.169546, -91.867805),
    'Maine': (44.693947, -69.381927), 'Maryland': (39.063946, -76.802101),
    'Massachusetts': (42.230171, -71.530106), 'Michigan': (43.326618, -84.536095),
    'Minnesota': (45.694454, -93.900192), 'Mississippi': (32.741646, -89.678696),
    'Missouri': (38.456085, -92.288368), 'Montana': (46.921925, -110.454353),
    'Nebraska': (41.125370, -98.268082), 'Nevada': (38.313515, -117.055374),
    'New Hampshire': (43.452492, -71.563896), 'New Jersey': (40.298904, -74.521011),
    'New Mexico': (34.840515, -106.248482), 'New York': (42.165726, -74.948051),
    'North Carolina': (35.630066, -79.806419), 'North Dakota': (47.528912, -99.784012),
    'Ohio': (40.388783, -82.764915), 'Oklahoma': (35.565342, -96.928917),
    'Oregon': (44.572021, -122.070938), 'Pennsylvania': (40.590752, -77.209755),
    'Rhode Island': (41.680893, -71.511780), 'South Carolina': (33.856892, -80.945007),
    'South Dakota': (44.299782, -99.438828), 'Tennessee': (35.747845, -86.692345),
    'Texas': (31.054487, -97.563461), 'Utah': (40.150032, -111.862434),
    'Vermont': (44.045876, -72.710686), 'Virginia': (37.769337, -78.169968),
    'Washington': (47.400902, -121.490494), 'West Virginia': (38.491226, -80.954453),
    'Wisconsin': (44.268543, -89.616508), 'Wyoming': (42.755966, -107.302490),
    'District of Columbia': (38.897438, -77.026817),
}


def download_file(url, dest):
    """Download a file."""
    print(f"Downloading {url}...")
    urllib.request.urlretrieve(url, dest)
    print(f"Saved to {dest}")


def parse_cities_csv(filepath):
    """Parse the US cities CSV file."""
    places = {}

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                state_abbrev = row['STATE_CODE'].strip()
                state_full = row['STATE_NAME'].strip()
                city = row['CITY'].strip()
                county = row.get('COUNTY', '').strip()
                lat = float(row['LATITUDE'])
                lng = float(row['LONGITUDE'])

                place_data = {
                    'name': city,
                    'state': state_full,
                    'state_abbrev': state_abbrev,
                    'county': county,
                    'lat': lat,
                    'lng': lng,
                    'type': 'place'
                }

                # Store with multiple key formats for flexible matching
                keys = [
                    f"{city}, {state_full}".lower(),
                    f"{city}, {state_abbrev}".lower(),
                ]

                for key in keys:
                    if key not in places:
                        places[key] = place_data

            except (ValueError, KeyError) as e:
                continue

    return places


def build_city_to_states_index(places):
    """Build an index of city names to possible states (for disambiguation)."""
    city_states = defaultdict(list)

    for key, data in places.items():
        city_name = data['name'].lower()
        state_info = {
            'state': data['state'],
            'state_abbrev': data['state_abbrev'],
            'lat': data['lat'],
            'lng': data['lng']
        }
        if state_info not in city_states[city_name]:
            city_states[city_name].append(state_info)

    return dict(city_states)


def main():
    os.makedirs('data', exist_ok=True)

    # Download cities data
    cities_file = 'data/us_cities.csv'
    if not os.path.exists(cities_file):
        download_file(CITIES_URL, cities_file)
    else:
        print(f"{cities_file} already exists, skipping download")

    # Parse cities
    print("\nParsing cities...")
    places = parse_cities_csv(cities_file)
    print(f"Loaded {len(places)} place entries")

    # Add state-level entries
    for state_name, (lat, lng) in STATE_CENTROIDS.items():
        abbrev = STATE_TO_ABBREV.get(state_name, '')
        state_data = {
            'name': state_name,
            'state': state_name,
            'state_abbrev': abbrev,
            'lat': lat,
            'lng': lng,
            'type': 'state'
        }
        places[state_name.lower()] = state_data
        if abbrev:
            places[abbrev.lower()] = state_data

    # Build city disambiguation index
    city_states = build_city_to_states_index(places)

    # Save to JSON
    print("\nSaving processed data...")

    with open('data/us_locations.json', 'w') as f:
        json.dump(places, f)
    print(f"Saved {len(places)} locations to data/us_locations.json")

    with open('data/city_states_index.json', 'w') as f:
        json.dump(city_states, f)
    print(f"Saved disambiguation index for {len(city_states)} city names")

    # Also save state info
    with open('data/states.json', 'w') as f:
        json.dump({
            'abbrev_to_full': STATE_ABBREV,
            'full_to_abbrev': STATE_TO_ABBREV
        }, f)

    print("\nDone! Data is ready for geography extraction.")

    # Print some stats
    print(f"\nSample locations:")
    samples = list(places.items())[:5]
    for key, data in samples:
        print(f"  {key}: {data['lat']}, {data['lng']}")


if __name__ == '__main__':
    main()
