#!/usr/bin/env python3
"""
Flask backend for Geography Dashboard.
Serves Leaflet.js map with clickable location bubbles.
"""

from flask import Flask, render_template, jsonify, request
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB connection
client = MongoClient("mongodb://localhost:27017")
db = client.toxic_docs


@app.route('/')
def index():
    """Serve the main dashboard page."""
    return render_template('index.html')


@app.route('/api/geographies')
def get_geographies():
    """
    Get geography data for the map with filtering support.
    Query params:
        min_count: minimum mention count (default 1)
        limit: max results (default 5000)
        state: filter by state name
        county: filter by county name
        type: filter by type (place, state, county)
    """
    min_count = int(request.args.get('min_count', 1))
    limit = min(int(request.args.get('limit', 5000)), 10000)
    state_filter = request.args.get('state', '').strip()
    county_filter = request.args.get('county', '').strip()
    type_filter = request.args.get('type', '').strip()

    # Build query
    query = {
        'count': {'$gte': min_count},
        'lat': {'$exists': True, '$ne': None},
        'lng': {'$exists': True, '$ne': None}
    }

    if state_filter:
        query['state'] = state_filter

    if county_filter:
        query['county'] = county_filter

    if type_filter:
        query['type'] = type_filter

    # Fetch locations with coordinates
    results = list(db.geography_counts.find(
        query,
        {
            '_id': 0,
            'location_key': 1,
            'name': 1,
            'state': 1,
            'state_abbrev': 1,
            'county': 1,
            'lat': 1,
            'lng': 1,
            'count': 1,
            'type': 1
        }
    ).sort('count', -1).limit(limit))

    return jsonify({
        'locations': results,
        'total': len(results),
        'min_count': min_count
    })


@app.route('/api/geographies/filters')
def get_filters():
    """
    Get distinct values for filter dropdowns.
    Returns lists of unique states, counties, and types.
    """
    states = sorted([s for s in db.geography_counts.distinct('state') if s])
    counties = sorted([c for c in db.geography_counts.distinct('county') if c])
    types = sorted(db.geography_counts.distinct('type'))

    return jsonify({
        'states': states,
        'counties': counties,
        'types': types
    })


@app.route('/api/geographies/counties')
def get_counties_for_state():
    """
    Get counties for a specific state (for cascading dropdown).
    Query params:
        state: state name to get counties for
    """
    state = request.args.get('state', '').strip()

    if not state:
        return jsonify([])

    counties = sorted([c for c in db.geography_counts.distinct('county', {'state': state}) if c])
    return jsonify(counties)


@app.route('/api/geographies/search')
def search_geographies():
    """
    Search for geographies by name prefix.
    Query params:
        q: search query (required)
        limit: max results (default 20)
    """
    query = request.args.get('q', '').strip()
    limit = min(int(request.args.get('limit', 20)), 100)

    if len(query) < 2:
        return jsonify([])

    # Search by name prefix (case-insensitive)
    results = list(db.geography_counts.find(
        {
            'location_key': {'$regex': f'^{query.lower()}'},
            'lat': {'$exists': True, '$ne': None}
        },
        {
            '_id': 0,
            'location_key': 1,
            'name': 1,
            'state': 1,
            'county': 1,
            'count': 1,
            'lat': 1,
            'lng': 1,
            'type': 1
        }
    ).sort('count', -1).limit(limit))

    return jsonify(results)


@app.route('/api/geographies/stats')
def get_stats():
    """Get summary statistics about the geography data."""
    total_locations = db.geography_counts.count_documents({})
    hotspots = db.geography_counts.count_documents({'count': {'$gte': 500}})
    places = db.geography_counts.count_documents({'type': 'place'})
    states = db.geography_counts.count_documents({'type': 'state'})

    # Get count distribution
    pipeline = [
        {'$group': {
            '_id': None,
            'total_mentions': {'$sum': '$count'},
            'max_count': {'$max': '$count'},
            'avg_count': {'$avg': '$count'}
        }}
    ]
    agg_result = list(db.geography_counts.aggregate(pipeline))

    stats = {
        'total_locations': total_locations,
        'total_places': places,
        'total_states': states,
        'hotspots_500plus': hotspots,
        'total_mentions': agg_result[0]['total_mentions'] if agg_result else 0,
        'max_mentions': agg_result[0]['max_count'] if agg_result else 0,
        'avg_mentions': round(agg_result[0]['avg_count'], 2) if agg_result else 0
    }

    return jsonify(stats)


# ============== World Geography Endpoints ==============

@app.route('/api/world/geographies')
def get_world_geographies():
    """
    Get world geography data for the map with filtering support.
    Query params:
        min_count: minimum mention count (default 1)
        limit: max results (default 5000)
        country: filter by country name
    """
    min_count = int(request.args.get('min_count', 1))
    limit = min(int(request.args.get('limit', 5000)), 10000)
    country_filter = request.args.get('country', '').strip()

    # Build query
    query = {
        'count': {'$gte': min_count},
        'lat': {'$exists': True, '$ne': None},
        'lng': {'$exists': True, '$ne': None}
    }

    if country_filter:
        query['country'] = country_filter

    # Fetch locations with coordinates
    results = list(db.world_geography_counts.find(
        query,
        {
            '_id': 0,
            'location_key': 1,
            'name': 1,
            'country': 1,
            'country_code': 1,
            'lat': 1,
            'lng': 1,
            'count': 1,
            'population': 1,
            'type': 1
        }
    ).sort('count', -1).limit(limit))

    return jsonify({
        'locations': results,
        'total': len(results),
        'min_count': min_count
    })


@app.route('/api/world/geographies/countries')
def get_world_countries():
    """
    Get country-level aggregated data (sum of all cities per country).
    Query params:
        min_count: minimum total mention count for country (default 1)
        limit: max results (default 500)
    """
    min_count = int(request.args.get('min_count', 1))
    limit = min(int(request.args.get('limit', 500)), 1000)

    # Aggregate cities by country
    pipeline = [
        {
            '$group': {
                '_id': '$country',
                'country_code': {'$first': '$country_code'},
                'total_count': {'$sum': '$count'},
                'city_count': {'$sum': 1},
                'avg_lat': {'$avg': '$lat'},
                'avg_lng': {'$avg': '$lng'}
            }
        },
        {'$match': {'total_count': {'$gte': min_count}}},
        {'$sort': {'total_count': -1}},
        {'$limit': limit}
    ]

    results = list(db.world_geography_counts.aggregate(pipeline))

    # Format results
    locations = [{
        'name': r['_id'],
        'country': r['_id'],
        'country_code': r['country_code'],
        'count': r['total_count'],
        'city_count': r['city_count'],
        'lat': r['avg_lat'],
        'lng': r['avg_lng'],
        'type': 'country'
    } for r in results if r['_id']]

    return jsonify({
        'locations': locations,
        'total': len(locations),
        'min_count': min_count
    })


@app.route('/api/world/geographies/filters')
def get_world_filters():
    """
    Get distinct values for filter dropdowns.
    Returns list of unique countries that have locations meeting min_count threshold.
    """
    min_count = int(request.args.get('min_count', 51))

    # Only return countries that have at least one location meeting the threshold
    countries = sorted([c for c in db.world_geography_counts.distinct(
        'country',
        {'count': {'$gte': min_count}}
    ) if c])

    return jsonify({
        'countries': countries
    })


@app.route('/api/world/geographies/search')
def search_world_geographies():
    """
    Search for world geographies by name prefix.
    Query params:
        q: search query (required)
        limit: max results (default 20)
    """
    query = request.args.get('q', '').strip()
    limit = min(int(request.args.get('limit', 20)), 100)

    if len(query) < 2:
        return jsonify([])

    # Search by name prefix (case-insensitive)
    results = list(db.world_geography_counts.find(
        {
            'location_key': {'$regex': f'^{query.lower()}'},
            'lat': {'$exists': True, '$ne': None}
        },
        {
            '_id': 0,
            'location_key': 1,
            'name': 1,
            'country': 1,
            'country_code': 1,
            'count': 1,
            'lat': 1,
            'lng': 1,
            'type': 1
        }
    ).sort('count', -1).limit(limit))

    return jsonify(results)


@app.route('/api/world/geographies/stats')
def get_world_stats():
    """Get summary statistics about the world geography data."""
    total_locations = db.world_geography_counts.count_documents({})
    hotspots = db.world_geography_counts.count_documents({'count': {'$gte': 500}})

    # Get count distribution
    pipeline = [
        {'$group': {
            '_id': None,
            'total_mentions': {'$sum': '$count'},
            'max_count': {'$max': '$count'},
            'avg_count': {'$avg': '$count'}
        }}
    ]
    agg_result = list(db.world_geography_counts.aggregate(pipeline))

    # Count unique countries
    unique_countries = len(db.world_geography_counts.distinct('country'))

    stats = {
        'total_locations': total_locations,
        'total_countries': unique_countries,
        'hotspots_500plus': hotspots,
        'total_mentions': agg_result[0]['total_mentions'] if agg_result else 0,
        'max_mentions': agg_result[0]['max_count'] if agg_result else 0,
        'avg_mentions': round(agg_result[0]['avg_count'], 2) if agg_result else 0
    }

    return jsonify(stats)


if __name__ == '__main__':
    app.run(debug=True, port=5001)
