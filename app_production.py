#!/usr/bin/env python3
"""
Flask backend for Geography Dashboard - Production version.
Serves Leaflet.js map with clickable location bubbles.
Configured for /geography URL prefix behind Apache proxy.
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


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5003)
