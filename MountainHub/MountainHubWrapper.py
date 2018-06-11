from datetime import datetime
import time

import pandas as pd
import requests
import config

BASE_URL = 'https://api.mountainhub.com/timeline'
BASE_ELEVATION_URL = 'https://maps.googleapis.com/maps/api/elevation/json'
HEADER = { 'Accept-version': '1' }

def removeEmptyParams(dict):
    return { k:v for k, v in dict.items() if v is not None }

def dateToTimestamp(date):
    if date is None:
        return date
    return int(time.mktime(date.timetuple())) * 1000

def timestampToDate(timestamp):
    if timestamp is None:
        return timestamp
    return datetime.fromtimestamp(timestamp / 1000)

def make_box(box):
    if box is None:
        return {}
    return {
        'north_east_lat': box['ymax'],
        'north_east_lng': box['xmax'],
        'south_west_lat': box['ymin'],
        'south_west_lng': box['xmin']
    }

def parse(record):
    obs = record['observation']
    actor = record['actor']
    details = obs.get('details', [{}])
    snow_depth = details[0].get('snowpack_depth') if len(details) > 0 and details[0] is not None else None
    # Remap record structure
    return {
        'author_name' : actor.get('full_name') or actor.get('fullName'),
        'id' : obs['_id'],
        'timestamp' : int(obs['reported_at']),
        'date' : timestampToDate(int(obs['reported_at'])),
        'lat' : obs['location'][1],
        'long' : obs['location'][0],
        'type' : obs['type'],
        'snow_depth' : float(snow_depth) if snow_depth is not None else None
    }

def parse_elevation(record):
    return {
        'elevation' : record['elevation']
    }

def snow_data(limit=100, start=None, end=None, box=None, filter=True):
    # Build API request
    params = removeEmptyParams({
        'publisher': 'all',
        'obs_type': 'snow_conditions',
        'limit': limit,
        'since': dateToTimestamp(start),
        'before': dateToTimestamp(end),
        **make_box(box)
    })

    # Make request
    response = requests.get(BASE_URL, params=params, headers=HEADER)
    data = response.json()

    if 'results' not in data:
        raise ValueError(data)

    # Parse request
    records = data['results']
    parsed = [ parse(record) for record in records ]

    # Convert to dataframe and drop invalid results if necessary
    df = pd.DataFrame.from_records(parsed)
    if filter:
        df = df.dropna()
    return df

def get_el_data(points=[]):
    params = {
        'locations': "|".join([",".join([str(point[0]), str(point[1])]) for point in points]),
        'key': config.GOOGLE_API_KEY
    }
    response = requests.get(BASE_ELEVATION_URL, params=params)
    data = response.json()

    if 'results' not in data:
        raise ValueError(data)

    records = data['results']
    parsed = [{ 'lat' : point[0], 'long' : point[1], **parse_elevation(record)} for point, record in zip(points, records)]
    df = pd.DataFrame.from_records(parsed)
    return df

def merge_el_data(df):

    points = list(zip(df['lat'], df['long']))
    elevations = get_el_data(points)
    return pd.merge(df, elevations)
