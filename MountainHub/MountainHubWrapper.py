from datetime import datetime
import time

import pandas as pd
import requests

BASE_URL = 'https://api.mountainhub.com/timeline'
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

def prepare_bbox(box):
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

def get_data(limit=100, start=None, end=None, box=None, filter=True):

    # Build API request
    params = removeEmptyParams({
        'publisher': 'all',
        'obs_type': 'snow_conditions',
        'limit': limit,
        'since': dateToTimestamp(start),
        'before': dateToTimestamp(end),
        **prepare_bbox(box)
    })

    # Make request
    response = requests.get(BASE_URL, params=params, headers=HEADER)
    data = response.json()

    if 'results' not in data:
        raise ValueError(data)

    # Parse request
    records = data['results']
    parsed = [ parse(record) for record in records ]

    # Convert to dataframe and drop invalid results
    df = pd.DataFrame.from_records(parsed)
    if filter:
        df = df.dropna()
    return df
