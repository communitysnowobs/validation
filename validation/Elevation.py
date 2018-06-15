import pandas as pd
import requests
import validation.utils as ut
import validation.creds as creds

BASE_ELEVATION_URL = 'https://maps.googleapis.com/maps/api/elevation/json'

def el_data(points=[]):
    """Retrieves elevation data from Google Elevation API.

    Keyword arguments:
    points -- List of coordinates to retrieve elevation data at
    """
    records = []
    # Split into batches for API requests
    for batch in ut.batches(points, 256):
        params = {
            'locations': "|".join([",".join([str(point[0]), str(point[1])]) for point in points]),
            'key': creds.get_credential('google_key')
        }
        response = requests.get(BASE_ELEVATION_URL, params=params)
        data = response.json()

        if 'results' not in data:
            raise ValueError(data)

        records.extend(data['results'])
    parsed = [{ 'lat' : point[0], 'long' : point[1], **parse_elevation(record)} for point, record in zip(points, records)]
    df = pd.DataFrame.from_records(parsed)
    return df

def parse_elevation(record):
    """Parses record returned by Google Elevation API into standard format.

    Keyword arguments:
    record -- Segment of JSON returned by Google Elevation API
    """
    return {
        'elevation' : record['elevation']
    }

def average_elevation(box, grid_size = 16):
    """Approximates elevation over a bounding box using a grid of points.

    Keyword arguments:
    box -- Dictionary representing box to retrieve elevation data over
    grid_size -- Number of intervals used in each direction to approximate elevation
    """
    # Restrict grid size to fit in API request
    grid_size = min(grid_size, 16)
    points = []
    for lat in ut.intervals(box['ymin'], box['ymax'], grid_size):
        for long in ut.intervals(box['xmin'], box['xmax'], grid_size):
            points.append((lat, long))

    params = {
        'locations': "|".join([",".join(['%.4f' % point[0], '%.4f' % point[1]]) for point in points]),
        'key': config.GOOGLE_API_KEY
    }
    print(params)
    response = requests.get(BASE_ELEVATION_URL, params=params)
    print(response.text)
    data = response.json()

    if 'results' not in data:
        raise ValueError(data)

    records = data['results']
    elevations = [record['elevation'] for record in records]
    print(sum(elevations) / len(elevations))
    return sum(elevations) / len(elevations)

def merge_el_data(df):
    """Merges elevation data with snow depth observations data.

    Keyword arguments:
    df -- Dataframe of SNODAS data to add elevation data to
    """
    points = list(zip(df['lat'], df['long']))
    elevations = el_data(points)
    return pd.merge(df, elevations)
