import re
import os
import xarray as xr


from datetime import datetime, timedelta

def dateFromFile(name):
    match = re.search("\d{8}", name)
    date = datetime.strptime(match.group(), '%Y%m%d')
    return date

def dataArrayFromFile(name):
    return xr.open_rasterio(name)
