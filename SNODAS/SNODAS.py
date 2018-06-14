import re
import os
import tarfile
import gzip
import urllib.request
from datetime import datetime, timedelta
from io import BytesIO
from osgeo import gdal, gdal_array, osr

import numpy as np
import xarray as xr

def dateFromFile(name):
    """Get date from filename.

    Keyword arguments:
    name -- File name to extract date from
    """
    match = re.search("\d{8}", name)
    date = datetime.strptime(match.group(), '%Y%m%d')
    return date

def get_metadata(source):
    """Get metadata from GDAL dataset.

    Keyword arguments:
    source -- GDAL dataset to retrieve metadata for
    """
    ndv = source.GetRasterBand(1).GetNoDataValue()
    width = source.RasterXSize
    height = source.RasterYSize
    transform = source.GetGeoTransform()
    projection = osr.SpatialReference()
    projection.ImportFromWkt(source.GetProjectionRef())
    dtype = gdal.GetDataTypeName(source.GetRasterBand(1).DataType)

    return ndv, width, height, transform, projection, dtype

def date_to_url(date):
    """Get url of SNODAS data for given date.

    Keyword arguments:
    date -- Date to fetch SNODAS data for
    """
    if date >= datetime(2003,9,30) and date < datetime(2010,1,1):
        return date.strftime('ftp://sidads.colorado.edu/DATASETS/NOAA/G02158/masked/%Y/%m_%b/SNODAS_%Y%m%d.tar')
    elif date >= datetime(2010,1,1):
        return date.strftime('ftp://sidads.colorado.edu/DATASETS/NOAA/G02158/unmasked/%Y/%m_%b/SNODAS_unmasked_%Y%m%d.tar')

def date_to_gz_format(date):
    """Get format string for gzipped SNODAS files for given date.

    Keyword arguments:
    date -- Date to fetch SNODAS data for
    """
    if date >= datetime(2003,9,30) and date < datetime(2010,1,1):
        return date.strftime('us_ssmv1%%itS__T0001TTNATS%Y%m%d05HP001.%%s.gz')
    elif date >= datetime(2010,1,1):
        return date.strftime('zz_ssmv1%%itS__T0001TTNATS%Y%m%d05HP001.%%s.gz')

def url_to_io(url):
    """Get raw bytes from url.

    Keyword arguments:
    url -- URL to fetch data from
    """
    stream = urllib.request.urlopen(url)
    bytes = BytesIO()
    while True:
        next = stream.read(16384)
        if not next:
            break

        bytes.write(next)

    stream.close()
    bytes.seek(0)
    return bytes

def url_to_tar(url):
    """Get tar object from url.

    Keyword arguments:
    url -- URL of SNODAS data for specific date
    """
    io = url_to_io(url)
    tar = tarfile.open(fileobj = io, mode = 'r')
    return tar

# Remove lines longer than 256 characters from header (GDAL requirement)
def clean_header(hdr):
    """Remove lines longer than 256 characters from header (GDAL requirement)."""
    new_hdr = BytesIO()
    for line in hdr:
        if len(line) <= 256:
            new_hdr.write(line)

    # Cleanup
    new_hdr.write(b'')
    new_hdr.seek(0)
    hdr.close()

    return new_hdr

def clean_tar_paths(paths, tar):
    """Corrects paths of files in tar."""
    new_paths = []
    for path in paths:
        try:
            info = tar.getmember(path)
        except:
            path = './' + path
        new_paths.append(path)
    return new_paths

def tar_to_data(tar, gz_format, code=1036):
    """Converts tar archive to GDAL dataset.

    Keyword arguments:
    tar -- tar object
    gz_format -- format for gzipped files in archive
    code -- SNODAS product code (default 1036 [Snow Depth])
    """

    extensions = ['dat', 'Hdr']
    # Untar and extract files
    gz_paths = [gz_format % (code, extension) for extension in extensions]
    vsi_paths = ['/vsimem/' + path[:-3] for path in gz_paths]

    # Some paths in tar file have ./ preceeding, some do not
    # Use clean_tar_paths to find and use correct paths
    gz_paths = clean_tar_paths(gz_paths, tar)

    gz_files = [tar.extractfile(path) for path in gz_paths]
    dat_file, hdr_file = [gzip.GzipFile(fileobj=file, mode='r') for file in gz_files]

    # Read data into buffers
    hdr_file = clean_header(hdr_file)
    dat = dat_file.read()
    hdr = hdr_file.read()

    # Convert to GDAL Dataset
    gdal.FileFromMemBuffer(vsi_paths[0], dat)
    gdal.FileFromMemBuffer(vsi_paths[1], hdr)
    ds = gdal.Open(vsi_paths[1])

    # Close / Unlink Virtual Files
    tar.close()
    dat_file.close()
    hdr_file.close()
    gdal.Unlink(vsi_paths[0])
    gdal.Unlink(vsi_paths[1])

    return ds

def save_ds(ds, path, driver):
    """Save GDAL dataset using arbitrary driver.

    Keyword arguments:
    ds -- GDAl dataset
    path -- Location where file will be saved
    driver -- Driver to use
    """

    band = ds.GetRasterBand(1)
    bytes = band.ReadAsArray()
    driver = gdal.GetDriverByName(driver)
    ndv, width, height, transform, projection, dtype = get_metadata(ds)
    bytes[np.isnan(bytes)] = ndv
    out_ds = driver.Create(path, width, height, 1, gdal.GDT_Int16)
    out_ds.SetGeoTransform(transform)
    out_ds.SetProjection(projection.ExportToWkt())

    out_ds.GetRasterBand(1).WriteArray(bytes)
    out_ds.GetRasterBand(1).SetNoDataValue(ndv)

def save_tiff(ds, path):
    """Save GDAL dataset as GeoTIFF file.

    Keyword arguments:
    ds -- GDAl dataset
    path -- Location where file will be saved
    """
    save_ds(ds, path, 'GTiff')

def save_netcdf(ds, path):
    """Save GDAL dataset as NetCDF file.

    Keyword arguments:
    ds -- GDAl dataset
    path -- Location where file will be saved
    """
    save_ds(ds, path, 'netCDF')

def snodas_ds(date, code=1036):
    """Get SNODAS data as GDAL dataset for specific date.

    Keyword arguments:
    date -- datetime object
    code -- integer specifying SNODAS product (default 1036 [Snow Depth])
    """
    url = date_to_url(date)
    gz_format = date_to_gz_format(date)
    tar = url_to_tar(url)
    return tar_to_data(tar, gz_format, code=code)
