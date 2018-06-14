import urllib.request
import tarfile
from io import BytesIO
from osgeo import gdal, gdal_array, osr

import numpy as np

def date_from_file(name):
    """Get date from filename.

    Keyword arguments:
    name -- File name to extract date from
    """
    match = re.search("\d{8}", name)
    date = datetime.strptime(match.group(), '%Y%m%d')
    return date

def gdal_metadata(source):
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
    ndv, width, height, transform, projection, dtype = gdal_metadata(ds)
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
