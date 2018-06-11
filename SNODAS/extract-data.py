import os
import numpy as np
import tarfile
import gzip
import subprocess

from datetime import datetime, timedelta
from osgeo import gdal, gdal_array, osr

def getMetadata(Source):
    NDV = Source.GetRasterBand(1).GetNoDataValue()
    xsize = Source.RasterXSize
    ysize = Source.RasterYSize
    GeoT = Source.GetGeoTransform()
    Projection = osr.SpatialReference()
    Projection.ImportFromWkt(Source.GetProjectionRef())
    DataType = Source.GetRasterBand(1).DataType
    DataType = gdal.GetDataTypeName(DataType)
    return NDV, xsize, ysize, GeoT, Projection, DataType

def createTiff(OutputName, Array, driver, NDV, xsize, ysize, GeoT, Projection, DataType):
    DataType = gdal.GDT_Int16
    OutputName += '.tif'
    # Clear
    Array[np.isnan(Array)] = NDV
    # Set up the dataset
    DataSet = driver.Create(OutputName, xsize, ysize, 1, DataType)
    DataSet.SetGeoTransform(GeoT)
    DataSet.SetProjection(Projection.ExportToWkt())
    # Write to output
    DataSet.GetRasterBand(1).WriteArray(Array)
    DataSet.GetRasterBand(1).SetNoDataValue(NDV)

def fetchData(url, outdir=None):
    fn = os.path.split(url)[-1]
    if outdir is not None:
        fn = os.path.join(outdir, fn)
    if not os.path.exists(fn):
        #Find appropriate urlretrieve for Python 2 and 3
        try:
            from urllib.request import urlretrieve
        except ImportError:
            from urllib import urlretrieve
        print("Retrieving: %s" % url)
        #Add progress bar
        if not os.path.exists(os.path.dirname(fn)):
            os.makedirs(os.path.dirname(fn))
        urlretrieve(url, fn)
    return fn

def fetchSNODAS(dem_dt, bindir=None, outdir=None, code=1036):

    dataset = None
    snodas_url_str = None
    # Note: unmasked products (beyond CONUS) are only available from 2010-present
    if dem_dt >= datetime(2003,9,30) and dem_dt < datetime(2010,1,1):
        snodas_url_str = 'ftp://sidads.colorado.edu/DATASETS/NOAA/G02158/masked/%Y/%m_%b/SNODAS_%Y%m%d.tar'
        tar_subfn_str_fmt = 'us_ssmv1%itS__T0001TTNATS%%Y%%m%%d05HP001.%s.gz'
    elif dem_dt >= datetime(2010,1,1):
        snodas_url_str = 'ftp://sidads.colorado.edu/DATASETS/NOAA/G02158/unmasked/%Y/%m_%b/SNODAS_unmasked_%Y%m%d.tar'
        tar_subfn_str_fmt = './zz_ssmv1%itS__T0001TTNATS%%Y%%m%%d05HP001.%s.gz'

    # Raise Exception if date is invalid
    if snodas_url_str is None:
        print("No SNODAS data available for input date")
        return

    # Format URL
    snodas_url = dem_dt.strftime(snodas_url_str)
    snodas_tar_fn = fetchData(snodas_url, outdir=bindir)

    #Extract both dat and Hdr files, tar.gz
    tar = tarfile.open(snodas_tar_fn)

    for ext in ('dat', 'Hdr'):
        tar_subfn_str = tar_subfn_str_fmt % (code, ext)
        tar_subfn_gz = dem_dt.strftime(tar_subfn_str)
        tar_subfn = os.path.splitext(tar_subfn_gz)[0]
        print(tar_subfn)
        if bindir is not None:
            tar_subfn = os.path.join(bindir, tar_subfn)
        if not os.path.exists(tar_subfn):
            #Should be able to do this without writing intermediate gz to disk
            tar.extract(tar_subfn_gz)
            with gzip.open(tar_subfn_gz, 'rb') as f:
                outf = open(tar_subfn, 'wb')
                outf.write(f.read())
                outf.close()
            os.remove(tar_subfn_gz)

    #Need to delete 'Created by module comment' line from Hdr, can contain too many characters
    bad_str = 'Created by module comment'
    snodas_fn = tar_subfn
    f = open(snodas_fn)
    output = []
    for line in f:
        if not bad_str in line:
            output.append(line)
    f.close()
    f = open(snodas_fn, 'w')
    f.writelines(output)
    f.close()

    # Open dataset using GDAL
    dataset = gdal.Open(snodas_fn)
    if dataset is None:
        return
    band = dataset.GetRasterBand(1)
    Array = band.ReadAsArray()
    NDV = band.GetNoDataValue()
    #Array[Array == NDV] = 0

    NDV, xsize, ysize, GeoT, Projection, DataType = getMetadata(dataset)
    driver = gdal.GetDriverByName('GTiff')

    OutputName = dem_dt.strftime('SNODAS_%Y%m%d')
    VisName = dem_dt.strftime('SNODAS_VIS_%Y%m%d')
    if outdir is not None:
        OutputName = os.path.join(outdir, OutputName)
        VisName = os.path.join(outdir, VisName)
    if not os.path.exists(os.path.dirname(OutputName)):
        os.makedirs(os.path.dirname(OutputName))

    createTiff(OutputName, Array, driver, NDV, xsize, ysize, GeoT, Projection, DataType)
    # subprocess.call(['gdaldem', 'color-relief', '%s.tif' % OutputName, 'color_map.txt', '%s.tif' % VisName, '-alpha'])

start_date = datetime(2017,11,1)
for date in (start_date + timedelta(n) for n in range(365)):
    fetchSNODAS(date, bindir=os.path.abspath("./bin"), outdir=os.path.abspath("./tiff"))
