#!/usr/bin/env python
# coding: utf-8

"""
# # C-SNOW data exploration
# 
# 11/2
# 9/18-13. 2021-8-30. Emilio Mayorga
# 
# **TODO:**

# ## Notes
# 
# Last available netcdf files per data product:
# - Published: 2019-5-19
# - Experimental: 2020-4-8
# 
# For reference:
# - https://nbviewer.jupyter.org/github/waterhackweek/waterdata/blob/master/mashup_waterbudget.ipynb
#
# Miscellaneous:
# - For lat & lon coordinates in the published dataset, longitude cell lengths are constant,
#   but latitude cell lengths are not. They increase with latitude, by a factor of 10
# - Testing the `ease_grid` package: HMM, `ease_grid.EASE2_grid(1000)` results in pixel dimensions
#   and grid shape that are not what's specified in https://nsidc.org/ease/ease-grid-projection-gt
"""

import gc
import datetime
from pathlib import Path
import re

import requests
from shapely.geometry import box
import geopandas as gpd

import pandas as pd
import xarray as xr
from rasterio.warp import Resampling
import rioxarray
from ease_lonlat import EASE2GRID, SUPPORTED_GRIDS

# from memory_profiler import profile

xr.set_options(keep_attrs=True)

EASE2G_epsg_str = "epsg:6933"


def area_of_interest(domain='WY'):
    """ Define area of interest to clip the data
    """
    if domain == 'CONUS':
        # Restrict cells to the coterminous US
        bbox = {
            'latmax' : 50,
            'latmin' : 25,
            'lonmax': -65,
            'lonmin': -125,
        }
    elif domain == 'NAmer':
        # Restrict cells to North America (US and Canada, including Alaska)
        # For now it's not including much of Alaska
        bbox = {
            'latmax' : 71,
            'latmin' : 25,
            'lonmax': -65,
            'lonmin': -169,
        }
    else:
        # Load CSO domain JSON
        cso_domains_url = "https://raw.githubusercontent.com/snowmodel-tools/preprocess_python/master/CSO_domains.json"
        domains_resp = requests.get(cso_domains_url)
        domains = domains_resp.json()
        # Domain bounding box and projection
        bbox = domains[domain]['Bbox']
        # bbox_delta = 0.1

    bbox_proj = 'epsg:4326'
    bbox_latlon_gs = gpd.GeoSeries(
        box(bbox['lonmin'], bbox['latmin'], bbox['lonmax'], bbox['latmax']),
        crs=bbox_proj
    )
    bbox_ease2_gs = bbox_latlon_gs.to_crs(EASE2G_epsg_str)

    return bbox_latlon_gs, bbox_ease2_gs


def ease2grid_coords():
    # Define the EASE2_G1km grid
    egrid = EASE2GRID(name='EASE2_G1km', **SUPPORTED_GRIDS['EASE2_G1km'])
    eg_easting = [egrid.x_min + (i+0.5)*egrid.res for i in range(34704)]
    # eg_northing = [egrid.y_max - (j+0.5)*egrid.res for j in range(4500)[::-1]]
    eg_northing = [egrid.y_max - (j+0.5)*egrid.res for j in range(4500)]
    return eg_easting, eg_northing

# ## Experimental data product
# 
# - This dataset has no global attributes whatsoever
# - The dimensions are not actually defined as variables. Therefore the coordinate values are unknown;
#   they're just indices
# - It has none of the additional CF information (attributes and variables) that would enable
#   automatic identification of the x & y coordinates and the projection

#@profile
def open_process(nc_fpath, eg_easting, eg_northing):
    ds = xr.open_dataset(nc_fpath, cache=False)
    ds = ds.rename_dims(dims_dict={"ease2_x": "easting", "ease2_y": "northing"})

    # TODO: easting and northing arrays could be calculated just once,
    #   with the first dataset only

    # calculate the y coordinate (cell center) for row j (base 0 from the top)
    ds.coords["northing"] = eg_northing
    ds["northing"].attrs = dict(
        axis='Y', 
        long_name='EASE2_G1km Northing', 
        standard_name='projection_y_coordinate', 
        units='m'
    )

    # lon_arr = pub_ds["easting"].values
    # exp_ds.coords["easting"] = np.append(lon_arr, lon_arr[-1] + (lon_arr[-1] - lon_arr[-2]))
    # calculate the x coordinate (cell center) for col i (base 0 from the left)
    ds.coords["easting"] = eg_easting
    ds["easting"].attrs = dict(
        axis='X', 
        long_name='EASE2_G1km Easting', 
        standard_name='projection_x_coordinate', 
        units='m'
    )

    clean_ds = (
        ds
        .rio.set_spatial_dims('easting', 'northing')
        .rio.write_crs(EASE2G_epsg_str)
    )

    # Add `grid_mapping` global and variable attributes
    grid_mapping_name = "spatial_ref"
    clean_ds.attrs['grid_mapping'] = grid_mapping_name
    clean_ds['snd'].attrs['grid_mapping'] = grid_mapping_name

    #ds.close()
    #del ds
    #gc.collect()

    return clean_ds


#@profile
def add_timecoord(ds, date_tindex):
    """Add time (date) dimension and coordinate
    """
    date_da = xr.DataArray([date_tindex], coords=[('time', [date_tindex])])
    ds["snd"] = ds.snd.expand_dims(time=date_da)
    ds["time"].attrs = dict(
        axis='T', 
        long_name='Date', 
        standard_name='time'
    )

    return ds


if __name__ == '__main__':
    base_dpath = Path("/usr/mayorgadat/workmain/aarendt/CSO/projectwork/CSNOW")
    pub_dpath = base_dpath / "sentinel1_snow_depth_data"
    exp_dpath = base_dpath / "experimental_sentinel1_snow_depth_data"

    # ## Process a set of files
    
    # output cell resolution, in degrees
    latlon_deg_res = 0.01
    
    # Number of days in median composite bins
    median_bin_days = '10D'

    domain = 'NAmer'
    bbox_latlon_gs, bbox_ease2_gs = area_of_interest(domain=domain)

    # Kernel dies on the 13th file, so I can only run 12 files for now,
    # and in some instances as few as 10
    dates = pd.date_range('2019-05-01', freq='D', periods=30)

    ds_lst = []
    first_flg = True
    for date in dates:
        fname = f"SD_{date.strftime('%Y%m%d')}.nc"
        print(f"File: {fname}")

        if first_flg:
            eg_easting, eg_northing = ease2grid_coords()
            first_flg = False
        exp_ds = open_process(exp_dpath / fname, eg_easting, eg_northing)

        # Add time coordinate from date
        exp_ds = add_timecoord(exp_ds, date)

        # Transposing is necessary for xarray to plot correctly and rioxarray
        # to handle reprojection to lat lon correctly.
        # Mainly, geographic coordinates should be Y, X rather than X, Y
        exp_ds = exp_ds.transpose('time', 'northing', 'easting')

        # Clip to area of interest
        exp_clip_ds = exp_ds.rio.clip(bbox_ease2_gs.geometry, all_touched=True).copy(deep=True)
        ds_lst.append(exp_clip_ds)

        exp_ds.close()
        del exp_ds
        gc.collect()

    exp_clipconcat_ds = xr.concat(ds_lst, dim="time")

    #del exp_clipconcat_ds
    #gc.collect()


    # **NOTES:**
    # - Variable attributes for `time` dropped out. 
    #   `xr.set_options(keep_attrs=True)` did not fix this.
    #   That's not an issue of the encoding, is it?

    # ## Export to netcdf

    # Error:
    # ```python
    # exp_clipconcat_ds.to_netcdf(path=exp_dpath / "Experimental_WY_2019May01to12.nc")
    #
    # ValueError: failed to prevent overwriting existing key grid_mapping in attrs.
    # This is probably an encoding field used by xarray to describe how a variable is serialized.
    # To proceed, remove this key from the variable's attributes manually.
    # ```
    del exp_clipconcat_ds.snd.attrs['grid_mapping']
    exp_clipconcat_ds.to_netcdf(
        path=exp_dpath / f"Experimental_{domain}_2019May.nc",
        encoding={"snd": {"dtype": "float32", "zlib": True}}
    )

    # ## Bin in time
    exp_cc_median_ds = exp_clipconcat_ds.resample(time=median_bin_days).median('time')
    del ds_lst, exp_clipconcat_ds
    gc.collect()

    # ## Reproject to lat lon
    
    x_name = 'longitude'
    y_name = 'latitude'
    
    # resampling=Resampling.bilinear
    exp_cc_median_ll_ds = (
        exp_cc_median_ds
        #.transpose('time', 'northing', 'easting')
        .rio.reproject(
            bbox_latlon_gs.crs,
            resolution=latlon_deg_res,
            resampling=Resampling.nearest  # resampling=Resampling.nearest
        )
    )
    exp_cc_median_ll_ds = (
        exp_cc_median_ll_ds
        .rio.set_spatial_dims('x', 'y')
        .rename({"x": x_name, "y": y_name})
    )

    # del exp_cc_median_ll_ds.snd.attrs['grid_mapping']
    exp_cc_median_ll_ds.to_netcdf(
        path=exp_dpath / f"Experimental_{domain}_2019May_{median_bin_days}median_latlon.nc",
        encoding={"snd": {"dtype": "float32", "zlib": True}}
    )
