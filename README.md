# validation

This repo is used to stage scripts aimed at validating NASA Community Snow Observations data against other reanalysis products.

## SNODAS

Useful resources:
- [Snow Data Assimilation System (SNODAS) Data Products at NSIDC, Version 1](https://nsidc.org/data/g02158)
- http://software.openwaterfoundation.org/cdss-app-snodas-tools-doc-user/, https://github.com/OpenWaterFoundation/cdss-app-snodas-tools
- https://gdal.org/drivers/raster/snodas.html
- http://snodas.cdss.state.co.us/app/index.html
- [Lv, Z., Pomeroy, J. W., & Fang, X. ( 2019). Evaluation of SNODAS snow water equivalent in western Canada and assimilation into a Cold Region Hydrological Model. Water Resources Research, 55, 11166– 11187. https://doi.org/10.1029/2019WR025333](https://doi.org/10.1029/2019WR025333)

## Setup

1. Set up virtual environment
  ```
  conda env create -f environment.yml
  ```

2. Activate virtual environment
  ```
  source activate validation
  ```

3. Setup validation package
  ```
  pip install -e .
  ```
