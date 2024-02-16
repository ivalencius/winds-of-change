#!/usr/bin/env python
# coding: utf-8

import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.ticker import LatitudeFormatter, LongitudeFormatter
import nc_time_axis
import numpy as np
import polars as pl
import pandas as pd
import xarray as xr
import cf_xarray as cfxr
import regionmask
from glob import glob
import os
import dask
# Cannot use scienceplots because of latex issue on BC cluster
# import scienceplots
# plt.style.use(["science", "nature"])

xr.set_options(keep_attrs=True)
from rich import print  # pretty printing
from tqdm import tqdm  # progress bar
import warnings  # deal with warnings

# from importlib import reload

# Playing nice with CMIP6
# from xmip.preprocessing import combined_preprocessing
from xclim.ensembles import create_ensemble, ensemble_mean_std_max_min


# ## Helper Functions

def mask_data(data, map, regions: list, drop=False):
    """Mask xarray data based on region names

    Args:
        data (xarray dataset): xarray dataset to mask
        map (regionmask): regionmask object
        regions (list): list of region names to mask
        drop (bool, optional): Whether to drop when masking. Defaults to False.

    Returns:
        xarray dataset: masked dataset
    """
    # Coercer region names to upper
    regions = [region.upper() for region in regions]
    # Load the region mask
    mask = map.mask(data.cf['X'], data.cf['Y'])
    # Extract keys for the region
    id_dict = map.region_ids
    # Good region names
    names = [name for name in id_dict.keys() if str(name).upper() in regions]
    assert len(names) == len(regions), 'Not enough regions found'
    # Get the key for the regions
    keys = [id_dict[name] for name in names]
    # Apply the mask to the data
    masked_data = data.where(mask.isin(keys), drop=drop)
    return masked_data


# ## CMIP vs AMIP
# - Using AMIP models (prescribed SSTs) to check against observational data
# - Observation is only realization of the state, so use the prescribed SST to capture that single state.
# - Pull in 7 amip-hist datasets, download and analyze
# - Model is fundamentally flawed if AMIP doesn't capture multi-decadal trend

def main():
    # Get all model names
    model_folders = glob('/data/projects/bccg/CMIP6/amip-hist/mon/sfcWind/*')
    model_names = [f.split('/')[-1] for f in model_folders]
    print('Model names:')
    print(model_names)
    # Create list to hold datasets
    dset_list = []
    # Create dictionary to display indices of models
    realizations_dict = dict()
    with warnings.catch_warnings():  # suppress warnings from xarray bookeeping
        warnings.simplefilter("ignore")
        for model in tqdm(model_names, desc='Loading models'):
            # Sort to make sure realization indices increasing
            paths = sorted(glob(f'/data/projects/bccg/CMIP6/amip-hist/mon/sfcWind/{model}/*'))
            # Get realization numbers
            realizations = [f.split('_')[-3] for f in paths]
            realizations_dict[model] = sorted(list(set(realizations)))  # eliminate duplicates
            # Create ensembles
            ens = create_ensemble(paths, realizations=realizations)
            # Combine realizations
            keep_vars = ['sfcWind_mean', 'sfcWind_stdev', 'sfcWind_max', 'sfcWind_min']
            reduced = ensemble_mean_std_max_min(ens)[keep_vars]
            # Convert the calendar to standard to merge across models
            calendar_corrected = reduced.convert_calendar('standard', use_cftime=True)
            # Filter to correct time range (1978-2014)
            sliced = calendar_corrected.sel(time=slice('1978', '2015'))
            # Add model (realization as axis to play nice with xclim)
            ds = sliced.assign_coords(realization=model)
            # Append to list to merge
            dset_list.append(ds.chunk())
        n_models = len(model_names)
        # Create ensemble (takes a bit of time)
        with dask.config.set(**{'array.slicing.split_large_chunks': True}):
            ensemble = xr.combine_by_coords(dset_list, coords='minimal', data_vars='minimal')
    print(realizations_dict)
    print(ensemble)
    return 0


    # Imports GSOD stations from [Zeng et. al (2019).](https://www.nature.com/articles/s41558-019-0622-6

    obs_path = 'data/Zeng-2019/41558_2019_622_MOESM2_ESM.xlsx'
    # GSOD data from Zeng (2019) -> not using HADISD
    excel = pd.ExcelFile(obs_path)
    # See sheet names
    stations = excel.parse('stations')
    obs_lats = stations['lats']
    obs_lons = stations['lons']  # don't want negative degrees
    station_nums = stations['stations']
    obs_winds = excel.parse('winds')

    gsod = xr.Dataset(
                data_vars={
                    'GSOD': (["station", "year"], obs_winds.to_numpy()[:, 1:-3])
                },
                coords={
                    'station': station_nums,
                    'year': pd.date_range(start='1978', end='2015', freq='Y'), # [1978, 2014]
                    'lon': ("station", obs_lons),
                    'lat': ("station", obs_lons)
                }
        )
    print(gsod)
    
    # Get anomaly across all realizations
    ensemble_anom = ensemble.cf - ensemble.cf.mean('T')
    # Mask all data to land
    land = mask_data(ensemble_anom, regionmask.defined_regions.natural_earth_v5_0_0.land_110, ['land'], drop=True)
    
    fig = plt.figure(figsize=(14, 6), constrained_layout=True)
    gs = fig.add_gridspec(1, 2, width_ratios=[3, 1])
    map = fig.add_subplot(gs[0], projection=ccrs.Mollweide())
    ts = fig.add_subplot(gs[1])
    # Plot map
    trend = (
        land.cf.groupby('T.year').mean()
        .polyfit('year', deg=1, skipna=True)
        .polyfit_coefficients.sel(degree=1)*10  # decadal
    )
    trend.mean('realization').plot(ax=map, vmin=-0.2, vmax=0.2, cmap='coolwarm', transform=ccrs.PlateCarree())
    # Plot timeseries
    # reduce across realization
    # land_mean = land
    land.sfcWind_mean.plot(ax=ts, hue='realization')
    # ts.fill_between(
    #     land.time.values,
    #     land.sfcWind_mean.sel(percentiles=15),
    #     land.sfcWind_mean.sel(percentiles=85),
    #     alpha=0.5,
    #     label="Perc. 15-85",
    # )
    plt.legend()
    ts.set_ylabel('Wind Speed Anomaly [m/s]')
    plt.savefig('realization_trend.png')
    
    # land_region = regionmask.defined_regions.natural_earth_v5_0_0.land_110  # Land has value 0

    # fig = plt.figure(figsize=(14, int(n_models*3)), constrained_layout=True)
    # gs = fig.add_gridspec(n_models, 2, width_ratios=[3, 1])

    # for i, model in enumerate(tqdm(model_names, desc='Plotting models')):
    #     # map axis
    #     map = fig.add_subplot(gs[i, 0], projection=ccrs.Mollweide())
    #     # timeseries axis
    #     ts = fig.add_subplot(gs[i, 1])
    #     # Get member
    #     ds = ensemble.sel(realization=model)
    #     ds = ds.cf.sel(T=slice('1978', '2010'))  # 1978-2010
    #     # Reduce the dataset
    #     da = ensemble_mean_std_max_min(ds)
    #     # Extract eastward wind
    #     sfcWind = da['sfcWind_mean']
    #     # Plot map
    #     trend = (
    #         sfcWind.cf.groupby('T.year').mean()
    #         .polyfit('year', deg=1, skipna=True)
    #         .polyfit_coefficients.sel(degree=1)*10  # decadal
    #     )
    #     im = trend.plot(ax=map, vmin=-0.2, vmax=0.2, cmap='coolwarm', transform=ccrs.PlateCarree(), add_colorbar=False)
    #     cb = plt.colorbar(im, orientation="vertical", pad=0.15)
    #     cb.set_label(label='Decadal Trend [m/s]')
    #     # Mask data
    #     land_mask = land_region.mask(sfcWind.cf['X'], sfcWind.cf['Y'])
    #     land = sfcWind.where(land_mask == 0)
    #     ocean = sfcWind.where(land_mask != 0)
    #     # Plot time series (normalize data to compare)
    #     l_ts = land.cf.resample(T='1Y').mean().mean(['lat','lon'])
    #     ((l_ts-l_ts.min())/(l_ts.max()-l_ts.min())).plot(label='land')
    #     o_ts = ocean.cf.resample(T='1Y').mean().mean(['lat','lon'])
    #     ((o_ts-o_ts.min())/(o_ts.max()-o_ts.min())).plot(label='ocean')
    #     # Map plot options
    #     map.coastlines()
    #     map.set_title(name)
    #     # Time series plot options
    #     ts.set_title(name)
    #     ts.set_xlabel('')
    #     ts.legend(loc='upper right')
    #     break

    # plt.savefig('test.png')

main()