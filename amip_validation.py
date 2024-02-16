#!/usr/bin/env python
# coding: utf-8

# # Atmospheric Model Intercomparison Project (AMIP) Validation
# 
# To investigate global stilling, we want to force the models realistic historical SST forcing in order to force the models with the observed realization. To do this we use the `amip-hist` model runs.
# 
# __Variables analyzed__
# <!-- - `uas`: eastward wind component (usually 10 m) [$m \ s^{-1}$]
# - `vas`: northward wind component (usually 10 m) [$m \ s^{-1}$] -->
# - `sfcWind`: Near-Surface Wind Speed [$m \ s^{-1}$]
# 
# 11 models are available on Andromeda (The BC Cluster) at `/data/projects/bccg/CMIP6/amip-hist/mon/uas` and `/data/projects/bccg/CMIP6/amip-hist/mon/vas` respectively. Models are at _monthly_ resolution and aggregated _yearly_ before any trend analysis is analyzed.
# 
# __Steps to connect to BC Cluster__
# 1. Install Remote SSH and Remote X11 extensions in VScode
# 2. `ssh -Y username@andromeda.bc.edu`
# 3. Enter password
# 4. You are now in your home directory located at `~/mmfs1/data/_username_`
# <!-- 5. `module load python/3.9.0` $\leftarrow$ add to .tcshrc file -->
# 
# __To start an interactive session__:
# 
# `interactive -t [DD-hh:mm] [-N nodes) [-n tasks] [-c cpus-per-task] [-m gb] [-p partition] [-G #] [-X] [-h]`
# 
# Options:  
# - `t`: Wall Time (default is 4 hours)
# - `N`: Number of nodes (default is 1) 
# - `m`: GB of Memory per node (default is 4GB) 
# - `n`: Number of tasks per node (default is 1) 
# - `c`: Number of cpu cores per task (default is 4) 
# - `X`: Use X11 
# - `p` <partition name>: Use the partition specified (default is shared) 
# - `G` #: Specify the number of GPUs per gpu node
# - `h`: help
# 
# My default command: `interactive -m 20GB`
# 
# Useful command to monitor usage: `htop`
# 
# __Getting Conda up and Running__
# 1. `module load anaconda/2023.07-p3.11`
# 2. `conda init tcsh`
# 3. `conda create -n _envname_ python=3.11`
# 4. `conda activate _envname_`
# 
# This will create a conda environment in the `/mmfs1/data/_username_/.conda/envs/_envname_` directory. To automatically use this environment on login use add `conda activate _envname_` to your `.tcshrc` file.
# 
# __For a faster environment solver__
# 1. `conda install -n _envname_ conda-libmamba-solver`
# 2. `conda config --set solver libmamba`
# 
# __Export Environment__: `conda env export > environment.yml`
# 
# __Note__: To use `matplotlib` we must install $\LaTeX$. Jupyter Notebooks use MathJax under the hood which is why we only need to install if it using $\LaTeX$ in python.

# In[5]:


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
get_ipython().run_line_magic('matplotlib', 'inline')

xr.set_options(keep_attrs=True)
get_ipython().run_line_magic('load_ext', 'rich')
from rich import print  # pretty printing
from tqdm import tqdm  # progress bar
import warnings  # deal with warnings

# from importlib import reload

# Playing nice with CMIP6
# from xmip.preprocessing import combined_preprocessing
from xclim.ensembles import create_ensemble, ensemble_mean_std_max_min


# ## Helper Functions

# In[6]:


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
    mask = map.mask(data.cf['X'], data.cf['Y'], wrap_lon=360)
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

# ## Create the ensemble

# In[10]:


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
        dset_list.append(ds)
    n_models = len(model_names)
    # Create ensemble (takes a bit of time)
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        ensemble = xr.merge(dset_list, compat='minimal')
print(realizations_dict)
ensemble


# ## Imports GSOD stations from [Zeng et. al (2019).](https://www.nature.com/articles/s41558-019-0622-6)

# In[ ]:


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
gsod


# In[9]:


get_ipython().system('sacct -e')


# In[ ]:


ensemble.isel(realization=0).mean(['time'])


# In[ ]:


foo = mask_data(ensemble.isel(realization=0).mean(['time']), regionmask.defined_regions.natural_earth_v5_0_0.countries_110, ['canada'], drop=True)
foo


# ## Plots by region

# In[ ]:


land_region = regionmask.defined_regions.natural_earth_v5_0_0.land_110  # Land has value 0

fig = plt.figure(figsize=(14, int(n_models*3)), constrained_layout=True)
gs = fig.add_gridspec(n_models, 2, width_ratios=[3, 1])

for i, k in enumerate(tqdm(model_names, desc='Plotting models')):


# ## Land vs. Ocean

# In[ ]:


land_region = regionmask.defined_regions.natural_earth_v5_0_0.land_110  # Land has value 0

fig = plt.figure(figsize=(14, int(n_models*3)), constrained_layout=True)
gs = fig.add_gridspec(n_models, 2, width_ratios=[3, 1])

for i, k in enumerate(tqdm(model_names, desc='Plotting models')):
    # Name of model
    # name = k.split('.')[2]
    name=k
    # map axis
    map = fig.add_subplot(gs[i, 0], projection=ccrs.Mollweide())
    # timeseries axis
    ts = fig.add_subplot(gs[i, 1])
    # Get member
    ds = dset_dict[k]  # rename to work with xclim ensembles
    ds = ds.cf.sel(T=slice('1978', None))  # 1978-2014
    # Reduce the dataset
    da = ensemble_mean_std_max_min(ds)
    # Extract eastward wind
    sfcWind = da['sfcWind_mean']
    # Plot map
    trend = (
        sfcWind.cf.groupby('T.year').mean()
        .polyfit('year', deg=1, skipna=True)
        .polyfit_coefficients.sel(degree=1)*10  # decadal
    )
    im = trend.plot(ax=map, vmin=-0.2, vmax=0.2, cmap='coolwarm', transform=ccrs.PlateCarree(), add_colorbar=False)
    cb = plt.colorbar(im, orientation="vertical", pad=0.15)
    cb.set_label(label='Decadal Trend [m/s]')
    # Mask data
    land_mask = land_region.mask(sfcWind.cf['X'], sfcWind.cf['Y'])
    land = sfcWind.where(land_mask == 0)
    ocean = sfcWind.where(land_mask != 0)
    # Plot time series (normalize data to compare)
    l_ts = land.cf.resample(T='1Y').mean().mean(['lat','lon'])
    ((l_ts-l_ts.min())/(l_ts.max()-l_ts.min())).plot(label='land')
    o_ts = ocean.cf.resample(T='1Y').mean().mean(['lat','lon'])
    ((o_ts-o_ts.min())/(o_ts.max()-o_ts.min())).plot(label='ocean')
    # Map plot options
    map.coastlines()
    map.set_title(name)
    # Time series plot options
    ts.set_title(name)
    ts.set_xlabel('')
    ts.legend(loc='upper right')
    break

plt.show()

