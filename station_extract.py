"""
Code to download model outputs for Global Summary of the Day (GSOD) stations provided in supplementary 1 by Zeng et. al (2019).
Shen et. al (2022) proved that the CESM2 model was the only model to effectively capture NSWS stilling and resurgence.
This code aims to investigate that trend the point rather than regional scale. 

Author: Ilan Valencius
Email: valencig@bc.edu
"""

import logging
import sys
import numpy as np
import pandas as pd
import xarray as xr
import intake
import dask
from dask_jobqueue import PBSCluster
from dask.distributed import Client, wait

def create_client(num_jobs=5):
    """Creates a dask client on NCAR servers.

    Args:
        num_jobs (int): number of dask workers, defaults to 5.

    Returns:
        Dask client.
    """
    logging.info('Creating cluster')
    # Create our NCAR Cluster - which uses PBSCluster under the hood
    cluster = PBSCluster(
        job_name='valencig-dask-hpc',
        cores=1,
        memory='15GiB',
        processes=1,
        local_directory='/glade/u/home/valencig/spilled/',
        log_directory='/glade/u/home/valencig/worker-logs/',
        resource_spec='select=1:ncpus=1:mem=15GB',
        queue='casper',
        walltime='00:30:00', # Change wall time if needed
        interface='ext',
        # silence_logs=log_level
    )
    # Assign the cluster to our Client
    client = Client(cluster)
    # Spin up workers
    cluster.scale(jobs=num_jobs)
    # Block progress until workers have spawned
    logging.info('Waiting for workers to spawn')
    client.wait_for_workers(num_jobs)
    print(f'Dask dashboard URL: {client.dashboard_link}')
    return client
    

def import_stations(filepath='/glade/u/home/valencig/wind-trend-analysis/data/Zeng-2019/41558_2019_622_MOESM2_ESM.xlsx'):
    """Imports GSOD stations from Zeng et. al (2019).

    Args:
        filepath (str): filepath to supplementary data 1 from Zeng et. al (2019).

    Returns:
        lats (np.array): lat coords of stations.
        lons (np.array): lon coords of stations.
        station_nums (np.array): station row indexes.
        winds (np.array): 2d array of size [station_nums, years].
    """
    # GSOD data from Zeng (2019) -> not using HADISD
    data = pd.ExcelFile(filepath)
    # See sheet names
    stations = data.parse('stations')
    lats = stations['lats']
    lons = stations['lons']  # don't want negative degrees
    station_nums = stations['stations']
    winds = data.parse('winds')
    return lats, lons, station_nums, winds


def cesm2_cmip6(lats, lons):
    """Extracts CESM2 CMIP6 model run.

    Note: Only the first member_id is selected.

    Args: 
        lats (np.array): lat coords of stations.
        lons (np.array): lon coords of stations.

    Returns:
        wind (np.array): array of mean yearly NSWS at each sample site.
        anoms (np.array): array of mean yearly NSWS anomaly at each sample site.
    """
    logging.info('Opening CMIP6 datastore')
    cat = intake.open_esm_datastore('/glade/collections/cmip/catalog/intake-esm-datastore/catalogs/glade-cmip6.json')
    cesm2 = cat.search(
        variable_id='sfcWind', # near surface wind
        source_id='CESM2',
        experiment_id='historical', # all historical forcings
        table_id='day', # day is highest resolution
    )
    logging.info(f'CESM2 CMIP6 key: {cesm2.keys()[0]}')
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        logging.info('CMIP6 Compute Step')
        reduced = (
            cesm2
            .to_dask()  # Load dataset
            .isel(dcpp_init_year=0)  # Only one initalization year so drop it
            .isel(member_id=0)
            .sel(time=slice('1978', '2014'))  # Clip to valid range
            .sel(lat=xr.DataArray(lats, dims='z'), lon=xr.DataArray(lons, dims='z'), method='nearest')
        ).persist().compute()
        wait(reduced)
    wind = reduced.sfcWind.resample(time='1Y').mean('time').values.T
    a = (reduced-reduced.mean('time')) # value - mean of value over time
    anoms = a.sfcWind.resample(time='1Y').mean('time').values.T
    return wind, anoms


def cesm2_lens2(lats, lons, forcing_variant='cmip6'):
    """Extracts CESM2 model runs from Large Ensemble 2 (LENS2).

    Forcing variant of cmip6 or smbb (smoothed biomass burning).

    Args: 
        lats (np.array): lat coords of stations.
        lons (np.array): lon coords of stations.
        forcing_variant (str): forcing variant, either 'cmip6' or 'smbb'. Defaults to 'cmip6'.

    Returns:
        wind (np.array): array of mean yearly NSWS at each sample site.
        anoms (np.array): array of mean yearly NSWS anomaly at each sample site.
    """
    # 'cesm.json' is copy of '/glade/collections/cmip/catalog/intake-esm-datastore/catalogs/glade-cesm2-le.json'
    # Comment out "options": null in aggregation_controls.aggregations.0 in order to get intake-esm to work
    logging.info('Opening LENS2 datastore')
    cat = intake.open_esm_datastore('cesm2.json', read_csv_kwargs={'low_memory': False})
    lens2 = cat.search(
        long_name='Horizontal total wind speed average at the surface',  # maximum daily near surface wind speed
        experiment='historical',  # future projection, alternative is 'historical
        frequency='day_1',
        forcing_variant=forcing_variant  # Smoothed biomass burning
    )
    logging.info(f'CESM2 LEN2 key: {lens2.keys()[0]}')
    df = cat[lens2.keys()[0]].df
    # Trim down files to load to speed up execution time
    recent = df[df['end_time'] > '1978-01-01']
    filepaths = recent['path']
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        logging.info('Loading LENS2 Files')
        ds = xr.open_mfdataset(
            list(filepaths),
            data_vars=['WSPDSRFAV'],
            coords='minimal',
            compat='override',
            parallel=True
        )
        logging.info('LENS2 Compute Step')
        # Extract variable of interest
        reduced = (
            ds  # Select variable of interest
            .sel(time=slice('1978', '2014'))  # Clip to valid range
            .sel(lat=xr.DataArray(lats, dims='z'), lon=xr.DataArray(lons, dims='z'), method='nearest')
        ).persist().compute()
        wait(reduced)
    wind = reduced.WSPDSRFAV.resample(time='1Y').mean('time').values.T
    a = (reduced-reduced.mean('time')) # value - mean of value over time
    anoms = a.WSPDSRFAV.resample(time='1Y').mean('time').values.T
    return wind, anoms


def station_data(filename='test.csv'):
    """Extract NSWS information at each site.

    Args: 
        filename (str): filepath to save data too. Defaults to 'test.csv'.

    Returns:
       None
    """
    logging.info('Importing stations')
    # Read in the station files
    lats, lons, station_nums, winds = import_stations()
    assert np.shape(lats) == np.shape(lons), 'Dimension mismatch (lat, lon)'
    assert np.shape(lats) == np.shape(station_nums), 'Dimension mismatch (lat, nums)'
    logging.info(f'There are {len(station_nums)} stations')

    # Create format
    ds = xr.Dataset(
            data_vars={
                'GSOD': (["station", "year"], winds.to_numpy()[:, 1:-3])
            },
            coords={
                'station': station_nums,
                'year': pd.date_range(start='1978', end='2015', freq='Y'), # [1978, 2014]
                'lon': ("station", lons),
                'lat': ("station", lons)
            }
    )
    logging.info('Dataset created')
    logging.debug(f'GSOD data shape: {np.shape(ds.GSOD.values)}')

    # Add gsod anomaly
    gsod_anom = (ds - ds.mean('year')).GSOD
    ds['GSOD_anomaly'] = gsod_anom
    
    # Import CESM2 CMIP6 data
    w, a = cesm2_cmip6(ds.lat.values, ds.lon.values)
    logging.debug(f'CMIP6 data shape: {np.shape(w)}')
    assert np.shape(w) == np.shape(ds.GSOD.values)
    ds['cmip6_wind'] = (('station', 'year'), w)
    ds['cmip6_anomaly'] = (('station', 'year'), a)
    
    # Import CESM2 LEN2 data (CMIP6 and SMBB forcing)
    for v in ['cmip6', 'smbb']:
        w, a  = cesm2_lens2(ds.lat.values, ds.lon.values, forcing_variant=v)
        logging.debug(f'LENS2 wind data shape: {np.shape(w)}')
        logging.debug(f'LENS2 anomaly data shape: {np.shape(a)}')
        assert np.shape(w) == np.shape(ds.GSOD.values)
        assert np.shape(a) == np.shape(ds.GSOD.values)
        ds[f'LENS2_{v}_wind'] = (('station', 'year'), w)
        ds[f'LENS2_{v}_anomaly'] = (('station', 'year'), a)
    
    # Save data
    logging.info(f'Writing to file {filename}')
    ds.to_dataframe().to_csv(filename)
    return None


if __name__ == "__main__":
    # Set up the logger
    logger = logging.getLogger()
    logging.basicConfig(
        filename='/glade/u/home/valencig/wind-trend-analysis/logs/station_extract.log',
        filemode='w',
        level=logging.INFO,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    client = create_client(5)
    station_data(filename='/glade/u/home/valencig/wind-trend-analysis/data/hist_comp_zeng2019stations.csv')
    print('Success!')
    client.shutdown()