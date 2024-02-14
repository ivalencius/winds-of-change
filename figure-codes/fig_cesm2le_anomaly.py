import cf_xarray # use cf-xarray so that we can use CF attributes
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.ticker import LatitudeFormatter, LongitudeFormatter
import nc_time_axis
import numpy as np

def fig_cesm2le_anomaly(cesm2):

    fig, ax = plt.subplots(figsize=(7,5), ncols=1, nrows=1, constrained_layout=True)
    ds = cesm2['h1.anom']
    da = ds.mean({'lat', 'lon'}).WSPDSRFAV.load()
    
    # Reduce across members
    da.mean('member_id').plot(ax=ax)
    # Get error across members
    q = da.quantile([0.25, 0.75], dim='member_id')

    # for m in da.member_id:
    #     da.sel(member_id=m).plot(ax=ax, color='k', alpha=0.1)

    ax.fill_between(da.time.values, q.sel(quantile=0.25).values, q.sel(quantile=0.75).values, alpha=0.2)

    # Add zero line
    ax.hlines(0, da.time.values[0], da.time.values[-1], linestyle='--', color='k')
    
    # Increase the ticksize
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)

    # Add some labels and increase the size
    plt.xlabel('Year', fontsize=14)
    plt.ylabel('Near Surface Wind Speed Anomaly [m/s]', fontsize=16)

    plt.title('Projected NSWS Anomalies from CESM2LE2', fontsize=22)
    
    plt.show()
