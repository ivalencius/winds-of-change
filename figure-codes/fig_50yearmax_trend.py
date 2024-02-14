import cf_xarray # use cf-xarray so that we can use CF attributes
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.ticker import LatitudeFormatter, LongitudeFormatter
import nc_time_axis
import numpy as np

def fig_50yearmax_trend(cesm2):

    fig, ax = plt.subplots(figsize=(7,5), ncols=1, nrows=1, constrained_layout=True)
    ds = cesm2['h1.50yrWSPDSRFMX']
    da = ds.mean({'lat', 'lon'}).load()
    x_axis = ['near', 'mid', 'far']
    
    for i in range(len(da.member_id)):
        y = da.isel(member_id=i).WSPDSRFMX.values
        # Standardize y by first value
        y = (y-y[0])/y[0]
        plt.plot(
            x_axis,
            y,
            # color='green',
            alpha=0.5,
            marker='o',
            markersize=3
        )
    # Add zero line
    ax.hlines(0, 'near', 'far', linestyle='--', color='k')
    # Increase the ticksize
    plt.xticks(ticks=x_axis, labels=['Near-term\n(2021-2040)', 'Mid-term\n(2041-2060)', 'Far-term\n(2081-2100)'],fontsize=14)
    plt.yticks(fontsize=14)

    # Add some labels and increase the size
    ax.set_xlabel('Projection', fontsize=16)
    ax.set_ylabel('Exteme 50-year Gust [\% change from near-term]', fontsize=12)

    plt.title('Extreme 50-year Gust from Generalized Extreme Value Distribution', fontsize=22)
    
    plt.show()
