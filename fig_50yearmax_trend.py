import cf_xarray # use cf-xarray so that we can use CF attributes
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.ticker import LatitudeFormatter, LongitudeFormatter
import nc_time_axis
import numpy as np

def fig_50yearmax_trend(cesm2):

    fig, ax = plt.subplots(figsize=(15,8), ncols=1, nrows=1, constrained_layout=True)
    data = [[], [], [], []]
    for i, key in enumerate(['ssp126', 'ssp245', 'ssp370', 'ssp585']):
        ds = cesm2[key+'.50yrmax']
        for f in ['near', 'mid', 'far']:
            da = ds.isel(member_id=0).sfcWind.sel(forecast=f)
            val = da.values.flatten()
            good_val = val[np.isfinite(val)]
            data[i].append(good_val)
        # for p in [1, 2]:
        #     data[i][p] = data[i][p]-data[i][p-1]
        # for x in range(len(data[i])):
        #     data[i][x] = data[i][x][np.isfinite(data[i][x])]
    positions = np.array([0, 0.25, 0.5])

    for d in data:
        bp = ax.boxplot(
            d,
            positions=positions,
            patch_artist=True
        )
        for element in ['boxes', 'whiskers', 'fliers', 'means', 'medians', 'caps']:
            plt.setp(bp[element], color='k')
        for patch, c in zip(bp['boxes'], ['#4EF5E2', '#F5D34E', '#F54FEB']):
            patch.set(facecolor=c)
        positions += 1

    # set style for the axes
    labels = ['ssp126', 'ssp245', 'ssp370', 'ssp585']
    ax.set_xticks(np.arange(0.25, len(labels)+0.25), labels=labels)
    # Add lines to seperate projections
    ymin, ymax = ax.get_ylim()
    ymax = 25
    ax.set_ylim(ymin, ymax)
    for i in range(3):
        ax.vlines(0.75+i, ymin, ymax, color='gray', linestyle='--')

    # Increase the ticksize
    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)

    # Add some labels and increase the size
    plt.xlabel('Projection', fontsize=16)
    plt.ylabel('Exteme 50-year Gust [m/s]', fontsize=16)

    plt.title('Extreme 50-year Gust from Generalized Extreme Value Distribution', fontsize=30)
    
    plt.show()
