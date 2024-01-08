import cf_xarray # use cf-xarray so that we can use CF attributes
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.ticker import LatitudeFormatter, LongitudeFormatter
import nc_time_axis
import geopandas as gp
import regionmask
import numpy as np

def fig_anomaly_power_grid(cesm2, model='ssp370'):
    # Load power grid data
    power_grids = gp.read_file('data/Control__Areas.geojson')
    usa = power_grids[power_grids.COUNTRY == 'USA']
    
    # Set up figure
    # plt.rcParams.update({'font.size': 14})
    fig, ax = plt.subplots(figsize=(8,6), ncols=1, nrows=1, sharex=True, layout="constrained")
    
    data = cesm2[model+'.anomaly'].isel(member_id=0).sfcWind
    
    
    mask = regionmask.mask_geopandas(usa, data.lon, data.lat, numbers='OBJECTID', overlap=False)
    vals = mask.values
    keys = sorted(np.unique(vals[np.isfinite(vals)]))
    
    plot_options = False

    for authority in keys:
        authority_name = usa[usa.OBJECTID == authority].NAME
        # Plot mean for this species
        y = (
            data.where(mask == authority, drop=True)
            .mean(['lat', 'lon'])
        )
        # q = (
        #     data.where(mask == authority, drop=True)
        #     .quantile([0.25, 0.75], dim=['lat', 'lon'])
        # )

        # Add plot
        ax.plot(y.time, y, label=authority_name, color='k', alpha=0.2)
        # Add error bars
        # ax.fill_between(y.time.values, q.sel(quantile=0.25).values, q.sel(quantile=0.75).values, alpha=0.2, color='k')
            
    # Add zero line
    ax.hlines(0, y.time.values[0], y.time.values[-1], linestyle='--', color='k')
    ax.set_ylim([-0.7, 0.7])
    ax.set_xlim([y.time.values[0], y.time.values[-1]])
    # ax.set_title(f'Forcing Scenario: {model}')
            # Add linear trend
            # t = q.polyfit('time', deg=1, skipna=True).polyfit_coefficients.sel(degree=1)
            # ax.plot(t.time, 

#     # Increase the ticksize
#     plt.xticks(fontsize=14)
#     plt.yticks(fontsize=14)


#     # Add some labels and increase the size
    fig.supxlabel('Year', fontsize=16)
    fig.supylabel('Near Surface Wind Speed Anomaly [m/s]', fontsize=16)

#     # Add a legend
#     plt.legend(title='Forcing Scenario', fontsize=12, title_fontsize=14)

#     # Add a title
    fig.suptitle(
        f'Wind Speed Anomaly by Balancing Authority (Forcing Scenario {model})',
        fontsize=20,
    );

    # Show the plot
    plt.show()
