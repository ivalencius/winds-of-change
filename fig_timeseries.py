import cf_xarray # use cf-xarray so that we can use CF attributes
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.ticker import LatitudeFormatter, LongitudeFormatter
import nc_time_axis

def fig_timeseries(cesm2):
    # Set up figure
    fig, ax = plt.subplots(figsize=(15,8))

    for key in ['ssp126','ssp245', 'ssp370', 'ssp585']:
            # Plot mean for this species
            y = (
                cesm2[key+'.mean'].sfcWind
                .mean(['lat', 'lon', 'member_id'])
                .rolling(time=5).mean()
            )
            # Add plot
            ax.plot(y.time, y, label=key)
    # Increase the ticksize
    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)

    # Add some labels and increase the size
    plt.xlabel('Year', fontsize=16)
    plt.ylabel('Near Surface Wind Speed [m/s]', fontsize=16)

    # Add a legend
    plt.legend(title='Forcing Scenario', fontsize=12, title_fontsize=14)

    # Add a title
    plt.title(
        'Lower 48 Average NSWS (5 year rolling mean)',
        fontsize=20,
    );

    # Show the plot
    plt.show()