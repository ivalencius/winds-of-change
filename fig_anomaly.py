import cf_xarray # use cf-xarray so that we can use CF attributes
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.ticker import LatitudeFormatter, LongitudeFormatter
import nc_time_axis

def fig_anomaly(cesm2):
    # Set up figure
    # plt.rcParams.update({'font.size': 14})
    fig, ax = plt.subplots(figsize=(10,6), ncols=2, nrows=2, sharex=True, layout="constrained")

    for key, ax, c in zip(['ssp126','ssp245', 'ssp370', 'ssp585'], ax.ravel(), plt.rcParams['axes.prop_cycle'].by_key()['color']):
            # Plot mean for this species
            y = (
                cesm2[key+'.anomaly'].sfcWind
                .mean(['lat', 'lon', 'member_id'])
                # .rolling(time=5).mean()
            )
            q = (
                cesm2[key+'.anomaly'].sfcWind
                .quantile([0.25, 0.75], dim=['lat', 'lon', 'member_id'])
                # .rolling(time=5).mean()
            )

            # Add plot
            ax.plot(y.time, y, label=key, color=c)
            # Add error bars
            ax.fill_between(y.time.values, q.sel(quantile=0.25).values, q.sel(quantile=0.75).values, alpha=0.2, color=c)
            # Add zero line
            ax.hlines(0, y.time.values[0], y.time.values[-1], linestyle='--', color='k')
            ax.set_ylim([-0.4, 0.3])
            ax.set_xlim([y.time.values[0], y.time.values[-1]])
            ax.set_title(f'Forcing Scenario: {key}')
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
        'Lower 48 Average NSWS Anomaly',
        fontsize=20,
    );

    # Show the plot
    plt.show()
