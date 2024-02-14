import cf_xarray # use cf-xarray so that we can use CF attributes
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.ticker import LatitudeFormatter, LongitudeFormatter
import nc_time_axis

def fig_50yearmax(cesm2, forecast='near'):
    # levels = 10
    
    fig, axes = plt.subplots(figsize=(20,5), ncols=2, nrows=1, constrained_layout=True, subplot_kw={"projection":ccrs.PlateCarree()})
    ds = cesm2['h1.50yrWSPDSRFMX']
    
    # Plot 1 -> mean climatology
    da = ds.mean('member_id').sel(forecast=forecast).WSPDSRFMX
    fg = da.plot.contourf(
        ax=axes[0],
        cmap='plasma',
        add_colorbar=False,
        vmin=18,
        vmax=30,
        levels=11
    )
    axes[0].set_title('Extreme 50-year gust', fontsize=22)
    # Add colorbar
    cb = plt.colorbar(fg, ax=axes[0], orientation="vertical", extend='both', fraction=0.046, pad=0.04)
    cb.set_label(label=f'Extreme 50-year Gust [m/s]', size=18, weight='bold')
    cb.ax.tick_params(labelsize=16)
    
    # Plot 2 -> model agreement (using std right now)
    da = ds.std('member_id').sel(forecast=forecast).WSPDSRFMX
    fg = da.plot(
        ax=axes[1],
        vmin=0,
        vmax=8,
        cmap='cividis',
        add_colorbar=False,
        levels=9,
    )
    axes[1].set_title('Disagreement among members', fontsize=22)
    # Add colobar
    cb = plt.colorbar(fg, ax=axes[1], orientation="vertical", extend='max', fraction=0.046, pad=0.04)
    cb.set_label(label=f'Std in 50-year Gust [m/s]', size=18, weight='bold')
    cb.ax.tick_params(labelsize=16)
    
    for ax in axes.flat:
        ax.add_feature(cfeature.STATES)
        ax.coastlines()
        gl = ax.gridlines(
            crs=ccrs.PlateCarree(), draw_labels=True,
            linewidth=1, color='k', alpha=1, linestyle='--')
        gl.right_labels = None
        gl.top_labels = None
        gl.xlines = None
        gl.ylines = None
        lon_formatter = LongitudeFormatter(zero_direction_label=True)
        lat_formatter = LatitudeFormatter()
        ax.xaxis.set_major_formatter(lon_formatter)
        ax.yaxis.set_major_formatter(lat_formatter)
        # Increase the ticksize
        gl.xlabel_style = {'size': 16, 'color': 'k', 'rotation':30, 'ha':'right'}
        gl.ylabel_style = {'size': 16, 'color': 'k', 'weight': 'normal'}
    # Add colorbar
    # cax = fig.add_axes([0.05, 0.05, 0.9, 0.1])
    # Clear settings from scienceplot package
    # cax.clear()
    # cb = plt.colorbar(fg, ax=axes.ravel().tolist(), orientation="vertical", extend='both', fraction=0.046, pad=0.04)
    #cb.set_label(label=f'Extreme 50-year Gust', size=18, weight='bold')
    #cb.ax.tick_params(labelsize=16)
    fig.suptitle(f'Extreme 50-year Gust ({forecast}-term)', fontsize=30)
    plt.show()
