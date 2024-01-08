import cf_xarray # use cf-xarray so that we can use CF attributes
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.ticker import LatitudeFormatter, LongitudeFormatter
import nc_time_axis

year_dict = {
    'Near-term':['2021', '2040'],
    'Mid-term':['2041', '2060'],
    'Long-term':['2081', '2100']
}

def fig_climatology(cesm2, period='near-term'):
    
    years = year_dict[period]

    fig, axes = plt.subplots(figsize=(25,10), ncols=2, nrows=2, constrained_layout=True, subplot_kw={"projection":ccrs.PlateCarree()})

    for key, ax in zip(['ssp126','ssp245', 'ssp370', 'ssp585'], axes.flat):
        ds = cesm2[key+'.mean'].isel(member_id=0)
        # Plot mean for this species
        fg = ((
                ds.sfcWind
                .sel(time=slice(years[0], years[1]))
                .mean('time')
            ).plot(ax=ax, vmin=2, vmax=5, cmap='plasma', add_colorbar=False)
        )
        # cb = plt.colorbar(fg, orientation="vertical", pad=0.05, extend='both')
        # cb.set_label(label='Decade NSWS Trend', size=18, weight='bold')
        # cb.ax.tick_params(labelsize=16)
        ax.set_title(key)
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
        # Add title
        ax.set_title(key, fontsize=22)
        # Increase the ticksize
        gl.xlabel_style = {'size': 16, 'color': 'k', 'rotation':30, 'ha':'right'}
        gl.ylabel_style = {'size': 16, 'color': 'k', 'weight': 'normal'}
    # Add colorbar
    # cax = fig.add_axes([0.05, 0.05, 0.9, 0.1])
    # Clear settings from scienceplot package
    # cax.clear()
    cb = plt.colorbar(fg, ax=axes.ravel().tolist(), orientation="vertical", extend='both', fraction=0.046, pad=0.04)
    cb.set_label(label=f'Mean Windspeeds [m/s]', size=18, weight='bold')
    cb.ax.tick_params(labelsize=16)
    fig.suptitle(f'{period} climatology ({years[0]}-{years[1]})', fontsize=30)
    plt.show()