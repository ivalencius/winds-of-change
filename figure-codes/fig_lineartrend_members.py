import cf_xarray # use cf-xarray so that we can use CF attributes
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.ticker import LatitudeFormatter, LongitudeFormatter
import nc_time_axis
import scipy.stats 
import numpy as np

def p_val(r, n,  lat, lon):
    var = np.diag(r.polyfit_covariance.sel(lat=lat, lon=lon))
    #The standard error of the parameter estimates from the variance-covariance matrix is the square root of the diagonal values
    std_err = np.sqrt(var)
    # std_err = std/np.sqrt(n)
    t = r.sel(lat=lat, lon=lon).polyfit_coefficients.values/std_err
    # Degrees of freedom = n-2 (fit two parameters)
    df = n-2
    # Only need to test for slop (degree=1)
    t = t[1]
    return scipy.stats.t.sf(abs(t), df=df)


def fig_lineartrend_members(cesm2, model='ssp370'):

    fig, axes = plt.subplots(figsize=(25,5), ncols=3, nrows=1, constrained_layout=True, subplot_kw={"projection":ccrs.PlateCarree()})
    
    data = cesm2[model+'.anomaly']
    for key, ax in zip(data.member_id.values, axes.flat):
        ds = data.sel(member_id=key)
        # Group by year
        yearly = ds.sfcWind.groupby('time.year').mean()
        # Determine trend data
        trended = yearly.polyfit('year', deg=1, skipna=True, cov=True) 
        # Plot mean for this species
        fg = (
            trended.polyfit_coefficients.sel(degree=1)*10 # Get decadal
            ).plot(ax=ax, vmin=-0.1, vmax=0.1, cmap='coolwarm', add_colorbar=False)
        
        
        # Get signifigance
        n = len(yearly.year)
        for lat in trended.lat:
            for lon in trended.lon:
                if p_val(trended, n, lat, lon) < 0.05:
                    ax.plot(lon, lat, 'ko', transform=ccrs.PlateCarree())
        
        # cb = plt.colorbar(fg, orientation="vertical", pad=0.05, extend='both')
        # cb.set_label(label='Decade NSWS Trend', size=18, weight='bold')
        # cb.ax.tick_params(labelsize=16)
        ax.set_title(f'Member ID: {key}')
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
    cb = plt.colorbar(fg, ax=axes.ravel().tolist(), orientation="vertical", extend='both', fraction=0.046, pad=0.04)
    cb.set_label(label='Decadal NSWS Trend [m s$^{-1}$ decade$^{-1}$]', size=18, weight='bold')
    cb.ax.tick_params(labelsize=16)
    fig.suptitle(f'NSWS Anomaly Trend for Model Run {model} (2014-2100)', fontsize=30)
    plt.show()
