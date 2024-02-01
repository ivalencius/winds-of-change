import polars as pl
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# import scienceplots
# plt.style.use(["science", "nature"])

def fig_historical_validation():
    
    df = pl.read_csv('/glade/u/home/valencig/wind-trend-analysis/data/hist_comp_zeng2019stations.csv')
    df_a = df.group_by('year').agg(
        [
            # GSOD
            pl.col('GSOD_anomaly').mean().alias('gsod_a'),
            pl.col('GSOD_anomaly').quantile(0.25).alias('gsod_25'),
            pl.col('GSOD_anomaly').quantile(0.75).alias('gsod_75'),
            
            pl.col('cmip6_anomaly').mean().alias('cmip6_a'),
            pl.col('cmip6_anomaly').quantile(0.25).alias('cmip6_25'),
            pl.col('cmip6_anomaly').quantile(0.75).alias('cmip6_75'),
            
            pl.col('LENS2_cmip6_anomaly').mean().alias('lens2_cmip6_a'),
            pl.col('LENS2_cmip6_anomaly').quantile(0.25).alias('lens2_cmip6_25'),
            pl.col('LENS2_cmip6_anomaly').quantile(0.75).alias('lens2_cmip6_75'),
    
            pl.col('LENS2_smbb_anomaly').mean().alias('lens2_smbb_a'),
            pl.col('LENS2_smbb_anomaly').quantile(0.25).alias('lens2_smbb_25'),
            pl.col('LENS2_smbb_anomaly').quantile(0.75).alias('lens2_smbb_75'),
        ]
    ).sort('year')
    years = pd.to_datetime(df_a['year'].unique().sort())
    # print(df_a)
    
    fig, axes = plt.subplots(figsize=(7,7), ncols=2, nrows=2, sharex=True, sharey=True, constrained_layout=True)

    # Plot 1: observational data
    ax = axes[0, 0]
    ax.plot(years, df_a['gsod_a'], label='GSOD')
    # ax.fill_between(years, df_a['gsod_25'], df_a['gsod_75'], alpha=0.2)
    ax.set_title('GSOD')

    # Plot 2: SMBB data
    ax = axes[0, 1]
    ax.plot(years, df_a['lens2_smbb_a'], label='LENS2 SMBB', c='green')
    ax.fill_between(years, df_a['lens2_smbb_25'], df_a['lens2_smbb_75'], color='green', alpha=0.2)
    ax.set_title('LENS2 Smoothed Biomass Burning')

    # Plot 3: CMIP6 data
    ax = axes[1, 0]
    ax.plot(years, df_a['cmip6_a'], label='CMIP6', c='orange')
    ax.fill_between(years, df_a['cmip6_25'], df_a['cmip6_75'], color='orange', alpha=0.2)
    ax.set_title('CMIP6')

    # Plot 4: LEN2 CMIP6 data
    ax = axes[1,1]
    ax.plot(years, df_a['lens2_cmip6_a'], label='LENS2 CMIP6', c='red')
    ax.fill_between(years, df_a['lens2_cmip6_25'], df_a['lens2_cmip6_75'], color='red', alpha=0.2)
    ax.set_title('LENS2 CMIP6')


    # Options for all plots,
    ax.set_xlim(years[0], years[-1])
    ax.set_ylim(-0.2, 0.2)
    plt.show()