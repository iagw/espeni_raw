# readme
# code to create parse ESPENI dataset from publicly available Elexon and National Grid data
# to be used once Elexon and National Grid files have been manually downloaded from:
# www.elexonportal.co.uk/fuelhh
# https://demandforecast.nationalgrid.com/efs_demand_forecast/faces/DataExplorer
# elexon_data is saved to elexon_manual folder path on local machine
# national grid data is saved to national grid folder path on local machine
# masterlocaltime.csv from https://zenodo.org/record/3887182 needs to be downloaded and saved
# in 'folderpath' folder


import os.path
import numpy as np
import datetime as dt
import pandas as pd
import glob
import time

from pandas import DataFrame

start_time = time.time()
downloaddate = dt.datetime.now().strftime("%Y-%m-%d")
suffix = 'ELEXM'
yearstr = time.strftime('%Y')
open_toggle = 1

# folders
home_folder = os.getenv('HOME')
folderpath = f'{home_folder}/PycharmProjects/shared_sandbox/create_espeni_folder_and_files'
elexon = f'{folderpath}/elexon_manual/elexon_data/'
ngembedrawraw = f'{folderpath}/ngembed/ngembedrawraw/'
ngembedrawpar = f'{folderpath}/ngembed/ngembedrawpar/'
ngembedoutput = f'{folderpath}/ngembed/ngembedoutputrawpar/'
out = f'{home_folder}/OneDrive - University of Birmingham/elexon_ng_espeni/'


# section to parse elexon data together and add utc and localtime
#  this chooses files in a folder where the filenames contain a particular string
#  append all of these together
os.chdir(elexon)
dfelexon = pd.DataFrame([])
for counter, file in enumerate(glob.glob('*.csv')):
    namedf = pd.read_csv(file, skiprows=0, header=0, encoding='Utf-8')
    dfelexon = dfelexon.append(namedf, sort=True)

# uppercase for all columns and strip whitespace
dfelexon = dfelexon.rename(columns=lambda x: x.upper().strip().replace('#', '').replace(' ', '_'))
dfelexon['SETTLEMENT_PERIOD'] = dfelexon['SETTLEMENT_PERIOD'].astype(str).str.zfill(2)
# SDSP = SETTLEMENT_DATE, SETTLEMENT_PERIOD: used as a check for duplicates and merging
dfelexon['SDSP_RAW'] = dfelexon['SETTLEMENT_DATE'] + '_' + dfelexon['SETTLEMENT_PERIOD']
dfelexon.duplicated(subset=['SDSP_RAW'], keep='last').sum()
dfelexon.sort_values(by=['SDSP_RAW'], ascending=True, inplace=True)
dfelexon.reset_index(drop=True, inplace=True)

# load masterlocaltime.csv file with datetimes against date and settlement period
os.chdir(folderpath)
masterlocaltime = pd.read_csv('masterlocaltime.csv', encoding='Utf-8', dtype={'settlementperiod': str})
localtimedict = dict(zip(masterlocaltime['datesp'], masterlocaltime['localtime']))
localtimedictutc = dict(zip(masterlocaltime['datesp'], masterlocaltime['utc']))
dfelexon['localtime'] = dfelexon['SDSP_RAW'].map(localtimedict)
dfelexon['utc'] = dfelexon['SDSP_RAW'].map(localtimedictutc)
dfelexon['SETTLEMENT_DATE'] = dfelexon['SDSP_RAW'].map(lambda x: x.split('_')[0])
dfelexon['SETTLEMENT_PERIOD'] = dfelexon['SDSP_RAW'].map(lambda x: x.split('_')[1])
dfelexon['ROWFLAG'] = '1'

dfelexonlist = ['SETTLEMENT_DATE',
                'SETTLEMENT_PERIOD',
                'SDSP_RAW',
                'ROWFLAG',
                'localtime',
                'utc',
                'CCGT',
                'OIL',
                'COAL',
                'NUCLEAR',
                'WIND',
                'PS',
                'NPSHYD',
                'OCGT',
                'OTHER',
                'BIOMASS',
                'INTFR',
                'INTIRL',
                'INTNED',
                'INTEW',
                'INTNEM']

dfelexon['utc'] = pd.to_datetime(dfelexon['utc'])
dfelexon = dfelexon.set_index('utc', drop=False)

dfelexon = dfelexon[dfelexonlist]

# renames column names
nosuffix = ['SETTLEMENT_DATE', 'SETTLEMENT_PERIOD', 'localtime', 'utc', 'ROWFLAG', 'SDSP_RAW']

for col in dfelexon.columns:
    if col in nosuffix:
        dfelexon.rename(columns={col: f'{suffix}_{col}'}, inplace=True)
    else:
        dfelexon.rename(columns={col: f'POWER_{suffix}_{col}_MW'}, inplace=True)


# section to parse national grid data together and add utc and localtime
os.chdir(ngembedrawraw)
existing_downloaded_rawraw_files = pd.DataFrame(glob.glob('*.csv'))
os.chdir(ngembedrawpar)
existing_parsed_raw_files = pd.DataFrame(glob.glob('*.csv'))
if existing_parsed_raw_files.empty:
    csvs_to_parse = existing_downloaded_rawraw_files[0]
else:
    existing_parsed_raw_files[0] = existing_parsed_raw_files[0].map(lambda x: x.split('_rawpar.csv')[0] + '_rawraw.csv')
    csvs_to_parse = existing_downloaded_rawraw_files[0][~existing_downloaded_rawraw_files[0].isin
                                                        (existing_parsed_raw_files[0])]


for fname in csvs_to_parse:
    os.chdir(ngembedrawraw)
    # then reads in the demandupdate file (the one that changes on a daily basis)
    df = pd.read_csv(fname, encoding='Utf-8')
    if 'FORECAST_ACTUAL_INDICATOR' in df.columns:
        df = df[df['FORECAST_ACTUAL_INDICATOR'] != 'F']
        df = df.drop('FORECAST_ACTUAL_INDICATOR', axis=1)

    # uppercase for all columns and strip whitespace
    df = df.rename(columns=lambda x: x.upper().strip())

    # get date from SETTLEMENT_DATE column
    # date format starts with lowercase month in format 01-Apr-2005
    # but changes to uppercase month text in recent years 01-APR-2015
    # therefore change everything to uppercase and change to datetime to standardise
    df['SETTLEMENT_DATE'] = pd.to_datetime(df['SETTLEMENT_DATE'].squeeze().str.upper().tolist(), format='%d-%b-%Y')

    # SETTLEMENT_DATE_TEXT and SETTLEMENT_PERIOD_TEXT created
    df['SETTLEMENT_DATE'] = df['SETTLEMENT_DATE'].dt.strftime('%Y-%m-%d')
    df['SETTLEMENT_PERIOD'] = df['SETTLEMENT_PERIOD'].astype(str).str.zfill(2)

    # SDSP = settlement date, settlement period: used as a check for duplicates etc.
    df['SDSP_RAW'] = df['SETTLEMENT_DATE'].astype(str) + '_' + df['SETTLEMENT_PERIOD'].astype(str)

    # drop all rows that have a forecast value i.e. keep all rows that do not have 'F'
    # drop FORECAST_ACTUAL_INDICATOR as column is no longer needed

    df.sort_values(['SETTLEMENT_DATE', 'SETTLEMENT_PERIOD'], ascending=[True, True], inplace=True)
    df.drop_duplicates(subset=['SDSP_RAW'], keep='first', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # next line makes a list? of all columns that are NOT in the list of 'SETTLEMENT_DATE' etc.
    nosuffix = ['SETTLEMENT_DATE', 'SETTLEMENT_PERIOD', 'FORECAST_ACTUAL_INDICATOR', 'SDSP_RAW']
    for col in df.columns:
        if col in nosuffix:
            df.rename(columns={col: f'{suffix}_{col}'}, inplace=True)
        else:
            df.rename(columns={col: f'POWER_{suffix}_{col}_MW'}, inplace=True)

    os.chdir(ngembedrawpar)
    fname = fname.split('_rawraw.csv')[0] + '_rawpar.csv'
    df.to_csv(fname, encoding='Utf-8', index=False)


#  loop chooses files in a folder who's names contain a particular string
#  and appends all of these together
os.chdir(ngembedrawpar)
suffix = 'NGEM'
dfng = pd.DataFrame([])
for counter, file in enumerate(glob.glob('*_rawpar.csv')):
    namedf = pd.read_csv(file, skiprows=0, encoding='utf-8')
    dfng = dfng.append(namedf, sort=True)

dfng[f'{suffix}_SETTLEMENT_PERIOD'] = dfng[f'{suffix}_SETTLEMENT_PERIOD'].astype(str).str.zfill(2)

dfng.sort_values([f'{suffix}_SDSP_RAW'], ascending=[True], inplace=True)
dfng.drop_duplicates(subset=[f'{suffix}_SDSP_RAW'], keep='last', inplace=True)

dfng.reset_index(drop=True, inplace=True)


# load masterlocaltime.csv file with datetimes against date and settlement period

os.chdir(ngembedoutput)
dfng[f'{suffix}_localtime'] = dfng[f'{suffix}_SDSP_RAW'].map(localtimedict)
dfng[f'{suffix}_utc'] = dfng[f'{suffix}_SDSP_RAW'].map(localtimedictutc)
dfng.insert(3, 'NGEM_ROWFLAG', value=1)

df = pd.merge(dfelexon, dfng, how='left', left_on=['ELEXM_SDSP_RAW'], right_on=['NGEM_SDSP_RAW'])
biomasslist = {'POWER_ELEXM_BIOMASS_MW': 'POWER_ELEXM_BIOMASS_PRECALC_MW',
               'POWER_ELEXM_OTHER_MW': 'POWER_ELEXM_OTHER_PRECALC_MW'}
df = df.rename(columns=biomasslist)

mask = df['ELEXM_utc'].astype(str).str.contains('/', regex=True)
df.loc[mask, 'ELEXM_utc'] = pd.to_datetime(df.loc[mask, 'ELEXM_utc'],
                                           format='%d/%m/%Y %H:%M').dt.strftime('%Y-%m-%d %H:%M')
df['ELEXM_utc'] = pd.to_datetime(df['ELEXM_utc'], utc=True, format='%Y-%m-%d %H:%M')

mask = df['ELEXM_SETTLEMENT_DATE'].astype(str).str.contains('/', regex=True)
df.loc[mask, 'ELEXM_SETTLEMENT_DATE'] = pd.to_datetime(df.loc[mask, 'ELEXM_SETTLEMENT_DATE'],
                                                       format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
df['ELEXM_SETTLEMENT_DATE'] = pd.to_datetime(df['ELEXM_SETTLEMENT_DATE'], utc=True,
                                             format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
# df['ELEXM_SETTLEMENT_DATE'] = df['ELEXM_SETTLEMENT_DATE'].dt.strftime('%Y-%m-%d')

mask = df['ELEXM_localtime'].astype(str).str.contains('/', regex=True)
df.loc[mask, 'ELEXM_localtime'] = pd.to_datetime(df.loc[mask, 'ELEXM_localtime'],
                                                 format='%d/%m/%Y %H:%M').dt.strftime('%Y-%m-%d %H:%M')
df['ELEXM_localtime'] = pd.to_datetime(df['ELEXM_localtime'], utc=True, format='%Y-%m-%d %H:%M')
df: DataFrame = df.set_index('ELEXM_utc', drop=False)

# elexonstartdate = '2008-11-06 00:00:00'
biomassstartdate = '2017-11-01 20:00:00+00:00'
otherfinishdate = '2017-11-01 20:30:00+00:00'

# sum of biomass since start date of OTHER split to OTHER and BIOMASS
bb = df['POWER_ELEXM_BIOMASS_PRECALC_MW'][biomassstartdate:].mean()
oo = df['POWER_ELEXM_OTHER_PRECALC_MW'][otherfinishdate:].mean()
otherb_ratio = oo/bb
oblist = ['POWER_ELEXM_OTHER_PRECALC_MW', 'POWER_ELEXM_BIOMASS_PRECALC_MW']
df['POWER_ELEXM_OTHER_POSTCALC_MW'] = \
    np.where(df.index < otherfinishdate, df[oblist].sum(axis=1).mul(otherb_ratio),
             df['POWER_ELEXM_OTHER_PRECALC_MW']).round(0)
df['POWER_ELEXM_BIOMASS_POSTCALC_MW'] = \
    np.where(df.index < biomassstartdate, df[oblist].sum(axis=1).mul(1-otherb_ratio),
             df['POWER_ELEXM_BIOMASS_PRECALC_MW']).round(0)

espenilist = ['POWER_ELEXM_CCGT_MW',
              'POWER_ELEXM_OIL_MW',
              'POWER_ELEXM_COAL_MW',
              'POWER_ELEXM_NUCLEAR_MW',
              'POWER_ELEXM_WIND_MW',
              'POWER_ELEXM_PS_MW',
              'POWER_ELEXM_NPSHYD_MW',
              'POWER_ELEXM_OCGT_MW',
              'POWER_ELEXM_OTHER_POSTCALC_MW',
              'POWER_ELEXM_BIOMASS_POSTCALC_MW',
              'POWER_NGEM_BRITNED_FLOW_MW',
              'POWER_NGEM_EAST_WEST_FLOW_MW',
              'POWER_NGEM_FRENCH_FLOW_MW',
              'POWER_NGEM_MOYLE_FLOW_MW',
              'POWER_NGEM_NEMO_FLOW_MW',
              'POWER_NGEM_EMBEDDED_SOLAR_GENERATION_MW',
              'POWER_NGEM_EMBEDDED_WIND_GENERATION_MW']

df['POWER_ESPENI_MW'] = df[espenilist].sum(axis=1)

natureespenilist = ['ELEXM_SETTLEMENT_DATE',
                    'ELEXM_SETTLEMENT_PERIOD',
                    'ELEXM_utc',
                    'ELEXM_localtime',
                    'ELEXM_ROWFLAG',
                    'NGEM_ROWFLAG',
                    'POWER_ESPENI_MW',
                    'POWER_ELEXM_CCGT_MW',
                    'POWER_ELEXM_OIL_MW',
                    'POWER_ELEXM_COAL_MW',
                    'POWER_ELEXM_NUCLEAR_MW',
                    'POWER_ELEXM_WIND_MW',
                    'POWER_ELEXM_PS_MW',
                    'POWER_ELEXM_NPSHYD_MW',
                    'POWER_ELEXM_OCGT_MW',
                    'POWER_ELEXM_OTHER_POSTCALC_MW',
                    'POWER_ELEXM_BIOMASS_POSTCALC_MW',
                    'POWER_NGEM_EMBEDDED_SOLAR_GENERATION_MW',
                    'POWER_NGEM_EMBEDDED_WIND_GENERATION_MW',
                    'POWER_NGEM_BRITNED_FLOW_MW',
                    'POWER_NGEM_EAST_WEST_FLOW_MW',
                    'POWER_NGEM_FRENCH_FLOW_MW',
                    'POWER_NGEM_MOYLE_FLOW_MW',
                    'POWER_NGEM_NEMO_FLOW_MW']

df = df[natureespenilist]
os.chdir(folderpath)
df.to_csv('espeni_raw.csv', encoding='Utf-8', index=False)
print("time elapsed: {:.2f}s".format(time.time() - start_time))
