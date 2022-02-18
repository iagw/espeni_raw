# readme
# code to create parse ESPENI dataset from publicly available Elexon and National Grid data
# to be used once Elexon and National Grid files have been manually downloaded from:
# www.elexonportal.co.uk/fuelhh (needs Elexon registration to login)
# https://data.nationalgrideso.com/demand/historic-demand-data
# elexon_data is saved to elexon_manual folder path on local machine
# national grid data is saved to national grid folder path on local machine
# masterlocaltime_iso8601.parquet from github is automatically downloaded to map settlement day and periods
# to utc and localtime

import os.path
import datetime as dt
import numpy as np
import pandas as pd
import glob
import time

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

start_time = time.time()
downloaddate = dt.datetime.now().strftime("%Y-%m-%d")
yearstr = time.strftime('%Y')
open_toggle = 1

# folders
home_folder = os.getenv('HOME')
working_folder = f'{home_folder}/OneDrive - University of Birmingham/elexon_ng_espeni/'  # change to own working folder
elexon = f'{working_folder}elexon/elexon_download_data/'  # location of raw Elexon files
ngembed = f'{working_folder}ngembed/'
ngembedrawraw = f'{ngembed}ngembedrawraw/'  # location of raw national grid files
ngembedrawpar = f'{ngembed}ngembedrawpar/'  # location of parsed national grid files
out = f'{working_folder}/'  # location of output files


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

# update to read masterdatetimefile in from github rather than local, it is a parquet file rather than a csv
githubMasterDatetimeFile = 'https://raw.githubusercontent.com/iagw/masterdatetime/master/masterlocaltime_iso8601.parquet'
masterlocaltime = pd.read_parquet(githubMasterDatetimeFile)

# load masterlocaltime.csv file with datetimes against date and settlement period
os.chdir(out)
# masterlocaltime = pd.read_csv('masterlocaltime_iso8601.csv', encoding='Utf-8', dtype={'settlementperiod': str})
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
                'INTELEC',
                'INTEW',
                'INTFR',
                'INTIFA2',
                'INTIRL',
                'INTNED',
                'INTNEM',
                'INTNSL']

dfelexon = dfelexon[dfelexonlist]

# renames column names
nosuffix = ['SETTLEMENT_DATE', 'SETTLEMENT_PERIOD', 'localtime', 'utc', 'ROWFLAG', 'SDSP_RAW']
prefix = 'ELEXM'
for col in dfelexon.columns:
    if col in nosuffix:
        dfelexon.rename(columns={col: f'{prefix}_{col}'}, inplace=True)
    else:
        dfelexon.rename(columns={col: f'POWER_{prefix}_{col}_MW'}, inplace=True)


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
    prefix = 'NGEM'
    for col in df.columns:
        if col in nosuffix:
            df.rename(columns={col: f'{prefix}_{col}'}, inplace=True)
        else:
            df.rename(columns={col: f'POWER_{prefix}_{col}_MW'}, inplace=True)

    os.chdir(ngembedrawpar)
    fname = fname.split('_rawraw.csv')[0] + '_rawpar.csv'
    df.to_csv(fname, encoding='Utf-8', index=False)


#  loop chooses files in a folder where the filename contains a particular string
#  and appends all of these together
os.chdir(ngembedrawpar)
prefix = 'NGEM'
dfng = pd.DataFrame([])
for counter, file in enumerate(glob.glob('*_rawpar.csv')):
    namedf = pd.read_csv(file, skiprows=0, encoding='utf-8')
    dfng = dfng.append(namedf, sort=True)
# dfng[f'{prefix}_SETTLEMENT_PERIOD'] = dfng[f'{prefix}_SETTLEMENT_PERIOD'].astype(str).str.zfill(2)

dfng.sort_values([f'{prefix}_SDSP_RAW'], ascending=[True], inplace=True)
dfng.drop_duplicates(subset=[f'{prefix}_SDSP_RAW'], keep='last', inplace=True)
dfng.reset_index(drop=True, inplace=True)


os.chdir(out)
dfng[f'{prefix}_localtime'] = dfng[f'{prefix}_SDSP_RAW'].map(localtimedict)
dfng[f'{prefix}_utc'] = dfng[f'{prefix}_SDSP_RAW'].map(localtimedictutc)
dfng.insert(3, 'NGEM_ROWFLAG', value=1)

df = pd.merge(dfelexon, dfng, how='left', left_on=['ELEXM_SDSP_RAW'], right_on=['NGEM_SDSP_RAW'])
biomasslist = {'POWER_ELEXM_BIOMASS_MW': 'POWER_ELEXM_BIOMASS_PRECALC_MW',
               'POWER_ELEXM_OTHER_MW': 'POWER_ELEXM_OTHER_PRECALC_MW'}
df = df.rename(columns=biomasslist)

df['ELEXM_utc'] = pd.to_datetime(df['ELEXM_utc'], utc=True)
df['ELEXM_localtime'] = df['ELEXM_utc'].dt.tz_convert('Europe/London')

df = df.set_index('ELEXM_utc', drop=False)

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

# list to sum to ESPENI
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
              'POWER_NGEM_BRITNED_FLOW_MW',  # added on 2021-03-02, INTNED_FLOW changed to BRITNED_FLOW
              'POWER_NGEM_EAST_WEST_FLOW_MW',
              'POWER_NGEM_MOYLE_FLOW_MW',
              'POWER_NGEM_NEMO_FLOW_MW',
              'POWER_NGEM_IFA_FLOW_MW',  # added on 2021-03-02, FRENCH_FLOW changed to IFA_FLOW
              'POWER_NGEM_IFA2_FLOW_MW',  # added on 2021-03-02, IFA2 data started
              'POWER_NGEM_EMBEDDED_SOLAR_GENERATION_MW',
              'POWER_NGEM_EMBEDDED_WIND_GENERATION_MW']

df['POWER_ESPENI_MW'] = df[espenilist].sum(axis=1)

espenifileoutput = ['ELEXM_SETTLEMENT_DATE',
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
                    'POWER_NGEM_BRITNED_FLOW_MW',  # added on 2021-03-02, INTNED_FLOW changed to BRITNED_FLOW
                    'POWER_NGEM_EAST_WEST_FLOW_MW',
                    'POWER_NGEM_MOYLE_FLOW_MW',
                    'POWER_NGEM_NEMO_FLOW_MW',
                    'POWER_NGEM_IFA_FLOW_MW',  # added on 2021-03-02, FRENCH_FLOW changed to IFA_FLOW
                    'POWER_NGEM_IFA2_FLOW_MW',  # added on 2021-03-02, IFA2 data started
                    ]

df = df[espenifileoutput]
os.chdir(out)
df.to_csv('espeni_rawa.csv', encoding='Utf-8', index=False)
print("time elapsed: {:.2f}s".format(time.time() - start_time))
