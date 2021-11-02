"""Alternative to get carbon data directly"""

from loguru import logger
import requests
import pandas as pd
import zipfile
import pathlib
import datetime
import os

import paths

CARBON_ARCHIVE_FILENAME = 'carbon_data.zip'
STATIC_URLS = {'FR': 'https://www.data.gouv.fr/fr/datasets/r/1ae6c731-991f-4441-9663-adc99005fac5'}

def is_fresh(file, freshness):
    if not os.path.exists(file):
        return False
    st = os.stat(file)
    modification_date = datetime.datetime.fromtimestamp(st.st_mtime)
    difference = datetime.datetime.now() - modification_date
    return difference < pd.to_timedelta(freshness)


def get_carbon_data_archive_path(country):
    """Return archive file path, create directory if not exists"""
    folder = pathlib.Path(f'{paths.DATA_FOLDER}/{country}')
    folder.mkdir(parents=True, exist_ok=True)
    return f'{folder}/{CARBON_ARCHIVE_FILENAME}'


def get_latest_file(country):
    """Download last available data"""
    response = requests.get(STATIC_URLS[country])
    response.raise_for_status()
    
    path = get_carbon_data_archive_path(country)
    with open(path, 'wb') as file:
        file.write(response.content)
    logger.info(f'file {path} saved')
    return


def refresh_file(country, freshness='3h'):
    if not is_fresh(get_carbon_data_archive_path(country), freshness):
        get_latest_file(country)
        
    
def get_carbon_data_france():
    """Open file, returns dataframe"""
    filename = 'eCO2mix_RTE_En-cours-TR.xls'
    useful_columns = {'Consommation': 'comsumption', 'Taux de Co2': 'carbon'}
    with zipfile.ZipFile(get_carbon_data_archive_path('FR')) as file:
        df = pd.read_csv(file.open(filename), encoding='ISO-8859-1', sep='\t', index_col=False)
        
    df['datetime'] = pd.to_datetime(df['Date'] + 'T' + df['Heures'])
    df = df.set_index('datetime')
    df['Prévision J-1'] = pd.to_numeric(df['Prévision J-1'], errors='coerce')
    df = df[list(useful_columns.keys())]
    df = df.rename(columns=useful_columns)
    df = df.dropna()
    return df


def get_current_carbon_impact(country, freshness='3h'):
    """Returns a tupple (value, date)
    corresponding to the last known carbon impact value and its date"""
    refresh_file(country, freshness)
    data = get_carbon_data(country)
    
    value = data.iloc[-1]['carbon']
    date = data.iloc[-1].name
    logger.info(f'Carbon impact is {value} on {date:%Y-%d-%m %H:%M} in {country}')
    return {'datetime': date, 'value': value}


def carbon_impact_threshold(country, period='1w', quantile=0.2):
    """Returns quantile of carbon impact over a period"""
    refresh_file(country)
    df = get_carbon_data(country)
    period_start = df.iloc[-1].name - pd.to_timedelta(period)
    df = df.truncate(before=period_start)
    return df['carbon'].quantile(quantile)


GET_CARBON_DATA_FUNCTIONS = {'FR': get_carbon_data_france}

def get_carbon_data(country):
    return GET_CARBON_DATA_FUNCTIONS[country]()