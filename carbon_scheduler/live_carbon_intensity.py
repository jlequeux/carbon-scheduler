"""Get Carbin Intensity from Co2Signal API"""

import requests
from decouple import config
import json
import functools
import time

CO2SIGNAL_URL = 'https://api.co2signal.com/v1/latest'


def get_ttlhash(seconds=300):
    return time.time() // seconds
    
@functools.lru_cache
def get_carbon_intensity(req, ttlhash=None):
    url = f'{CO2SIGNAL_URL}?{req}'
    response = requests.get(url, headers={'auth-token': config('CO2SIGNAL_SECRET_KEY')})
    response.raise_for_status
    result = json.loads(response.content)
    
    if 'data' not in result:
        # will crash if lat/lon are not on land
        print(f'error in request {req} returned {result}')
        return
    
    dt = result['data']['datetime']
    value = result['data']['carbonIntensity']
    
    return {'datetime': dt, 'value': value}


def get_carbon_intensity_by_country_code(country_code):
    req = f'countryCode={country_code}'
    return get_carbon_intensity(req, get_ttlhash())


def get_carbon_intensity_by_coordinates(lat, lon):
    req = f'lon={lon:.04f}&lat={lat:.04f}'
    return get_carbon_intensity(req, get_ttlhash())