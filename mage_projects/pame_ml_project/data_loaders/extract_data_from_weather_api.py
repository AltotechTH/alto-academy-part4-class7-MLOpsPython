if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

import os
import requests
import re
import json
import pandas as pd
import pendulum
from datetime import datetime, timedelta
import pytz


def _retrieve_api_key():
    """
    Scrape the API key from the WU website homepage.
    """
    url = 'https://www.wunderground.com/'
    response = requests.get(url)

    text = response.text
    pattern = r'https://api.weather.com.{0,100}?apiKey=(.*?)&'
    
    # Search for matches using the pattern
    match = re.search(pattern, text)

    # Print the captured text
    if match:
        api_key = match.group(1)
        return api_key

@data_loader
def load_data(*args, **kwargs):
    """[Retrieve forecast outdoor weather a day and pack into dataframe]
    Args:
        api_key str: Weather Underground API key
        start_date str: Start date of the historical weather data  (YYYYMMDD) ex. 20231101 (2023-Nov-01)
        end_date str: End date of the historical weather data  (YYYYMMDD) ex. 20231101 (2023-Nov-01)
        location str: Location of the weather data

    Returns:
        [dataframe]: [~]
    """
    api_key = _retrieve_api_key()
    location = "VTBD:9:TH"
    now_ = pendulum.parse('2023-09-01')
    start_date = now_.format('YYYYMMDD')
    # end of the month
    end_date = now_.end_of('month').format('YYYYMMDD')
    total_df = pd.DataFrame()
    for _ in range(3):
        link = f'https://api.weather.com/v1/location/{location}/observations/historical.json?apiKey={api_key}&units=s&startDate={start_date}&endDate={end_date}'
        res = requests.get(link)
        df = pd.DataFrame(json.loads(res.text)['observations'])
        
        df['timestamp'] = df['valid_time_gmt']
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')

        # Convert Celcius to Fareheit
        df['drybulb_temperature'] = df['temp'].apply(lambda c: (c*1.8)+32)
        df['dewpoint_temperature'] = df['dewPt'].apply(lambda c: (c*1.8)+32)
        df['feels_like'] = df['feels_like'].apply(lambda c: (c*1.8)+32)

        selected_cols = ['timestamp', 'drybulb_temperature', 'dewpoint_temperature', 'feels_like', 'rh',
                        'wx_phrase', 'wdir', 'wspd', 'uv_index', 'pressure']

        df = df[selected_cols].copy()

        df = df.rename(columns={'wx_phrase': 'phrase',
                                'rh': 'humidity',
                                'wspd': 'wind_speed',
                                'wdir':  'wind_direction',
                                'clds':  'cloud_cover',
                                'pressure': 'msl_pressure'})
        # concat to total_df
        total_df = pd.concat([total_df, df])
        # update start_date and end_date
        now_ = now_.add(months=1)
        start_date = now_.format('YYYYMMDD')
        end_date = now_.end_of('month').format('YYYYMMDD')

    return total_df
