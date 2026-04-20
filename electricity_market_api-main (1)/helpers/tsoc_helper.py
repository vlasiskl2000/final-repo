import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import pytz
import io
from pydantic import BaseModel
from typing import Optional

class Tsocdata(BaseModel):
    Timestamp : Optional[datetime] = None
    TotalAvailableConventionalCapacity : Optional[float] = None
    WindProduction : Optional[float] = None
    EstimatedDistributedGeneration : Optional[float] = None 
    TotalSystemGeneration : Optional[float] = None
    ConventionalCapacity : Optional[float] = None




def convert_to_utc(time_input, hash_time, input_format='%Y-%m-%d %H:%M:%S', output_tz='UTC'):    
    #Convert string to datetime object in the local time zone
    local_time = datetime.strptime(time_input, input_format)

    # Assuming the input time is in the 'local' time zone, adjust as needed
    local_tz = pytz.timezone('Europe/Athens')

    try:
        local_time = local_tz.localize(local_time, is_dst=None)
    except pytz.AmbiguousTimeError:
        flag = local_time not in hash_time
        hash_time.add(local_time)
        local_time = local_tz.localize(local_time, is_dst=flag)

    except pytz.exceptions.NonExistentTimeError:
        flag = local_time not in hash_time
        hash_time.add(local_time)
        local_time = local_tz.localize(local_time, is_dst=flag)
        

    # Convert to UTC time
    utc_time = local_time.astimezone(pytz.timezone(output_tz))

    return utc_time

def get_df_utc_with_DST(df, input_format='%Y-%m-%d %H:%M:%S', output_tz='UTC'):

    hash_time = set()
    df['Timestamp'] = df['Timestamp'].apply(lambda x: convert_to_utc(x, hash_time, input_format, output_tz))

    return df


def tsoc_url(year: str):
    return 'https://tsoc.org.cy/files/electrical-system/daily-system-generation/%CE%97%CE%BC%CE%B5%CF%81%CE%AE%CF%83%CE%B9%CE%B1%20%CE%A0%CE%B1%CF%81%CE%B1%CE%B3%CF%89%CE%B3%CE%AE%20%CE%97%CE%BB%CE%B5%CE%BA%CF%84%CF%81%CE%B9%CE%BA%CE%BF%CF%8D%20%CE%A3%CF%85%CF%83%CF%84%CE%AE%CE%BC%CE%B1%CF%84%CE%BF%CF%82%20-%20'+ year +'.xlsx'

def tsoc_live_url(date_from: datetime, date_to: datetime = None):
    base_url = "https://tsoc.org.cy/electrical-system/archive-total-daily-system-generation-on-the-transmission-system/?"

    # If date_to is provided and difference is exactly 2 days
    if date_to is not None and (date_to - date_from).days == 2:
        return f"{base_url}startdt=-2days&enddt=today"
    
    # Default case to request one day data
    return f"{base_url}startdt={date_from.strftime('%d-%m-%Y')}&enddt=%2B1days"

def file_does_not_exist(year : int):
    # userequest 
    url = tsoc_url(str(year))
    response = requests.get(url, verify=False)
    return response.status_code == 404