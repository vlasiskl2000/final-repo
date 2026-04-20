import requests

from helpers.download_helper import *
from helpers.xml_parse_helper import *
from helpers.metadata_helper import *

import datetime
import time
import pandas as pd

def getGFSMinTemperature():
    #Minimum_temperature_height_above_ground_1_Hour_Minimum

    return 

def getGFSMaxTemperature():
    #Maximum_temperature_height_above_ground_1_Hour_Maximum
    url = "http://api.planetos.com/v1/datasets/noaa_gfs_pgrb2_global_forecast_recompute_0.25degree"

    querystring = {
    "apikey":"dab6c393c1114cb387811e868788d813",
    "var": "Maximum_temperature_height_above_ground_1_Hour_Maximum"
    }

    response = requests.request("GET", url, params=querystring)

    # response.text is raw output
    result = response.json()  # turn JSON into python data structure
    return result

def getGFSUComponentOfWind():
    #u-component_of_wind_hybrid

    return 

def getGFSVComponentOfWind():
    #v-component_of_wind_hybrid

    return 

def getGFSRelativeHumidity():
    return 