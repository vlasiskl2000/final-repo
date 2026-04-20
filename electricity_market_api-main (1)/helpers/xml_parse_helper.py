import datetime
import time
import pytz
from bs4 import BeautifulSoup, FeatureNotFound
import requests
from dateutil.relativedelta import relativedelta
from helpers.date_helper import dst_periods, getTimeDiffernce
from helpers.log_helper import logException
import pandas as pd

from helpers.external_api_token_helpers import *

def DownloadXMLFromUrl(url, retry = False):
    try:
        resp = requests.get(url, stream=True, verify=False)
        print(url + " --- " + str(resp.status_code))

        if resp.status_code == 200:
            return BeautifulSoup(resp.content, features='xml')
    
        if resp.status_code == 429 or resp.status_code == 401:
            # cache token to cooldown
            token_to_cooldown = extract_token(url, "securityToken")
            mark_cooldown(token_to_cooldown)

            #replace new token with a random one
            # if all tokens are on cooldown, throw runtime error
            new_token = getRandomSecurityToken()
            url = replace_token(url, new_token, "securityToken")

            return DownloadXMLFromUrl(url)

        return None
    except:
        if retry:
            return None
        # On connection error
        time.sleep(2)
        return DownloadXMLFromUrl(url, True)

def is_dst(dt=None, timezone="Europe/Athens"):
    timezone = pytz.timezone(timezone)
    timezone_aware_date = timezone.localize(dt, is_dst=None)
    return timezone_aware_date.tzinfo._dst.seconds != 0

def getCETFromUTC(date: datetime):
    return date + datetime.timedelta(hours = 1 if not is_dst(date) else 2)

def getUTCFromCET(date):
    if type(date) is not datetime.datetime:
        date = datetime.datetime.combine(date, datetime.time())
        
    offset = -2 if is_dst(date, 'Europe/Berlin') else -1
    return date + datetime.timedelta(hours = offset)

def getDateTimeCET(date: datetime):
    date = date.replace(hour = 0)
    return date + datetime.timedelta(hours = -1)



def getHourlyDataFromXMLResponse(url, value_column = "quantity", timeseries_index = 0):

    xml = DownloadXMLFromUrl(url)
    array = []
    
    if xml is None or xml.TimeSeries is None:
        return []
    
    #retrieve data from possible resolutions of the timeseries
    target_timeseries = list(filter(lambda x: x.Period.resolution.text == "PT60M", xml.findAll('TimeSeries')))


    if len(target_timeseries) == 0:
        return []
    
    timeseries = target_timeseries[timeseries_index]
    timeInterval = timeseries.find('timeInterval')
    timeStart = datetime.datetime.strptime(timeInterval.find('start').contents[0], '%Y-%m-%dT%H:%M%z')
    # timeEnd = datetime.datetime.strptime(timeInterval.find('end').contents[0], '%Y-%m-%dT%H:%MZ')
    resolution = timeseries.Period.resolution.text

    #cet_d = getTimeDiffernce(timeStart)[0]
    hour_diff = getTimeDiffernce(timeStart)[1]

    
    value_array = []
    for point in timeseries.findAll('Point'):
        try:
            hour = int(point.position.text)
            utc_date = getNextDateFromResolution(timeStart, resolution, hour)
            #date = pytz.timezone('UTC').localize(date)
            #date = pytz.timezone('CET').normalize(date)
            date, hour = getTimeDiffernce(utc_date)

            load = float(point.find(value_column).text)

            value_array.append([date,hour,load])
        except FeatureNotFound as e:
            logException(e)
            continue

    return value_array

def getHourlyDAMDataFromXMLResponse(url, value_column = "quantity", timeseries_index = 0):

    xml = DownloadXMLFromUrl(url)
    
    if xml is None or xml.TimeSeries is None:
        return []
    
    #retrieve data from possible resolutions of the timeseries
    resolution_list = ['PT15M', 'PT60M']

    # Retrieve all TimeSeries elements
    all_timeseries = xml.findAll('TimeSeries')

    # Find the best available resolution in priority order
    target_timeseries = list(filter(lambda x: x.Period.resolution.text in resolution_list, all_timeseries))
    
    if not target_timeseries:
        return []
    
    value_array = []

    for timeseries in target_timeseries:

        timeInterval = timeseries.find('timeInterval')
        timeStart = datetime.datetime.strptime(timeInterval.find('start').contents[0], '%Y-%m-%dT%H:%M%z')

        resolution = timeseries.Period.resolution.text

        
        for point in timeseries.findAll('Point'):
            try:
                position = int(point.position.text)

                utc_date = getNextDateFromResolution(timeStart, resolution, position)

                date, hour = getTimeDiffernce(utc_date)

                load = float(point.find(value_column).text)

                value_array.append([date, position, load, resolution])
            except FeatureNotFound as e:
                logException(e)
                continue

    return value_array

def getHourlyDataFromXMLResponseWithGranularity(url, value_column = "quantity", timeseries_index = 0):

    xml = DownloadXMLFromUrl(url)
    array = []
    
    if xml is None or xml.TimeSeries is None:
        return []
 
    target_timeseries = xml.findAll('TimeSeries')
    if len(target_timeseries) == 0:
        return []
    
    value_array = []
    for timeseries in target_timeseries:
        timeInterval = timeseries.find('timeInterval')
        timeStart = datetime.datetime.strptime(timeInterval.find('start').contents[0], '%Y-%m-%dT%H:%M%z')
        # timeEnd = datetime.datetime.strptime(timeInterval.find('end').contents[0], '%Y-%m-%dT%H:%MZ')
        resolution = timeseries.Period.resolution.text
        try:
            for point in timeseries.findAll('Point'):
            
                hour = int(point.position.text)
                utc_date = getNextDateFromResolution(timeStart, resolution, hour)
                #utc_date = pytz.timezone('UTC').localize(utc_date)
                #date = pytz.timezone('CET').normalize(date)
                #date, hour = getTimeDiffernce(utc_date)

                load = float(point.find(value_column).text)

                if resolution == 'PT60M':
                    repetition = 60/15
                elif resolution == 'PT30M':
                    repetition = 30/15
                elif resolution == 'PT15M':
                    repetition = 15/15

                value_array.append([utc_date,load,int(repetition)])

        except FeatureNotFound as e:
            logException(e)
            continue

    return value_array

def getDailyOrAboveDataFromXMLResponse(url, value_column = "quantity", timeseries_index = 0):
    xml = DownloadXMLFromUrl(url)    
    if xml is None or xml.TimeSeries is None:
        return []
 
    target_timeseries = xml.findAll('TimeSeries')
    if len(target_timeseries) == 0:
        return []
    
    timeseries = target_timeseries[timeseries_index]
    timeInterval = timeseries.find('timeInterval')
    timeStart = datetime.datetime.strptime(timeInterval.find('start').contents[0], '%Y-%m-%dT%H:%MZ')
    # timeEnd = datetime.datetime.strptime(timeInterval.find('end').contents[0], '%Y-%m-%dT%H:%MZ')
    resolution = timeseries.Period.resolution.text

    def getRoundedDate(date):
        if date.hour < 12:
            return date.date()
        else:
            return (date + datetime.timedelta(days=1)).date()

    value_array = []
    for point in timeseries.findAll('Point'):
        try:
            position = int(point.position.text)
            date = getNextDateFromResolution(timeStart, resolution, position)
            date = pytz.timezone('UTC').localize(date)
            date = pytz.timezone('CET').normalize(date)
            load = float(point.find(value_column).text)
            
            value_array.append([getRoundedDate(date), load])

        except pytz.NonExistentTimeError:
            logException(Exception("NonExistentTimeError: " + str(date)))
            pass
    
    return value_array


def getNextDateFromResolution(date: datetime, resolution: str, position: int):
    if resolution == "P1Y":
        return date + relativedelta(years=position - 1)
    elif resolution == "P1M":
        return date + relativedelta(months=position - 1)
    elif resolution == "P7D":
        return date + relativedelta(days=7*(position - 1))
    elif resolution == "PT60M":
        return date + relativedelta(hours=position - 1)
    elif resolution == "PT30M":
        return date + relativedelta(minutes=30*(position - 1))
    elif resolution == "PT15M":
        return date + relativedelta(minutes=15*(position - 1))
    else:
        raise Exception("Unknown resolution: " + resolution)

def getDetailHourlyDataFromXMLResponse(url, value_column = "quantity"):
    xml = DownloadXMLFromUrl(url)
    array = []
    
    if xml is None:
        return None

    for timeseries in xml.find_all('TimeSeries'):
        PsrType = timeseries.MktPSRType.psrType.text
        UnitId = timeseries.MktPSRType.PowerSystemResources.mRID.text
        Name = timeseries.MktPSRType.PowerSystemResources.find_all("name")[0].text
        for point in timeseries.findAll('Point'):
            Hour = int(point.position.text)
            Load = float(point.find(value_column).text)
            array.append([PsrType, UnitId, Name, Hour, Load])

    return array


def getHourlyDataDayAheadGenerationFromXMLResponse(url,date):
    soup = DownloadXMLFromUrl(url)

    # Extract TimeSeries elements
    data_list = []
    data_dict = {}
    timeseries_data = soup.find_all('TimeSeries')

    if len(timeseries_data) == 0:
        return None
    
    time_check = (date - datetime.timedelta(days = 1)).strftime('%Y-%m-%d')
    for timeseries in timeseries_data:

        #check if the timeseries data is for the correct date.
        #This check occurs because the api returns data for the next day as well in some cases due to different timezones.
        if timeseries.find('timeInterval').find('start').text.split('T')[0] != time_check:
            quantity = [None] * len(timestamp)
            time_series_data = dict(zip(timestamp, quantity))
            data_list.append(time_series_data)
            continue

        timestamp  = []
        quantity = []
        time_start = datetime.datetime.strptime(timeseries.find('timeInterval').find('start').text,'%Y-%m-%dT%H:%MZ')

        # The api returns the timestamp in utc tz. 
        # The utc timestamp depends on the  local timezone of the bidding zone of each request made.

        resolution = timeseries.find('resolution').text
        #find generation timeseries data
        for point in timeseries.find_all('Point'):
            position = int(point.position.text)
            date = getNextDateFromResolution(time_start,resolution, position)
            date = pytz.utc.localize(date)

            #resample to 15 minutes
            if resolution != 'PT15M':
                for i in range(0,4): 
                    timestamp.append(date + datetime.timedelta(minutes=15 * i))
                    quantity.append(float(point.find('quantity').text))
            else:
                timestamp.append(date)
                quantity.append(float(point.find('quantity').text))

        time_series_data = dict(zip(timestamp, quantity))
        data_list.append(time_series_data)
    
    if len(data_list) == 1:
        quantity = [None] * len(timestamp)
        time_series_data = dict(zip(timestamp, quantity))
        data_list.append(time_series_data)

    data_dict['Generation'] = data_list[0]
    data_dict['Consumption'] = data_list[1]

    return data_dict

def getDetailPsrHourlyDataFromXMLResponse(url, value_column = "quantity"):
    xml = DownloadXMLFromUrl(url)
    array = []
    
    if xml is None:
        return None

    for timeseries in xml.find_all('TimeSeries'):
        PsrType = timeseries.MktPSRType.psrType.text
        
        timeInterval = timeseries.find('timeInterval')
        timeStart = datetime.datetime.strptime(timeInterval.find('start').contents[0], '%Y-%m-%dT%H:%MZ')
        timeStart = getCETFromUTC(timeStart)

        for point in timeseries.findAll('Point'):
            Date = timeStart + datetime.timedelta(hours=int(point.position.text)-1)
            Load = float(point.find(value_column).text)
            array.append([PsrType, Date.date(), Date.hour+1, Load])

    return array

def getDetailQuarterlyReserveDataFromXMLResponse(url):
    xml = DownloadXMLFromUrl(url)
    array = []

    if xml is None:
        return [None, None]
    

    for ts in xml.find_all("TimeSeries"):

        reserve_dir = ts.find("flowDirection.direction").text 
        is_up = reserve_dir == "A01"  

        for period in ts.find_all("Period"):

            interval = period.find("timeInterval")
            t_start = datetime.datetime.strptime(interval.start.text, "%Y-%m-%dT%H:%MZ")
            t_end   = interval.end.text                       
            last_date_end = t_end                           
            t_start_cet = getCETFromUTC(t_start)

            resolution = period.resolution.text           
            minutes_per_step = 15 if resolution == "PT15M" else 0

            for point in period.find_all("Point"):
                pos = int(point.position.text)
                dt   = t_start_cet + datetime.timedelta(minutes=minutes_per_step * (pos - 1))

                qty  = float(point.quantity.text)
                sec  = point.secondaryQuantity
                sec  = float(sec.text) if sec is not None else None

                array.append(
                    [is_up, dt.date(),getQuarterFromDate(dt), qty, sec]
                    )   

    if not array:
        return [None, None]
    else:
        return [array, last_date_end]


def getDetailQuarterlyReservePricesFromXMLResponse(url):
    xml = DownloadXMLFromUrl(url)
    array = []
    
    if xml is None:
        return None
    

    current_period = 0 
    previous_period = 0
    for ts in xml.find_all("TimeSeries"):

        reserve_dir = ts.find("flowDirection.direction").text 
        is_up = reserve_dir == "A01"  

        for period in ts.find_all("Period"):

            interval = period.find("timeInterval")
            t_start = datetime.datetime.strptime(interval.start.text, "%Y-%m-%dT%H:%MZ")
            t_end   = interval.end.text                       
            last_date_end = t_end                           
            t_start_cet = getCETFromUTC(t_start)

            resolution = period.resolution.text           
            minutes_per_step = 15 if resolution == "PT15M" else 0

            for point in period.find_all("Point"):
                pos = int(point.position.text)
                dt   = t_start_cet + datetime.timedelta(minutes=minutes_per_step * (pos - 1))

                qty  = float(point.find('activation_Price.amount').text)

                array.append(
                    [is_up, dt.date(),getQuarterFromDate(dt), qty]
                    )   

    if not array:
        return [None, None]
    else:
        return [array, last_date_end]
 


    


def TimeDiffernceInPeriods(start : datetime, end : datetime):
    diff = start-end

    return int(abs((start - end).total_seconds()// 3600) * 4)



def getQuarterFromDate(Date):
    return int(Date.minute) // 15 + 4 * Date.hour + 1

def fillMissingPeriodsInData(data, date, isForecast = True):
    ''' Fills missing periods in the data DataFrame based on the last known value for each resolution.
        Args:
            data (list): List of data points, each containing dispatchDate, period, quantity, and Resolution.
            date (datetime): The date for which the data is being processed.
            isForecast (bool): Indicates if the data is forecasted or actual. (False for actual data to limit to current period)
        Returns:
            pd.DataFrame: DataFrame with missing periods filled.
        '''

    if not data:
        return pd.DataFrame()

    data_df = pd.DataFrame(data, columns=["dispatchDate", "period", "quantity", "Resolution"])

    list_of_resolutions = data_df['Resolution'].unique()

    results = pd.DataFrame()

    for resolution in list_of_resolutions:

        df = data_df[data_df['Resolution'] == resolution]

        summer_dst, winter_dst = dst_periods(date)  

        resolution_offset = 60 / int(resolution.replace('PT', '').replace('M', ''))   

        base_range = int(24 * resolution_offset)
        if date.strftime('%Y-%m-%d') == summer_dst:
            base_range -= int(1 * resolution_offset)
        elif date.strftime('%Y-%m-%d') == winter_dst:
            base_range += int(1 * resolution_offset)
        max_range = base_range


        full_period_range = range(1, max_range + 1)  

        # # Create an empty DataFrame to collect results
        # filled_df = pd.DataFrame()

        # Sort each group by period
        group = df.sort_values(by="period").reset_index(drop=True)
        
        # Identify missing periods in the current group
        existing_periods = set(group["period"])
        missing_periods = sorted(set(full_period_range) - existing_periods)
        
        # List to hold new rows
        new_rows = []

        # Iterate through missing periods and fill based on the last known row
        for period in missing_periods:
            # Find the last row before the missing period
            last_valid_row = group[group["period"] < period].iloc[-1] if not group[group["period"] < period].empty else None
            if last_valid_row is not None:
                new_row = last_valid_row.copy()
                new_row["period"] = period
                new_rows.append(new_row)

        # Convert the list of new rows to a DataFrame
        new_rows_df = pd.DataFrame(new_rows)

        # Append new rows to the original group DataFrame
        group = pd.concat([group, new_rows_df], ignore_index=True)

        # Sort the DataFrame by 'period' to maintain order
        group = group.sort_values(by="period").reset_index(drop=True)
        
        # Append to the final DataFrame
        results = pd.concat([results, group], ignore_index=True)


                # #if date is today filter data for to get until now
        if date.date() == datetime.datetime.now().date() and not isForecast:
            now = datetime.datetime.now(pytz.timezone('CET'))
            current_period = (now.hour) - 1 if resolution == 'PT60M' else getQuarterFromDate(now) - 4
            results = results[results['period'] < current_period]

    return results
