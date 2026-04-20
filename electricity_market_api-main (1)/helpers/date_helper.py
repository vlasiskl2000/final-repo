from datetime import datetime, timedelta, date
import pytz
from dateutil import parser
import pandas as pd

def EETtoCET(date):
    return date - timedelta(hours = 1)

def getDispatchPeriodFromHourString(hour: str):
    hour_data = hour.split(':')

    return (int(hour_data[0])  * 2 + int(hour_data[1]) // 30 + 1)

def getQuarterlyDispatchPeriodFromHourString(hour: str):
    hour_data = hour.split(':')

    hour = int(hour_data[0])
    minute = int(hour_data[1])
    return getQuarterDispatchPeriodFromHourMinute(hour, minute)

def getQuarterDispatchPeriodFromHourMinute(hour, minute):
    return int(hour * 4 + (minute / 15) + 1)

def getInfoFromDate(date: datetime, to_cet = True, half_hour_format = False, quarterly_hour_format = False) -> (str, int):
    if type(date) is not datetime:
        return None, None

    if to_cet:
        date = date + timedelta(hours=-1)

    if half_hour_format and not quarterly_hour_format:
        dispatchPeriod = getDispatchPeriodFromHourString(date.strftime('%H:%M'))
    elif not half_hour_format and quarterly_hour_format:
        dispatchPeriod = getQuarterlyDispatchPeriodFromHourString(date.strftime('%H:%M'))
    else:
        dispatchPeriod = date.hour + 1

    return (date.strftime('%Y-%m-%d'), dispatchPeriod)

def getDate(date : datetime):
    if type(date) is not datetime:
        if not checkDateToString(date):
            return None, None
        else:
            return parser.parse(date).strftime('%Y-%m-%d')

    return date.strftime('%Y-%m-%d')


def getInfoFromDateStyleFrame(date: datetime, to_cet = True, half_hour_format = False) -> (str, int):
    if type(date.value) is not datetime:
        return None, None

    if to_cet:
        date.value = date.value + timedelta(hours=-1)

    if half_hour_format:
        dispatchPeriod = getDispatchPeriodFromHourString(date.value.strftime('%H:%M'))
    else:
        dispatchPeriod = date.value.hour + 1

    return (date.value.strftime('%Y-%m-%d'), dispatchPeriod)

def formatDateTimeForJson(date: datetime):
    if str(date) == "NaT":
        return None
    return date.strftime("%Y-%m-%d") 


def formatDateTimeStrForJson(date: str):
    return (
        datetime.strptime(date, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d") 
    )


def formatDateTimeStrForJson(date: str):
    return datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")

def parseDateTimeFromArgs(date: str):
    return datetime.strptime(date, '%Y-%m-%d')

def getTimeDiffernce(date : datetime, timezone = 'CET'):
    
    # Convert the UTC datetime to CET
    cet_timezone = pytz.timezone('Europe/Berlin')
    cet_datetime = date.astimezone(cet_timezone)

    # Get the date in CET
    cet_date = cet_datetime.date()

    # Convert the CET date back to a datetime object (midnight)
    cet_datetime_midnight = datetime.combine(cet_date, datetime.min.time())

    # Convert the CET datetime back to UTC
    cet_datetime_midnight = cet_timezone.localize(cet_datetime_midnight)

    #normalize
    utc_date = pytz.utc.normalize(cet_datetime_midnight)

    hour_diff = date-utc_date

    return cet_datetime_midnight, int(abs(hour_diff.total_seconds()) // 3600) + 1


def getUtcTimestamp(date: datetime, add_hours = 0 ):
    str_to_date =  datetime.strptime(date,'%d.%m.%Y')
    cet_tz = pytz.timezone('CET')
    utc_tz = pytz.timezone('UTC')

    cet_day = cet_tz.localize(str_to_date)
    last_date = cet_day + timedelta(hours= add_hours)
    utc_day = last_date.astimezone(utc_tz)
    return utc_day


def getUtcStrDate(date: datetime):
    str_to_date =  datetime.strptime(date,'%d.%m.%Y')
    cet_tz = pytz.timezone('CET')
    utc_tz = pytz.timezone('UTC')

    cet_day = cet_tz.localize(str_to_date)
    utc_day = cet_day.astimezone(utc_tz)
    return utc_day

def extractHours(time_range):
    flag = False
    try:
        if time_range[-3:] == '(1)':
            time_range = time_range[:-4]
            
        elif time_range[-3:] == '(2)':
            flag = True
            time_range = time_range[:-4]
        
        start_time_str, end_time_str = map(str.strip, time_range.split('-'))

        start_time_hours, start_time_minutes= map(int,start_time_str.split(':'))
        end_time_hours, end_time_minutes = map(int,end_time_str.split(':'))  


        return start_time_hours, end_time_hours, flag
    
    except ValueError:
        # Handle invalid input gracefully
        print("Invalid time range format.")
        return None
    
def dst_periods(date :datetime):

    if date is None:
        date = datetime.now()

    current_year = date.year

    # Find the last Sunday in March (potential start date of DST for summer)
    last_sunday_march = datetime(current_year, 3, 31)
    while last_sunday_march.weekday() != 6:  # 6 corresponds to Sunday
        last_sunday_march -= timedelta(days=1)

    if date < last_sunday_march:
        current_year -= 1
        return dst_periods(datetime(current_year, 12, 31))

    # Find the last Sunday in October (potential start date of DST for winter)
    last_sunday_october = datetime(current_year, 10, 31)
    while last_sunday_october.weekday() != 6:
        last_sunday_october -= timedelta(days=1)

    if date < last_sunday_october:
        current_year -= 1
        last_dst = dst_periods(datetime(current_year, 12, 31))
        return (last_sunday_march.strftime('%Y-%m-%d') ,last_dst[1])   
        

    return (last_sunday_march.strftime('%Y-%m-%d'), last_sunday_october.strftime('%Y-%m-%d'))


def getTimestampToUtc(date: pd.Timestamp):
    #str_to_date =  datetime.strptime(date,'%Y-%m-%d %H:%M:%S')
    cet_tz = pytz.timezone('Europe/Berlin')
    utc_tz = pytz.timezone('UTC')

    cet_day = cet_tz.localize(date)
    last_date = cet_day 
    utc_day = last_date.astimezone(utc_tz)
    return utc_day 

def getTimeDifferncePandas(date : pd.Timestamp, seconds_granularity = 3600):
    
    cet_time = date.tz_localize(pytz.timezone('Europe/Berlin'), ambiguous = True)

    # Convert the UTC datetime to CET
    cet_timezone = pytz.timezone('Europe/Berlin')
    #cet_datetime = date.astimezone(cet_timezone)

    # Get the date in CET
    cet_date = cet_time.date()

    # Convert the CET date back to a datetime object (midnight)
    cet_datetime_midnight = datetime.combine(cet_date, datetime.min.time())

    # Convert the CET datetime back to UTC
    cet_datetime_midnight = cet_timezone.localize(cet_datetime_midnight)

    #normalize
    utc_date = pytz.utc.normalize(cet_datetime_midnight)

    cet_date = pytz.utc.normalize(cet_time)

    hour_diff = cet_date-utc_date

    return int(abs(hour_diff.total_seconds()) // seconds_granularity) + 1



def getTimeDifferncePandasBallancingCap(date : pd.Timestamp, seconds_granularity = 3600):
    
    # Convert the UTC datetime to CET
    cet_timezone = pytz.timezone('Europe/Berlin')
    #cet_datetime = date.astimezone(cet_timezone)

    # Get the date in CET
    cet_date = date.date()

    # Convert the CET date back to a datetime object (midnight)
    cet_datetime_midnight = datetime.combine(cet_date, datetime.min.time())

    # Convert the CET datetime back to UTC
    cet_datetime_midnight = cet_timezone.localize(cet_datetime_midnight)

    #normalize
    utc_date = pytz.utc.normalize(cet_datetime_midnight)

    cet_date = pytz.utc.normalize(date)

    hour_diff = cet_date-utc_date

    return int(abs(hour_diff.total_seconds()) // seconds_granularity) + 1
    

    
def checkDateToString(date : str):
    try:
        if parser.parse(date):
            return True
    except ValueError:
        return False