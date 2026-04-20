from datetime import datetime, timedelta, date
import pytz

def EETtoCET(date):
    return date - timedelta(hours = 1)

def getDispatchPeriodFromHourString(hour: str):
    hour_data = hour.split(':')

    return (int(hour_data[0]) + 1) * 2 - (0 if hour_data[1] == '30' else 1)

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

def getTimeDiffernce(date : datetime):
    
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