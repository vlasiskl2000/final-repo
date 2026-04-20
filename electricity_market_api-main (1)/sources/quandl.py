from helpers.log_helper import logException
from models.metadata import getMetadata, getVirtualFileMetadata
from helpers.file_helper import getVirtualFileMetadataFromUrl
import quandl
from datetime import datetime
import interface.envelope as envelope

QUANDL_API_KEY = 'vby51aYYWrjkHeuVa7-W'

def getTTF(dateStart: datetime, dateEnd: datetime):
    response = []
    fileName = 'ICE_TFM1'
    file = getVirtualFileMetadataFromUrl('', fileName, f'{fileName}_{dateStart.strftime("%Y%m%d")}_{dateEnd.strftime("%Y%m%d")}', dateStart, '.json', targetDateTo=dateEnd)

    try:
        table = quandl.get("CHRIS/ICE_TFM1", start_date=dateStart, end_date=dateEnd, authtoken=QUANDL_API_KEY)

        values = [{ 'DispatchDay': date, 'Value': table.Settle[date] } for date in table.index]
        response.append(getVirtualFileMetadata(values, getMetadata(True, file.Id), file))
    except Exception as e:
        message = f'Failed to parse {file.Url}: {e}'
        logException(e)
        response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
    
    return envelope.getSuccessEnvelope(response)

def getCO2(dateStart: datetime, dateEnd: datetime):
    response = []
    fileName = 'ICE_C1'
    file = getVirtualFileMetadataFromUrl('', fileName, f'{fileName}_{dateStart.strftime("%Y%m%d")}_{dateEnd.strftime("%Y%m%d")}', dateStart, '.json', targetDateTo=dateEnd)

    try:
        table = quandl.get("CHRIS/ICE_C1", start_date=dateStart, end_date=dateEnd, authtoken=QUANDL_API_KEY)

        values = [{ 'DispatchDay': date, 'Value': table.Settle[date] } for date in table.index]
        response.append(getVirtualFileMetadata(values, getMetadata(True, file.Id), file))
    except Exception as e:
        message = f'Failed to parse {file.Url}: {e}'
        logException(e)
        response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
    
    return envelope.getSuccessEnvelope(response)

