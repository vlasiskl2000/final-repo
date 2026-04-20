from typing import Optional
import numpy as np
from pydantic import BaseModel
from helpers.log_helper import logException
from models.metadata import getMetadata, getVirtualFileMetadata
from helpers.download_helper import downloadJson
import random
from datetime import datetime, timedelta
import interface.envelope as envelope
from helpers.file_helper import getVirtualFileMetadataFromUrl

#create pydantic model for rate
class ExchangeRateModel(BaseModel):
    DispatchDay : datetime
    CurrencyFrom : str
    CurrencyTo : str
    Value: Optional[float] = None


API_KEYS = ['3d2531683a904e498280191046a12f4e']

def getExchangeRate(dateFrom: datetime, dateTo: datetime, CurrencyFrom = 'EUR',CurrencyTo = 'USD'):
    response = []

    fileName = 'ExhangeRatesApi'
    file = getVirtualFileMetadataFromUrl('https://api.exchangeratesapi.io/v1/', fileName, f'{fileName}_{dateFrom.strftime("%Y%m%d")}_{dateTo.strftime("%Y%m%d")}_{CurrencyFrom}_{CurrencyTo}', dateFrom, '.json', None, dateTo)
      
    try:
        date = dateFrom
        rates = []
        while date <= dateTo:
            api_key = random.choice(API_KEYS)
        
            url = f'https://api.exchangeratesapi.io/v1/{date.strftime("%Y-%m-%d")}?access_key={api_key}&base={CurrencyFrom}&symbols={CurrencyTo}'
            data = downloadJson(url)

            if data is not None and "rates" in data:
                current_date = datetime.strptime(data["date"], '%Y-%m-%d').date()
                
                for rate, value in data['rates'].items():
                    rates.append(ExchangeRateModel(
                        DispatchDay=current_date,
                        CurrencyFrom=CurrencyFrom,
                        CurrencyTo=rate,
                        Value=np.round(value, 6)
                    ))
                    rates.append(ExchangeRateModel(
                        DispatchDay=current_date,
                        CurrencyFrom=rate,
                        CurrencyTo=CurrencyFrom,
                        Value=np.round(1 / value, 6) if value != 0 else 0
                    ))

            date += timedelta(days = 1)

        response.append(getVirtualFileMetadata(rates, getMetadata(True, file.Id), file))
    except Exception as e:
        message = f'Failed to parse {file.Url}: {e}'
        logException(e)
        response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

    return envelope.getSuccessEnvelope(response)
