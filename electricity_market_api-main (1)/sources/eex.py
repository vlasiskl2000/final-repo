import requests
import urllib.parse

from datetime import datetime, timedelta
from dateutil.relativedelta import *

from helpers.file_helper import getVirtualFileMetadataFromUrl
from helpers.log_helper import logException
from models.metadata import getMetadata, getVirtualFileMetadata
from interface import envelope
from sources.eex_metadata import *

def getProductCode(loadType: str, country: str, commodityType, futureType, futureOffset):
    commodityTypeCode = COMMODITY_TYPE_CODES[commodityType]
    countryCode = country['eexCodeWeekend'] if futureType == 'WKEnd' else country['eexCodeDay'] if futureType == 'D' else country['eexCode']

    now = datetime.now().date()
    if futureType == 'Y':
        targetDate = now.replace(year=now.year + futureOffset)
        suffix = 'YF' + targetDate.strftime('%y')
        productName = 'Cal-' + targetDate.strftime('%y')
    elif futureType == 'Q':
        targetDate = now + relativedelta(months=3 * futureOffset)
        quarter = (targetDate.month - 1) // 3 + 1
        suffix = 'Q' + QUARTERS[quarter] + targetDate.strftime('%y')
        productName = str(quarter) + '/' + targetDate.strftime('%y')
    elif futureType == 'M':
        targetDate = now + relativedelta(months=futureOffset)
        suffix = 'M' + MONTH[targetDate.month] + targetDate.strftime('%y')
        productName = targetDate.strftime('%b/%y')
    elif futureType == 'W':
        targetDate = now + timedelta(days= 7 * futureOffset)

        # get first monday before target date
        targetDate = targetDate - timedelta(days=targetDate.weekday())
        weekNumber = (targetDate.day - 1) // 7 + 1
        targetDate = targetDate - timedelta(days=1)
        suffix = '0' + str(weekNumber) + MONTH[targetDate.month] + targetDate.strftime('%y')
        productName = 'Week ' + targetDate.strftime('%W/%y')
    elif futureType == 'WKEnd':
        targetDate = now + timedelta(days= 7 * futureOffset)

         # Saturday before target date
        targetDate = targetDate - timedelta(days=targetDate.weekday() + 2)
        weekNumber = (targetDate.day - 1) // 7 + 1
        suffix = '0'+ str(weekNumber) + MONTH[targetDate.month] + targetDate.strftime('%y')
        productName = 'WkEnd ' + targetDate.strftime('%d/%m')
    elif futureType == 'D':
        targetDate = now + timedelta(days=futureOffset)
        suffix = targetDate.strftime('%d') + MONTH[targetDate.month] + targetDate.strftime('%y')
        productName = targetDate.strftime('%d/%m/%Y')
    else:
        return None

    return (f'"/{commodityTypeCode}.{countryCode}{loadType}{suffix}"', productName)

def getValidProducts(country):
    products = []

    for commodityType in country['commodityTypes']:
        loadTypes = country['commodityTypes'][commodityType]
        for loadType in loadTypes:
            for future in country['futures']:
                futureHorizon = country['futures'][future]
                for i in range(futureHorizon):
                    productCode = getProductCode(loadType, country, commodityType, future, i)
                    if productCode is not None:
                        products.append({
                            'productCode': productCode[0],
                            'name': productCode[1],
                            'countryName': country['code'],
                            'productType': COMMODITY_FUTURE_PRODUCT_TYPE[future],
                            'commodityType': commodityType,
                            'commodityLoadType': LOAD_TYPES[loadType],
                            'index': country['index']
                        })
    
    return products

def getRequest(url):
    return requests.get(f'{HOST}/{url}', headers=headers, verify=False).json()

def getLastInfo(country, date):
    data = {}
    for commodityType in country['commodityTypes']:
        loadTypes = country['commodityTypes'][commodityType]
        for loadType in loadTypes:
            for future in country['futures']:
                countryCode = country['eexCodeWeekend'] if future == 'WKEnd' else country['eexCodeDay'] if future == 'D' else country['eexCode']
                suffix = 'Y' if future == 'Y' else 'Q' if future == 'Q' else 'M' if future == 'M' else '_WEEK' if future == 'W' else '_WEEK' if future == 'WKEnd' else '_DAILY'
                
                try:
                    response = getRequest(f'query/json/getChain/gv.pricesymbol/gv.displaydate/gv.expirationdate/tradedatetimegmt/gv.eexdeliverystart/ontradeprice/close/onexchsingletradevolume/onexchtradevolumeeex/offexchtradevolumeeex/openinterest/?optionroot=%22%2FE.{countryCode}{loadType}{suffix}%22&expirationdate={date}')
                    if response['results'] is not None and response['results']['items'] is not None:
                        for productInfo in (response['results']['items']):
                            data[productInfo['gv.pricesymbol']] = {
                                'lastPrice': productInfo['ontradeprice'],
                                'lastVolume': productInfo['onexchsingletradevolume'],
                                'openInterest': productInfo['openinterest'],
                                'dispatchDay': datetime.now().date(),
                            }
                except Exception as e:
                    logException(e)

    return data
    
def getPowerFutures(date: datetime):
    response = []

    chartstopdate = date.strftime('%Y/%m/%d')
    chartstartdate = (date - timedelta(days=60)).strftime('%Y/%m/%d')
    
    for country in COUNTRIES:
        tradeLastInfo = getLastInfo(country, date)
        
        for product in getValidProducts(country):
            actual_url = f'query/json/getDaily/close/offexchtradevolumeeex/onexchtradevolumeeex/tradedatetimegmt/?priceSymbol={product["productCode"]}&chartstartdate={chartstartdate}&chartstopdate={chartstopdate}&dailybarinterval=Days&aggregatepriceselection=First'
            
            fileName = f'eex_{date:%Y-%m-%d}_{product["productCode"]}'
            file = getVirtualFileMetadataFromUrl(actual_url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date, extension='.json')

            try:
                results = []
                actual_data = getRequest(actual_url)
                
                results = list(map(lambda x: {
                    'dispatchDay': datetime.strptime(x['tradedatetimegmt'], '%m/%d/%Y %H:%M:%S %p').date(),
                    'settlementPrice': x['close'],
                    'volumeTradeRegistration': x['offexchtradevolumeeex'],
                    'volumeExchange': x['onexchtradevolumeeex'],
                    'eexApiCode': product['productCode'],
                 }, actual_data['results']['items']))

                product['commodityFutureProductTimeseries'] = results

                productCodeKey = product['productCode'].replace('"', '')
                if productCodeKey in tradeLastInfo:
                    product['commodityFutureLastInfoTimeseries'] = [tradeLastInfo[productCodeKey]]

                product['commodityIndexName'] = country['index']
                response.append(getVirtualFileMetadata([product], getMetadata(len(results) > 0, file.Id), file))
            except Exception as e:
                message = f'Failed to parse {file.Url}: {e}'
                logException(e)
                response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
            
    return envelope.getSuccessEnvelope(response)