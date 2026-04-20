from requests.models import Request
from helpers.file_helper import *
from dateutil.parser import parse as parsedate
from dateutil import tz
from helpers.download_helper import getVirtualFileMetadataFromUrl

def getHenexMarketDataUrl(date: datetime, maxVersion = 4):
    for version in range(maxVersion, 0, -1):
        url = f'https://www.enexgroup.gr/documents/20126/214481/{date.strftime("%Y%m%d")}_EL-DAM_POSNOMs_EN_v0{version}.xlsx'

        request = requests.get(url, verify=False)
        status_code = request.status_code
        print(url + ' ' + str(status_code))
        if status_code >= 200 and status_code < 300:
            return getVirtualFileMetadataFromUrl(url, version, date, 'HenexMarketDataDAM', request)

    return None

def getHenexMarketResultsFile(date: datetime, market: str, maxVersion = 4):
    marketId = None
    marketName = market

    crida = date >= datetime(2021,9,22)

    if market == 'DAM':
        marketId = '200106'
    elif market == 'XBID':
        marketId = '1550281'
    else:
        if market == 'LIDA1':
            if crida:
                marketName = 'CRIDA1'
                marketId = '853663'
            else:
                marketId = '235155'
        elif market == 'LIDA2':
            if crida:
                marketName = 'CRIDA2'
                marketId = '853680'
            else:
                marketId = '263261'
        elif market == 'LIDA3':
            if crida:
                marketName = 'CRIDA3'
                marketId = '853704'
            else:
                marketId = '263280'
        else:
            return None
    
    for version in range(maxVersion, 0, -1):
        url = f'https://www.enexgroup.gr/documents/20126/{marketId}/{date.strftime("%Y%m%d")}_EL-{marketName}_Results_EN_v0{version}.xlsx'

        request = requests.get(url, verify=False)
        status_code = request.status_code
        print(url + ' ' + str(status_code))
        if status_code >= 200 and status_code < 300:
            return getVirtualFileMetadataFromUrl(url, version, date, 'HenexMarketResults' + market, request)

    return None

def getHenexMarketResultsSummaryFile(date: datetime, maxVersion = 4):
    for version in range(maxVersion, 0, -1):
        url = f'https://www.enexgroup.gr/documents/20126/366820/{date.strftime("%Y%m%d")}_EL-DAM_ResultsSummary_EN_v0{version}.xlsx'

        request = requests.get(url, verify=False)
        status_code = request.status_code
        print(url + ' ' + str(status_code))
        if status_code >= 200 and status_code < 300:
            return getVirtualFileMetadataFromUrl(url, version, date, 'HenexDamResultsSummary', request)

    return None

def getHenexAggregatedCurvesFile(date: datetime, market: str, maxVersion = 4):
    marketId = None
    marketName = market

    crida = date >= datetime(2021,9,22)
    
    if market == 'DAM':
        marketId = '200034'
    else:
        if market == 'LIDA1':
            if crida:
                marketName = 'CRIDA1'
                marketId = '853660'
            else:
                marketId = '222466'
        elif market == 'LIDA2':
            if crida:
                marketName = 'CRIDA2'
                marketId = '853695'
            else:
                marketId = '222498'
        elif market == 'LIDA3':
            if crida:
                marketName = 'CRIDA3'
                marketId = '853701'
            else:
                marketId = '222520'
        else:
            return None
    
    for version in range(maxVersion, 0, -1):
        url = f'https://www.enexgroup.gr/documents/20126/{marketId}/{date.strftime("%Y%m%d")}_EL-{marketName}_AggrCurves_EN_v0{version}.xlsx'

        request = requests.get(url, verify=False)
        status_code = request.status_code
        print(url + ' ' + str(status_code))
        if status_code >= 200 and status_code < 300:
            return getVirtualFileMetadataFromUrl(url, version, date, 'HenexMarketAggregatedCurves' + market, request)

    return None

def getBlockOrderAcceptanceFile(date: datetime, maxVersion = 4):
    for version in range(maxVersion, 0, -1):
        url = f'https://www.enexgroup.gr/documents/20126/270103/{date.strftime("%Y%m%d")}_EL-DAM_BLKORDRs_EN_v0{version}.xlsx'

        request = requests.get(url, verify=False)
        status_code = request.status_code
        print(url + ' ' + str(status_code))
        if status_code >= 200 and status_code < 300:
            return getVirtualFileMetadataFromUrl(url, version, date, 'MarketBlockOrderAcceptanceStatus', request)

    return None

def downloadResults(dateFrom: str, dateTo: str, market: str, force = False):
    markets = ['DAM', 'LIDA1', 'LIDA2', 'LIDA3', 'XBID'] if market == None else [market]

    files = []
    date = dateFrom
    while date <= dateTo:
        for market in markets:
            file = getHenexMarketResultsFile(date, market)
            if file is not None:
                files.append(file)
        date = date + timedelta(days=1)
    
    return downloadFiles(files, force)

def downloadResultsSummary(dateFrom: str, dateTo: str, force = False):
    files = []
    date = dateFrom
    while date <= dateTo:
        file = getHenexMarketResultsSummaryFile(date)
        if file is not None:
            files.append(file)
        date = date + timedelta(days=1)
    
    return downloadFiles(files, force)
    
def downloadAggregatedCurves(dateFrom: str, dateTo: str, market: str, force = False):
    markets = ['DAM', 'LIDA1', 'LIDA2', 'LIDA3'] if market == None else [market]

    files = []
    date = dateFrom
    while date <= dateTo:
        for market in markets:
            file = getHenexAggregatedCurvesFile(date, market)
            if file is not None:
                files.append(file)
        date = date + timedelta(days=1)
    
    return downloadFiles(files, force)

def downloadData(dateFrom: str, dateTo: str, force = False):
    files = []
    date = dateFrom
    while date <= dateTo:
        file = getHenexMarketDataUrl(date)
        if file is not None:
            files.append(file)
        date = date + timedelta(days=1)
    
    return downloadFiles(files, force)

def downloadMarketBlockOrderAcceptanceStatus(dateFrom: str, dateTo: str, force = False):
    files = []
    date = dateFrom
    while date <= dateTo:
        file = getBlockOrderAcceptanceFile(date)
        if file is not None:
            files.append(file)
        date = date + timedelta(days=1)
    
    return downloadFiles(files, force)