from helpers.file_helper import *
from helpers.download_helper import getVirtualFileMetadataFromUrl

# 1: DAM
# 2: LIDA1
# 3: LIDA2
# 4: LIDA3
def getHybridOffersFile(date: datetime, market: int):
    if market == 1:
        url = f"https://www.dapeep.gr/pdf/DAM/{date.strftime('%Y%m%d')}_DAPEEP_DAM.xlsx"
        marketName = "Dam"
    else:
        lida = market - 1
        url = f"https://www.dapeep.gr/pdf/IDM/{date.strftime('%Y%m%d')}_DAPEEP_IDM_(LIDA{lida}).xlsx"
        marketName = "Lida" + str(lida)

    request = requests.get(url, verify=False)
    status_code = request.status_code
    print(url + ' ' + str(status_code))
    if status_code >= 200 and status_code < 300:
        return getVirtualFileMetadataFromUrl(url, market, date, f'Dapeep{marketName}HybridOffers', request)

    return None

def downloadDAMHybridOffers(dateFrom: str, dateTo: str, market, force = False):
    files = []
    date = dateFrom
    while date <= dateTo:
        if market is not None:
            file = getHybridOffersFile(date, market)
            if file is not None:
                files.append(file)
        else:
            for i in range(1,5):
                file = getHybridOffersFile(date, i)
                if file is not None:
                    files.append(file)

        date = date + timedelta(days=1)
    
    return downloadFiles(files, force)