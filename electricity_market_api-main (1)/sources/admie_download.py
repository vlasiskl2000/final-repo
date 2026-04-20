import re
from helpers.metadata_helper import getISPRequirementsGeneratedLinks
from helpers.admie_helper import *
from helpers.file_helper import *

# ISP Files Download
def downloadUnitAvailabilities(dateFrom: str, dateTo: str, isp: int, version:int = None, force = False):
    files = getISPAvailabilityFiles(dateFrom, dateTo, isp, version)

    return downloadFiles(files, force)

def downloadISPEnergyOfferFiles(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi("ISPEnergyOffers", dateFrom, dateTo)

    return downloadFiles(files, force)

def downloadISPCapacityOffers(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi("ISPCapacityOffers", dateFrom, dateTo)

    return downloadFiles(files, force)

def downloadISPResults(dateFrom: str, dateTo: str, isp: int = None, adhoc: bool = False, force = False):
    files = getISPResultsFiles(dateFrom, dateTo, isp, adhoc)

    return downloadFiles(files, force)

def downloadIspItalyDev(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi("Devit", dateFrom, dateTo) 
    return downloadFiles(files, force)

def downloadIspNorthDev(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi("Devnor", dateFrom, dateTo)
    return downloadFiles(files, force)
    
def downloadIspLoadForecasts(dateFrom: str, dateTo: str, isp=None, version=None, force = False):
    if isp is not None:
        if isp == 1 or isp == 2:
            files = getFileUrlsFromApi(f"ISP{isp}DayAheadLoadForecast", dateFrom, dateTo, version)
        else:
            files = getFileUrlsFromApi("ISP3IntraDayLoadForecast", dateFrom, dateTo, version)
    else:
        files = getFileUrlsFromApi("ISP1DayAheadLoadForecast", dateFrom, dateTo, version) + getFileUrlsFromApi("ISP2DayAheadLoadForecast", dateFrom, dateTo, version) + getFileUrlsFromApi("ISP3IntraDayLoadForecast", dateFrom, dateTo, version)
    
    if version is not None:
        files = list(filter(lambda row: row.Version == version, files))

    return downloadFiles(files, force)

def downloadIspResForecasts(dateFrom: str, dateTo: str, isp=None, version=None, force = False):
    if isp is not None:
        if isp == 1 or isp == 2:
            files = getFileUrlsFromApi(f"ISP{isp}DayAheadResForecast", dateFrom, dateTo, version)
        else:
            files = getFileUrlsFromApi("ISP3IntraDayResForecast", dateFrom, dateTo, version)
    else:
        files = getFileUrlsFromApi("ISP1DayAheadResForecast", dateFrom, dateTo, version) + getFileUrlsFromApi("ISP2DayAheadResForecast", dateFrom, dateTo, version) + getFileUrlsFromApi("ISP3IntraDayResForecast", dateFrom, dateTo, version)
    
    if version is not None:
        files = list(filter(lambda row: row.Version == version, files))
    return downloadFiles(files, force)

def downloadIspWeekAheadLoadForecasts(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi(f"ISPWeekAheadLoadForecast", dateFrom, dateTo)
    
    return downloadFiles(files, force)

def downloadIspRequirements(dateFrom: str, dateTo: str, isp=None, generateLinks = False, force = False):
    if generateLinks:
        files = getISPRequirementsGeneratedLinks(dateFrom, dateTo)
    else:
        files = getISPFilesWithPrefix("Requirements", dateFrom, dateTo, isp, None, 4)
    return downloadFiles(files, force)

def downloadBalancingEnergySettlements(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi("IMBABE", dateFrom, dateTo)
    for file in files:
        # check if file description ends with v and a number
        if re.search(r'v\d+$', file.FileDescription):
            file.Version = int(re.search(r'v\d+$', file.FileDescription).group(0).replace('v', ''))
        elif file.FileDescription.endswith("Activated Balancing Energy and Settlement Prices v2"):
            file.Version = 2
        elif "ΕΝΕΡΓΟΠΟΙΗΜΕΝΗ ΕΝΕΡΓΕΙΑ ΚΑΙ ΤΙΜΕΣ ΕΚΚΑΘΑΡΙΣΗΣ" in file.FileDescription:
            file.Version = 1
        elif file.Version > 2:
            file.Version = 1
        
    return downloadFiles(files, force)
    
def downloadScada(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi("SystemRealizationSCADA", dateFrom, dateTo)

    return downloadFiles(files, force)

def downloadRealTimeScadaRes(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi("RealTimeScadaRes", dateFrom, dateTo)

    return downloadFiles(files, force)
    
def downloadReservoirFillingRate(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi("ReservoirFillingRate", dateFrom, dateTo)

    return downloadFiles(files, force)

def downloadAvailableTransferCapacity(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi("DailyAuctionsSpecificationsATC", dateFrom, dateTo)

    return downloadFiles(files, force)

def downloadResMvAdmie(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi("RESMV", dateFrom, dateTo)

    return downloadFiles(files, force)

def downloadLtNominations(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi("LTPTRsNominationsSummary", dateFrom, dateTo)

    return downloadFiles(files, force)
    
def downloadBalancingEnergyProducts(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi("BalancingEnergyProduct", dateFrom, dateTo)

    return downloadFiles(files, force)
    
def downloadBalancingCapacityProducts(dateFrom: str, dateTo: str, force = False):
    files = getFileUrlsFromApi("BalancingCapacityProduct", dateFrom, dateTo)

    return downloadFiles(files, force)
