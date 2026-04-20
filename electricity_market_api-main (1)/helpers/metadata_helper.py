from datetime import datetime
from helpers.scada_helper import isParsableToInt
from models.file import CustomFile

import numpy
from helpers.date_helper import *
import pandas as pd

import os
from dotenv import load_dotenv

import requests
from models.file import CustomFile

STEPS_BACK_RESIDENTIAL = 48
STEPS_FORWARD_FOR_DAY_AHEAD_RESIDENTIAL = 42
SKIP_STEPS_FOR_DAY_AHEAD_RESIDENTIAL = 18

STEPS_BACK_INDUSTRIAL = 48
STEPS_FORWARD_FOR_DAY_AHEAD_INDUSTRIAL = 38
SKIP_STEPS_FOR_DAY_AHEAD_INDUSTRIAL = 14

STEPS_FORWARD_FOR_INTRADAY = 24
SKIP_STEPS_FOR_INTRADAY = 0

SPLIT_PERCENTAGE = 0.8
N_EPOCHS = 200
TUNER_PATIENCE = 1
PATIENCE = 15
N_MEMBERS = 2 #4 #Number of models that will be used from tuner
MODELS_FOLDER = 'TrainedModels'

FILE_SEPARATOR = os.sep # For Docker (linux) it should be /

RESERVE_TYPE = {
    "mFRR":  1,
    "aFRR": 2,
    "FCR": 3,
    "RR": 4,
    "sRR": 5
}

RESERVE_TYPE_ENTSOE = {
    "mFRR":  1,
    "aFRR": 2,
    "FCR": 3,
    "mFRRSA": 4,
    "mFRRDA": 5,
    "aFRRCS": 6,
    "aFRRLA": 7
}

EntityStateByColor = {
    "indexed": 0, ##Market
    "8EA9DB": 1,##Comissioning
    "FFC7CE": 2,##Unavailable
    "FFF2CC": 3,## Sync, Soak or Transition
    "D9D9D9": 4 ##Generic Constraint
}

def getISPFileDateVersion(file, versionIndex = 1, splitOn = "_"):
    fileName = file["FileName"]
    nameData = fileName.split(splitOn)

    #dispatchDate = datetime.strptime(nameData[dateIndex].replace('-DR',''), dateFormat).date()
    version = file["PublicationDate"]
    versionNum = int(nameData[-1])

    if versionNum == 1:
        versionDescription = nameData[versionIndex]
    else: # ISP1 v2
        versionDescription = nameData[versionIndex] + ' ' + str(versionNum)
        
    return version, versionDescription

def getISPResultsMetadata(file: dict):
    if 'DR' in file['FileName'] or ' ' not in file['FileName']:
        if 'Ad-hoc' in file['FileName']:
            return getISPFileDateVersion(file, 0, None, splitOn='_')
        return getISPFileDateVersion(file, 0, 3, splitOn='_')

    if 'Ad-hoc' in file['FileName']:
        return getISPFileDateVersion(file, 0, None)

    return getISPFileDateVersion(file, 0)

def getISPLoadOrResMetadata(file: dict):
    fileData = file['FileName'].split(' ')

    date = datetime.strptime(fileData[0], '%Y%m%d')
    version = datetime.strptime(file['PublicationDate'], '%d.%m.%Y %H:%M')
    
    if len(fileData) > 5:
        versionDescription = f'{fileData[1]} {fileData[5]}'
    else:
        versionDescription = fileData[1]

    return date, version, versionDescription



def getISPRequirementsGeneratedLinks(dateStart: datetime, dateEnd: datetime):
    # https://www.admie.gr/sites/default/files/attached-files/type-file/2021/03/20210317_ISP1Requirements_01.xlsx
    date = dateStart
    fileInfo = []
    while date <= dateEnd:
        previousDay = date + timedelta(days = -1)

        suffix = 'ISPRequirements' if date < datetime(2020, 11, 13) else 'Requirements'
        fileInfo.append(CustomFile(
            FileName = f"{date.strftime('%Y%m%d')}_ISP1_{suffix}_01.xlsx",
            FileDescription = f"{date.strftime('%Y%m%d')} ISP1 {suffix}",
            Url = f"https://www.admie.gr/sites/default/files/attached-files/type-file/{previousDay.strftime('%Y')}/{previousDay.strftime('%m')}/{date.strftime('%Y%m%d')}_ISP1{suffix}_01.xlsx",
            PublicationDate = datetime.strptime(f"{previousDay.strftime('%d.%m.%Y')} 13:35", '%d.%m.%Y %H:%M'),
            TargetDateFrom = datetime.strptime(date.strftime('%d.%m.%Y'), '%d.%m.%Y'),
            TargetDateTo = datetime.strptime(date.strftime('%d.%m.%Y'), '%d.%m.%Y'),
            FileType = 'ISP1Requirements',
            Id=0
        ))

        if date.year == 2020 and date.month == 11 and date.day == 2:
            fileInfo.append(CustomFile(
            FileName = f"{date.strftime('%Y%m%d')}_ISP1_{suffix}_02",
            FileDescription = f"{date.strftime('%Y%m%d')} ISP1 {suffix} v2",
            Url = f"https://www.admie.gr/sites/default/files/attached-files/type-file/{previousDay.strftime('%Y')}/{previousDay.strftime('%m')}/{date.strftime('%Y%m%d')}_ISP1{suffix}_05.xlsx",
            PublicationDate = datetime.strptime(f"{previousDay.strftime('%d.%m.%Y')} 18:35", '%d.%m.%Y %H:%M'),
            TargetDateFrom = datetime.strptime(date.strftime('%d.%m.%Y'), '%d.%m.%Y'),
            TargetDateTo = datetime.strptime(date.strftime('%d.%m.%Y'), '%d.%m.%Y'),
            FileType = 'ISP1Requirements',
            Id=0))

        else:
            fileInfo.append(CustomFile(
            FileName = f"{date.strftime('%Y%m%d')}_ISP1_{suffix}_02",
            FileDescription = f"{date.strftime('%Y%m%d')} ISP1 {suffix} v2",
            Url = f"https://www.admie.gr/sites/default/files/attached-files/type-file/{previousDay.strftime('%Y')}/{previousDay.strftime('%m')}/{date.strftime('%Y%m%d')}_ISP1{suffix}_02.xlsx",
            PublicationDate = datetime.strptime(f"{previousDay.strftime('%d.%m.%Y')} 18:35", '%d.%m.%Y %H:%M'),
            TargetDateFrom = datetime.strptime(date.strftime('%d.%m.%Y'), '%d.%m.%Y'),
            TargetDateTo = datetime.strptime(date.strftime('%d.%m.%Y'), '%d.%m.%Y'),
            FileType = 'ISP1Requirements',
            Id=0))

        fileInfo.append(CustomFile(
            FileName = f"{date.strftime('%Y%m%d')}_ISP2_{suffix}_01.xlsx",
            FileDescription = f"{date.strftime('%Y%m%d')} ISP2 {suffix}",
            Url = f"https://www.admie.gr/sites/default/files/attached-files/type-file/{previousDay.strftime('%Y')}/{previousDay.strftime('%m')}/{date.strftime('%Y%m%d')}_ISP2{suffix}_01.xlsx",
            PublicationDate = datetime.strptime(f"{previousDay.strftime('%d.%m.%Y')} 21:05", '%d.%m.%Y %H:%M'),
            TargetDateFrom = datetime.strptime(date.strftime('%d.%m.%Y'), '%d.%m.%Y'),
            TargetDateTo = datetime.strptime(date.strftime('%d.%m.%Y'), '%d.%m.%Y'),
            FileType = 'ISP2Requirements',
            Id=0))

        fileInfo.append(CustomFile(
            FileName = f"{date.strftime('%Y%m%d')}_ISP3_{suffix}_01.xlsx",
            FileDescription = f"{date.strftime('%Y%m%d')} ISP3 {suffix}",
            Url = f"https://www.admie.gr/sites/default/files/attached-files/type-file/{previousDay.strftime('%Y')}/{previousDay.strftime('%m')}/{date.strftime('%Y%m%d')}_ISP1{suffix}_05.xlsx",
            PublicationDate = datetime.strptime(f"{date.strftime('%d.%m.%Y')} 09:05", '%d.%m.%Y %H:%M'),
            TargetDateFrom = datetime.strptime(date.strftime('%d.%m.%Y'), '%d.%m.%Y'),
            TargetDateTo = datetime.strptime(date.strftime('%d.%m.%Y'), '%d.%m.%Y'),
            FileType = 'ISP3Requirements',
            Id=0))

        date += timedelta(days = 1)

    for row in fileInfo:
        response = requests.get(row.Url, verify=False)
        if response.status_code != 200:
            fileInfo.remove(row)

    return fileInfo

def getISPResultsColouredFlagDataFromRange(df: pd.DataFrame, dfStyle, begin: int, end: int, array: list, color:str, end_offset=0):
    for i in range(begin, end - end_offset):
        data = df.iloc[i, :].values

        for j in range(1, len(df.columns) - 1):
            dispatchDateStr, hour = getInfoFromDate(df.columns[j], False, True)

            if dispatchDateStr is None:
                continue

            cellColour = dfStyle.active.cell(i+3,j+1).fill.start_color
            row = [dispatchDateStr, hour, data[0], False if cellColour.type == 'indexed' else cellColour.index[2:] == color]

            array.append(row)

def applyMarketDataTransformation(data: list) -> list:
    for item in data:
        item['Side'] = 1 if item['Side'] == 'Buy' else 2

        classification = item['ClassificationName']

        # Naming compatibility fix between different files
        if classification == 'Import':
            classification = 'Imports'
        elif classification == 'Export':
            classification = 'Exports'
        item['ClassificationName'] = classification
        
        if item['ClassificationName'] == 'Imports' or item['ClassificationName'] == 'Exports':
            countryCode = item['AssetTypeName'].replace('-GR', '').replace('GR-','')

            item['AssetTypeName'] = 'COUNTRY'
            item['CountryName'] = countryCode
        else:
            item['CountryName'] = 'GR'

    return data

def getISPResultsDataFromRange(df: pd.DataFrame, begin, end, type, array, end_offset=0, start_col = 1, entityName_col = 0, dispatchDateStr = None, rawHour = False, split_name = False, dfStyle = None, skipLastColumn=True, entitySuffix = None, breakOnTotals = True):
    """
        df: the dataframe that includes the data we are intrested to parse
        begin: the index of row we want to start parsing from
        end: the index of row we want to stop parsing from
        type:
        array: the array that the function will fill with the wanted data
        end_offset: the number of last colums that will be skiped while parsing 
        start_col: the number of begining colums that will be skiped while parsing 
        entityName_col: the column that contains the Entity's name
        dispatchDateStr: datetime to be used if we know the dispatchdate before the data transformation
        rawHour: flag to note that the period info is in raw form (e.g 1, 2, 3 ...)
        dfStyle: reffers to the styling of the excel, to be able to extract the entity's state from isp cells coloring
        skipLastColumn: flag to note that the last column should be skiped
        entitySuffix: the suffix that will be added to the entity's name, if any
        breakOnTotals: flag to note that the parsing should stop when the totals row are reached (searching for Total or Aggregated)
    """
    hours = df.iloc[0, 1:].values if rawHour else df.iloc[0, :].values
    outer_break = False
    starting_period_formatted = None
    for i in range(begin, end - end_offset):

        if outer_break:
            break

        data = df.iloc[i, :].values
        
        starting_period = df.columns[start_col]
        
        #if clause for handling Winter DST issues
        if isinstance(starting_period,str) and len(starting_period) != 5:
                    starting_period = starting_period[:-2]

        if isinstance(starting_period, str) :
            starting_period = dispatchDateStr.strftime('%Y-%m-%dT') + starting_period
            if not checkDateToString(starting_period):
                continue

        starting_period_pd = pd.Timestamp(starting_period)

        #dynacally set the market granularity based on the date of market change
        market_granularity_seconds = 1800 if starting_period_pd < pd.Timestamp('2025-10-01') else 900
        period = getTimeDifferncePandas(starting_period_pd, seconds_granularity = market_granularity_seconds)

        for j in range(start_col, len(df.columns) - (1 if skipLastColumn else 0)):


            dispatchDate = getDate(df.columns[j])

            if dispatchDate is None:
                continue

            entityExtraInfo = None
            if entityName_col is not None:
                entityInfo = data[entityName_col]
                if breakOnTotals and (entityInfo.lower() == 'total' or entityInfo.lower().endswith(' total') or entityInfo.lower().startswith('total ') or entityInfo.lower() == 'aggregated' or entityInfo.lower().endswith(' aggregated') or entityInfo.lower().startswith('aggregated ')):
                    outer_break = True
                    break

                entity = entityInfo if not split_name else entityInfo.split(' ')[0]
                if entitySuffix is not None and not entity.endswith(entitySuffix):
                    entity = f"{entity}{entitySuffix}"
                
                if split_name:
                    entityExtraInfo = entityInfo.split(' ')[1]

            if type is None and entityName_col is not None:
                row = [dispatchDate, period, entity]
            elif type is not None and entityName_col is not None:
                row = [dispatchDate, period, type, entity]
            elif entityName_col is None:
                row = [dispatchDate, period]

            if entityExtraInfo is not None:
                row.append(entityExtraInfo)

            row.append(data[j])

            if dfStyle is not None:
                cellColour = dfStyle.active.cell(i+3,j+1).fill.start_color
                row.append(EntityStateByColor['indexed' if cellColour.type == 'indexed' or cellColour.index[2:] == '000000' else cellColour.index[2:]])

            array.append(row)
            period += 1
            

def getCCGTFromRange(df : pd.DataFrame, begin, end, array):
    hours = df.iloc[0, 1:].values
    first_hours = hours[0]
    first_hours_pd = pd.Timestamp(first_hours)
    dispatchPeriod = getTimeDifferncePandas(first_hours_pd, seconds_granularity = 1800)
    for i in range(begin, end, 3):
        gtData = df.iloc[i, :].values
        stData = df.iloc[i + 1, :].values
        DispatchPeriod = getTimeDifferncePandas(first_hours_pd, seconds_granularity = 1800)
        for h in range(1, df.shape[1]):
            
            gtNan = type(gtData[h]) == float or type(gtData[h]) == numpy.float64
            stNan = type(stData[h]) == float or type(stData[h]) == numpy.float64
            hourNan = type(hours[h-1]) == float or type(hours[h-1]) == numpy.float64
            if hourNan:
                continue

            date = hours[h-1]
            DispatchDate = date.date()

            if gtNan:
                row = [DispatchDate, DispatchPeriod, gtData[0], None]
            else:
                temp = stData[h].replace("+","_") if not stNan else ""
                gasExtension = gtData[0] + "_G" if gtData[0] != "ALOUMINIO" and gtData[0] != "MEGALOPOLI_V" else gtData[0]
                row = [DispatchDate, DispatchPeriod, gtData[0], f"{gasExtension}_{gtData[h]}{temp}"] # Date, Period, PrimaryEntityName, VirtualEntityName
            
            array.append(row)
            DispatchPeriod += 1
            

# ENTSOE
SecurityTokens = [
    '8ea0a69d-d660-4355-873c-da4e75f6a28f', 
    '888606bf-7d27-4fb2-bfbc-d83de5c8998a', 
    '4d11b533-c83b-4ca2-8e16-7ad3cc16d41b',
    '52421d8d-c7fa-4851-a8f3-0386a367d994',
    'c2c314ee-aa64-4c33-a53d-90a9bd851fa0',
    'e5c034e3-d7e5-465d-9932-75d56ba43d1e',
    '38aa6344-c508-44aa-ae32-b97a512c7277',
    "54b5bbbb-4a0d-4e2f-8e93-6f683d07bc33"
]

Endpoint = 'https://web-api.tp.entsoe.eu/api'

GreeceBiddingZone = '10YGR-HTSO-----Y'
BulgaryBiddingZone = '10YCA-BULGARIA-R'
FYROMBiddingZone = '10YMK-MEPSO----8'
AlbaniaBiddingZone = '10YAL-KESH-----5'
TurkeyBiddingZone = '10YTR-TEIAS----W'
HungaryBiddingZone = '10YHU-MAVIR----U'
SerbiaBiddingZone = '10YCS-SERBIATSOV'
RomaniaBiddingZone = '10YRO-TEL------P'
BosniaBiddingZone = '10YBA-JPCC-----D'
MontenegroBiddingZone = '10YCS-CG-TSO---S'
PolandBiddingZone = '10YPL-AREA-----S'
SlovakiaBiddingZone = '10YSK-SEPS-----K'
SloveniaBiddingZone = '10YSI-ELES-----O'
MoldovaBiddingZone = '10Y1001A1001A990'
UkraineBiddingZone = '10Y1001C--00003F'
CroatiaBiddingZone = '10YHR-HEP------M'
FranceBiddingZone = '10YFR-RTE------C'
GermanyBiddingZone = '10Y1001A1001A63L'
GermanyBiddingZone2 = '10Y1001A1001A82H'
AustriaBiddingZone = '10YAT-APG------L'
CzechBiddingZone = '10YCZ-CEPS-----N'
SwitzerlandBiddingZone = '10YCH-SWISSGRIDZ'
BelgiumBiddingZone = '10YBE----------2'
NetherlandsBiddingZone = '10YNL----------L'
SpainBiddingZone = '10YES-REE------0'
PortugalBiddingZone = '10YPT-REN------W'
ItalyBiddingZone = '10YIT-GRTN-----B'
ItalyBrindisi = '10Y1001A1001A699'
ItalyCalabria = '10Y1001C--00096J'
ItalyCenterNorth = '10Y1001A1001A70O'
ItalyCenterSouth = '10Y1001A1001A71M'
ItalyFoggiaBiddingZone = '10Y1001A1001A72K'
ItalyMaltaBiddingZone = '10Y1001A1001A877'
ItalyMonthBiddingZone = '10Y1001A1001A73I'
ItalyNorthATBiddingZone = '10Y1001A1001A80L'
ItalyNorthCHBiddingZone = '10Y1001A1001A68B'
ItalyNorthFRBiddingZone = '10Y1001A1001A81J'
ItalyNorthSIBiddingZone = '10Y1001A1001A67D'
ItalyPrioloBiddingZone = '10Y1001A1001A76C'
ItalyRossanoBiddingZone = '10Y1001A1001A77A'
ItalySacoacBiddingZone = '10Y1001A1001A885'
ItalySacodcBiddingZone = '10Y1001A1001A893'
ItalySardiniaBiddingZone = '10Y1001A1001A74G'
ItalySicilyBiddingZone = '10Y1001A1001A75E'
ItalySouthBiddingZone = '10Y1001A1001A788'
GermanyCountryBiddingZone = '10Y1001A1001A83F'
CyprusBiddingZone = '10YCY-1001A0003J'



MainBiddingZone = os.getenv('MainBiddingZone')
if MainBiddingZone is None:
    raise Exception('MainBiddingZone environment variable is not set')



COUNTRY_SHORT_CODE_MAPPING = {
    GreeceBiddingZone: 'GR',
    BulgaryBiddingZone: 'BG',
    AlbaniaBiddingZone: 'AL',
    FYROMBiddingZone: 'MK',
    TurkeyBiddingZone: 'TR',
    ItalyBiddingZone: 'IT',
    HungaryBiddingZone: 'HU',
    SerbiaBiddingZone: 'RS',
    RomaniaBiddingZone: 'RO',
    BosniaBiddingZone: 'BA',
    MontenegroBiddingZone: 'ME',
    PolandBiddingZone: 'PL',
    SlovakiaBiddingZone: 'SK',
    SloveniaBiddingZone: 'SI',
    MoldovaBiddingZone: 'MD',
    UkraineBiddingZone: 'UA',
    CroatiaBiddingZone: 'HR',
    FranceBiddingZone: 'FR',
    GermanyBiddingZone: 'DE',
    GermanyBiddingZone2: 'DE',
    AustriaBiddingZone: 'AT',
    CzechBiddingZone: 'CZ',
    SwitzerlandBiddingZone: 'CH',
    BelgiumBiddingZone: 'BE',
    NetherlandsBiddingZone: 'NL',
    SpainBiddingZone: 'ES',
    PortugalBiddingZone: 'PT',
    ItalyBiddingZone: 'IT',
    ItalyBrindisi: 'IT',
    ItalyCalabria: 'IT',
    ItalyCenterNorth: 'IT',
    ItalyCenterSouth: 'IT',
    ItalyFoggiaBiddingZone: 'IT',
    ItalyMaltaBiddingZone: 'IT',
    ItalyMonthBiddingZone: 'IT',
    ItalyNorthATBiddingZone: 'IT',
    ItalyNorthCHBiddingZone: 'IT',
    ItalyNorthFRBiddingZone: 'IT',
    ItalyNorthSIBiddingZone: 'IT',
    ItalyPrioloBiddingZone: 'IT',
    ItalyRossanoBiddingZone: 'IT',
    ItalySacoacBiddingZone: 'IT',
    ItalySacodcBiddingZone: 'IT',
    ItalySardiniaBiddingZone: 'IT',
    ItalySicilyBiddingZone: 'IT',
    ItalySouthBiddingZone: 'IT',
    GermanyCountryBiddingZone: 'DE',
}


LEV_EUR = 1.9558

RequestedCountries = [("Greece", GreeceBiddingZone),
                 ("Bulgary", BulgaryBiddingZone),
                 ("Albania", AlbaniaBiddingZone),
                 ("North Macedonia", FYROMBiddingZone),
                 ("Turkey", TurkeyBiddingZone),
                 ("Italy", ItalyBiddingZone),
                 ("Hungary", HungaryBiddingZone),
                 ("Serbia", SerbiaBiddingZone),
                 ("Romania", RomaniaBiddingZone),
                 ("Bosnia & Herzegovina", BosniaBiddingZone),
                 ("Montenegro", MontenegroBiddingZone),
                 ("Poland", PolandBiddingZone),
                 ("Slovakia", SlovakiaBiddingZone),
                 ("Slovenia", SloveniaBiddingZone),
                 ("Moldova", MoldovaBiddingZone),
                 ("Ukraine", UkraineBiddingZone),
                 ("Croatia", CroatiaBiddingZone)]

ReserveTypes = [
    ("mFRR", "A47"),
    ("mFRRSA", "A60"),
    ("mFRRDA", "A61"),
    ("aFRR", "A51"),
    ("aFRRCS", "A67"),
    ("aFRRLA", "A68"),
    ("FCR", "A52")
]
ReserveTypesPrices = [
    ("mFRR", "A97"),
    ("aFRR", "A96"),
    ("FCR", "A95")
]

# PsrType, Name
ProductionCategories = [
    ('B01', 'Biomass'),
    ('B02', 'Fossil Brown coal/Lignite'),
    ('B03', 'Fossil Coal-derived gas'),
    ('B04', 'Fossil Gas'),
    ('B05', 'Fossil Hard coal'),
    ('B06', 'Fossil Oil'),
    ('B07', 'Fossil Oil shale'),
    ('B08', 'Fossil Peat'),
    ('B09', 'Geothermal'),
    ('B10', 'Hydro Pumped Storage'),
    ('B11', 'Hydro Run-of-river and poundage'),
    ('B12', 'Hydro Water Reservoir'),
    ('B13', 'Marine'),
    ('B14', 'Nuclear'),
    ('B15', 'Other renewable'),
    ('B16', 'Solar'),
    ('B17', 'Waste'),
    ('B18', 'Wind Offshore'),
    ('B19', 'Wind Onshore'),
    ('B20', 'Other')
]

# Subset of ProductionCategories
WindSolarCategories = [('B16', 'Solar'),
              ('B18', 'Wind Offshore'),
              ('B19', 'Wind Onshore')]

DAMPriceCountries = {
    GreeceBiddingZone : [GreeceBiddingZone],
    BulgaryBiddingZone : [BulgaryBiddingZone],
    RomaniaBiddingZone : [RomaniaBiddingZone],
    HungaryBiddingZone : [HungaryBiddingZone],
    FranceBiddingZone : [FranceBiddingZone],
    SloveniaBiddingZone : [SloveniaBiddingZone],
    SlovakiaBiddingZone : [SlovakiaBiddingZone],
    AustriaBiddingZone : [AustriaBiddingZone],
    GermanyBiddingZone: [GermanyBiddingZone2],
    CzechBiddingZone : [CzechBiddingZone],
    CroatiaBiddingZone : [CroatiaBiddingZone],
    SwitzerlandBiddingZone : [SwitzerlandBiddingZone],
    PolandBiddingZone : [PolandBiddingZone],
    BelgiumBiddingZone : [BelgiumBiddingZone],
    NetherlandsBiddingZone : [NetherlandsBiddingZone],
    SpainBiddingZone : [SpainBiddingZone],
    PortugalBiddingZone : [PortugalBiddingZone],
    SerbiaBiddingZone: [SerbiaBiddingZone],
    ItalyBiddingZone: [
        ItalyBiddingZone
    ]
}

LoadCountryCodes ={
    GreeceBiddingZone : [GreeceBiddingZone],
    BulgaryBiddingZone : [BulgaryBiddingZone],
    RomaniaBiddingZone : [RomaniaBiddingZone],
    HungaryBiddingZone : [HungaryBiddingZone],
    FranceBiddingZone : [FranceBiddingZone],
    SloveniaBiddingZone : [SloveniaBiddingZone],
    SlovakiaBiddingZone : [SlovakiaBiddingZone],
    AustriaBiddingZone : [AustriaBiddingZone],
    GermanyCountryBiddingZone: [GermanyCountryBiddingZone],
    CzechBiddingZone : [CzechBiddingZone],
    CroatiaBiddingZone : [CroatiaBiddingZone],
    SwitzerlandBiddingZone : [SwitzerlandBiddingZone],
    PolandBiddingZone : [PolandBiddingZone],
    BelgiumBiddingZone : [BelgiumBiddingZone],
    NetherlandsBiddingZone : [NetherlandsBiddingZone],
    SpainBiddingZone : [SpainBiddingZone],
    PortugalBiddingZone : [PortugalBiddingZone],
    SerbiaBiddingZone: [SerbiaBiddingZone],
    ItalyBiddingZone: [
        ItalyBiddingZone
    ]
}

Feature_Countries = os.getenv('Countries')
if Feature_Countries is None or Feature_Countries == '':

    CountryPairs = [(GreeceBiddingZone, AlbaniaBiddingZone),
        (GreeceBiddingZone, FYROMBiddingZone),
        (GreeceBiddingZone, BulgaryBiddingZone),
        (GreeceBiddingZone, ItalyBiddingZone),
        (GreeceBiddingZone, TurkeyBiddingZone)]

    Countries = [GreeceBiddingZone,
        BulgaryBiddingZone,
        AlbaniaBiddingZone,
        FYROMBiddingZone,
        TurkeyBiddingZone,
        ItalyBiddingZone]


else:
    Feature_Countries = Feature_Countries.split(',')

    seen_codes = set()
    Countries = [
        country_code 
        for country_code, two_letter_code in COUNTRY_SHORT_CODE_MAPPING.items()
        if two_letter_code in Feature_Countries and two_letter_code not in seen_codes and not seen_codes.add(two_letter_code)
    ]

    CountryPairs = [(MainBiddingZone, country) for country in Countries if country != MainBiddingZone]
    

