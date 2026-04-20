from helpers.download_helper import *
from helpers.metadata_helper import *

import pandas as pd
import math
from models.file import CustomFile
from models.metadata import *
import pytz

def getMarketAggregatedCurvesFromFile(file: CustomFile):
    results = []

    path = getFileNameFromInfo(file)
    if not os.path.exists(path):
        message = f'{path} does not exist'
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    Columns = ["Market", "Side", "DispatchDay", "DeliveryMTU",
        "Sort", "DeliveryDuration", "AA", "Quantity", "UnitPrice", "PublicationTime", "Version"]

    try:
        DataFrame = pd.read_excel(path, names = Columns)
    except Exception as e:
        message = f'Failed to read {path}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    try:
        for _, row in DataFrame.iterrows():
            dispatchDate = datetime.strptime(str(row['DispatchDay']), '%Y%m%d')
            
            try:
                deliveryMtu = datetime.strptime(str(row['DeliveryMTU']), '%Y/%m/%d %H:%M:%S')
            except Exception as e:
                logException(e)
                deliveryMtu = datetime.strptime(str(row['DeliveryMTU']), '%Y-%m-%d %H:%M:%S')

            for i in range(1, 5):
                results.append({
                    'DispatchDay': dispatchDate,
                    'DispatchPeriod': deliveryMtu.hour * 4 + i,
                    'MarketName' : row['Market'],
                    'Side' : 1 if row['Side'] == 'Buy' else 2,
                    'AA' : int(row['AA']),
                    'Quantity' : float(row['Quantity']),
                    'UnitPrice' : float(row['UnitPrice']),
                    'PublicationTime' : datetime.strptime(str(row['PublicationTime']), '%Y/%m/%d %H:%M:%S'),
                    'Version' : row['Version'],
                    'FileId': file.Id
                })
        
        return envelope.getSuccessEnvelope(getParsingMetadataPayload(results, getMetadata(True, file.Id)))
    except Exception as e:
        message = f'Failed to parse {path}: {e}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

def getMarketBlockOrderAcceptanceStatusFromFile(file: CustomFile):
    results = []

    path = getFileNameFromInfo(file)
    if not os.path.exists(path):
        message = f'{path} does not exist'
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    Columns = ["Target", "BiddingZone", "Side", "DispatchDay", "Classification",
        "DeliveryMTU", "DeliveryDuration", "Sort", "TotalOrders", "TotalQuantity", "MatchedOrders", "MatchedQuantity", "PublicationTime", "Version"]

    try:
        DataFrame = pd.read_excel(path, names = Columns)
    except Exception as e:
        message = f'Failed to read {path}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    try:
        for _, row in DataFrame.iterrows():
            if type(row['Classification']) is float and math.isnan(row['Classification']):
                continue
            dispatchDate = datetime.strptime(str(row['DispatchDay']), '%Y%m%d')
            deliveryMtu = datetime.strptime(str(row['DeliveryMTU']), '%Y-%m-%d %H:%M:%S')

            for i in range(1, 5):
                results.append({
                    'DispatchDay': dispatchDate,
                    'DispatchPeriod': deliveryMtu.hour * 4 + i,
                    'Side' : 1 if row['Side'] == 'Buy' else 2,
                    'ClassificationName' : row['Classification'],
                    'ZoneName' : row['BiddingZone'],
                    'TotalOrders' : int(row['TotalOrders']),
                    'TotalQuantity' : float(row['TotalQuantity']),
                    'MatchedOrders' : int(row['MatchedOrders']),
                    'MatchedQuantity' : float(row['MatchedQuantity']),
                    'PublicationTime' : datetime.strptime(str(row['PublicationTime']), '%Y-%m-%d %H:%M:%S'),
                    'Version' : row['Version'],
                    'FileId': file.Id
                })
        
        return envelope.getSuccessEnvelope(getParsingMetadataPayload(results, getMetadata(True, file.Id)))
    except Exception as e:
        message = f'Failed to parse {path}: {e}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

def getResultsFromFile(file: CustomFile):
    results = []

    path = getFileNameFromInfo(file)
    if not os.path.exists(path):
        message = f'{path} does not exist'
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    isXBID = file.FileType == 'HenexMarketResultsXBID'
    if isXBID:
        Columns = ["Market", "Zone", "Side", "DispatchDay", "AssetType", "Classification", "DeliveryMTU", "ContractId", "DeliveryDuration",
            "VWAP", "MinPrice", "MaxPrice", "TotalTrades", "Version", "PublicationTime"]
    else:
        Columns = ["Market", "Zone", "Side", "DispatchDay", "AssetType", "Classification", "DeliveryMTU", "DeliveryDuration",
            "Sort", "MCP", "TotalTrades", "PublicationTime", "Version"]

    try:
        DataFrame = pd.read_excel(path, names = Columns)
    except Exception as e:
        message = f'Failed to read {path}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    try:
        for _, row in DataFrame.iterrows():
            dispatchDate = datetime.strptime(str(row['DispatchDay']), '%Y%m%d') if not isXBID else datetime.strptime(str(row['DispatchDay']), '%Y-%m-%d %H:%M:%S')
            deliveryMtu = datetime.strptime(str(row['DeliveryMTU']), '%Y-%m-%d %H:%M:%S')
            for i in range(1, 5):
                results.append({
                    'DispatchDay': dispatchDate,
                    'DispatchPeriod': deliveryMtu.hour * 4 + i,
                    'MarketName' : row['Market'],
                    'ZoneName': row['Zone'],
                    'Side' : row['Side'],
                    'AssetTypeName' : row['AssetType'],
                    'ClassificationName' : row['Classification'],
                    'MCP' : row['MCP'] if 'MCP' in row else row['VWAP'],
                    'TotalTrades' : float(row['TotalTrades']),
                    'PublicationTime' : datetime.strptime(str(row['PublicationTime']), '%Y-%m-%d %H:%M:%S'),
                    'Version' : row['Version'],
                    'FileId': file.Id,
                    'MinPrice': row['MinPrice'] if 'MinPrice' in row else None,
                    'MaxPrice': row['MaxPrice'] if 'MaxPrice' in row else None,
                    'ContractId': row['ContractId'] if 'ContractId' in row else None,
                })

        results = applyMarketDataTransformation(results)
        return envelope.getSuccessEnvelope(getParsingMetadataPayload(results, getMetadata(True, file.Id)))
    except Exception as e:
        message = f'Failed to parse {path}: {e}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

def getMarketResultsDataFromRange(df : pd.DataFrame, begin, end, Date, array):
    for i in range(begin + 1, end):
        transition_timedelta = 0
        data = df.iloc[i, :].values

        for h in range(1, 25):
            try:
                NewDate = pytz.timezone('CET').localize(Date + timedelta(hours = h - 1 + transition_timedelta), is_dst=None)
            except pytz.NonExistentTimeError:
                transition_timedelta = 1
                NewDate = pytz.timezone('CET').localize(Date + timedelta(hours = h - 1 + transition_timedelta), is_dst=None)
            except pytz.AmbiguousTimeError:
                transition_timedelta = -1
                continue

            DispatchDate = NewDate.date() 
            DispatchPeriod = NewDate.hour + 1

            row = [DispatchDate, DispatchPeriod, data[0]]
            if type(data[0]) is not str and math.isnan(data[0]):
                continue
            row.append(data[h])
            array.append(row)

def getResultsSummaryFromFile(file: CustomFile):
    results = []

    path = getFileNameFromInfo(file)
    if not os.path.exists(path):
        message = f'{path} does not exist'
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    try:
        Imports_DataFrame = pd.read_excel(path, sheet_name = 0, verbose = True)
        Exports_DataFrame = pd.read_excel(path, sheet_name = 1, verbose = True)
    except Exception as e:
        message = f'Failed to read {path}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)
    Date = file.TargetDateFrom

    # BORDER IMPORTS (IMPLICIT)
    Implicit_BORDER_Imports_index = Imports_DataFrame[Imports_DataFrame.iloc[:, 0].str.contains('IMPLICIT') == True].index[0]
    Implicit_Imports_index = Imports_DataFrame[(Imports_DataFrame.iloc[:, 0].str.contains('IMPLICIT') == True)].index[1]
    Imports = []
    getMarketResultsDataFromRange(Imports_DataFrame, Implicit_BORDER_Imports_index, Implicit_Imports_index, Date, Imports)
    
    # BORDER EXPORTS (IMPLICIT)
    Implicit_BORDER_Exports_index = Exports_DataFrame[Exports_DataFrame.iloc[:, 0].str.contains('IMPLICIT') == True].index[0]
    Implicit_Exports_index = Exports_DataFrame[(Exports_DataFrame.iloc[:, 0].str.contains('IMPLICIT') == True)].index[1]
    Exports = []
    getMarketResultsDataFromRange(Exports_DataFrame, Implicit_BORDER_Exports_index, Implicit_Exports_index, Date, Exports)
    
    # MCP
    MCP_index = Exports_DataFrame[Exports_DataFrame.iloc[:, 0].str.contains('Clearing Price') == True].index[0]
    MCPs = []
    getMarketResultsDataFromRange(Exports_DataFrame, MCP_index, MCP_index + 2, Date, MCPs)

    try:
        for i in range(len(Imports)):
            for period in range(1, 5): 
                results.append({
                        'DispatchDay': Imports[i][0],
                        'DispatchPeriod': (int(Imports[i][1]) - 1) * 4 + period,
                        'MarketName' : "DAM",
                        'ZoneName': "Crete" if Imports[i][2] == 'CR-GR' else "Mainland Greece",
                        'CountryName': "GR" if Imports[i][2] == 'CR-GR' else Imports[i][2].split("-")[0],
                        'Side' : 2, #SELL
                        'AssetTypeName' : "COUNTRY",
                        'ClassificationName' : "Imports (Implicit)",
                        'MCP' : float(MCPs[i % 24][3]) if not math.isnan(MCPs[i % 24][3]) else 0,
                        'TotalTrades' : float(Imports[i][3]) if not math.isnan(Imports[i][3]) else 0,
                        'PublicationTime' : file.PublicationDate,
                        'FileId': file.Id,
                        'Version': file.Version
                    })
            
        for i in range(len(Exports)):
            for period in range(1, 5): 
                results.append({
                        'DispatchDay': Exports[i][0],
                        'DispatchPeriod': (int(Exports[i][1]) - 1) * 4 + period,
                        'MarketName' : "DAM",
                        'ZoneName': "Crete" if Exports[i][2] == 'GR-CR' else "Mainland Greece",
                        'CountryName': "GR" if Exports[i][2] == 'GR-CR' else Exports[i][2].split("-")[1],
                        'Side' : 1, #BUY
                        'AssetTypeName' : "COUNTRY",
                        'ClassificationName' : "Exports (Implicit)",
                        'MCP' : float(MCPs[i % 24][3]) if not math.isnan(MCPs[i % 24][3]) else 0,
                        'TotalTrades' : float(Exports[i][3]) if not math.isnan(Exports[i][3]) else 0,
                        'PublicationTime' : file.PublicationDate,
                        'FileId': file.Id,
                        'Version': file.Version
                    })

        return envelope.getSuccessEnvelope(getParsingMetadataPayload(results, getMetadata(True, file.Id)))
    except Exception as e:
        message = f'Failed to parse {path}: {e}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

def getDAMDataFromFile(file: CustomFile):
    results = []

    path = getFileNameFromInfo(file)
    if not os.path.exists(path):
        message = f'{path} does not exist'
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    Columns = ["Market", "Side", "DispatchDay", "AssetType", "Classification", "DeliveryMTU", "DeliveryDuration",
        "Sort", "TotalOrders", "PublicationTime", "Version"]

    try:
        DataFrame = pd.read_excel(path, names = Columns)
    except Exception as e:
        message = f'Failed to read {path}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    try:
        for _, row in DataFrame.iterrows():
            dispatchDate = datetime.strptime(str(row['DispatchDay']), '%Y%m%d')
            deliveryMtu = datetime.strptime(str(row['DeliveryMTU']), '%Y-%m-%d %H:%M:%S')
            for i in range(1, 5):
                results.append({
                    'DispatchDay': dispatchDate,
                    'DispatchPeriod': deliveryMtu.hour * 4 + i,
                    'MarketName' : row['Market'],
                    'Side' : row['Side'],
                    'FileId': file.Id,
                    'AssetTypeName' : row['AssetType'],
                    'ClassificationName' : row['Classification'],
                    'TotalOrders' : float(row['TotalOrders']),
                    'PublicationTime' : datetime.strptime(str(row['PublicationTime']), '%Y-%m-%d %H:%M:%S'),
                    'Version' : row['Version']
                })

        results = applyMarketDataTransformation(results)
        return envelope.getSuccessEnvelope(getParsingMetadataPayload(results, getMetadata(True, file.Id)))
    except Exception as e:
        message = f'Failed to parse {path}: {e}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)