host = 'ftp.meteologica.com'
username = ''
password = ''

import math
import os
from models.metadata import getMetadata, getParsingMetadataPayload
from helpers.download_helper import getFileNameFromInfo
from models.file import CustomFile
import pandas as pd
from datetime import datetime
from helpers.date_helper import getInfoFromDate
from helpers.log_helper import logException
import interface.envelope as envelope

def getHydroForecast(file: CustomFile):
    results = []
    path = getFileNameFromInfo(file)
    
    if not os.path.exists(path):
        message = f'{path} does not exist'
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)
        
    try:
        df = pd.read_csv(path, skiprows=5, delimiter=';')
    except Exception as e:
        message = f'Failed to read {path}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    try:
        for row in df.iterrows():
            dispatchDay, dispatchPeriod = getInfoFromDate(datetime.strptime(row[1][0], '%Y-%m-%d %H:%M'))
            #timezone = row[1][1]
            value = row[1][4]
            for i in range(4):
                results.append({
                    'dispatchDay': dispatchDay,
                    'dispatchPeriod': 4*dispatchPeriod-(3-i),
                    'zoneId': 1,
                    'version': file.PublicationDate,
                    'value': value
                })

        return envelope.getSuccessEnvelope(getParsingMetadataPayload(results, getMetadata(True, file.Id)))
    except Exception as e:
        message = f'Failed to parse {path}: {e}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

def getPowerDemand(file: CustomFile):
    results = []
    path = getFileNameFromInfo(file)
    
    if not os.path.exists(path):
        message = f'{path} does not exist'
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)
        
    try:
        df = pd.read_csv(path, skiprows=4, delimiter=';')
    except Exception as e:
        message = f'Failed to read {path}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    try:
        for row in df.iterrows():
            dispatchDay, dispatchPeriod = getInfoFromDate(datetime.strptime(row[1][0], '%Y-%m-%d %H:%M'))
            #timezone = row[1][1]
            perc10 = row[1][4]
            value = row[1][5]
            perc90 = row[1][6]
            
            for i in range(4):
                results.append({
                    'dispatchDay': dispatchDay,
                    'dispatchPeriod': 4*dispatchPeriod-(3-i),
                    'version': file.PublicationDate,
                    'zoneId': 1,
                    'perc10': None if math.isnan(perc10) else perc10,
                    'value': value,
                    'perc90': None if math.isnan(perc90) else perc90,
                })

        return envelope.getSuccessEnvelope(getParsingMetadataPayload(results, getMetadata(True, file.Id)))
    except Exception as e:
        message = f'Failed to parse {path}: {e}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

def getPVProduction(file: CustomFile):
    results = []
    path = getFileNameFromInfo(file)
    
    if not os.path.exists(path):
        message = f'{path} does not exist'
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)
        
    try:
        df = pd.read_csv(path, skiprows=5, delimiter=';')
    except Exception as e:
        message = f'Failed to read {path}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    try:
        for row in df.iterrows():
            dispatchDay, dispatchPeriod = getInfoFromDate(datetime.strptime(row[1][0], '%Y-%m-%d %H:%M'))
            #timezone = row[1][1]
            perc10 = row[1][4]
            value = row[1][5]
            perc90 = row[1][6]
            for i in range(4):
                results.append({
                    'dispatchDay': dispatchDay,
                    'dispatchPeriod': 4*dispatchPeriod-(3-i),
                    'version': file.PublicationDate,
                    'zoneId': 1,
                    'perc10': perc10,
                    'value': value,
                    'perc90': perc90,
                })

        return envelope.getSuccessEnvelope(getParsingMetadataPayload(results, getMetadata(True, file.Id)))
    except Exception as e:
        message = f'Failed to parse {path}: {e}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

def getWindProduction(file: CustomFile):
    results = []
    path = getFileNameFromInfo(file)
    
    if not os.path.exists(path):
        message = f'{path} does not exist'
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)
        
    try:
        df = pd.read_csv(path, skiprows=5, delimiter=';')
    except Exception as e:
        message = f'Failed to read {path}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    try:
        for row in df.iterrows():
            dispatchDay, dispatchPeriod = getInfoFromDate(datetime.strptime(row[1][0], '%Y-%m-%d %H:%M'))
            #timezone = row[1][1]
            perc10 = row[1][4]
            value = row[1][5]
            perc90 = row[1][6]
            for i in range(4):
                results.append({
                    'dispatchDay': dispatchDay,
                    'dispatchPeriod': 4*dispatchPeriod-(3-i),
                    'version': file.PublicationDate,
                    'zoneId': 1,
                    'perc10': perc10,
                    'value': value,
                    'perc90': perc90,
                })

        return envelope.getSuccessEnvelope(getParsingMetadataPayload(results, getMetadata(True, file.Id)))
    except Exception as e:
        message = f'Failed to parse {path}: {e}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)