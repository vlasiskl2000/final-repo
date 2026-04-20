from helpers.download_helper import *
from helpers.metadata_helper import *

import pandas as pd
import math

from models.file import CustomFile
from models.metadata import *

def getDAMHybridOffersFromFile(file: CustomFile):
    results = []

    path = getFileNameFromInfo(file)
    if not os.path.exists(path):
        message = f'{path} does not exist'
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    try:
        df = pd.read_excel(path, skiprows=13)
    except Exception as e:
        message = f'Failed to read {path}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)

    try:
        mtus = set()

        df = df.to_dict('records')
        for row in df:
            mtu = row['MTU']
            if mtu == 'PORTFOLIO' or mtu is None:
                continue
            if (type(mtu) is not str and math.isnan(mtu)) or (mtu in mtus):
                break
            mtus.add(mtu)
            
            for i in range(1, 25):
                if i not in row:
                    continue
                value = row[i]
                if math.isnan(value):
                    continue
                for j in range(1, 5):
                    results.append({
                        'zoneId': 1,
                        'dispatchDay': file.TargetDateFrom,
                        'version': file.PublicationDate,
                        'fileId': file.Id,
                        'market': file.Version,
                        'dispatchPeriod': 4*(i-1)+j,
                        'entityName': mtu,
                        'value': value
                    })

        return envelope.getSuccessEnvelope(getParsingMetadataPayload(results, getMetadata(True, file.Id)))
    except Exception as e:
        message = f'Failed to parse {path}: {e}'
        logException(e)
        return envelope.getFailedEnvelope(getParsingMetadataPayload([], getMetadata(False, file.Id, message)), None)