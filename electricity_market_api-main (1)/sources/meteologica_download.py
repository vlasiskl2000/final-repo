import os

host = 'sftp.meteologica.com'
username = os.getenv('METEOLOGICA_USERNAME', '')
password = os.getenv('METEOLOGICA_PASSWORD', '')
port = 2121

import pysftp
from helpers.log_helper import logException
import os
import pathlib
from helpers.download_helper import getPathInformation
import io, pandas as pd
from models.file import CustomFile
from datetime import datetime
import interface.envelope as envelope

def getFiles(basePath: str, fileType: str, targetDateFrom: datetime, targetDateTo: datetime, skiprows=4, queryStartsWith: str = None):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Disable host key checking.
    
    with pysftp.Connection(host, username=username, password=password, port=port, cnopts=cnopts) as sftp:
        sftp.cwd(basePath)
        
        fileObjects = []
        success = 0
        failed = 0
        
        for filename in sftp.listdir():
            if queryStartsWith is not None and not filename.startswith(queryStartsWith):
                continue
            if filename.endswith('.csv'):
                try:
                    with sftp.open(filename) as download_file:
                        df = pd.read_csv(download_file, skiprows=skiprows, delimiter=';')
                        column = 'From yyyy-mm-dd hh:mm'
                        target_date_from = datetime.strptime(df[column].values[0], '%Y-%m-%d %H:%M')
                        target_date_to = datetime.strptime(df[column].values[-1], '%Y-%m-%d %H:%M')

                        if targetDateFrom <= target_date_to.date() and targetDateTo >= target_date_from.date():
                            download_file.seek(0)
                            version = datetime.strptime(filename.split('_')[-1].split('.')[0], '%Y%m%d%H%M')

                            file = CustomFile(
                                FileName=filename,
                                FileDescription=filename,
                                Version=1,
                                Url=filename,
                                PublicationDate=version,
                                TargetDateFrom=target_date_from.date(),
                                TargetDateTo=target_date_to.date(),
                                FileType=fileType,
                                Id=0,
                                ByUser=False
                            )

                            file.StatusCode = downloadFileFromBytes(download_file, file, fileType)
                            file.Success = True
                            fileObjects.append(file)

                            success += 1
                except Exception as e:
                    logException(e)
                    file.Success = False
                    file.Log = str(e)
                    file.StatusCode = 3

                    failed += 1
                    fileObjects.append(file)

    if success == 0 and failed == 0:
        return envelope.getFailedEnvelope(fileObjects, 100, 'No files found')
    elif success == 0 and failed != 0:
        return envelope.getFailedEnvelope(fileObjects, 104, 'Failed to downloaded all files')
    elif success != 0 and failed == 0:
        return envelope.getSuccessEnvelope(fileObjects, False)
    else:
        return envelope.getSuccessEnvelope(fileObjects, True)

def downloadFileFromBytes(bytes_file, file: CustomFile, folder):
    fileType = file.FileType
    targetDateFrom = file.TargetDateFrom
    targetDateTo = file.TargetDateTo
    url = file.Url

    folder, _, path = getPathInformation(fileType, targetDateFrom, targetDateTo, url)
    
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    with open(path, "wb") as f:
        f.write(bytes_file.read())

def getHydroForecastFiles(targetDateFrom: datetime, targetDateTo: datetime):
    return getFiles('/20160301/Europe/Greece/Hydro/PowerGeneration/TotalHydro/Forecast/Meteologica/Total/Hourly/', 'MeteologicaHydro', targetDateFrom, targetDateTo)

def getPowerDemandFiles(targetDateFrom: datetime, targetDateTo: datetime):
    return getFiles('/20160301/Europe/Greece/PowerDemand/Forecast/Meteologica/Total/Hourly/', 'MeteologicaPowerDemand', targetDateFrom, targetDateTo)
    
def getPVProductionFiles(targetDateFrom: datetime, targetDateTo: datetime):
    return getFiles('/20160301/Europe/Greece/PV/PowerGeneration/Forecast/Meteologica/Total/Hourly/', 'MeteologicaPVProduction', targetDateFrom, targetDateTo, skiprows=5)
    
def getWindProductionFiles(targetDateFrom: datetime, targetDateTo: datetime):
    return getFiles('/20160301/Europe/Greece/Wind/PowerGeneration/Forecast/Meteologica/Total/Hourly/', 'MeteologicaWindProduction', targetDateFrom, targetDateTo, skiprows=5)
