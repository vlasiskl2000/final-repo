from models.file import CustomFile
import requests
import json
import os
import interface.envelope as envelope
from helpers.log_helper import logException
from pathlib import Path
from datetime import datetime
from dateutil.parser import parse as parsedate
from dateutil import tz
from requests.models import Request
import stamina 
from datetime import timedelta

downloadFolder = '/usr/share/market_data'
fileSeparator = os.sep # For Docker (linux) it should be /

def download(url: str):
    # Session initialization
    session = requests.Session()
    session.trust_env = True

    resp = session.get(url, stream=True, verify=False)
    print(url + " --- " + str(resp.status_code))

    if resp.status_code == 200:
        return resp.content
    
    return None

@stamina.retry(on= ('FileExistsError','BlockingIOError','InterruptedError'),attempts= 3,wait_initial=0.1, wait_max=5.0)
def checkFileStatus(file, path, force, url):
    #raise FileExistsError
    if force or not file.exists():
        WriteFile(url, path)
        return 0
    elif file.exists() and file.stat().st_size == 0:
        WriteFile(url, path)
        return 1
    else:
        return 2

    
def downloadJson(url: str):
    response = download(url)
    if response is None:
        return None
    return json.loads(response)

# 0: New CustomFile downloaded
# 1: CustomFile was probably corrupt. Redownloaded
# 2: CustomFile already exists
# 3: Download Failed
def downloadFile(file, force=False):
    fileType = file.FileType
    targetDateFrom = file.TargetDateFrom
    targetDateTo = file.TargetDateTo
    url = file.Url

    folder, _, path = getPathInformation(fileType, targetDateFrom, targetDateTo, url)
    
    if not os.path.exists(folder):
        os.makedirs(folder)
    file = Path(path)
    
    checkFileStatus(file, path, force, url)

# 0: New CustomFile downloaded
# 1: CustomFile was probably corrupt. Redownloaded
# 2: CustomFile already exists
# 3: Download Failed
def importFile(fileMeatadata, file, force=False) -> int:
    fileType = fileMeatadata.FileType
    targetDateFrom = fileMeatadata.TargetDateFrom
    targetDateTo = fileMeatadata.TargetDateTo
    url = fileMeatadata.FileName

    folder, _, path = getPathInformation(fileType, targetDateFrom, targetDateTo, url)
    
    if not os.path.exists(folder):
        os.makedirs(folder)
    filePath = Path(path)
    
    if force or not filePath.exists():
        WriteFile(url, path, file)
        return 0
    elif filePath.exists() and filePath.stat().st_size == 0:
        WriteFile(url, path, file)
        return 1
    else:
        return 2

def getVirtualFileMetadataFromUrl(url: str, version: int, targetDate: datetime, fileType: str, req: Request):
    publicationDate = parsedate(req.headers['Last-Modified']).astimezone()
    fileName = url.split('/')[-1]
    to_zone = tz.tzlocal()
    publicationDate = publicationDate.astimezone(to_zone)

    return CustomFile(Id=0, Url=url, Version=version, PublicationDate=publicationDate, FileName=fileName, FileDescription=fileName, FileType=fileType, TargetDateFrom=targetDate, TargetDateTo=targetDate)

def WriteFile(url, path, file = None):
    tempFile = download(url) if file is None else file
    open(path, 'wb').write(tempFile)

def getPathInformationFromFileObject(file: CustomFile):
    fileType = file.FileType
    targetDateFrom = file.TargetDateFrom
    targetDateTo = file.TargetDateTo
    url = file.Url if file.Url is not None else file.FileName

    return getPathInformation(fileType, targetDateFrom, targetDateTo, url)

def getPathInformation(fileType, targetDateFrom, targetDateTo, url):
    folder = fileSeparator.join([downloadFolder, fileType, f"{targetDateFrom.strftime('%Y%m%d')}_{targetDateTo.strftime('%Y%m%d')}"])
    fileName = getFileNameFromUrl(url)
    path = fileSeparator.join([folder, fileName])
    return folder, fileName, path

def getFileNameFromUrl(url: str):
    parts = url.split('/')
    fileName = parts[-1]

    return fileName

def getVersionFromUrl(url: str, fileType: str):
    fileName = getFileNameFromUrl(url)
    fileInfo = fileName.split('_')
        
    versionInfo = fileInfo[-1].split('.')[0]
    try:
        return int(versionInfo)
    except:
        try:
            if fileType == 'IMBABE':
                if fileInfo[0] == 'Recalc':
                    return 2
                else:
                    return 1
        finally:
            return 1

def getFileName(url, fileName, extensionFromUrl = True):
    fileName = f'{downloadFolder}\\{fileName}'
    if extensionFromUrl:
        fileName += f'.{url.split(".")[-1]}'
    return fileName

def getFileNameFromInfo(fileInfo):
    _, _, path = getPathInformationFromFileObject(fileInfo)

    return path

def downloadFiles(files, force = False):
    success = 0
    failed = 0
    for file in files:
        file.ByUser = False
        try:
            file.StatusCode = downloadFile(file, force)
            file.Success = True

            success += 1
        except Exception as e:
            logException(e)
            file.Success = False
            file.Log = str(e)
            file.StatusCode = 3

            failed += 1

    #files = list(filter(lambda row: row.StatusCode != 2, files)) # Skip already downloaded files
    if success == 0 and failed == 0:
        return envelope.getFailedEnvelope(files, 100, 'No files found')
    elif success == 0 and failed != 0:
        return envelope.getFailedEnvelope(files, 104, 'Failed to downloaded all files')
    elif success != 0 and failed == 0:
        return envelope.getSuccessEnvelope(files, False)
    else:
        return envelope.getSuccessEnvelope(files, True)

def importFiles(fileMetadata: CustomFile, file, force = False) -> envelope.Envelope[CustomFile]:
    success = 0
    failed = 0
    fileMetadata.ByUser = True
    try:
        fileMetadata.StatusCode = importFile(fileMetadata, file, force)
        fileMetadata.Success = True

        success += 1
    except Exception as e:
        logException(e)
        fileMetadata.Success = False
        fileMetadata.Log = str(e)
        fileMetadata.StatusCode = 3

        failed += 1

    #files = list(filter(lambda row: row.StatusCode != 2, files)) # Skip already downloaded files
    if success == 0 and failed == 0:
        return envelope.getFailedEnvelope([fileMetadata], 100, 'No files found')
    elif success == 0 and failed != 0:
        return envelope.getFailedEnvelope([fileMetadata], 104, 'Failed to downloaded all files')
    elif success != 0 and failed == 0:
        return envelope.getSuccessEnvelope([fileMetadata], False)
    else:
        return envelope.getSuccessEnvelope([fileMetadata], True)
    