from datetime import datetime
from helpers.admie_helper import *
from helpers.download_helper import *
from models.file import CustomFile

def getFileUrlsFromApi(fileType: str, dateFrom: datetime, dateTo: datetime, version: int = None):
    files = getAdmieFiletype(fileType, dateFrom, dateTo)

    if files is None or len(files) == 0:
        return []
    
    fileObjects = []
    for i in range(len(files)):
        row = files[i]

        file = CustomFile(
            FileName = getFileNameFromUrl(row['file_path']),
            FileDescription = row['file_description'],
            Version = getVersionFromUrl(row['file_path'], fileType),
            Url = row['file_path'],
            PublicationDate = datetime.strptime(row['file_published'], '%d.%m.%Y %H:%M'),
            TargetDateFrom = row['file_fromdate'],
            TargetDateTo = row['file_todate'],
            FileType = fileType,
            Id=0
        )
        if version is None or file.Version == version:
            fileObjects.append(file)
    
    return fileObjects

def getISPAvailabilityFiles(dateFrom, dateTo, isp, version):
    if isp is not None:
        prefix = "ISP1" if isp == 1 else "ISP2" if isp == 2 else "ISP3"
        files = getFileUrlsFromApi(f"{prefix}UnitAvailabilities", dateFrom, dateTo)
    else:
        files = getFileUrlsFromApi("ISP1UnitAvailabilities", dateFrom, dateTo) + getFileUrlsFromApi("ISP2UnitAvailabilities", dateFrom, dateTo) + getFileUrlsFromApi("ISP3UnitAvailabilities", dateFrom, dateTo) 

    if version is not None:
        files = list(filter(lambda row: row.Version == version, files))
    return files

def getISPFilesWithPrefix(FileSuffix, dateFrom, dateTo, isp, version, maxIsps = 3):
    files = []
    if isp is not None:
        prefix = "ISP1" if isp == 1 else "ISP2" if isp == 2 else "ISP3" if isp == 3 else "ISP4"
        files = getFileUrlsFromApi(f"{prefix}{FileSuffix}", dateFrom, dateTo)
    else:
        for ispNum in range(1, maxIsps + 1):
            files += getFileUrlsFromApi(f"ISP{ispNum}{FileSuffix}", dateFrom, dateTo)

    if version is not None:
        files = list(filter(lambda row: row.Version == version, files))
    return files

def getISPResultsFiles(dateFrom, dateTo, isp, adhoc):
    if isp is not None:
        prefix = "ISP1" if isp == 1 else "ISP2" if isp == 2 else "ISP3"
        if adhoc: prefix = "Adhoc"
        files = getFileUrlsFromApi(f"{prefix}ISPResults", dateFrom, dateTo)
    else:
        files = getFileUrlsFromApi("AdhocISPResults", dateFrom, dateTo) + getFileUrlsFromApi("ISP1ISPResults", dateFrom, dateTo) + getFileUrlsFromApi("ISP2ISPResults", dateFrom, dateTo) + getFileUrlsFromApi("ISP3ISPResults", dateFrom, dateTo)

    return files


def getVirtualFileMetadataFromUrl(url: str, fileType: str, fileName: str, targetDate: datetime, extension='.xml', filterSensitive: str = None, targetDateTo: datetime = None):
    if filterSensitive is not None:
        url = url.replace(filterSensitive, '<KEY>')
    return CustomFile(Id=0, Url=url, FileName=fileName + extension, FileDescription=fileType, FileType=fileType, TargetDateFrom=targetDate, TargetDateTo=targetDate if targetDateTo is None else targetDateTo)