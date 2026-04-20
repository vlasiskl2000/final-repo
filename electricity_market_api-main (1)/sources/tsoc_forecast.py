from datetime import datetime, timedelta
from fileinput import filename
import requests
import pandas as pd
from helpers.file_helper import getVirtualFileMetadataFromUrl 
from helpers.log_helper import logException
from helpers.tsoc_forecast_helpers import *
from interface.envelope import *
from models.file import CustomFile
from models.metadata import Metadata, getVirtualFileMetadata
import io




import pandas as pd 

def getTsocForecastData(date: datetime ,mode = None):
    
    
    url = tsocForecastUrl(date, mode = mode)


    columns = ['Timestamp',
               'TotalDemandForecast',
               'TotalSolarProductionForecast',
               'TotalWindProductionForecast',
               'FCRUpForecast',
               'AFRRUpForecast',
               'MFRRUpForecast',
               'FCRDownForecast',
               'AFRRDownForecast',
               'MFRRDownForecast', 
               'ThermalCommissioningForecast',
               'InstalledSolarPower',
               'InstalledWindPower']

    r = requests.get(url)

    if r.status_code != 200:
        raise Exception(f'File not found: {url}')
    

    df = pd.read_excel(io.BytesIO(r.content),  sheet_name = 'Προβλέψεις')

    #drop unwanted columns
    
    df = df.drop(["Περίοδος", "Πρόβλεψη ολικής παραγωγής ΑΠΕ \n(MW)"
            ,'Unnamed: 3'],axis=1)
    # drop df column that starts with "Προκαταρκτική Πρόβλεψη ΠΗΑ" or "Προκαταρκτική Πρόβλεψη για τις" or "Πρόβλεψη ΔΟΠ για τις"
    df = df[df.columns.drop(list(df.filter(regex='Προκαταρκτική Πρόβλεψη ΠΗΑ')) + list(df.filter(regex='Προκαταρκτική Πρόβλεψη για τις') ))]
                
    df = df[df.columns.drop(list(df.filter(regex='Πρόβλεψη ΔΟΠ για τις')))]
    

    #add column names
    df.columns = columns

    df['Timestamp'] = df['Timestamp'].dt.tz_localize('Europe/Athens',ambiguous='infer',nonexistent='NaT').dt.tz_convert('UTC')
    
    #drop row if Timestamp is NaT
    df.dropna(subset=['Timestamp'], inplace=True)

    final_data = df.apply(lambda row: TsocForecastData(**row.to_dict()), axis=1)


    return final_data



def getTsocForecast(dateFrom : datetime, dateTo : datetime, mode = ""):
    try:

        response = []

        date = dateFrom
        while date <= dateTo:
            description_mode = mode + "_" if mode != "" else ""
            meta_data_file = CustomFile(
                Id=0, 
                FileName=f"Tsoc_{description_mode}Forecast_{dateFrom.strftime('%Y-%m-%d')}_{dateTo.strftime('%Y-%m-%d')}.xlsx", 
                FileDescription=f"Tsoc_{description_mode}Forecast_data",
                FileType=f"Tsoc{mode}ForecastData.xlsx",
                Url = tsocForecastUrl(dateFrom, mode),
                PublicationDate=datetime.today().strftime('%Y-%m-%dT%H:%M:%S'),
                TargetDateFrom=dateFrom,
                TargetDateTo=dateFrom,
            )

            try:
                
                fetch_tsoc_fore_cast_data = getTsocForecastData(date, mode)
                data = [item.dict() for item in fetch_tsoc_fore_cast_data]

                response.append(getVirtualFileMetadata(data, Metadata(Success=True), meta_data_file))
            except Exception as e:
                logException(e)

                meta_data_file.Success = False
                meta_data_file.Log = str(e)
                    
                response.append(getVirtualFileMetadata(None, Metadata(Success=False, Log=str(e)), meta_data_file))
            date += timedelta(days=1)    
        return getSuccessEnvelope(response)
         
    except Exception as e:
        logException(e)
        
        return getFailedEnvelope(None, -1, "Failed to get symbol offers: " + str(e))
                                
