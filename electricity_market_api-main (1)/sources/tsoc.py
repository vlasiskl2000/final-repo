import cloudscraper
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta, date
import io
import stamina
from helpers.log_helper import logException
from helpers.tsoc_helper import *
from interface.envelope import getFailedEnvelope, getSuccessEnvelope
from models.file import CustomFile
from models.metadata import Metadata, getVirtualFileMetadata 

class TsocData(BaseModel):
    Timestamp : Optional[datetime] 
    TotalAvailableConventionalCapacity : Optional[float] = None
    WindProduction : Optional[float]
    EstimatedDistributedGeneration : Optional[float]  
    TotalSystemGeneration : Optional[float] 
    ConventionalCapacity : Optional[float] 


def getTsocFileMetadata(dateFrom, dateTo):
    meta_data_file = CustomFile(
                    Id=0, 
                    FileName=f"Tsoc_{dateFrom.strftime('%Y-%m-%d')}_{dateTo.strftime('%Y-%m-%d')}.xlsx", 
                    FileDescription="Tsoc_live_data",
                    FileType="Tsoc.xlsx",
                    Url = tsoc_live_url(dateFrom),
                    PublicationDate=datetime.today().strftime('%Y-%m-%dT%H:%M:%S'),
                    TargetDateFrom=dateFrom,
                    TargetDateTo=dateFrom,
            )
    return meta_data_file 

def Get_Tsoc_Live(dateFrom : datetime, dateTo : datetime):
    

    columns = [
    "Timestamp",
    "TotalAvailableConventionalCapacity",
    "WindProduction",
    "EstimatedDistributedGeneration",
    "TotalSystemGeneration",
    "ConventionalCapacity"
    ]

    request_three_days_range_flag = False
    starting_date = dateFrom
    tsoc_df = pd.DataFrame(columns = columns)

    # check date range to minimizze webscraping requests
    if dateTo - dateFrom  == timedelta(days=2):
        request_three_days_range_flag = True

    while starting_date <= dateTo:

        if request_three_days_range_flag:
            url = tsoc_live_url(starting_date, dateTo)
            
            #set starting date to dateTo to exit loop
            starting_date = dateTo
        else:
            url = tsoc_live_url(starting_date, None)

        #scrape data
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url)

        soup = BeautifulSoup(response.content, "html.parser")

        content = soup.find_all('td')

        #initialize pandas df
        df = pd.DataFrame(columns = columns)

        #collect and store data in pandas df
        new_row = []
        for row in range(2,len(content)):
            new_row.append(content[row].text)
            if len(new_row) == 6:
                df.loc[len(df)] = new_row
                new_row = []


        starting_date += timedelta(days=1)

        tsoc_df = pd.concat([tsoc_df, df], axis=0, ignore_index=True)
    
    #convert timestamps to tackle DST
    tsoc_df  = get_df_utc_with_DST(tsoc_df, '%Y-%m-%d %H:%M:%S', 'UTC')

    tsoc_df.replace('', np.nan, inplace=True)
    tsoc_df.dropna(inplace=True,axis=0)

    if tsoc_df.empty:
        return None

    #convert df rows to pydantic models
    final_data = tsoc_df.apply(lambda row: TsocData(**row.to_dict()), axis=1)

    return final_data 



def Get_Tsoc_Year(dateFrom : datetime, dateTo : datetime):
    
    url = tsoc_url(str(dateFrom.year))

     
    columns = [
    "Timestamp",
    "WindProduction",
      "TotalSystemGeneration",
    "EstimatedDistributedGeneration",
    "ConventionalCapacity"
    ]
    
    #load byte response data to bytes object
    request_xlsx = requests.get(url)
    byte_response = io.BytesIO(request_xlsx.content)

    #clean df
    df_ex = pd.read_excel(byte_response,header=3)
    clean_df = df_ex.dropna(axis=1)

    clean_df.drop('Unnamed: 1',axis=1,inplace=True)
    clean_df.drop('Unnamed: 2',axis=1,inplace=True)
    clean_df.columns = columns

    data_df = pd.DataFrame(columns = columns)
    starting_date = dateFrom
    while starting_date <= dateTo:
       
        clean_df['Timestamp'] = clean_df['Timestamp'].astype(str)
        day = clean_df[clean_df['Timestamp'].str.startswith(starting_date.strftime('%Y-%m-%d'))]
       
        data_df = pd.concat([data_df, day], axis=0, ignore_index=True)
        
        starting_date += timedelta(days=1)


    # insert null value data
    data_df.insert(loc=1, column="TotalAvailableConventionalCapacity", value=None)

    #convert timestamps to tackle DST
    data_df  = get_df_utc_with_DST(data_df, '%Y-%m-%d %H:%M:%S', 'UTC')

    #convert data to pydantic model
    final_data = data_df.apply(lambda row: TsocData(**row.to_dict()), axis=1)

    return final_data




def getTsoc(dateFrom : datetime, dateTo : datetime):
    try:
        # Convert datetime values to the desired string format
        response = []

        for i in range(dateFrom.year, dateTo.year + 1):
            currentDateFrom = date(i, 1, 1) if dateFrom.year != i else dateFrom
            currentDateTo = date(i, 12, 31) if dateTo.year != i else dateTo


            try:
                file = getTsocFileMetadata(currentDateFrom, currentDateTo)

                #live data
                if currentDateFrom.year == datetime.today().year or file_does_not_exist(currentDateFrom.year):
                    fetch_tsoc_data = Get_Tsoc_Live(currentDateFrom, currentDateTo)
                #historical data
                else:
                    fetch_tsoc_data = Get_Tsoc_Year(currentDateFrom, currentDateTo)

                data = [model.dict() for model in fetch_tsoc_data] if fetch_tsoc_data is not None else []
    
                response.append(getVirtualFileMetadata(data, Metadata(Success=True), file))
            
            except Exception as e:
                logException(e)

                file.Success = False
                file.Log = str(e)
                    
                response.append(getVirtualFileMetadata(None, Metadata(Success=False, Log=str(e)), file))
        return getSuccessEnvelope(response)
            
    except Exception as e:
        logException(e)
        
        return getFailedEnvelope(None, -1, "Failed to get symbol offers: " + str(e))
