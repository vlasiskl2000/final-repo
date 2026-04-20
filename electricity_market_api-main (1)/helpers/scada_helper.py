import math
from typing import Optional
import pandas as pd
from datetime import datetime

from pydantic import BaseModel
from helpers.admie_helper import getTimeDiffernceCET
from helpers.date_helper import *

def handleScadaUnitData(results, file, date, array):
    """
        Results lidt is filled with data. Cumulatice production on different configuration of virtual units.

        reults: the list that the data will be parsed
        file: dictionary with the file that will be arsed info
        date: the date
    """

    for row in array:
        entityName = row[2]

        for period in range(1, 5): 
            results.append({
                    'DispatchDay': row[0].strftime('%Y-%m-%d'),
                    'DispatchPeriod': 4*(row[1] - 1) + period ,
                    'Value': float(row[3]) if not math.isnan(row[3]) else 0,
                    'Version': file.PublicationDate,
                    'FileId': file.Id,
                    'EntityName': entityName,
                })

        
        


def getSCADACountryNames(date: datetime):
    return ['ΑΛΒΑΝΙΑ', 'ΒΟΥΛΓΑΡΙΑ','FYROM','ΙΤΑΛΙΑ','ΤΟΥΡΚΙΑ']

def getSCADAAggregatedNames():
     return ['NET LOAD',
             "NET LOAD WITHOUT CRETE",
             "TOTAL LIGNITE",
             "ΣΥΝΟΛΟ ΠΕΤΡΕΛΑΙΚΩΝ",
             "TOTAL GAS",
             "TOTAL HYDRO",
             "TOTAL RES"
             ]
    
def getScadaDataFromRange(df : pd.DataFrame, begin, end, Date, array):
    for i in range(begin, end + 1):
        data = df.iloc[i, :].values

        hours = getTimeDiffernceCET(Date + timedelta(days = 1))
        hour_columns = df.iloc[1,2:2 + hours]
        
        for h in range(1, hours + 1):
            
            dispatchPeriod = hour_columns[h - 1]
            
            if not isParsableToInt(dispatchPeriod):
                        continue
            
            DispatchDate = EETtoCET(Date  + timedelta(hours = int(dispatchPeriod) - 1))

            row = [DispatchDate , DispatchDate.hour + 1  , data[1]]

            if type(data[1]) is not str and math.isnan(data[1]):
                continue
            row.append(data[h + 1])
            array.append(row)

def getScadaHydroData(file, results, df, date):
    hydro_begin = df[df.iloc[:, 1].str.contains("ΥΔΡΟΗΛΕΚΤΡΙΚΕΣ ΜΟΝΑΔΕΣ") == True].index[0] + 1
    hydro_end = df[(df.iloc[:, 1].str.contains("TOTAL HYDRO") == True) | (df.iloc[:, 1].str.contains("ΣΥΝΟΛΟ ΥΔΡΟΗΛΕΚΤΡΙΚΩΝ") == True)].index[0] - 1
    HydroProduction = []
    getScadaDataFromRange(df, hydro_begin, hydro_end, date, HydroProduction)
    handleScadaUnitData(results, file, date, HydroProduction)
    
def getNaturalGasFromScada(file, results, df, date):
    natural_gas_begin = df[df.iloc[:, 1].str.contains("ΜΟΝΑΔΕΣ Φ. ΑΕΡΙΟΥ") == True].index[0] + 1
    natural_gas_end = df[(df.iloc[:, 1].str.contains("TOTAL GAS") == True) | (df.iloc[:, 1].str.contains("ΣΥΝΟΛΟ Φ. ΑΕΡΙΟΥ") == True)].index[0] - 1
        
    NaturalGasProduction = []
    getScadaDataFromRange(df, natural_gas_begin, natural_gas_end, date, NaturalGasProduction)
    handleScadaUnitData(results, file, date, NaturalGasProduction)

def getThermalProductionFromScada(file, results, df, date):
    thermo_begin = df[df.iloc[:, 1].str.contains("ΛΙΓΝΙΤΙΚΕΣ ΜΟΝΑΔΕΣ") == True].index[0] + 1
    thermo_end = df[(df.iloc[:, 1].str.contains("TOTAL LIGNITE") == True) | (df.iloc[:, 1].str.contains("ΣΥΝΟΛΟ ΛΙΓΝΙΤΙΚΩΝ") == True)].index[0] - 1
    ThermoProduction = []
    getScadaDataFromRange(df, thermo_begin, thermo_end, date, ThermoProduction)

    handleScadaUnitData(results, file, date, ThermoProduction)
    
def getOilProductionFromScada(file, results, df, date):
    oil_begin = df[df.iloc[:, 1].str.contains("ΠΕΤΡΕΛΑΙΚΕΣ ΜΟΝΑΔΕΣ") == True].index[0] + 1
    oil_end = df[(df.iloc[:, 1].str.contains("TOTAL OIL") == True) | (df.iloc[:, 1].str.contains("ΣΥΝΟΛΟ ΠΕΤΡΕΛΑΙΚΩΝ") == True)].index[0] - 1
    OilProduction = []
    getScadaDataFromRange(df, oil_begin, oil_end, date, OilProduction)

    handleScadaUnitData(results, file, date, OilProduction)


def isParsableToInt(number):
    try:
        int(number)
        return True
    except ValueError:
        return False
    
class ScadaAggregatedProdModel(BaseModel):
    Timestamp : Optional[datetime] 
    NetLoad : Optional[float] = None
    NetLoadWithoutCrete : Optional[float] = None
    TotalLignite: Optional[float] = None
    TotalOil : Optional[float] = None
    TotalGas : Optional[float] = None
    TotalHydro : Optional[float] = None
    TotalRes : Optional[float] = None
    FileId : Optional[int] = None
    Version : Optional[datetime] = None