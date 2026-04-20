from pydantic import BaseModel, validator
from typing import Optional
from datetime import datetime
import pandas as pd

class TsocForecastData(BaseModel):
    Timestamp : Optional[datetime] 
    TotalDemandForecast : Optional[float] = None
    TotalSolarProductionForecast : Optional[float]
    TotalWindProductionForecast: Optional[float]  
    FCRUpForecast : Optional[float]
    AFRRUpForecast : Optional[float]
    MFRRUpForecast : Optional[float]
    FCRDownForecast : Optional[float]
    AFRRDownForecast : Optional[float]
    MFRRDownForecast : Optional[float]
    ThermalCommissioningForecast : Optional[float]
    InstalledSolarPower : Optional[float]
    InstalledWindPower : Optional[float]

    #add validator to round to 3 decimal places
    @validator('TotalDemandForecast', 'TotalSolarProductionForecast', 'TotalWindProductionForecast', 'FCRUpForecast', 'AFRRUpForecast', 'MFRRUpForecast', 'FCRDownForecast', 'AFRRDownForecast', 'MFRRDownForecast', 'ThermalCommissioningForecast', 'InstalledSolarPower', 'InstalledWindPower')
    def round_to_3_decimal_places(cls, v):
        return round(v, 3)
    


def tsocForecastUrl(date: datetime, mode= ""):

    if mode == "ISP":
        return f'https://s3-eu-central-1.amazonaws.com/tso-cy/ISP_FCST/excel/{date.strftime("%Y")}/{date.strftime("%m")}/ISP_FCST_{date.strftime("%Y%m%d")}.xlsx'
    elif mode == "DAM":
        return   f'https://s3-eu-central-1.amazonaws.com/tso-cy/DAM_FCST/excel/{date.strftime("%Y")}/{date.strftime("%m")}/DAM_FCST_{date.strftime("%Y%m%d")}.xlsx'
    else:
        return f'https://s3-eu-central-1.amazonaws.com/tso-cy/MMS-dry-run/FCST/ISP_FCST/excel/{date.strftime("%Y")}/{date.strftime("%m")}/ISP_FCST_{date.strftime("%Y%m%d")}.xlsx'
