from datetime import datetime
from typing import Optional, Union
from pydantic import BaseModel


class EntsoeDayAheadAggregatedForecastModel(BaseModel):
    Timestamp: datetime = None
    ScheduledGeneration : Optional[float]  = None
    ScheduledConsumption : Optional[float] = None
    BiddingZone : str 

class EntsoeActualLoadPerCountryModel(BaseModel):
    Timestamp: datetime = None
    ShortCode : str 
    Value : Optional[float]  = None