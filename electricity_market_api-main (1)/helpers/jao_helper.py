from datetime import datetime
from pydantic import BaseModel, validator
from typing import Optional, List

class eicCode(BaseModel):
    eicCode: Optional[str] = None

    
class JaoDataResults(BaseModel):
    OfferedCapacity: Optional[float]
    RequestedCapacity: Optional[float]
    AllocatedCapacity: Optional[float]
    AuctionPrice: Optional[float]
    ProductIdentification: Optional[str]
    ProductHour: Optional[str]
    BidderPartyCount: Optional[int]
    WinnerPartyCount: Optional[int]
    TimestampFrom: Optional[datetime]
    TimestampTo: Optional[datetime]

    @validator("WinnerPartyCount","BidderPartyCount","AuctionPrice","AllocatedCapacity","RequestedCapacity","ProductIdentification", "ProductHour", pre=True, always=True)
    def validate_string(cls, value):
        if value is not None and value == '--':
            return None  # Return the value unchanged
        return value



class JaoData(BaseModel):
    Identification: Optional[str]
    StartDate: Optional[datetime]
    EndDate: Optional[datetime]
    Horizon: Optional[int]
    Ftroption: Optional[str] = None
    Cancelled: Optional[str|bool]
    BorderName: Optional[str]
    Results: Optional[List[JaoDataResults]] = []
    WinningParties: Optional[List[eicCode]] = []
    
    @validator("Identification", "Cancelled", "BorderName", pre=True, always=True)
    def validate_string(cls, value):
        if value is not None and value == '--':
            return None  # Return the value unchanged
        return value

    @validator("Horizon", pre=True, always=True)
    def validate_horizon(cls, value):
        if value is not None and value == '--':
            return None  # Return the value unchanged
        return value