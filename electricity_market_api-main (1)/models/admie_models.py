from typing import Optional
import pandas as pd
from pydantic import BaseModel, root_validator
from datetime import date, datetime


class BalancingEnergyData(BaseModel):
    DispatchDay: Optional[date] = None
    DispatchPeriod: Optional[int] = None
    Version: Optional[datetime] = None
    ZoneId: Optional[int] = None
    totalActivatedBalancingEnergyUp: Optional[float] = None
    totalActivatedBalancingEnergyDown: Optional[float] = None
    imbalancePrice: Optional[float] = None
    mFrrUpPrice: Optional[float] = None
    mFrrDownPrice: Optional[float] = None
    upliftAccount1: Optional[float] = None
    upliftAccount2: Optional[float] = None
    upliftAccount3: Optional[float] = None
    balancingEnergyIdev: Optional[float] = None
    balancingEnergyUdev: Optional[float] = None
    ISPEnergyDPkDF: Optional[float] = None
    SystemDeviation: Optional[float] = None
    CurtFlag: Optional[bool] = None
    FileId: Optional[int] = None

    @root_validator(pre=True)
    def validate_floats(cls, values: dict) -> dict:
        for field, value in values.items():
            if isinstance(value, float) and pd.isna(value):
                values[field] = None
        return values
    

class EnergySurplusData(BaseModel):
    Timestamp: Optional[datetime] = None
    Value: Optional[float] = None
    Version: Optional[datetime] = None
    ZoneId : Optional[int] = None
    DispatchDay: Optional[date] = None
