import calendar
from datetime import datetime
import pandas as pd
from pydantic import BaseModel, Field, validator
from typing import List, Optional

COUNTRIES = [
    {
        "countryCode" : "BE",
        "commodityTypes" : {
            "Power" : ["B", "P"]
        },
        "physicalCommodities": ["BPB", "DBP", "BEB", "BEP"]
    },
    {
        "countryCode" : "IT",
        "commodityTypes" : {
            "Power" : ["B", "P"]
        },
        "physicalCommodities": ["IPB", "IPP","DIF"]
    },
    {
        "countryCode" : "FR",
        "commodityTypes" : {
            "Power" : ["B", "P"]
        },
        "physicalCommodities": ["DFB", "FNB","DFA","FNA"]
    },
    {
        "countryCode" : "DE",
        "commodityTypes" : {
            "Power" : ["B", "P"]
        },
        "physicalCommodities": ["GAB", "DGB","DGA", "GAP"]
    },
    {
        "countryCode" : "AT",
        "commodityTypes" : {
            "Power" : ["B", "P"]
        },
        "physicalCommodities": ["DBH", "AOT","AOU"]
    },
    {
        "countryCode" : "NL",
        "commodityTypes" : {
            "Power" : ["B", "P"]
        },
        "physicalCommodities": ["DPB", "DDA","NLB", "NLP", "DPA"]
    },
    {
        "countryCode" : None,
        "commodityTypes" : {
            "Power" : ["B", "P"]
        },
        "physicalCommodities": ["DNB","NRB"]    
    },
    {
        "countryCode" : "ES",
        "commodityTypes" : {
            "Power" : ["B", "P"]
        },
        "physicalCommodities": ["DSB", "SPB"]
    }
]

GAS_COUNTRIES = [
    {
        "countryCode" : "NL",
        "commodityTypes" : {
            "Gas" : ["B", "P"]
        },
        "physicalCommodities": ["TFM"]#, "TFE"
    },
]

ENVIRONMENTAL = [
    {
        "countryCode" : "",
        "commodityTypes" : {
            "Environmental" : ["B", "P"]
        },
        "physicalCommodities": ["C"]
    }
]

OIL = [
    {
        "countryCode" : "",
        "commodityTypes" : {
            "Oil" : ["B", "P"]
        },
    "physicalCommodities": ["B"]
    }
]


COMMODITY_FUTURE_PRODUCT_TYPE = {
    'Invalid': -1,
    "Daily": 1,
    "Weekened": 2,
    "Week": 3,
    "Monthly": 4,
    "Quarterly": 5,
    "Yearly": 6,
    "Season": 7,
    "December": 8
}

COMMODITY_TYPE = {
    "Power": 1,
    "Gas": 2,
    "Environmental": 3,
    "Oil" : 4
}


#physicalCommodities as keys and their corresponding unit of measurement as values
UNIT_OF_MEASUREMENT = {
    "BPB" : "EurosPerMWh",
    "DBP" : "EurosPerMWh",
    "BEB" : "EurosPerMWh",
    "BEP" : "EurosPerMWh",
    "IPB" : "EurosPerMWh",
    "IPP" : "EurosPerMWh",
    "DIF" : "EurosPerMWh",
    "DFB" : "EurosPerMWh", 
    "FNB" : "EurosPerMWh",
    "DFA" : "EurosPerMWh",
    "FNA" : "EurosPerMWh",
    "GAB" : "EurosPerMWh", 
    "DGB" : "EurosPerMWh",
    "DGA" : "EurosPerMWh",
    "GAP" : "EurosPerMWh",
    "DBH" : "EurosPerMWh", 
    "AOT" : "EurosPerMWh",
    "AOU" : "EurosPerMWh", 
    "DPB" : "EurosPerMWh",
    "DDA" : "EurosPerMWh",
    "NLB" : "EurosPerMWh", 
    "NLP" : "EurosPerMWh", 
    "DPA" : "EurosPerMWh",
    "DNB" : "EurosPerMWh",
    "NRB" : "EurosPerMWh",
    "DSB" : "EurosPerMWh", 
    "SPB" : "EurosPerMWh",
    ### Gas
    "TFM" : "EurosPerMWh",
    "TFE" : "EurosPerMWh",
    ### Environmental
    "C" : "EurosPerTonne",
    ### oil
    "B" : "DollarsPerBarrel"
}

TIMEZONE = {
    "EDT": "America/Chicago",
    "EST": "America/Chicago",
}



def getFutureTypeFromDateRange(dateFrom: datetime, dateTo: datetime):
    # Calculate the duration in days between dateFrom and dateTo
    duration = (dateTo - dateFrom).days


    # Map the duration to the appropriate future type key
    if duration < 0:
        return COMMODITY_FUTURE_PRODUCT_TYPE['Invalid']
    elif duration == 0:
        return COMMODITY_FUTURE_PRODUCT_TYPE['Daily']
    elif duration == 1:
        return COMMODITY_FUTURE_PRODUCT_TYPE['Weekened']
    elif duration <= 6:
        return COMMODITY_FUTURE_PRODUCT_TYPE['Week']
    elif duration <= 31:
        return COMMODITY_FUTURE_PRODUCT_TYPE['Monthly']
    elif duration <= 92:
        return COMMODITY_FUTURE_PRODUCT_TYPE['Quarterly']
    elif duration <= 183:  # 6 months (Season)
        return COMMODITY_FUTURE_PRODUCT_TYPE['Season']
    elif duration <= 366:
        return COMMODITY_FUTURE_PRODUCT_TYPE['Yearly']
    
    return COMMODITY_FUTURE_PRODUCT_TYPE['Invalid']

LOAD_TYPE ={
    "Invalid":-1,
    "Base":1,
    "Peak":2,
}

COMMODITY_LOAD_TYPE_NAME =[
    "Financial",
    # "Base",
    # "Peak"
]


def get_load_type(Name):
    if Name is None:
        return LOAD_TYPE["Invalid"]
    
    for i in LOAD_TYPE.keys():
        if i in Name.split(" "):
            return LOAD_TYPE[i]

    return LOAD_TYPE["Invalid"] 

def get_commodity_name(countryCode: str, commodityName: str, commodityLoadType: str):
    if countryCode is None:
        countryCode = "Nordic"
    load_type = [i for i in COMMODITY_LOAD_TYPE_NAME if i in commodityName] if commodityLoadType is not None else []

    return countryCode +  " " + " ".join(load_type) + " " + commodityLoadType

def get_date_from_expiration_metadata(expiration_metadata, product : str, dateFromCol: str, dateToCol: str):
    if expiration_metadata is None or product is None:
        return None, None

    if product in expiration_metadata.index:
        return expiration_metadata.loc[product][dateFromCol], expiration_metadata.loc[product][dateToCol]
    
    else:
        if product.startswith("Cal"):
            _, year = product.split(" ")
            year = "20" + year
            dateStart = datetime(int(year), 1, 1)
            dateEnd = datetime(int(year), 12, 31)
            return dateStart.strftime("%m/%d/%Y"), dateEnd.strftime("%m/%d/%Y")
        
        elif product.startswith("Q"):
            quarter, year = product.split(" ")
            year = "20" + year
            return get_dates_from_quarter(quarter, year)
        
        elif product.startswith("Summer"):
            year = "20" + product[-2:]
            dateStart = datetime(int(year), 6, 1)
            dateEnd = datetime(int(year), 8, 31)
            return dateStart.strftime("%m/%d/%Y"), dateEnd.strftime("%m/%d/%Y")
        elif product.startswith("Winter"):
            year = "20" + product[-2:]
            dateStart= datetime(int(year), 12, 1)
            dateEnd = datetime(int(year)+1, 2, 28)
            return dateStart.strftime("%m/%d/%Y"), dateEnd.strftime("%m/%d/%Y")

    return None, None

def get_delivery_period_contract_name(contract):
        # Extract the year and month from the contract name (e.g., OCT24)
    month_str = contract[:3].upper()  # First 3 letters: 'OCT'
    year = int('20' + contract[-2:])  # Last 2 digits: '24' -> 2024
    
    # Convert month abbreviation to month number
    month_map = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
    }
    
    delivery_month = month_map[month_str]
    
    # Assume the delivery starts on the 1st and ends on the last day of the delivery month
    start_date = datetime(year=year, month=delivery_month, day=1)
    
    # Find the last day of the delivery month
    last_day = calendar.monthrange(year, delivery_month)[1]
    end_date = datetime(year=year, month=delivery_month, day=last_day)
    
    return start_date, end_date

def get_dates_from_quarter(quarter, year):
    if quarter == "Q1":
        dateStart = datetime(int(year), 1, 1)
        dateEnd = datetime(int(year), 3, 31)
    elif quarter == "Q2":
        dateStart = datetime(int(year), 4, 1)
        dateEnd = datetime(int(year), 6, 30)
    elif quarter == "Q3":
        dateStart = datetime(int(year), 7, 1)
        dateEnd = datetime(int(year), 9, 30)
    elif quarter == "Q4":
        dateStart = datetime(int(year), 10, 1)
        dateEnd = datetime(int(year), 12, 31)
    else:
        return None, None

    return dateStart.strftime("%m/%d/%Y"), dateEnd.strftime("%m/%d/%Y")


def get_product_name(name):

    if name.startswith("Q"):
        return "/".join(name.split(" "))
    
    return name

class CommodityFutureProductTimeseries(BaseModel):
    dispatchDay: Optional[str] = None
    openPrice: Optional[float] = None
    highPrice: Optional[float] = None
    lowPrice: Optional[float] = None
    volumeTradeRegistration: Optional[float] = None
    volumeExchange : Optional[float] = None
    settlementPrice: Optional[float] = None
    
    @validator("openPrice","highPrice", "lowPrice", "volumeTradeRegistration", "volumeExchange", "settlementPrice", pre=True, always=True)
    def replace_nan(cls, v):

        return v if not pd.isna(v) else None

class CommodityFutureLastInfoTimeseries(BaseModel):
    lastPrice: Optional[float] = None
    lastVolume: Optional[int] = None
    openInterest: Optional[float] = None
    dispatchDay: Optional[str] = None

    @validator("lastPrice", "lastVolume", "openInterest", pre=True, always=True)
    def replace_nan(cls, v):

        return v if not pd.isna(v) else None

class Price(BaseModel):
    productCode: Optional[str] = None
    name: Optional[str] = None
    commodityLoadType: Optional[int] = None
    index: Optional[str] = None
    unitOfMeasurement: Optional[str] = None
    productType: Optional[int] = None
    dateFrom: Optional[str] = None
    dateTo: Optional[str] = None
    commodityFutureProductTimeseries: Optional[List[CommodityFutureProductTimeseries]] = Field(default_factory=list)
    commodityFutureLastInfoTimeseries: Optional[List[CommodityFutureLastInfoTimeseries]] = Field(default_factory=list)
    commodityIndexName: Optional[str] = None
    countryName: Optional[str] = None
    commodityType: Optional[int] = None



