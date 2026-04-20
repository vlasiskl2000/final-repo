from helpers.download_helper import *
from helpers.path_helper import *
from helpers.array_helper import *
from helpers.metadata_helper import *

import pandas as pd
import datetime


def getHungaryIDM():
    IDM_File = DownloadRawFile(DownloadFolder, cDateToday['FullDate'], "HungaryIDM", IDMExcelFile, ".xlsx")
                            
    dataframes = list()

    IDM_File = DownloadFolder + "//" + cDateToday["FullDate"] + "_HungaryIDM.xlsx"
    
    IDM_DataFrame = pd.ExcelFile(IDM_File).parse(skiprows = [0])
    
    # Prices
    Prices = []
    PricesColumns = ["TradingDay", "Date", "Hour", "Country", "Price"]

    for _, row in IDM_DataFrame.iterrows():
        date = datetime.datetime.strptime(row[0], '%d.%m.%Y')
        hour = int(row[1].split('H')[1])
        for i in range(2, 6):
            Prices.append([Trading_Day, date, hour, IDM_DataFrame.columns[i], row[i]])
    
    PricesDataFrame = pd.DataFrame(data = Prices, columns = PricesColumns)
    PricesDataFrame.to_csv(MergedFolder + cDateTomorrow["DateString"] + "_IDM_Hungary_Prices.csv", encoding='utf-8-sig')
    dataframes.append(PricesDataFrame)
    
    # Cross-Border Capacity
    CrossBorderCapacityFlow = []
    CrossBorderCapacityFlowColumns = ["TradingDay", "Date", "Hour", "CountryFrom", "CountryTo", "Capacity", "Flow"]

    for _, row in IDM_DataFrame.iterrows():
        date = datetime.datetime.strptime(row[0], '%d.%m.%Y')
        hour = int(row[1].split('H')[1])
        for i in range(6, 12):
            countries = IDM_DataFrame.columns[i]
            country_from = countries.split('-')[0]
            country_to = countries.split('-')[1]
            CrossBorderCapacityFlow.append([Trading_Day, date, hour, country_from, country_to, row[i], row[i+6]])
    
    CrossBorderCapacityFlowDataFrame = pd.DataFrame(data = CrossBorderCapacityFlow, columns = CrossBorderCapacityFlowColumns)
    CrossBorderCapacityFlowDataFrame.to_csv(MergedFolder + cDateTomorrow["DateString"] + "_IDM_Hungary_CrossBorderCapacityFlow.csv", encoding='utf-8-sig')
    dataframes.append(CrossBorderCapacityFlowDataFrame)

    return dataframes


def getHungaryMarketData():
    Market_File = DownloadRawFile(DownloadFolder, cDateToday['FullDate'], "HungaryMarketData", MarketExcelFile + cDateToday['FullDate'], ".xlsx")
                            
    dataframes = list()

    Market_File = DownloadFolder + "//" + cDateToday["FullDate"] + "_HungaryMarketData.xlsx"
    
    Market_SourceDataFrame = pd.ExcelFile(Market_File).parse(skiprows = [0,1,2])
    
    # Prices
    MarketData = []
    MarketColumns = ["TradingDay", "Timestamp", "BestBid", "BestAsk", "VolumeWeightedAveragePrice", 
                     "LastTradedPrice", "BuyTradedVolumeMW", "SellTradedVolumeMW", "BuyTradedVolumeMWh", "SellTradedVolumeMWh", "ExportVolume", "NetPosition"]

    for _, row in Market_SourceDataFrame.iterrows():
        date = datetime.datetime.strptime(row[0].split('-')[1], '%Y%m%d %H:%M')
        best_bid = row[1]
        best_ask = row[2]
        volume_weighted_average_price = row[3]
        last_traded_price = row[4]
        buy_traded_volume_mw = row[5]
        sell_traded_volume_mw = row[6]
        buy_traded_volume_mwh = row[7]
        sell_traded_volume_mwh = row[8]
        export_volume = row[9]
        net_position = row[10]

        MarketData.append([Trading_Day, date, best_bid, best_ask, volume_weighted_average_price, last_traded_price, buy_traded_volume_mw, sell_traded_volume_mw,
                           buy_traded_volume_mwh, sell_traded_volume_mwh, export_volume, net_position])

    
    MarketDataFrame = pd.DataFrame(data = MarketData, columns = MarketColumns)
    MarketDataFrame.to_csv(MergedFolder + cDateTomorrow["DateString"] + "_Hungary_Market.csv", encoding='utf-8-sig')
    dataframes.append(MarketDataFrame)

    return dataframes