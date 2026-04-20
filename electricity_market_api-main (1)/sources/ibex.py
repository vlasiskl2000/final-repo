from helpers.download_helper import *
from helpers.path_helper import *
from helpers.array_helper import *
from helpers.metadata_helper import *

import pandas as pd
import datetime

def getBulgaryDAM():
    DAM_File = DownloadIBEXFile(DownloadFolder, cDateTomorrow['FullDate'], "BulgaryDAM", ServerIBEX, "download-prices-volumes-data-table.php")
                            
    dataframes = list()

    DAM_File = DownloadFolder + "//" + cDateTomorrow["FullDate"] + "_BulgaryDAM.xls"

    
    DAM_DataFrame = pd.ExcelFile(DAM_File).parse(skiprows = [0])
    
    # Prices and Volumes
    PricesAndVolumes = []
    PricesAndVolumesColumns = ["TradingDay", "Date", "Prices", "Volume"]

    first_column = DAM_DataFrame.columns[0]
    index_pav_prices = DAM_DataFrame.index[DAM_DataFrame[first_column] == 'Prices (EUR/MWh)'].values[0]
    index_pav_volumes = DAM_DataFrame.index[DAM_DataFrame[first_column] == 'Volumes (MWh)'].values[0]
    
    for hour in range(0, 7):
        date = (datetime.datetime.now() + datetime.timedelta(days = hour - 5)).date()
        price = DAM_DataFrame.iloc[index_pav_prices][2:][hour]
        volume = DAM_DataFrame.iloc[index_pav_volumes][2:][hour]

        PricesAndVolumes.append([Trading_Day, date, price, volume])
    
    PricesAndVolumesDataFrame = pd.DataFrame(data = PricesAndVolumes, columns = PricesAndVolumesColumns)
    PricesAndVolumesDataFrame.to_csv(MergedFolder + cDateTomorrow["DateString"] + "_DAM_Bulgary_PricesAndVolume.csv", encoding='utf-8-sig')
    dataframes.append(PricesAndVolumesDataFrame)

    # Block Products
    BlockProducts = []
    BlockProductsColumns = ["TradingDay", "Date", "Base", "Peak", "OffPeak"]

    index_block_base = DAM_DataFrame.index[DAM_DataFrame[first_column] == 'Base(01-24)'].values[0]
    index_block_peak = DAM_DataFrame.index[DAM_DataFrame[first_column] == 'Peak(9-20)'].values[0]
    index_block_offpeak = DAM_DataFrame.index[DAM_DataFrame[first_column] == 'Off-Peak(1-8 & 21-24)'].values[0]
    
    for hour in range(0, 7):
        date = (datetime.datetime.now() + datetime.timedelta(days = hour - 5)).date()
        base = DAM_DataFrame.iloc[index_block_base][2:][hour]
        peak = DAM_DataFrame.iloc[index_block_peak][2:][hour]
        offpeak = DAM_DataFrame.iloc[index_block_offpeak][2:][hour]

        BlockProducts.append([Trading_Day, date, base, peak, offpeak])
    
    BlockProductsDataFrame = pd.DataFrame(data = BlockProducts, columns = BlockProductsColumns)
    BlockProductsDataFrame.to_csv(MergedFolder + cDateTomorrow["DateString"] + "_DAM_Bulgary_BlockProducts.csv", encoding='utf-8-sig')
    dataframes.append(BlockProductsDataFrame)

    # Hour Products
    HourProducts = []
    HourProductsColumns = ["TradingDay", "Date","Hour", "Price", "Energy"]
    
    index_hour_products = DAM_DataFrame.index[DAM_DataFrame[first_column] == '0-1'].values[0]
    
    for day in range(0, 7):
        for hour in range(0, 24):
            date = (datetime.datetime.now() + datetime.timedelta(days = day - 5)).date()
            price = DAM_DataFrame.iloc[index_hour_products + hour*2][2:][day]
            energy = DAM_DataFrame.iloc[index_hour_products + hour*2 + 1][2:][day]

            HourProducts.append([Trading_Day, date, hour+1, price, energy])
    
    HourProductsDataFrame = pd.DataFrame(data = HourProducts, columns = HourProductsColumns)
    HourProductsDataFrame.to_csv(MergedFolder + cDateTomorrow["DateString"] + "_DAM_Bulgary_HourProducts.csv", encoding='utf-8-sig')
    dataframes.append(HourProductsDataFrame)

    return dataframes


def getBulgaryIDM():
    IDM_File = DownloadIBEXFile(DownloadFolder, cDateToday['FullDate'], "BulgaryIDM", ServerIBEX, "download-xls/idm.php")
                            
    dataframes = list()

    IDM_File = DownloadFolder + "//" + cDateToday["FullDate"] + "_BulgaryIDM.xls"
    
    IDM_DataFrame = pd.ExcelFile(IDM_File).parse()
    
    # Prices and Volumes
    PricesAndVolumes = []
    PricesAndVolumesColumns = ["TradingDay", "Date", "Hour", "WeightedAveragePrice","MaxPrice", "MinPrice", "LastPrice", "TradedVolume"]

    for _, row in IDM_DataFrame.iterrows():
        hour = int(row[0].split('-')[2])
        PricesAndVolumes.append([Trading_Day, cDateToday['FullDate'], hour, row[1], row[2], row[3], row[4], row[5]])
    
    PricesAndVolumesDataFrame = pd.DataFrame(data = PricesAndVolumes, columns = PricesAndVolumesColumns)
    PricesAndVolumesDataFrame.to_csv(MergedFolder + cDateTomorrow["DateString"] + "_IDM_Bulgary.csv", encoding='utf-8-sig')
    dataframes.append(PricesAndVolumesDataFrame)

    return dataframes