from helpers.download_helper import *
from helpers.path_helper import *
from helpers.array_helper import *
from helpers.metadata_helper import *

import pandas as pd
from bs4 import BeautifulSoup
import datetime

def getPolandDAM():
    dataframes = []

    # Helper format functions
    def p2f(x):
        return float(x.strip('%'))/100
    
    def fixRowEmptyValues(row, start, end):
        for i in range(start, end):
            row[i] = row[i] if str(row[i]) != '-' else None

        return row

    # Indices
    DAM_IndicesDataFrame = ReadTableToDf(DayAheadMarketURL, "footable_indeksy_0")
    DAM_IndicesDataFrame2 = ReadTableToDf(DayAheadMarketURL, "footable_indeksy_1")
    
    Indices = []
    IndicesColumns = ["TradingDay", "Date", "Index", "Price", "PriceChange", "Volume", "VolumeChange"]

    DAM_AllIndices = pd.concat([DAM_IndicesDataFrame, DAM_IndicesDataFrame2])

    date = cDateToday['FullDate']
    for array in DAM_AllIndices.values:
        Indices.append([Trading_Day, date, array[0], array[2], p2f(array[3]), array[4], p2f(array[5])])
        
    IndicesDataFrame = pd.DataFrame(data = Indices, columns = IndicesColumns)
    IndicesDataFrame.to_csv(MergedFolder + cDateTomorrow["DateString"] + "_DAM_Poland_Indices.csv", encoding='utf-8-sig')
    dataframes.append(IndicesDataFrame)

    # Hourly Contracts
    DAM_HourlyContractsDataFrame = ReadTableToDf(DayAheadMarketURL, "footable_kontrakty_godzinowe")

    HourlyContracts = []
    HourlyContractsColumns = ["TradingDay", "Date", "Hour", "FixingIPrice", "FixingIVolume", "FixingIIPrice", "FixingIIVolume", "ContinuousTradingPrice", "ContinuousTradingVolume"]

    for _, row in DAM_HourlyContractsDataFrame.iterrows():
        if '-' not in row[0]:
            break
        date = cDateToday['FullDate']
        hour = int(row[0].split('-')[1])
        row = fixRowEmptyValues(row, 1, 7)
        HourlyContracts.append([Trading_Day, date, hour,row[1], row[2], row[3], row[4], row[5], row[6]])
    
    HourlyContractsDataFrame = pd.DataFrame(data = HourlyContracts, columns = HourlyContractsColumns)
    HourlyContractsDataFrame.to_csv(MergedFolder + cDateTomorrow["DateString"] + "_DAM_Poland_HourlyContracts.csv", encoding='utf-8-sig')
    dataframes.append(HourlyContractsDataFrame)

    # Block Contracts
    DAM_BlockContractsDataFrame = ReadTableToDf(DayAheadMarketURL, "footable_kontrakty_blokowe_0")

    BlockContracts = []
    BlockContractsColumns = ["TradingDay", "Date", "Contract", "MinPrice", "MaxPrice", "Volume"]

    for _, row in DAM_BlockContractsDataFrame.iterrows():
        if '-' not in row[0]:
            break
        date = cDateToday['FullDate']
        row = fixRowEmptyValues(row, 1, 4)
        BlockContracts.append([Trading_Day, date, row[0], row[1], row[2], row[3]])
    
    BlockContractsDataFrame = pd.DataFrame(data = BlockContracts, columns = BlockContractsColumns)
    BlockContractsDataFrame.to_csv(MergedFolder + cDateTomorrow["DateString"] + "_DAM_Poland_BlockContracts.csv", encoding='utf-8-sig')
    dataframes.append(BlockContractsDataFrame)

    return dataframes

def getIntradayCouplingSIDC():
    dataframes = []

    # https://tge.pl/pub/TGE/Raport_publiczny_RDB_2020_07_12.xlsx
    ReportURL = IntraDayCouplingSIDC + cDateYesterday['FullDateDash'] + ".xlsx"
    ReportFile = DownloadRawFile(DownloadFolder, cDateYesterday['FullDate'], "PolandIDM", ReportURL, ".xlsx")

    IDM_DataFrame = pd.ExcelFile(ReportFile).parse(skiprows=range(0,18))

    # Trading Session
    IDM_Data = []
    IDM_Columns = ["TradingDay", "Date", "Hour", "MinPrice", "MaxPrice", "AverageWeightedPrice", "Volume"]

    for i in range(0, 24):
        row = IDM_DataFrame.values[i]
        IDM_Data.append([Trading_Day, cDateYesterday['FullDate'], i + 1, row[3], row[4], row[5], row[6]])

    IDM_DataFrame = pd.DataFrame(data = IDM_Data, columns = IDM_Columns)
    IDM_DataFrame.to_csv(MergedFolder + cDateTomorrow["DateString"] + "_IDM_Poland.csv", encoding='utf-8-sig')
    dataframes.append(IDM_DataFrame)

    return dataframes