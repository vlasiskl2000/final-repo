from typing import List, Set, Tuple
from urllib.request import urlopen
from helpers.external_api_token_helpers import getRandomSecurityToken
from interface.envelope import Envelope
from models.entsoe_models import EntsoeActualLoadPerCountryModel, EntsoeDayAheadAggregatedForecastModel
from models.metadata import VirtualFileMetadataPayload, getMetadata, getVirtualFileMetadata
from helpers.download_helper import *
from helpers.xml_parse_helper import *
from helpers.metadata_helper import *
from helpers.log_helper import logException
from helpers.file_helper import getVirtualFileMetadataFromUrl
import xmltodict, json
import zipfile
from io import BytesIO
from dateutil import parser
from pytz import timezone
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import random
import datetime


def formatDateAsCompatibleString(date):
    return datetime.datetime.strftime(date, "%Y%m%d%H" + "00")



def getRequestUrl(DocumentType,ProcessType=None, InDomain=None, OutDomain=None, OutBiddingZone_Domain=None, PeriodStart=None,PeriodEnd=None, PsrType=None, ContractMarketAgreementType=None, AreaDomain=None, ControlArea_Domain=None, BiddingZone_Domain=None, BusinessType=None, AuctionType=None):
    url = Endpoint + '?securityToken=' + getRandomSecurityToken() + "&"

    if DocumentType is not None:
        url += "documentType=" + DocumentType + "&"
    if ProcessType is not None:
        url += "processType=" + ProcessType + "&"
    if BusinessType is not None:
        url += "businessType=" + BusinessType + "&"
    if OutBiddingZone_Domain is not None:
        url += "outBiddingZone_Domain=" + OutBiddingZone_Domain + "&"
    if InDomain is not None:
        url += "in_Domain=" + InDomain + "&"
    if AreaDomain is not None:
        url += "area_Domain=" + AreaDomain + "&"
    if OutDomain is not None:
        url += "out_Domain=" + OutDomain + "&"
    if ControlArea_Domain is not None:
        url += "controlArea_Domain=" + ControlArea_Domain + "&"
    if PsrType is not None:
        url += "psrType=" + PsrType + "&"
    if PeriodStart is not None:
        url += "periodStart=" + formatDateAsCompatibleString(PeriodStart) + "&"
    if PeriodEnd is not None:
        url += "periodEnd=" + formatDateAsCompatibleString(PeriodEnd) + "&"
    if ContractMarketAgreementType is not None:
        url += "contract_MarketAgreement.Type=" + ContractMarketAgreementType + "&"
    if BiddingZone_Domain is not None:
        url += "biddingZone_Domain=" + BiddingZone_Domain + "&"
    if AuctionType is not None:
        url += "auction.Type=" + AuctionType + "&"

    return url.strip('&')

def aggregate_results_average(results, dateColumn = 'dispatchDay', periodColumn = 'dispatchPeriod', entityColumn = 'zoneName'):
    # group by results per date and period and average value
    grouped_response = []
    grouped_results = {}
    for result in results:
        key = f'{result[dateColumn]}_{result[periodColumn]}'
        if key not in grouped_results:
            grouped_results[key] = []
        grouped_results[key].append(result)
                
    for key in grouped_results:
        values = grouped_results[key]
        value = sum([x['value'] for x in values]) / len(values)
        grouped_response.append({
                        dateColumn: values[0][dateColumn],
                        periodColumn: values[0][periodColumn],
                        entityColumn: values[0][entityColumn],
                        'value': value
                    })
        
    return grouped_response

def aggregate_results_sum(results):
    # group by results per date and period and sum value
    grouped_response = []
    grouped_results = {}
    for result in results:
        key = f'{result["dispatchDate"]}_{result["dispatchPeriod"]}'
        if key not in grouped_results:
            grouped_results[key] = []
        grouped_results[key].append(result)
                
    for key in grouped_results:
        values = grouped_results[key]
        value = sum([x['value'] for x in values])
        grouped_response.append({
                        'dispatchDay': values[0]['dispatchDay'],
                        'dispatchPeriod': values[0]['dispatchPeriod'],
                        'zoneName': values[0]['zoneName'],
                        'value': value
                    })
        
    return grouped_response

def getENTSOEDayAheadPredictedLoad(dateFrom: datetime, dateTo: datetime):
    response = []

    date = dateFrom
    while date <= dateTo:
        predicted_url = getRequestUrl("A65", "A01", OutBiddingZone_Domain = MainBiddingZone, PeriodStart = date.replace(hour = 0),
                        PeriodEnd = date.replace(hour = 0) + datetime.timedelta(days = 1))
        
        fileName = 'EntsoeDayAheadLoadForecast'
        file = getVirtualFileMetadataFromUrl(predicted_url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)
        results = []

        try:
            predicted_data = getHourlyDataFromXMLResponse(predicted_url)
            
            for t in range(0, len(predicted_data)):
                if t < len(predicted_data):
                    for period in range(1, 5): 
                        results.append({
                            'dispatchDay': formatDateTimeForJson(predicted_data[t][0]),
                            'dispatchPeriod': (predicted_data[t][1] - 1) * 4 + period,
                            'zoneId': 1,
                            'value': predicted_data[t][2]
                        })

            response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
        except Exception as e:
            message = f'Failed to parse {file.Url}: {e}'
            logException(e)
            response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
            
        date += datetime.timedelta(days = 1)
    
    return envelope.getSuccessEnvelope(response)

def getENTSOEDayAheadPredictedLoadPerCountry(dateFrom: datetime, dateTo: datetime) -> Envelope[List[VirtualFileMetadataPayload[List[EntsoeActualLoadPerCountryModel]]]]:
    response = []

    date = dateFrom
    while date <= dateTo:
        for bidding_zone in LoadCountryCodes:
            predicted_url = getRequestUrl("A65", "A01", OutBiddingZone_Domain = bidding_zone, PeriodStart = date.replace(hour = 0),
                            PeriodEnd = date.replace(hour = 0) + datetime.timedelta(days = 1))
            
            fileName = 'EntsoeDayAheadLoadForecastPerCountry'
            file = getVirtualFileMetadataFromUrl(predicted_url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)
            results = []

            try:
                results = []
                predicted_data = getHourlyDataFromXMLResponseWithGranularity(predicted_url)

                if predicted_data is None:
                    continue

                for row in predicted_data:
                    for rep in range(0,row[2]):
                        data_dict = {
                            'Timestamp': (row[0]+ timedelta(minutes =15*rep)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                            'ShortCode': COUNTRY_SHORT_CODE_MAPPING[bidding_zone],
                            'Value': row[1]
                        }
                        data = EntsoeActualLoadPerCountryModel(**data_dict)
                        results.append(data)

                response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
            except Exception as e:
                message = f'Failed to parse {file.Url}: {e}'
                logException(e)
                response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
            
        date += datetime.timedelta(days = 1)
    
    return envelope.getSuccessEnvelope(response)

def getENTSOEActualLoad(dateFrom: datetime, dateTo: datetime):
    response = []

    date = dateFrom
    while date <= dateTo:
        actual_url = getRequestUrl("A65", "A16", OutBiddingZone_Domain = MainBiddingZone, PeriodStart = getDateTimeCET(date.replace(hour = 0)),
                        PeriodEnd = getDateTimeCET(date.replace(hour = 0) + datetime.timedelta(days = 1)))
        
        fileName = 'EntsoeActualLoad'
        file = getVirtualFileMetadataFromUrl(actual_url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

        try:
            results = []
            actual_data = getHourlyDataFromXMLResponse(actual_url)
            
            for t in range(0, len(actual_data)):
                if t < len(actual_data):
                    for period in range(1, 5): 
                        results.append({'dispatchDay': formatDateTimeForJson(actual_data[t][0]),
                                        'dispatchPeriod': (actual_data[t][1] - 1) * 4 + period,
                                        'zoneId': 1,
                                        'value': actual_data[t][2]})
                                        
            response.append(getVirtualFileMetadata(results, getMetadata(len(results) > 0, file.Id), file))
        except Exception as e:
            message = f'Failed to parse {file.Url}: {e}'
            logException(e)
            response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
            
        date += datetime.timedelta(days = 1)

    return envelope.getSuccessEnvelope(response)

def getENTSOEActualLoadPerCountry(dateFrom: datetime, dateTo: datetime) -> Envelope[List[VirtualFileMetadataPayload[List[EntsoeActualLoadPerCountryModel]]]]:
    response = []

    date = dateFrom
    while date <= dateTo:
        for bidding_zone in LoadCountryCodes:
            
            url = getRequestUrl("A65", "A16", OutBiddingZone_Domain = bidding_zone, PeriodStart =date.replace(hour = 0),
                        PeriodEnd = date.replace(hour = 0) + datetime.timedelta(days = 1))
            fileName = 'EntsoeActualLoadPerCountry'
            file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)


            try:
                results = []
                actual_data = getHourlyDataFromXMLResponseWithGranularity(url)

                if actual_data is None:
                    continue

                for row in actual_data:
                    for rep in range(0,row[2]):
                        data_dict = {
                            'Timestamp': (row[0]+ timedelta(minutes =15*rep)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                            'ShortCode': COUNTRY_SHORT_CODE_MAPPING[bidding_zone],
                            'Value': row[1]
                        }
                        data = EntsoeActualLoadPerCountryModel(**data_dict)
                        results.append(data)
                                            
                response.append(getVirtualFileMetadata(results, getMetadata(len(results) > 0, file.Id), file))
            except Exception as e:
                message = f'Failed to parse {file.Url}: {e}'
                logException(e)
                response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
            
        date += datetime.timedelta(days = 1)

    return envelope.getSuccessEnvelope(response)


def getScheduledCommercialExchanges(dateFrom: datetime.datetime, dateTo: datetime.datetime):
    response = []
    date = dateFrom.date()

    def getCountryData(date, bidding_zone_in, bidding_zone_out):
        url = getRequestUrl('A09', InDomain = bidding_zone_in, OutDomain=bidding_zone_out, PeriodStart = getUTCFromCET(date),
                    PeriodEnd = getUTCFromCET(date + datetime.timedelta(days=1)))

        fileName = 'EntsoeScheduledCommercialExchanges'
        file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}_{bidding_zone_in}_{bidding_zone_out}', date)

        try:
            data = getHourlyDataFromXMLResponse(url)
            if data is None:
                return

            results = []
            for date, hour, power in data:
                for period in range(1, 5): 
                    results.append({
                        'dispatchDay': date.strftime("%Y-%m-%d"),
                        'dispatchPeriod': (hour - 1) * 4 + period,
                        'countryInName': bidding_zone_in,
                        'countryOutName': bidding_zone_out,
                        'value': power
                    })
            response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
        except Exception as e:
            message = f'Failed to parse {file.Url}: {e}'
            logException(e)
            response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
            
    while date <= dateTo.date():
        for bidding_zone_in, bidding_zone_out in CountryPairs:
            getCountryData(date, bidding_zone_in, bidding_zone_out)
            getCountryData(date, bidding_zone_out, bidding_zone_in)
        
        date += timedelta(days = 1)
    
    return envelope.getSuccessEnvelope(response)

def getForecastedCapacity(dateFrom: datetime.datetime, dateTo: datetime.datetime):
    response = []
    date = dateFrom.date()

    def getCountryData(date, bidding_zone_in, bidding_zone_out):
        url = getRequestUrl('A61&contract_MarketAgreement.Type=A01', InDomain = bidding_zone_in, OutDomain=bidding_zone_out, PeriodStart = getUTCFromCET(date),
                    PeriodEnd = getUTCFromCET(date + datetime.timedelta(days=1)))

        fileName = 'EntsoeForecastedCapacity'
        file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}_{bidding_zone_in}_{bidding_zone_out}', date)

        try:
            data = getHourlyDataFromXMLResponse(url)
            if data is None:
                return

            results = []
            for date, hour, power in data:
                for period in range(1, 5): 
                    results.append({
                        'dispatchDay': date.strftime("%Y-%m-%d"),
                        'dispatchPeriod': (hour - 1) * 4 + period,
                        'countryInName': bidding_zone_in,
                        'countryOutName': bidding_zone_out,
                        'value': power
                    })
            
            response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
        except Exception as e:
            message = f'Failed to parse {file.Url}: {e}'
            logException(e)
            response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

    while date <= dateTo.date():
        for bidding_zone_in, bidding_zone_out in CountryPairs:
            getCountryData(date, bidding_zone_in, bidding_zone_out)
            getCountryData(date, bidding_zone_out, bidding_zone_in)
        
        date += timedelta(days = 1)
    
    return envelope.getSuccessEnvelope(response)

def getCrossBorderPhysicalFlow(dateFrom: datetime.datetime, dateTo: datetime.datetime):
    response = []
    date = dateFrom.date()

    def getCountryData(date, bidding_zone_in, bidding_zone_out):

        url = getRequestUrl('A11', InDomain = bidding_zone_in, OutDomain=bidding_zone_out, PeriodStart = getUTCFromCET(date),
                    PeriodEnd = getUTCFromCET(date + datetime.timedelta(days=1)))

        fileName = 'EntsoeCrossBorderPhysicalFlow'
        file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}_{bidding_zone_in}_{bidding_zone_out}', date)


        try:
            data = getHourlyDataFromXMLResponse(url)
            if data is None:
                return

            results = []
            for date, hour, power in data:
                for period in range(1, 5): 
                    results.append({
                        'dispatchDay': date.date(),
                        'dispatchPeriod': (hour - 1) * 4 + period,
                        'countryInName': bidding_zone_in,
                        'countryOutName': bidding_zone_out,
                        'value': power
                    })
                    
            response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
        except Exception as e:
            message = f'Failed to parse {file.Url}: {e}'
            logException(e)
            response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

    while date <= dateTo.date():
        for bidding_zone_in, bidding_zone_out in CountryPairs:
            getCountryData(date, bidding_zone_in, bidding_zone_out)
            getCountryData(date, bidding_zone_out, bidding_zone_in)
        
        date += timedelta(days = 1)
    
    return envelope.getSuccessEnvelope(response)


def getAggregatedHydroFillingRate(dateFrom: datetime, dateTo: datetime):
    response = []
    date = dateFrom
    while date <= dateTo:
        
        for country in Countries:
            addedDates = set()
            
            url = getRequestUrl("A72", "A16", InDomain=country, PeriodStart = date.replace(hour = 0),
                            PeriodEnd = date.replace(hour = 0) + datetime.timedelta(days = 1))

            fileName = 'EntsoeAggregatedFillingRate'
            file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}_{country}', date)
            
            try:
                LoadData = []
                actual_data = getDailyOrAboveDataFromXMLResponse(url)
                
                for row in actual_data:
                    dispatchDate = row[0]
                    if dispatchDate not in addedDates:
                        LoadData.append({
                            'dispatchDay': formatDateTimeForJson(dispatchDate),
                            'countryName': country,
                            'value': row[1]
                            })
                        
                        addedDates.add(dispatchDate)
                
                response.append(getVirtualFileMetadata(LoadData, getMetadata(True, file.Id), file))
            except Exception as e:
                message = f'Failed to parse {file.Url}: {e}'
                logException(e)
                response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
                
        date += datetime.timedelta(days = 7)

    return envelope.getSuccessEnvelope(response)


def getEntsoeDamPrice(dateFrom: datetime, dateTo: datetime, countryShortCode: bool = True):
    response = []
    date = dateFrom

    granularity_dict = {
        'PT60M': "Hour",
        'PT15M': "QuarterHour"
    }
    
    while date <= dateTo:
        for country in DAMPriceCountries:
            url = getRequestUrl("A44", InDomain=country, OutDomain=country, PeriodStart = date.replace(hour = 0),
                            PeriodEnd = date.replace(hour = 0) + datetime.timedelta(days = 1))

            fileName = 'EntsoeDamPrice'
            file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}_{country[1]}', date)
            
            try:
                results = []
                for zone in DAMPriceCountries[country]:
                    url = getRequestUrl("A44", InDomain=zone, OutDomain=zone, PeriodStart = date.replace(hour = 0),
                                    PeriodEnd = date.replace(hour = 0) )

                    actual_data = getHourlyDAMDataFromXMLResponse(url, 'price.amount')

                    if actual_data is None:
                        continue

                    data_df = pd.DataFrame(actual_data, columns=["dispatchDate", "period", "quantity", "Resolution"])

                    list_of_resolutions = data_df['Resolution'].unique()

                    for resolution in list_of_resolutions:

                        df = data_df[data_df['Resolution'] == resolution]

                        summer_dst, winter_dst = dst_periods(date)  

                        resolution_offset = 60 / int(resolution.replace('PT', '').replace('M', ''))   

                        base_range = int(24 * resolution_offset)
                        if date.strftime('%Y-%m-%d') == summer_dst:
                            base_range -= int(1 * resolution_offset)
                        elif date.strftime('%Y-%m-%d') == winter_dst:
                            base_range += int(1 * resolution_offset)
                        max_range = base_range
                    

                        full_period_range = range(1, max_range + 1)  

                        # Create an empty DataFrame to collect results
                        filled_df = pd.DataFrame()

                        # Sort each group by period
                        group = df.sort_values(by="period").reset_index(drop=True)
                        
                        # Identify missing periods in the current group
                        existing_periods = set(group["period"])
                        missing_periods = sorted(set(full_period_range) - existing_periods)
                        
                        # List to hold new rows
                        new_rows = []

                        # Iterate through missing periods and fill based on the last known row
                        for period in missing_periods:
                            # Find the last row before the missing period
                            last_valid_row = group[group["period"] < period].iloc[-1] if not group[group["period"] < period].empty else None
                            if last_valid_row is not None:
                                new_row = last_valid_row.copy()
                                new_row["period"] = period
                                new_rows.append(new_row)

                        # Convert the list of new rows to a DataFrame
                        new_rows_df = pd.DataFrame(new_rows)

                        # Append new rows to the original group DataFrame
                        group = pd.concat([group, new_rows_df], ignore_index=True)

                        # Sort the DataFrame by 'period' to maintain order
                        group = group.sort_values(by="period").reset_index(drop=True)
                        
                        # Append to the final DataFrame
                        filled_df = pd.concat([filled_df, group], ignore_index=True)
                            
                        #quarterly_offset = 0
                        for dispatchDate, hour, price, res in filled_df.values.tolist():
                            
                            dispatch_day = formatDateTimeForJson(dispatchDate)
                            country_name = COUNTRY_SHORT_CODE_MAPPING[country] if countryShortCode else country

                            
                            if res == "PT60M":

                                if "PT15M" not in list_of_resolutions:
                                    for period in range(1, 5):
                                        results.append(
                                            {
                                                'dispatchDay': dispatch_day,
                                                'dispatchPeriod': (hour - 1) * 4 + period,
                                                'countryName': country_name,
                                                'value': price,
                                                'resolution': granularity_dict["PT15M"]
                                            } 
                                        )
                            # Append the original resolution (PT60M or any other)
                            results.append({
                                'dispatchDay': dispatch_day,
                                'dispatchPeriod': hour,
                                'countryName': country_name,
                                'value': price,
                                'resolution': granularity_dict[res]
                            })
                                                    
                    
                #results = aggregate_results_average(results, 'dispatchDay', 'dispatchPeriod', 'countryName')
                response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
            except Exception as e:
                message = f'Failed to parse {file.Url}: {e}'
                logException(e)
                response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
                
        date += datetime.timedelta(days = 1)

    return envelope.getSuccessEnvelope(response)

def getProductionPerCategory(dateFrom: datetime, dateTo: datetime):
    response = []
    date = dateFrom
    
    while date <= dateTo:
        for country in Countries:
            for psr_type, _ in ProductionCategories:
                url = getRequestUrl('A75', 'A16', InDomain = country, PsrType = psr_type, PeriodStart = getUTCFromCET(date),
                            PeriodEnd = getUTCFromCET(date + datetime.timedelta(days = 1)))
                        
                fileName = 'EntsoeProductionPerCategory'
                file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}_{country}_{psr_type}', date)
                results = []

                try:
                    data = getHourlyDAMDataFromXMLResponse(url)
                    if data is None:
                        continue

                    filled_df = fillMissingPeriodsInData(data, date, isForecast=False)

                    if filled_df.empty:
                        response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
                        continue

                    
                    for dispatchDate, period, power, resolution in filled_df.values.tolist():

                        if resolution == 'PT60M':
                            repetition = 60/15
                        elif resolution == 'PT30M':
                            repetition = 30/15
                        elif resolution == 'PT15M':
                            repetition = 15/15

                        for rep in range(1, int(repetition) + 1):
                            period_offset = rep if resolution != 'PT15M' else 0

                            results.append({
                                'dispatchDay': dispatchDate.strftime('%Y-%m-%d'),
                                'dispatchPeriod':int(repetition * (period - 1) + period_offset if resolution != 'PT15M' else period),
                                'countryName': country,
                                'productionCategory': int(psr_type.replace('B', '')),
                                'value': power
                                })
                
                    response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
                except Exception as e:
                    message = f'Failed to parse {file.Url}: {e}'
                    logException(e)
                    response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
    
        date += datetime.timedelta(days = 1)

    return envelope.getSuccessEnvelope(response)


def getWindAndSolarForecast(dateFrom: datetime, dateTo: datetime):
    response = []
    date = dateFrom
    
    while date <= dateTo:
        for bidding_zone in Countries:
            for psr_type, label in WindSolarCategories:
                url = getRequestUrl('A69', 'A01', InDomain = bidding_zone, PsrType = psr_type, PeriodStart = getUTCFromCET(date),
                            PeriodEnd = getUTCFromCET(date + datetime.timedelta(days = 1)))
                        
                fileName = 'EntsoeWindAndSolarForecast'
                file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}_{bidding_zone}_{psr_type}', date)
                
                try:
                    data = getHourlyDAMDataFromXMLResponse(url)
                    if data is None:
                        continue

                    filled_df = fillMissingPeriodsInData(data, date)
                    
                    results = []
                    for dispatchDate, period, power, resolution in filled_df.values.tolist():

                        if resolution == 'PT60M':
                            repetition = 60/15
                        elif resolution == 'PT30M':
                            repetition = 30/15
                        elif resolution == 'PT15M':
                            repetition = 15/15

                        for rep in range(1, int(repetition) + 1):
                            period_offset = rep if resolution != 'PT15M' else 0

                            results.append({
                                'dispatchDay': dispatchDate.strftime('%Y-%m-%d'),
                                'dispatchPeriod':int(repetition * (period - 1) + period_offset if resolution != 'PT15M' else period),
                                'countryName': bidding_zone,
                                'productionCategory': int(psr_type.replace('B', '')),
                                'value': power
                                })
                
                    response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
                except Exception as e:
                    message = f'Failed to parse {file.Url}: {e}'
                    logException(e)
                    response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
    
        date += datetime.timedelta(days = 1)

    return envelope.getSuccessEnvelope(response)


def getProductionPerGeneratingUnit(dateFrom: datetime, dateTo: datetime):
    response = []
    date = dateFrom
    
    while date <= dateTo:
        url = getRequestUrl('A73', 'A16', InDomain = '10YGR-HTSO-----Y', PeriodStart = getUTCFromCET(date),
                    PeriodEnd = getUTCFromCET(date + timedelta(days=1)))
            
        fileName = 'EntsoeWProductionPerGeneratingUnit'
        file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

        try:
            data = getDetailHourlyDataFromXMLResponse(url)
            if data is None:
                date += datetime.timedelta(days = 1)
                continue

            results = []
            for PsrType, UnitId, Name, Hour, Load in data:
                for period in range(1, 5): 
                    results.append({
                        'DispatchDay': date,
                        'DispatchPeriod': (Hour - 1) * 4 + period,
                        'EntityName': Name,
                        'ProductionCategory': int(PsrType.replace('B', '')),
                        'Value': Load
                        })
                
            response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
        except Exception as e:
            message = f'Failed to parse {file.Url}: {e}'
            logException(e)
            response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
            
        date += datetime.timedelta(days = 1)

    return envelope.getSuccessEnvelope(response)

def getDayAheadGenerationForecast(dateFrom: datetime, dateTo: datetime):
    response = []
    date = dateFrom
    
    while date <= dateTo:
        for bidding_zone in Countries:
            for psr_type, label in ProductionCategories:
                url = getRequestUrl('A69', 'A01', PsrType=psr_type, InDomain = bidding_zone, PeriodStart = getUTCFromCET(date),
                            PeriodEnd = getUTCFromCET(date + datetime.timedelta(days = 1)))
                
                fileName = 'EntsoeDayAheadGenerationForecast'
                file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

                try:
                    data = getHourlyDAMDataFromXMLResponse(url)
                    if data is None:
                        continue

                    filled_df = fillMissingPeriodsInData(data, date)
                    
                    
                    results = []
                    for dispatchDate, period, power, resolution in filled_df.values.tolist():

                        if resolution == 'PT60M':
                            repetition = 60/15
                        elif resolution == 'PT30M':
                            repetition = 30/15
                        elif resolution == 'PT15M':
                            repetition = 15/15

                        for rep in range(1, int(repetition) + 1):
                            period_offset = rep if resolution != 'PT15M' else 0

                            results.append({
                                'dispatchDay': dispatchDate.strftime('%Y-%m-%d'),
                                'dispatchPeriod':int(repetition * (period - 1) + period_offset if resolution != 'PT15M' else period),
                                'countryName': bidding_zone,
                                'productionCategory': int(psr_type.replace('B', '')),
                                'value': power
                                })
                
                    response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
                except Exception as e:
                    message = f'Failed to parse {file.Url}: {e}'
                    logException(e)
                    response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
        
        date += datetime.timedelta(days = 1)

    return envelope.getSuccessEnvelope(response)

    
def getAggregatedBids(dateFrom: datetime, dateTo: datetime):
    response = []
    date = dateFrom
    
    while date <= dateTo:
        for reserveType in ReserveTypes:
            url = getRequestUrl('A24', reserveType[1], AreaDomain = MainBiddingZone, PeriodStart = getUTCFromCET(date),
                        PeriodEnd = getUTCFromCET(date + datetime.timedelta(days = 1)))
            
            fileName = 'EntsoeAggregatedBids'
            file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

            try:
                data, date_end = getDetailQuarterlyReserveDataFromXMLResponse(url)
                if data is None or data == []:
                    continue

                df = pd.DataFrame(data, columns=["reserveDirection", "dispatchDate", "period", "quantity", "secondaryQuantity"])

                # Convert reserveDirection to actual booleans if they are stored as strings
                df['reserveDirection'] = df['reserveDirection'].astype(bool)


                summer_dst, winter_dst = dst_periods(date)        
                if date_end == getUTCFromCET(date + datetime.timedelta(days = 1)).strftime('%Y-%m-%dT%H:%MZ'):
                    if date.strftime('%Y-%m-%d') == summer_dst:
                        max_range = 93
                    elif date.strftime('%Y-%m-%d') == winter_dst:
                        max_range = 101
                    else:
                        max_range = 97 
                else:
                    max_range = df['period'].max() + 1

                full_period_range = range(1, max_range)  

                # Create an empty DataFrame to collect results
                filled_df = pd.DataFrame()

                # Process each reserveDirection group individually
                for reserve_direction, group in df.groupby("reserveDirection"):
                    # Sort each group by period
                    group = group.sort_values(by="period").reset_index(drop=True)
                    
                    # Identify missing periods in the current group
                    existing_periods = set(group["period"])
                    missing_periods = sorted(set(full_period_range) - existing_periods)
                    
                    # List to hold new rows
                    new_rows = []

                    # Iterate through missing periods and fill based on the last known row
                    for period in missing_periods:
                        # Find the last row before the missing period
                        last_valid_row = group[group["period"] < period].iloc[-1] if not group[group["period"] < period].empty else None
                        if last_valid_row is not None:
                            new_row = last_valid_row.copy()
                            new_row["period"] = period
                            new_rows.append(new_row)

                    # Convert the list of new rows to a DataFrame
                    new_rows_df = pd.DataFrame(new_rows)

                    # Append new rows to the original group DataFrame
                    group = pd.concat([group, new_rows_df], ignore_index=True)

                    # Sort the DataFrame by 'period' to maintain order
                    group = group.sort_values(by="period").reset_index(drop=True)
                    
                    # Append to the final DataFrame
                    filled_df = pd.concat([filled_df, group], ignore_index=True)


                results = []
                for reserveDirection, dispatchDate, period, quantity, secondaryQuantity in filled_df.values.tolist():

                    results.append({
                        'DispatchDay': dispatchDate,
                        'DispatchPeriod': period,
                        'Direction': reserveDirection,
                        'ZoneId': 1,
                        'ReserveTypeName': reserveType[0],
                        'ReserveType': RESERVE_TYPE_ENTSOE[reserveType[0]],
                        'Value': round(quantity, 3),
                        'SecondaryValue': round(secondaryQuantity, 3) if secondaryQuantity is not None else None
                        })
                
                response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
            except Exception as e:
                message = f'Failed to parse {file.Url}: {e}'
                logException(e)
                response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
    
        date += datetime.timedelta(days = 1)

    return envelope.getSuccessEnvelope(response)
    
    
def getActivatedEnergyPrices(dateFrom: datetime, dateTo: datetime):
    response = []
    date = dateFrom
    
    while date <= dateTo:
        for reserveType in ReserveTypesPrices:
            url = getRequestUrl('A84', BusinessType= reserveType[1], ControlArea_Domain = MainBiddingZone, PsrType= 'A03',
                                PeriodStart = getUTCFromCET(date), PeriodEnd = getUTCFromCET(date + datetime.timedelta(days = 1)))
            
            fileName = 'EntsoeActivatedEnergyPrices'
            file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

            try:

                data = getDetailQuarterlyReservePricesFromXMLResponse(url)

                data, date_end = getDetailQuarterlyReservePricesFromXMLResponse(url)
                if data is None:
                    continue
                

                df = pd.DataFrame(data, columns=["reserveDirection", "dispatchDate", "period", "quantity"])

                # Convert reserveDirection to actual booleans if they are stored as strings
                df['reserveDirection'] = df['reserveDirection'].astype(bool)


                summer_dst, winter_dst = dst_periods(date)        
                if date_end == getUTCFromCET(date + datetime.timedelta(days = 1)).strftime('%Y-%m-%dT%H:%MZ'):
                    if date.strftime('%Y-%m-%d') == summer_dst:
                        max_range = 93
                    elif date.strftime('%Y-%m-%d') == winter_dst:
                        max_range = 101
                    else:
                        max_range = 97 
                else:
                    max_range = df['period'].max() + 1

                full_period_range = range(1, max_range)  

                # Create an empty DataFrame to collect results
                filled_df = pd.DataFrame()

                # Process each reserveDirection group individually
                for reserve_direction, group in df.groupby("reserveDirection"):
                    # Sort each group by period
                    group = group.sort_values(by="period").reset_index(drop=True)
                    
                    # Identify missing periods in the current group
                    existing_periods = set(group["period"])
                    missing_periods = sorted(set(full_period_range) - existing_periods)
                    
                    # List to hold new rows
                    new_rows = []

                    # Iterate through missing periods and fill based on the last known row
                    for period in missing_periods:
                        # Find the last row before the missing period
                        last_valid_row = group[group["period"] < period].iloc[-1] if not group[group["period"] < period].empty else None
                        if last_valid_row is not None:
                            new_row = last_valid_row.copy()
                            new_row["period"] = period
                            new_rows.append(new_row)

                    # Convert the list of new rows to a DataFrame
                    new_rows_df = pd.DataFrame(new_rows)

                    # Append new rows to the original group DataFrame
                    group = pd.concat([group, new_rows_df], ignore_index=True)

                    # Sort the DataFrame by 'period' to maintain order
                    group = group.sort_values(by="period").reset_index(drop=True)
                    
                    # Append to the final DataFrame
                    filled_df = pd.concat([filled_df, group], ignore_index=True)


                results = []
                for reserveDirection, dispatchDate, period, price in filled_df.values.tolist():

                    results.append({
                            'DispatchDay': dispatchDate,
                            'DispatchPeriod': period,
                            'Direction': reserveDirection,
                            'ZoneId': 1,
                            'ReserveTypeName': reserveType[0],
                            'ReserveType': RESERVE_TYPE[reserveType[0]],
                            'Value': round(price, 2)
                            })
                
                response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
                
            except Exception as e:
                message = f'Failed to parse {file.Url}: {e}'
                logException(e)
                response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
    
        date += datetime.timedelta(days = 1)

    return envelope.getSuccessEnvelope(response)
def getBalancingEnergyBids(dateFrom: datetime, dateTo: datetime):
    response = []
    date = dateFrom

    while date <= dateTo:
        url = getRequestUrl('A37', ControlArea_Domain=MainBiddingZone,
                            PeriodStart=getUTCFromCET(date),
                            PeriodEnd=getUTCFromCET(date + datetime.timedelta(days=1)))

        fileName = 'EntsoeBalancingEnergyBids'
        file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

        try:
            data, date_end = getDetailQuarterlyReservePricesFromXMLResponse(url)
            if data is None:
                date += datetime.timedelta(days=1)
                continue

            df = pd.DataFrame(data, columns=["reserveDirection", "dispatchDate", "period", "quantity"])
            df['reserveDirection'] = df['reserveDirection'].astype(bool)

            summer_dst, winter_dst = dst_periods(date)
            if date_end == getUTCFromCET(date + datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%MZ'):
                if date.strftime('%Y-%m-%d') == summer_dst:
                    max_range = 93
                elif date.strftime('%Y-%m-%d') == winter_dst:
                    max_range = 101
                else:
                    max_range = 97
            else:
                max_range = df['period'].max() + 1

            full_period_range = range(1, max_range)
            filled_df = pd.DataFrame()

            for reserve_direction, group in df.groupby("reserveDirection"):
                group = group.sort_values(by="period").reset_index(drop=True)
                existing_periods = set(group["period"])
                missing_periods = sorted(set(full_period_range) - existing_periods)
                new_rows = []
                for period in missing_periods:
                    last_valid_row = group[group["period"] < period].iloc[-1] if not group[group["period"] < period].empty else None
                    if last_valid_row is not None:
                        new_row = last_valid_row.copy()
                        new_row["period"] = period
                        new_rows.append(new_row)
                new_rows_df = pd.DataFrame(new_rows)
                group = pd.concat([group, new_rows_df], ignore_index=True)
                group = group.sort_values(by="period").reset_index(drop=True)
                filled_df = pd.concat([filled_df, group], ignore_index=True)

            results = []
            for reserveDirection, dispatchDate, period, quantity in filled_df.values.tolist():
                results.append({
                    'DispatchDay': dispatchDate,
                    'DispatchPeriod': period,
                    'Direction': reserveDirection,
                    'ZoneId': 1,
                    'Quantity': round(quantity, 2)
                })

            response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))

        except Exception as e:
            message = f'Failed to parse {file.Url}: {e}'
            logException(e)
            response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

        date += datetime.timedelta(days=1)

    return envelope.getSuccessEnvelope(response)
def getCurrentBalancingState(dateFrom: datetime, dateTo: datetime):
    response = []
    date = dateFrom

    while date <= dateTo:
        url = getRequestUrl('A86', ControlArea_Domain=MainBiddingZone,
                            PeriodStart=getUTCFromCET(date),
                            PeriodEnd=getUTCFromCET(date + datetime.timedelta(days=1)))

        fileName = 'EntsoeCurrentBalancingState'
        file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

        try:
            data, date_end = getDetailQuarterlyReservePricesFromXMLResponse(url)
            if data is None:
                date += datetime.timedelta(days=1)
                continue

            df = pd.DataFrame(data, columns=["dispatchDate", "period", "value"])

            summer_dst, winter_dst = dst_periods(date)
            if date_end == getUTCFromCET(date + datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%MZ'):
                if date.strftime('%Y-%m-%d') == summer_dst:
                    max_range = 93
                elif date.strftime('%Y-%m-%d') == winter_dst:
                    max_range = 101
                else:
                    max_range = 97
            else:
                max_range = df['period'].max() + 1

            full_period_range = range(1, max_range)
            filled_df = pd.DataFrame()

            group = df.sort_values(by="period").reset_index(drop=True)
            existing_periods = set(group["period"])
            missing_periods = sorted(set(full_period_range) - existing_periods)
            new_rows = []
            for period in missing_periods:
                last_valid_row = group[group["period"] < period].iloc[-1] if not group[group["period"] < period].empty else None
                if last_valid_row is not None:
                    new_row = last_valid_row.copy()
                    new_row["period"] = period
                    new_rows.append(new_row)
            new_rows_df = pd.DataFrame(new_rows)
            group = pd.concat([group, new_rows_df], ignore_index=True)
            group = group.sort_values(by="period").reset_index(drop=True)
            filled_df = pd.concat([filled_df, group], ignore_index=True)

            results = []
            for dispatchDate, period, value in filled_df.values.tolist():
                results.append({
                    'DispatchDay': dispatchDate,
                    'DispatchPeriod': period,
                    'ZoneId': 1,
                    'Value': round(value, 2)
                })

            response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))

        except Exception as e:
            message = f'Failed to parse {file.Url}: {e}'
            logException(e)
            response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

        date += datetime.timedelta(days=1)

    return envelope.getSuccessEnvelope(response)
def getAFRRCBMP(dateFrom: datetime, dateTo: datetime):
    response = []
    date = dateFrom

    while date <= dateTo:
        url = getRequestUrl('A84', BusinessType='A96', ControlArea_Domain=MainBiddingZone,
                            PeriodStart=getUTCFromCET(date),
                            PeriodEnd=getUTCFromCET(date + datetime.timedelta(days=1)))

        fileName = 'EntsoeAFRRCBMP'
        file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

        try:
            data, date_end = getDetailQuarterlyReservePricesFromXMLResponse(url)
            if data is None:
                date += datetime.timedelta(days=1)
                continue

            df = pd.DataFrame(data, columns=["dispatchDate", "period", "price"])

            summer_dst, winter_dst = dst_periods(date)
            if date_end == getUTCFromCET(date + datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%MZ'):
                if date.strftime('%Y-%m-%d') == summer_dst:
                    max_range = 93
                elif date.strftime('%Y-%m-%d') == winter_dst:
                    max_range = 101
                else:
                    max_range = 97
            else:
                max_range = df['period'].max() + 1

            full_period_range = range(1, max_range)
            filled_df = pd.DataFrame()

            group = df.sort_values(by="period").reset_index(drop=True)
            existing_periods = set(group["period"])
            missing_periods = sorted(set(full_period_range) - existing_periods)
            new_rows = []
            for period in missing_periods:
                last_valid_row = group[group["period"] < period].iloc[-1] if not group[group["period"] < period].empty else None
                if last_valid_row is not None:
                    new_row = last_valid_row.copy()
                    new_row["period"] = period
                    new_rows.append(new_row)
            new_rows_df = pd.DataFrame(new_rows)
            group = pd.concat([group, new_rows_df], ignore_index=True)
            group = group.sort_values(by="period").reset_index(drop=True)
            filled_df = pd.concat([filled_df, group], ignore_index=True)

            results = []
            for dispatchDate, period, price in filled_df.values.tolist():
                results.append({
                    'DispatchDay': dispatchDate,
                    'DispatchPeriod': period,
                    'ZoneId': 1,
                    'Value': round(price, 2)
                })

            response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))

        except Exception as e:
            message = f'Failed to parse {file.Url}: {e}'
            logException(e)
            response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

        date += datetime.timedelta(days=1)

    return envelope.getSuccessEnvelope(response)

def getUnavailabilityOfUnits(dateFrom: datetime, dateTo: datetime):
    response = []
    date = dateFrom

    try:

        for documentType in ['A80', 'A77']:
            results = []
            
            url = getRequestUrl(documentType, BusinessType='A53', BiddingZone_Domain=MainBiddingZone, PeriodStart = getUTCFromCET(dateFrom), PeriodEnd = getUTCFromCET(dateTo))
            fileName = 'EntsoeUnavailabilityOfUnits'
            file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

            resp = urlopen(url)
            zip = zipfile.ZipFile(BytesIO(resp.read()))
            for name in zip.namelist():
                if name.endswith('.xml'):
                    xml = zip.open(name)
                    content = xml.read()
                    dict = xmltodict.parse(content)
                    row = dict['Unavailability_MarketDocument']['TimeSeries']
                
                    validFrom = row['start_DateAndOrTime.date']
                    validFromTime = row['start_DateAndOrTime.time']
                    validTo = row['end_DateAndOrTime.date'] 
                    validToTime = row['end_DateAndOrTime.time']

                    validFrom = parser.parse(validFrom + 'T' + validFromTime)
                    validTo = parser.parse(validTo + 'T' + validToTime)

                    tz = timezone('Europe/Berlin')

                    validFromCet = validFrom.astimezone(tz)
                    validToCet = validTo.astimezone(tz)

                    results.append({
                        'DateFrom': validFromCet.strftime('%Y-%m-%dT%H:%M:%S'),
                        'DateTo': validToCet.strftime('%Y-%m-%dT%H:%M:%S'),
                        'EntityName': row['production_RegisteredResource.name'],
                        'Value': float(row['Available_Period']['Point']['quantity']),
                        'Version': datetime.datetime.now()
                    })
                
            response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))

    except Exception as e:
        message = f'Failed to parse {file.Url}: {e}'
        logException(e)
        response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
    
    return envelope.getSuccessEnvelope(response)
    
def getUnavailabilityOfInterconnections(dateFrom: datetime, dateTo: datetime):
    response = []
    date = dateFrom
    
    for countryPair in CountryPairs:
        try:
            countryFrom = countryPair[0]
            countryTo = countryPair[1]

            url = getRequestUrl("A78", InDomain=countryFrom, OutDomain=countryTo, PeriodStart = getUTCFromCET(dateFrom), PeriodEnd = getUTCFromCET(dateTo))
            fileName = 'EntsoeUnavailabilityOfInterconnections'
            file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

            results = []

            resp = urlopen(url)
            content_type = resp.info().get_content_type()
            if content_type != 'application/zip':
                continue

            resp = urlopen(url)
            zip = zipfile.ZipFile(BytesIO(resp.read()))
            for name in zip.namelist():
                if name.endswith('.xml'):
                    xml = zip.open(name)
                    content = xml.read()
                    dict = xmltodict.parse(content)
                    row = dict['Unavailability_MarketDocument']['TimeSeries']
                
                    validFrom = row['start_DateAndOrTime.date']
                    validFromTime = row['start_DateAndOrTime.time']
                    validTo = row['end_DateAndOrTime.date'] 
                    validToTime = row['end_DateAndOrTime.time']

                    validFrom = parser.parse(validFrom + 'T' + validFromTime)
                    validTo = parser.parse(validTo + 'T' + validToTime)

                    tz = timezone('Europe/Berlin')

                    validFromCet = validFrom.astimezone(tz)
                    validToCet = validTo.astimezone(tz)

                    results.append({
                        'DateFrom': validFromCet.strftime('%Y-%m-%dT%H:%M:%S'),
                        'DateTo': validToCet.strftime('%Y-%m-%dT%H:%M:%S'),
                        'CountryName': countryTo,
                        'Value': float(row['Available_Period']['Point']['quantity']),
                        'Version': datetime.datetime.now()
                    })
                        
                    response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))

        except Exception as e:
            message = f'Failed to parse {file.Url}: {e}'
            logException(e)
            response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
    
    return envelope.getSuccessEnvelope(response)


def getImplicitAllocationsDayAhead(dateFrom: datetime.datetime, dateTo: datetime.datetime):
    response = []
    date = dateFrom.date()

    def getCountryData(date, bidding_zone_in, bidding_zone_out):

        url = getRequestUrl('A31', InDomain = bidding_zone_in, OutDomain=bidding_zone_out, PeriodStart = getUTCFromCET(date),
                    PeriodEnd = getUTCFromCET(date + datetime.timedelta(days=1)), ContractMarketAgreementType='A01', AuctionType='A01')

        fileName = 'EntsoeImplicitAllocationsDayAhead'
        file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}_{bidding_zone_in}_{bidding_zone_out}', date)


        try:
            data = getHourlyDataFromXMLResponse(url)
            if data is None:
                return

            results = []
            for date, hour, power in data:
                for period in range(1, 5): 
                    results.append({
                        'dispatchDay': date,
                        'dispatchPeriod': (hour - 1) * 4 + period,
                        'countryInName': bidding_zone_in,
                        'countryOutName': bidding_zone_out,
                        'value': power
                    })
                    
            response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
        except Exception as e:
            message = f'Failed to parse {file.Url}: {e}'
            logException(e)
            response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

    while date <= dateTo.date():
        for bidding_zone_in, bidding_zone_out in CountryPairs:
            getCountryData(date, bidding_zone_in, bidding_zone_out)
            getCountryData(date, bidding_zone_out, bidding_zone_in)
        
        date += timedelta(days = 1)
    
    return envelope.getSuccessEnvelope(response)

import csv
partyTypes = {
    "Other": 0,
    "X": 1,
    "BalanceResponsibleParty": 2,
    "ConsumptionResponsibleParty": 3,
    "ESB04990727": 4,
    "TradeResponsibleParty": 5
}

def getEntsoeParties():
    fileName = 'eic-codes-csv/X_eiccodes'
    date = datetime.datetime.now()
    url = "https://eepublicdownloads.entsoe.eu/eic-codes-csv/X_eiccodes.csv"
    file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

    data = requests.get(url, verify=False)
    decoded_content = data.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=';')
    my_list = list(cr)
    parties = pd.DataFrame(my_list[1:], columns = my_list[0])
    #parties = parties.drop(11, axis = 1)
    
    #replace column name type with PartyType
    parties = parties.rename(columns = {'type':'PartyType'})
    
    data = parties.fillna('').to_dict('records')
    parsingMetadata = getMetadata(True, file.Id)

    response = [getVirtualFileMetadata(data, parsingMetadata, file)]

    return envelope.getSuccessEnvelope(response)

def getEntsoeAreas():
    fileName = 'eic-codes-csv/Y_eiccodes'
    date = datetime.datetime.now()
    url = "https://eepublicdownloads.entsoe.eu/eic-codes-csv/Y_eiccodes.csv"
    file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

    data = requests.get(url, verify=False)
    decoded_content = data.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=';')
    my_list = list(cr)
    areas = pd.DataFrame(my_list[1:])
    #areas = areas.drop(11, axis = 1)
    areas.columns = my_list[0]
    
    data = areas.fillna('').to_dict('records')
    metadata = getMetadata(True, file.Id)
    response = [getVirtualFileMetadata(data, metadata, file)]
    
    return envelope.getSuccessEnvelope(response)



def getDayAheadAggregatedGenerationForecast(dateFrom: datetime, dateTo: datetime) -> Envelope[List[VirtualFileMetadataPayload[List[EntsoeDayAheadAggregatedForecastModel]]]]:
    response = []
    date = dateFrom
    
    while date <= dateTo:
        for bidding_zone in DAMPriceCountries:
            url = getRequestUrl('A71', 'A01', InDomain = bidding_zone, PeriodStart = getUTCFromCET(date),
                        PeriodEnd = getUTCFromCET(date + datetime.timedelta(days=1)))
            
            fileName = 'EntsoeDayAheadGenerationForecast'
            file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

            try:
                data = getHourlyDataDayAheadGenerationFromXMLResponse(url, date)
                if data is None:
                    continue

                results = []

                generation = data['Generation']
                consumption = data['Consumption']
                timestamps = list(generation.keys())

                for i in timestamps:
                    data_to_dict = {
                                    'Timestamp': i,
                                    'ScheduledGeneration': generation[i],
                                    'ScheduledConsumption': consumption[i],
                                    'BiddingZone': COUNTRY_SHORT_CODE_MAPPING[bidding_zone],
                                }
                    day_ahead_generation_data = EntsoeDayAheadAggregatedForecastModel(**data_to_dict)
                    results.append(day_ahead_generation_data)
                
                response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
            except Exception as e:
                message = f'Failed to parse {file.Url}: {e}'
                logException(e)
                response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
    
        date += datetime.timedelta(days = 1)

    return envelope.getSuccessEnvelope(response)