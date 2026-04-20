from datetime import datetime
import io
import requests
import json
import pandas as pd
import helpers.log_helper as log_helper
import sources.eia as eia
from dateutil import parser
from models.metadata import VirtualFileMetadataPayload, getMetadata, getVirtualFileMetadata
from helpers.file_helper import getVirtualFileMetadataFromUrl
import pytz
from interface import envelope
from sources.the_ice_metadata import *

# Set url dictionary
url_dict = dict()
url_dict["eua"] = "https://www.ice.com/marketdata/DelayedMarkets.shtml?getContractsAsJson=&productId=390&hubId=564"
url_dict["ttf"] = "https://www.ice.com/marketdata/DelayedMarkets.shtml?getContractsAsJson=&productId=4331&hubId=7979"
url_dict["brent"] = "https://www.ice.com/marketdata/DelayedMarkets.shtml?getContractsAsJson=&productId=254&hubId=403"
url_dict["power"] = "https://www.ice.com/api/productguide/groups/products/Futures-Options?filter=power&sortType=name&sortOrder=up&max=50&offset=0"
url_dict["gas"] = "https://www.ice.com/api/productguide/groups/products?filter=ttf+gas&sortType=name&sortOrder=up&max=50&offset=0"
url_dict["EUA"] = "https://www.ice.com/api/productguide/groups/products?filter=eua&sortType=name&sortOrder=up&max=50&offset=0"
url_dict["oil"] = "https://www.ice.com/api/productguide/groups/products?filter=oil&sortType=name&sortOrder=up&max=50&offset=0"

def getProduct(product: str):
    responses = []
    try:
        """
         Get Real-Time Prices
        """
        # Set url
        url = url_dict[product]

        fileName = 'ICE_RealTime_' + product
        file = getVirtualFileMetadataFromUrl(url, fileName, product, datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0), '.json')
        
        # Send get request
        response = requests.get(url=url, verify=False)

        # Get json file
        json_data = json.loads(response.text)

        # Create dataframe
        product_data = pd.DataFrame(data=json_data)
        product_data = product_data.dropna().reset_index()
        product_data["dispatchDay"] = product_data["lastTime"].apply(lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M %p %Z').strftime("%Y-%m-%d %H:%M"))
        product_data["endDate"] = product_data["endDate"].apply(lambda x: str(x.split(" ")[0]) + " " + str(x.split(" ")[2])
                                                                          + " " + str(x.split(" ")[1]) + " " + str(x.split(" ")[len(x.split(" "))-1]))
        product_data["marketStrip"] = product_data["marketStrip"].apply(lambda x: x.replace(" ", "."))
        product_data = product_data.drop(columns=["index", "marketId"])
        # product_data.insert(0, "product", product.upper())
        product_data = product_data[["marketStrip", "lastTime", "endDate", "change", "volume", "lastPrice"]]

        # Add product data to product dictionary
        data = product_data.to_dict(orient="records")
        responses.append(getVirtualFileMetadata(data, getMetadata(True, file.Id), file))

    except Exception as e:
        log_helper.logException(e)

        message = f'Failed to parse {file.Url}: {e}'
        responses.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

    return envelope.getSuccessEnvelope(responses)

def getProductClosePrices(product: str):
    responses = []
    try:
        url = url_dict[product]
        fileName = 'ICE_' + product
        file = getVirtualFileMetadataFromUrl(url, fileName, product, datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0), '.json')

        response = requests.get(url, verify=False)

        product_data = pd.DataFrame(data=json.loads(response.text))
        prices = []
        # Get active market strips and ids
        for r in product_data.iterrows():
            market_strip = r[1]["marketStrip"]
            market_id = r[1]["marketId"]

            # Set url for close prices
            url = f"https://www.ice.com/marketdata/DelayedMarkets.shtml?getHistoricalChartDataAsJson=&marketId={market_id}&historicalSpan=2"

            # Get data
            response = requests.get(url, verify=False)

            # Get close data
            json_data = json.loads(response.text)["bars"]

            prices.extend({ 'product': market_strip.upper(), 'dispatchDay': datetime.strptime(row[0], "%c").strftime("%Y-%m-%d"), 'value': row[1] } for row in json_data)

        responses.append(getVirtualFileMetadata(prices, getMetadata(True, file.Id), file))
    except Exception as e:
        log_helper.logException(e)

        message = f'Failed to parse {file.Url}: {e}'
        responses.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

    return envelope.getSuccessEnvelope(responses)

def getTTFFutures():
    return getProductClosePrices('ttf')
    
def getBRENT(dateStart: datetime, dateEnd: datetime):
    response = []
    fileName = 'RBRTEd'
    file = getVirtualFileMetadataFromUrl(eia.BRENT_SOURCE, fileName, f'{fileName}_{dateStart.strftime("%Y%m%d")}_{dateEnd.strftime("%Y%m%d")}', dateStart, '.xls', targetDateTo=dateEnd)

    try:
        spot_prices = eia.getBrentPrice(dateStart, dateEnd)

        response.append(getVirtualFileMetadata(spot_prices, getMetadata(True, file.Id), file))
    except Exception as e:
        log_helper.logException(e)

        message = f'Failed to parse Brent prices: {e}'
        response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

    return envelope.getSuccessEnvelope(response)
    

def getTheIceFuturesPower(product: str, dateFrom : str, dateTo : str)-> envelope.Envelope[List[VirtualFileMetadataPayload[List[Price]]]]:
    responses = []
    
    url = url_dict[product]
    
    # Send get request
    available_future_power_response = requests.get(url=url, verify=False)

    # Get json file
    list_of_country_products = json.loads(available_future_power_response.text)

    if list_of_country_products is None:
        return envelope.getSuccessEnvelope(responses)
    
    product_metadata_df = pd.DataFrame(data=list_of_country_products["results"])

    for country in COUNTRIES:

        filtered_country_metadata = product_metadata_df[product_metadata_df["physicalCommodity"].isin(country["physicalCommodities"])]

        for _, product_metadata in filtered_country_metadata.iterrows():

            try:
                fileName = 'The_ICE_'+ "_".join(product_metadata["specName"].split(" "))
                file = getVirtualFileMetadataFromUrl(url, fileName, product, datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0), '.json')
                
                url = f"https://www.ice.com/marketdata/DelayedMarkets.shtml?getContractsAsJson=&productId={product_metadata['productId']}&hubId={product_metadata['hubId']}"

                expiration_date_metadata_url = f"https://www.ice.com/api/productguide/spec/{product_metadata['specId']}/expiry/csv"

                #get contract expiration date metadata
                response = requests.get(url=expiration_date_metadata_url, verify=False)
                expiration_date_metadata = pd.read_csv(io.BytesIO(response.content), index_col=False)
                expiration_date_metadata.index = expiration_date_metadata["CONTRACT SYMBOL"].map(lambda x: x.split('"')[1])

                #get contract metadata
                response = requests.get(url=url, verify=False)
                json_data = json.loads(response.text)
                contract_metadata = pd.DataFrame(data=json.loads(response.text))

                load_type = get_load_type(product_metadata["specName"])

                prices = []
                # Get active market strips and ids
                for r in contract_metadata.iterrows():
                    
                    #get contract name
                    market_strip = r[1]["marketStrip"]
                    market_id = r[1]["marketId"]
                    
                    #discard if contract is not in the date range

                    contract_date_from, contract_date_to = get_date_from_expiration_metadata(expiration_date_metadata , market_strip, "LTD", "FTD")
                    
                    contract_date_to = datetime.strptime(contract_date_to, "%m/%d/%Y")
                    contract_date_from = datetime.strptime(contract_date_from, "%m/%d/%Y")


                    if contract_date_from < datetime.strptime(dateFrom, "%Y-%m-%d") and contract_date_to > datetime.strptime(dateTo, "%Y-%m-%d"):
                        continue


                    dateStart, dateEnd = get_date_from_expiration_metadata(expiration_date_metadata , market_strip, "FDD", "LDD")

                    url = f"https://www.ice.com/marketdata/DelayedMarkets.shtml?getHistoricalChartDataAsJson=&marketId={market_id}&historicalSpan=2"

                    # Get contract timeseries
                    response = requests.get(url, verify=False)

                    if response.text == "":
                        continue

                    json_data = json.loads(response.text)["bars"]

                    product_name = get_product_name(market_strip.upper())

                    #load data to pydantic models
                    prices.append(
                                Price(
                                    productCode = product_name,
                                    name = product_name,
                                    commodityLoadType = load_type,
                                    index = get_commodity_name(country["countryCode"], product_metadata["specName"],product.capitalize()),
                                    unitOfMeasurement = UNIT_OF_MEASUREMENT[product_metadata["physicalCommodity"]],
                                    productType = getFutureTypeFromDateRange(
                                        datetime.strptime(dateStart, "%m/%d/%Y"),
                                        datetime.strptime(dateEnd, "%m/%d/%Y")
                                    ),
                                    dateFrom = datetime.strptime(dateStart, "%m/%d/%Y").strftime("%Y-%m-%d"),
                                    dateTo = datetime.strptime(dateEnd, "%m/%d/%Y").strftime("%Y-%m-%d"),
                                    commodityIndexName = get_commodity_name(country["countryCode"], product_metadata["specName"],product.capitalize()),
                                    countryName = country["countryCode"] ,
                                    commodityType = COMMODITY_TYPE[product.capitalize()],
                            )
                        )

                    for row in json_data:
                        prices[-1].commodityFutureProductTimeseries.append(
                            CommodityFutureProductTimeseries(
                                dispatchDay=datetime.strptime(row[0], "%c").strftime("%Y-%m-%d"),
                                settlementPrice=row[1]
                            )
                        )

                    prices[-1].commodityFutureLastInfoTimeseries.append(
                        CommodityFutureLastInfoTimeseries(
                            lastPrice=r[1].get("lastPrice"),
                            lastVolume=r[1].get("volume"),
                            dispatchDay=datetime.strptime(dateEnd, "%m/%d/%Y").strftime("%Y-%m-%d")
                        )
                    )
                responses.append(getVirtualFileMetadata(prices, getMetadata(True, file.Id), file))
            except Exception as e:
                log_helper.logException(e)

                message = f'Failed to parse {file.Url}: {e}'
                responses.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

    return envelope.getSuccessEnvelope(responses)


def getTheICEFuturesNatGas(product: str, dateFrom : str, dateTo : str)-> envelope.Envelope[List[VirtualFileMetadataPayload[List[Price]]]]:
    responses = []
    url = url_dict[product]
    
    # Send get request
    available_future_power_response = requests.get(url=url, verify=False)

    # Get json file
    list_of_country_products = json.loads(available_future_power_response.text)

    if list_of_country_products is None:
        return envelope.getSuccessEnvelope(responses)
    
    product_metadata_df = pd.DataFrame(data=list_of_country_products["results"])

    for country in GAS_COUNTRIES:

        filtered_country_metadata = product_metadata_df[product_metadata_df["physicalCommodity"].isin(country["physicalCommodities"])]

        for _, product_metadata in filtered_country_metadata.iterrows():

            try:


                fileName = 'TTF_ICE_'+ "_".join(product_metadata["specName"].split(" "))
                file = getVirtualFileMetadataFromUrl(url, fileName, product, datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0), '.json')
                
                url = f"https://www.ice.com/marketdata/DelayedMarkets.shtml?getContractsAsJson=&productId={product_metadata['productId']}&hubId={product_metadata['hubId']}"

                expiration_date_metadata_url = f"https://www.ice.com/api/productguide/spec/{product_metadata['specId']}/expiry/csv"

                #get contract expiration date metadata
                response = requests.get(url=expiration_date_metadata_url, verify=False)
                expiration_date_metadata = pd.read_csv(io.BytesIO(response.content), index_col=False)
                expiration_date_metadata.index = expiration_date_metadata["CONTRACT SYMBOL"].map(lambda x: x.split('"')[1])

                #get contract metadata
                response = requests.get(url=url, verify=False)
                json_data = json.loads(response.text)
                contract_metadata = pd.DataFrame(data=json.loads(response.text))

                load_type = get_load_type(product_metadata["specName"])

                prices = []
                # Get active market strips and ids
                for r in contract_metadata.iterrows():
                    
                    #get contract name
                    market_strip = r[1]["marketStrip"]
                    market_id = r[1]["marketId"]
                    
                    #discard if contract is not in the date range

                    contract_date_from, contract_date_to = get_date_from_expiration_metadata(expiration_date_metadata , market_strip, "LTD", "FTD")

                    if contract_date_from is None or contract_date_to is None:
                        continue

                    
                    contract_date_to = datetime.strptime(parse_contract_date(contract_date_to), "%m/%d/%Y")
                    contract_date_from = datetime.strptime(parse_contract_date(contract_date_from), "%m/%d/%Y")

                    if contract_date_from < datetime.strptime(dateFrom, "%Y-%m-%d") and contract_date_to > datetime.strptime(dateTo, "%Y-%m-%d"):
                        continue


                    dateStart, dateEnd = get_date_from_expiration_metadata(expiration_date_metadata , market_strip, "FDD", "LDD")

                    dateStart = parse_contract_date(dateStart)
                    dateEnd = parse_contract_date(dateEnd)

                    url = f"https://www.ice.com/marketdata/DelayedMarkets.shtml?getHistoricalChartDataAsJson=&marketId={market_id}&historicalSpan=2"

                    # Get contract timeseries
                    response = requests.get(url, verify=False)

                    if response.text == "":
                        continue

                    json_data = json.loads(response.text)["bars"]

                    product_name = get_product_name(market_strip.upper())

                    #load data to pydantic models
                    prices.append(
                                Price(
                                    productCode = product_name,
                                    name = product_name,
                                    #commodityLoadType = load_type,
                                    index = "TTF " + get_commodity_name(country["countryCode"], product_metadata["specName"],product.capitalize()),
                                    unitOfMeasurement = UNIT_OF_MEASUREMENT[product_metadata["physicalCommodity"]],
                                    productType = getFutureTypeFromDateRange(
                                        datetime.strptime(dateStart, "%m/%d/%Y"),
                                        datetime.strptime(dateEnd, "%m/%d/%Y")
                                    ),
                                    dateFrom = datetime.strptime(dateStart, "%m/%d/%Y").strftime("%Y-%m-%d"),
                                    dateTo = datetime.strptime(dateEnd, "%m/%d/%Y").strftime("%Y-%m-%d"),
                                    commodityIndexName ="TTF " +  get_commodity_name(country["countryCode"], product_metadata["specName"],product.capitalize()),
                                    countryName = country["countryCode"] ,
                                    commodityType = COMMODITY_TYPE[product.capitalize()],
                            )
                        )

                    for row in json_data:
                        prices[-1].commodityFutureProductTimeseries.append(
                            CommodityFutureProductTimeseries(
                                dispatchDay=datetime.strptime(row[0], "%c").strftime("%Y-%m-%d"),
                                settlementPrice=row[1]
                            )
                        )

                    prices[-1].commodityFutureLastInfoTimeseries.append(
                        CommodityFutureLastInfoTimeseries(
                            lastPrice=r[1].get("lastPrice"),
                            lastVolume=r[1].get("volume"),
                            dispatchDay=datetime.strptime(dateEnd, "%m/%d/%Y").strftime("%Y-%m-%d")
                        )
                    )
                responses.append(getVirtualFileMetadata(prices, getMetadata(True, file.Id), file))
            except Exception as e:
                log_helper.logException(e)

                message = f'Failed to parse {file.Url}: {e}'
                responses.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

    return envelope.getSuccessEnvelope(responses)

def getTheIceFuturesEnvironmental(product: str, dateFrom : str, dateTo : str)-> envelope.Envelope[List[VirtualFileMetadataPayload[List[Price]]]]:
    responses = []
    url = url_dict[product]
    
    # Send get request
    available_future_power_response = requests.get(url=url, verify=False)

    # Get json file
    list_of_country_products = json.loads(available_future_power_response.text)

    if list_of_country_products is None:
        return envelope.getSuccessEnvelope(responses)
    
    product_metadata_df = pd.DataFrame(data=list_of_country_products["results"])

    for country in ENVIRONMENTAL:

        filtered_country_metadata = product_metadata_df[product_metadata_df["physicalCommodity"].isin(country["physicalCommodities"])]

        for _, product_metadata in filtered_country_metadata.iterrows():

            try:
                fileName = 'Environmental_ICE_'+ "_".join(product_metadata["specName"].split(" "))
                file = getVirtualFileMetadataFromUrl(url, fileName, product, datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0), '.json')
                
                url = f"https://www.ice.com/marketdata/DelayedMarkets.shtml?getContractsAsJson=&productId={product_metadata['productId']}&hubId={product_metadata['hubId']}"

                expiration_date_metadata_url = f"https://www.ice.com/api/productguide/spec/{product_metadata['specId']}/expiry/csv"

                #get contract expiration date metadata
                response = requests.get(url=expiration_date_metadata_url, verify=False)
                expiration_date_metadata = pd.read_csv(io.BytesIO(response.content), index_col=False)
                expiration_date_metadata.index = expiration_date_metadata["CONTRACT SYMBOL"].map(lambda x: x.split('"')[1])

                #get contract metadata
                response = requests.get(url=url, verify=False)
                json_data = json.loads(response.text)
                contract_metadata = pd.DataFrame(data=json.loads(response.text))

                load_type = get_load_type(product_metadata["specName"])

                prices = []
                # Get active market strips and ids
                for r in contract_metadata.iterrows():
                    
                    #get contract name
                    market_strip = r[1]["marketStrip"]
                    market_id = r[1]["marketId"]
                    
                    #discard if contract is not in the date range

                    contract_date_from, contract_date_to = get_date_from_expiration_metadata(expiration_date_metadata , market_strip, "LTD", "FTD")

                    if contract_date_from is None or contract_date_to is None:
                        continue

                    contract_date_to = datetime.strptime(contract_date_to, "%m/%d/%Y")
                    contract_date_from = datetime.strptime(contract_date_from, "%m/%d/%Y")

                    if contract_date_from < datetime.strptime(dateFrom, "%Y-%m-%d") and contract_date_to > datetime.strptime(dateTo, "%Y-%m-%d"):
                        continue


                    dateStart, dateEnd = get_date_from_expiration_metadata(expiration_date_metadata , market_strip, "FDD", "LDD")

                    url = f"https://www.ice.com/marketdata/DelayedMarkets.shtml?getHistoricalChartDataAsJson=&marketId={market_id}&historicalSpan=2"

                    # Get contract timeseries
                    response = requests.get(url, verify=False)

                    if response.text == "":
                        continue

                    json_data = json.loads(response.text)["bars"]

                    product_name = get_product_name(market_strip.upper())

                    #load data to pydantic models
                    prices.append(
                                Price(
                                    productCode = product_name,
                                    name = product_name,
                                    #commodityLoadType = load_type,
                                    index = f"ICE {product} Environmental",
                                    unitOfMeasurement = UNIT_OF_MEASUREMENT[product_metadata["physicalCommodity"]],
                                    productType = getFutureTypeFromDateRange(
                                        datetime.strptime(dateStart, "%m/%d/%Y"),
                                        datetime.strptime(dateEnd, "%m/%d/%Y")
                                    ),
                                    dateFrom = datetime.strptime(dateStart, "%m/%d/%Y").strftime("%Y-%m-%d"),
                                    dateTo = datetime.strptime(dateEnd, "%m/%d/%Y").strftime("%Y-%m-%d"),
                                    commodityIndexName = f"ICE {product} Environmental",
                                    countryName = country["countryCode"] ,
                                    commodityType = COMMODITY_TYPE["Environmental"],
                            )
                        )

                    for row in json_data:
                        prices[-1].commodityFutureProductTimeseries.append(
                            CommodityFutureProductTimeseries(
                                dispatchDay=datetime.strptime(row[0], "%c").strftime("%Y-%m-%d"),
                                settlementPrice=row[1]
                            )
                        )

                    prices[-1].commodityFutureLastInfoTimeseries.append(
                        CommodityFutureLastInfoTimeseries(
                            lastPrice=r[1].get("lastPrice"),
                            lastVolume=r[1].get("volume"),
                            dispatchDay=datetime.strptime(dateEnd, "%m/%d/%Y").strftime("%Y-%m-%d")
                        )
                    )
                responses.append(getVirtualFileMetadata(prices, getMetadata(True, file.Id), file))
            except Exception as e:
                log_helper.logException(e)

                message = f'Failed to parse {file.Url}: {e}'
                responses.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
    return envelope.getSuccessEnvelope(responses)

                                                          
def getTheIceFuturesOil(product: str,dateFrom :str, dateTo: str)-> envelope.Envelope[List[VirtualFileMetadataPayload[List[Price]]]]:
    responses = []
    url = url_dict[product]
    
    # Send get request
    available_future_power_response = requests.get(url=url, verify=False)

    # Get json file
    list_of_country_products = json.loads(available_future_power_response.text)

    if list_of_country_products is None:
        return envelope.getSuccessEnvelope(responses)
    
    product_metadata_df = pd.DataFrame(data=list_of_country_products["results"])

    for country in OIL:

        filtered_country_metadata = product_metadata_df[product_metadata_df["physicalCommodity"].isin(country["physicalCommodities"])]

        for _, product_metadata in filtered_country_metadata.iterrows():

            try:

                fileName = 'OIL_ICE_'+ "_".join(product_metadata["specName"].split(" "))
                file = getVirtualFileMetadataFromUrl(url, fileName, product, datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0), '.json')
                
                url = f"https://www.ice.com/marketdata/DelayedMarkets.shtml?getContractsAsJson=&productId={product_metadata['productId']}&hubId={product_metadata['hubId']}"

                expiration_date_metadata_url = f"https://www.ice.com/api/productguide/spec/{product_metadata['specId']}/expiry/csv"

                #get contract expiration date metadata
                response = requests.get(url=expiration_date_metadata_url, verify=False)
                expiration_date_metadata = pd.read_csv(io.BytesIO(response.content), index_col=False)
                expiration_date_metadata.index = expiration_date_metadata["CONTRACT SYMBOL"].map(lambda x: x.split('"')[1])

                #get contract metadata
                response = requests.get(url=url, verify=False)
                json_data = json.loads(response.text)
                contract_metadata = pd.DataFrame(data=json.loads(response.text))

                load_type = get_load_type(product_metadata["specName"])

                prices = []
                # Get active market strips and ids
                for r in contract_metadata.iterrows():
                    
                    #get contract name
                    market_strip = r[1]["marketStrip"]
                    market_id = r[1]["marketId"]
                    
                    #discard if contract is not in the date range

                    contract_date_from, contract_date_to = get_date_from_expiration_metadata(expiration_date_metadata , market_strip, "LTD", "FTD")

                    if contract_date_from is None or contract_date_to is None:
                        continue

                    contract_date_to = datetime.strptime(contract_date_to, "%m/%d/%Y")
                    contract_date_from = datetime.strptime(contract_date_from, "%m/%d/%Y")

                    if contract_date_from < datetime.strptime(dateFrom, "%Y-%m-%d") and contract_date_to > datetime.strptime(dateTo, "%Y-%m-%d"):
                        continue

                    
                    url = f"https://www.ice.com/marketdata/DelayedMarkets.shtml?getHistoricalChartDataAsJson=&marketId={market_id}&historicalSpan=2"

                    # Get contract timeseries
                    response = requests.get(url, verify=False)

                    if response.text == "":
                        continue

                    json_data = json.loads(response.text)["bars"]

                    #get min and max date from json_data
                    dateStart = datetime.strptime(json_data[0][0], "%c")
                    dateEnd = datetime.strptime(json_data[-1][0], "%c")
                    

                    product_name = get_product_name(market_strip.upper())

                    #load data to pydantic models
                    prices.append(
                                Price(
                                    productCode = product_name,
                                    name = product_name,
                                    #commodityLoadType = load_type,
                                    index = "ICE Brent Crude Oil",
                                    unitOfMeasurement = UNIT_OF_MEASUREMENT[product_metadata["physicalCommodity"]],
                                    productType = getFutureTypeFromDateRange(
                                        dateStart,
                                        dateEnd
                                    ),
                                    dateFrom =dateStart.strftime("%Y-%m-%d"),
                                    dateTo = dateEnd.strftime("%Y-%m-%d"),
                                    commodityIndexName = "ICE Brent Crude Oil",
                                    countryName = country["countryCode"] ,
                                    commodityType = COMMODITY_TYPE[product.capitalize()],
                            )
                        )

                    for row in json_data:
                        prices[-1].commodityFutureProductTimeseries.append(
                            CommodityFutureProductTimeseries(
                                dispatchDay=datetime.strptime(row[0], "%c").strftime("%Y-%m-%d"),
                                settlementPrice=row[1]
                            )
                        )

                    prices[-1].commodityFutureLastInfoTimeseries.append(
                        CommodityFutureLastInfoTimeseries(
                            lastPrice=r[1].get("lastPrice"),
                            lastVolume=r[1].get("volume"),
                            dispatchDay=dateEnd.strftime("%Y-%m-%d")
                        )
                    )
                responses.append(getVirtualFileMetadata(prices, getMetadata(True, file.Id), file))
            except Exception as e:
                log_helper.logException(e)

                message = f'Failed to parse {file.Url}: {e}'
                responses.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

    return envelope.getSuccessEnvelope(responses)


def parse_contract_date(date : str):
    if len(date.split('/')[2]) == 2:
        return date.split('/')[0] + "/" + date.split('/')[1] + "/20" + date.split('/')[2]
    else:
        return date

def getCO2():
    return getProductClosePrices('eua')
    
def getTTFFuturesIntra():
    return getProduct('ttf')
    
def getBRENTIntra():
    return getProduct('brent')

def getCO2Intra():
    return getProduct('eua')

def getTheICEFuturesPower(dateStart: datetime, dateEnd: datetime):
    return getTheIceFuturesPower('power', dateStart.strftime("%Y-%m-%d"), dateEnd.strftime("%Y-%m-%d"))

def getTHEICEFuturesNatGas(dateStart: datetime, dateEnd: datetime):
    return getTheICEFuturesNatGas('gas', dateStart.strftime("%Y-%m-%d"), dateEnd.strftime("%Y-%m-%d"))

def getTHEICEFuturesEnvironmental(dateStart: datetime, dateEnd: datetime):
    return getTheIceFuturesEnvironmental('EUA', dateStart.strftime("%Y-%m-%d"), dateEnd.strftime("%Y-%m-%d"))

def getTHEICEFuturesOil(dateStart: datetime, dateEnd: datetime):
    return getTheIceFuturesOil('oil', dateStart.strftime("%Y-%m-%d"), dateEnd.strftime("%Y-%m-%d"))
