import requests
from datetime import datetime, timedelta

from helpers.file_helper import getVirtualFileMetadataFromUrl
from helpers.log_helper import logException
from helpers.jao_helper import *
from models.metadata import getMetadata, getVirtualFileMetadata
from interface import envelope

HORIZON = {
    "Daily" : 1,
    "Intraday" : 2,
    "Monthly" : 3,
    "Seasonal" : 4,
    "Yearly" : 5,
    "Weekly" : 6,
    "Quarterly": 7
}

API_TOKEN = 'fcbfa458-c80e-49a0-af15-d576f8aa88b8'
HOST = 'https://api.jao.eu'


def getRequest(url, horizon: str = None):
    headers = { 'AUTH_API_KEY': API_TOKEN }
    return requests.get(url, verify=False, headers=headers).json()

def getAuctions(dateFrom: datetime, dateTo: datetime, horizon: str = None):
    response = []


    date = dateFrom
    while date <= dateTo:
        dateStart = date - timedelta(days=1)
        dateEnd = min(date + timedelta(days=31), dateTo)
        actual_url = f'{HOST}/OWSMP/getauctions?fromdate={dateStart:%Y-%m-%d}&todate={dateEnd:%Y-%m-%d}{f"&horizon={horizon}" if horizon != None else ""}'
        fileName = f'jao_{dateStart:%Y-%m-%d}_{dateEnd:%Y-%m-%d}_auctions.json'

        file = getVirtualFileMetadataFromUrl(actual_url, fileName, f'{fileName}_{date.strftime("%Y%m%d")}', date)

        try:
            actual_data = getRequest(actual_url)
            if len(actual_data) == 2 and actual_data["status"] != 200:
                raise Exception(actual_data["message"])
            payload = []
            for action in actual_data:
                if action['horizonName'] not in HORIZON:
                    raise Exception(f'Unknown horizon: {action["horizonName"]}')
                
                start_date = datetime.strptime(action['marketPeriodStart'], '%Y-%m-%dT%H:%M:%S.%f%z')
                action_data = {
                    'Identification': action['identification'],
                    'StartDate': action['marketPeriodStart'],
                    'EndDate': action['marketPeriodStop'],
                    'Horizon': HORIZON[action['horizonName']],
                    'Ftroption': action['ftroption'],
                    'Cancelled': action['cancelled'],
                    "BorderName": action["products"][0]["corridorCode"],
                }
                
                action_data_model = JaoData(**action_data)
                action_results = []
                index = 0
                for result in sorted(action['results'], key=lambda x: x['productHour']):                   
                    product_hour = result['productHour']
                    if product_hour.startswith('Every'):
                        timestamp_from = action['marketPeriodStart']
                        timestamp_to = action['marketPeriodStop']
                    else:
                        timestamp_from = start_date + timedelta(hours = index)
                        timestamp_to = start_date + timedelta(hours = index + 1)

                    product = list(filter(lambda x: x['productIdentification'] == result['productIdentification'], action["products"]))[0]
                    action_results.append({
                        'OfferedCapacity': result['offeredCapacity'],
                        'RequestedCapacity': result['requestedCapacity'],
                        'AllocatedCapacity': result['allocatedCapacity'],
                        'AuctionPrice': result['auctionPrice'],
                        'ProductIdentification': result['productIdentification'],
                        'ProductHour': product_hour,
                        "BidderPartyCount": product["bidderPartyCount"],
                        "WinnerPartyCount": product["winnerPartyCount"],
                        'TimestampFrom': timestamp_from,
                        'TimestampTo': timestamp_to,
                    })
                    index += 1

                action_winning_parties = []
                for winning_party in action['winningParties']:
                    action_winning_parties.append(eicCode(eicCode=winning_party['eicCode']))
        
                for row in action_winning_parties:
                    action_data_model.WinningParties.append(row)

                for row in action_results:
                    jao_row_results = JaoDataResults(**row)
                    action_data_model.Results.append(jao_row_results)
                   

                payload.append(action_data_model)
            
            response.append(getVirtualFileMetadata(payload, getMetadata(len(payload) > 0, file.Id), file))
        except Exception as e:
            message = f'Failed to parse {file.Url}: {e}'
            logException(e)
            response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
            
        date += timedelta(days = 31)

    return envelope.getSuccessEnvelope(response)