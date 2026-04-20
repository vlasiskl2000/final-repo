import requests
from datetime import datetime, timedelta
from ratelimit import limits, sleep_and_retry
from helpers.date_helper import *
from helpers.file_helper import getVirtualFileMetadataFromUrl
from helpers.log_helper import logException
from helpers.jao_helper import *
from helpers.seecao_helpers import *
from models.metadata import getMetadata, getVirtualFileMetadata
from interface import envelope
import random

HORIZON = {
    "daily" : 1,
    "monthly" : 3,
    "yearly" : 5,
}

API_TOKEN = ['f18a0536-043d-41bd-b89f-a6510c4263ba']
HOST = 'https://api.seecao.com'

@sleep_and_retry
@limits(calls=60, period=60)
def getRequest(url):
    auth_token = random.choice(API_TOKEN)
    headers = { "SeeCao-API-key": auth_token }
    request = requests.get(url, headers=headers)

    if request.status_code != 200:
        request.raise_for_status()
    
    return request.json() 


def getSeecaoapi(dateFrom: datetime, dateTo: datetime, horizon: str = None):

    response = []

    
    user_horizon = []
    

    if horizon == None:
        user_horizon = HORIZON.keys()
    else:
        user_horizon.append(horizon)


    for horizon_type in user_horizon:

        list_of_auctions = getRequest(SeecaoAuctionListUrl(dateFrom, dateTo, horizon_type))

        for auction in list_of_auctions:

            fileName = f'Seecao_{dateFrom:%Y-%m-%d}_{dateTo:%Y-%m-%d}_auctions.json'
            url = SeecaoAuctionUrl(auction)
            file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{dateFrom.strftime("%Y%m%d")}_{dateTo.strftime("%Y%m%d")}', dateTo)

            try:
                actual_data = getRequest(url)

                if len(actual_data) == 2 and actual_data["status"] != 200:
                    raise Exception(actual_data["message"])
                
                payload = []
                
                StartDate = getUtcStrDate(actual_data[0]["DeliveryPeriodStart"])
                EndDate = getUtcStrDate(actual_data[0]["DeliveryPeriodEnd"])

                #get data from the first instance of auction
                action_data = {
                    'Identification': actual_data[0]['AuctionIdentification'],
                    'StartDate': StartDate,
                    'EndDate': EndDate,
                    'Horizon': HORIZON[horizon_type],
                    'Ftroption': "PTR",
                    'Cancelled': actual_data[0]['Cancelled'],
                    "BorderName": get_border_name(actual_data[0]["AuctionIdentification"]),
                }


                action_data_model = JaoData(**action_data)

                DST_Flag = False
                for action in actual_data:
                    
                    action_results = []
                                    
                    timetable = action['TimeTable']
                    if timetable.startswith('Everyday'):
                        timetable = timetable.replace('Everyday', '')

                    hour_diff_start, hour_diff_end, inside_flag = extractHours(timetable)
                    timestamp_from = action['DeliveryPeriodStart']
                    timestamp_to = action['DeliveryPeriodEnd']

                    #handle DST winter
                    if inside_flag:
                        DST_Flag = True

                    if not DST_Flag:
                        action_results.append({
                            'OfferedCapacity': action['OfferedCapacity'],
                            'RequestedCapacity': action['RequestedCapacity'],
                            'AllocatedCapacity': action['AllocatedCapacity'],
                            'AuctionPrice': action['Price'],
                            'ProductIdentification': actual_data[0]['AuctionIdentification'],
                            'ProductHour': action['TimeTable'],
                            "BidderPartyCount": action["NumberOfParticipants"],
                            "WinnerPartyCount": action["NumberOfSuccessfulParticipants"],
                            'TimestampFrom': getUtcTimestamp(timestamp_from, hour_diff_start),
                            'TimestampTo': getUtcTimestamp(timestamp_from, hour_diff_end),
                        })
                    else:
                            action_results.append({
                            'OfferedCapacity': action['OfferedCapacity'],
                            'RequestedCapacity': action['RequestedCapacity'],
                            'AllocatedCapacity': action['AllocatedCapacity'],
                            'AuctionPrice': action['Price'],
                            'ProductIdentification': actual_data[0]['AuctionIdentification'],
                            'ProductHour': action['TimeTable'],
                            "BidderPartyCount": action["NumberOfParticipants"],
                            "WinnerPartyCount": action["NumberOfSuccessfulParticipants"],
                            'TimestampFrom': getUtcTimestamp(timestamp_from, 1 + hour_diff_start),
                            'TimestampTo':getUtcTimestamp(timestamp_from, 1 +  hour_diff_end),
                        })

                    
                    for row in action_results:
                        jao_row_results = JaoDataResults(**row)
                        action_data_model.Results.append(jao_row_results)

                DST_Flag = False
                action_winning_parties = []
                for winning_party in action["SuccessfulParticipants"]:
                    action_winning_parties.append(eicCode(eicCode=winning_party))
            
                for row in action_winning_parties:
                    action_data_model.WinningParties.append(row)

                    

                payload.append(action_data_model)
                    
                
                response.append(getVirtualFileMetadata(payload, getMetadata(len(payload) > 0, file.Id), file))
            except Exception as e:
                message = f'Failed to parse {file.Url}: {e}'
                logException(e)
                response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
                

    return envelope.getSuccessEnvelope(response)