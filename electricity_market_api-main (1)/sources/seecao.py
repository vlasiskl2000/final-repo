import requests
import pandas as pd

from io import BytesIO

from datetime import datetime, timedelta, timezone

from helpers.file_helper import getVirtualFileMetadataFromUrl
from helpers.log_helper import logException
from models.metadata import getMetadata, getVirtualFileMetadata
from interface import envelope
import pytz

HOST = 'https://www.seecao.com/'

def getRequest(date: datetime):
    date_formatted = date.strftime('%B %Y')
    filename = f'Results - Daily Auctions {date_formatted}_0.xlsx'
    url = f'{HOST}sites/default/files/documents/basic_page/{filename.replace(" ", "%20")}'
    response = requests.get(url, verify=False)

    if response.status_code == 200:
        return (response.content, url, filename)
    
    filename = f'Results - Daily Auctions {date_formatted}.xlsx'
    url = f'{HOST}sites/default/files/documents/basic_page/{filename.replace(" ", "%20")}'
    response = requests.get(url, verify=False)

    if response.status_code == 200:
        return (response.content, url, filename)
    
    return (None, url, filename)

def getAuctions(dateFrom: datetime, dateTo: datetime, horizon: str = None):
    response = []

    date = datetime(dateFrom.year, dateFrom.month, 1)
    while date <= dateTo:
        excel_data, actual_url, fileName = getRequest(date)

        file = getVirtualFileMetadataFromUrl(actual_url, fileName, fileName, date)

        try:
            if excel_data is None:
                raise Exception(f'File not found: {actual_url}')
            
            payload = []
            with BytesIO(excel_data) as f:
                all_data = []
                for sheet_name in pd.ExcelFile(f).sheet_names:
                    # Check if the sheet name is a valid date
                    try:
                        _ = datetime.strptime(sheet_name, '%d %B').strftime('%d.%m.%Y')
                    except ValueError:
                        continue

                    # Read the sheet into a DataFrame
                    df = pd.read_excel(f, sheet_name=sheet_name, header=None).dropna()

                    # Find the index of the first row with valid data
                    start_index = df.index[df[0].str.contains('Auction ID', na=False)].min() + 2

                    # Create a new DataFrame with the valid data
                    df = df.iloc[start_index:]

                    # Rename the columns
                    df.columns = ['Identification', 'Date', 'Time', 'OfferedCapacity', 'TotalRequestedCapacity',
                                'TotalAllocatedCapacity', 'AuctionClearingPrice', 'CongestionIncome',
                                'ParticipantsNumber', 'SuccessfulParticipantsNumber', 'AuctionBidsNumber']
                    
                    df['BorderName'] = df['Identification'].str[:2] + '-' + df['Identification'].str[2:4]

                    grouped_data = df.groupby(['Identification', 'Date', 'BorderName'])

                    for name, group in grouped_data:
                        if name[0] == 'Auction ID':
                            continue

                        cet_tz = pytz.timezone('CET')
                        utc_tz = pytz.timezone('UTC')

                        cet_day = cet_tz.localize(datetime.strptime(name[1], '%d.%m.%Y'))
                        utc_day = cet_day.astimezone(utc_tz)

                        action_data = {
                            'Identification': name[0],
                            'StartDate': utc_day,
                            'EndDate': utc_day + timedelta(days = 1),
                            'Horizon': 1,
                            'Cancelled': False,
                            'Ftroption': 'PTR',
                            'BorderName': name[2],
                        }

                        action_results = []
                        index = 0
                        for _, row in group.iterrows():
                            time = row['Time']

                            action_results.append({
                                'OfferedCapacity': row['OfferedCapacity'],
                                'RequestedCapacity': row['TotalRequestedCapacity'],
                                'AllocatedCapacity': row['TotalAllocatedCapacity'],
                                'AuctionPrice': row['AuctionClearingPrice'],
                                'ProductIdentification': name[0],
                                'ProductHour': time,
                                'TimestampFrom': utc_day + timedelta(hours = index),
                                'TimestampTo': utc_day + timedelta(hours = index + 1),
                                'BidderPartyCount': row['ParticipantsNumber'],
                                'WinnerPartyCount': row['SuccessfulParticipantsNumber']
                            })

                            index += 1

                        action_data['Results'] = action_results

                        payload.append(action_data)
            
            response.append(getVirtualFileMetadata(payload, getMetadata(len(payload) > 0, file.Id), file))
        except Exception as e:
            message = f'Failed to parse {file.Url}: {e}'
            logException(e)
            response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
            
        date += timedelta(days = 31)

    return envelope.getSuccessEnvelope(response)