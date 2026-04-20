
from datetime import datetime


def SeecaoAuctionUrl(auction_id: str):
    return f'https://api.seecao.com/api/auction?auctionId={auction_id}'


def SeecaoAuctionListUrl(dateFrom: datetime, dateTo : datetime, horizon: str = None):
    return f'https://api.seecao.com/api/auctions?date_from={dateFrom:%Y-%m-%d}&date_to={dateTo:%Y-%m-%d}&type={horizon}'

def get_border_name(identification: str):
    return identification[:2] + '-' + identification[2:4]