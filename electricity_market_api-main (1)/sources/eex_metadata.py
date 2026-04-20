HOST = 'https://webservice-eex.gvsi.com'

headers = {
    "Host": "webservice-eex.gvsi.com",
    "Origin": "https://www.eex.com",
    "Referer": "https://www.eex.com/",
    "sec-ch-ua": '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
    "sec-ch-ua-mobile": '?0',
    "sec-ch-ua-platform": "Windows",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
}

MONTH = {
    1: 'F',
    2: 'G',
    3: 'H',
    4: 'J',
    5: 'K',
    6: 'M',
    7: 'N',
    8: 'Q',
    9: 'U',
    10: 'V',
    11: 'X',
    12: 'Z',
}

QUARTERS = {
    1: 'F',
    2: 'J',
    3: 'N',
    4: 'V',
}

LOAD_TYPES = {
    'B': 1,
    'P': 2,
}

COMMODITY_FUTURE_PRODUCT_TYPE = {
    "D": 1,
    "WKEnd": 2,
    "W": 3,
    "M": 4,
    "Q": 5,
    "Y": 6,
}

COMMODITY_TYPE = {
    "Power": 1,
    "Gas": 2,
}

COMMODITY_TYPE_CODES = {
    "Power": 'E',
}

COUNTRIES = [
    { 
        'eexCode': 'DE', 
        'eexCodeWeekend': 'DW',
        'eexCodeDay': 'D',
        'code': 'DE', 
        'index': 'DE Power',
        'commodityTypes': { 'Power': ['B', 'P'] },
        'futures': { 'Y': 10, 'M': 10, 'Q': 11, 'W': 6, 'WKEnd': 3, 'D': 6 },
    },
    { 
        'eexCode': 'FF', 
        'code': 'GR', 
        'index': 'GR Power',
        'commodityTypes': { 'Power': ['B'] },
        'futures': { 'Y': 7, 'M': 7, 'Q': 8 },
    },
    { 
        'eexCode': 'FU', 
        'code': 'GB', 
        'index': 'GB Power',
        'commodityTypes': { 'Power': ['B', 'P'] },
        'futures': { 'Y': 3, 'M': 4, 'Q': 5, },
    },
    { 
        'eexCode': 'AT', 
        'eexCodeWeekend': 'AW',
        'eexCodeDay': 'A',
        'code': 'AT', 
        'index': 'AT Power',
        'commodityTypes': { 'Power': ['B', 'P'] },
        'futures': { 'Y': 7, 'M': 10, 'Q': 11, 'W': 4, 'WKEnd': 2, 'D': 11 },
    },
    { 
        'eexCode': 'Q1', 
        'code': 'BE', 
        'index': 'BE Power',
        'commodityTypes': { 'Power': ['B'] },
        'futures': { 'Y': 3, 'M': 4, 'Q': 5, },
    },
    {
        'eexCode': 'Q0',
        'code': 'NL',
        'index': 'NL Power',
        'commodityTypes': { 'Power': ['B', 'P'] },
        'futures': { 'Y': 3, 'M': 4, 'Q': 5, },
    },
    {
        'eexCode': 'F7',
        'eexCodeWeekend': 'F7',
        'eexCodeDay': '7',
        'code': 'FR',
        'index': 'FR Power',
        'commodityTypes': { 'Power': ['B', 'P'] },
        'futures': { 'Y': 6, 'M': 7, 'Q': 11, 'W': 4 },
    },
    {
        'eexCode': 'F9',
        'eexCodeWeekend': 'F9',
        'eexCodeDay': '9',
        'code': 'HU',
        'index': 'PXE HU Power',
        'commodityTypes': { 'Power': ['B', 'P'] },
        'futures': { 'Y': 6, 'M': 7, 'Q': 7, 'W': 5 },
    },
    {
        'eexCode': 'FD',
        'eexCodeWeekend': 'FDW',
        'eexCodeDay': 'FD',
        'code': 'IT',
        'index': 'IT Power',
        'commodityTypes': { 'Power': ['B', 'P'] },
        'futures': { 'Y': 10, 'M': 7, 'Q': 10, 'W': 5, 'D': 12 },
    },
    {
        'eexCode': 'FE',
        'eexCodeWeekend': 'FEW',
        'eexCodeDay': 'FE',
        'code': 'ES',
        'index': 'ES Power',
        'commodityTypes': { 'Power': ['B'] },
        'futures': { 'Y': 10, 'M': 7, 'Q': 11, 'W': 5 },
    },
    {
        'eexCode': 'FC',
        'eexCodeWeekend': 'FCW',
        'eexCodeDay': 'FC',
        'code': 'CH',
        'index': 'CH Power',
        'commodityTypes': { 'Power': ['B'] },
        'futures': { 'Y': 6, 'M': 7, 'Q': 7, 'W': 5 },
    },
    {
        'eexCode': 'FP',
        'eexCodeWeekend': 'FPW',
        'eexCodeDay': 'FP',
        'code': 'PL',
        'index': 'PXE PL Power',
        'commodityTypes': { 'Power': ['B', 'P'] },
        'futures': { 'Y': 6, 'M': 7, 'Q': 7 },
    },
    {
        'eexCode': 'FH',
        'eexCodeWeekend': 'FHW',
        'eexCodeDay': 'FH',
        'code': 'RO',
        'index': 'PXE RO Power',
        'commodityTypes': { 'Power': ['B'] },
        'futures': { 'Y': 6, 'M': 7, 'Q': 7, 'W': 5 },
    },
    {
        'eexCode': 'FZ',
        'eexCodeWeekend': 'FZW',
        'eexCodeDay': 'FZ',
        'code': 'SR',
        'index': 'PXE SR Power',
        'commodityTypes': { 'Power': ['B'] },
        'futures': { 'Y': 6, 'M': 7, 'Q': 7, 'W': 5 },
    },
    {
        'eexCode': 'FY',
        'eexCodeWeekend': 'FYW',
        'eexCodeDay': 'FY',
        'code': 'SK',
        'index': 'PXE SK Power',
        'commodityTypes': { 'Power': ['B', 'P'] },
        'futures': { 'Y': 6, 'M': 7, 'Q': 7 },
    },
    {
        'eexCode': 'FV',
        'eexCodeWeekend': 'FVW',
        'eexCodeDay': 'FV',
        'code': 'SI',
        'index': 'PXE SI Power',
        'commodityTypes': { 'Power': ['B', 'P'] },
        'futures': { 'Y': 6, 'M': 7, 'Q': 7 },
    },
    {
        'eexCode': 'FK',
        'eexCodeWeekend': 'FKW',
        'eexCodeDay': 'FK',
        'code': 'BG',
        'index': 'PXE BG Power',
        'commodityTypes': { 'Power': ['B'] },
        'futures': { 'Y': 6, 'M': 7, 'Q': 7, 'W': 5 },
    },
    {
        'eexCode': 'FX',
        'eexCodeWeekend': 'FXW',
        'eexCodeDay': 'FX',
        'code': 'CZ',
        'index': 'PXE CZ Power',
        'commodityTypes': { 'Power': ['B', 'P'] },
        'futures': { 'Y': 6, 'M': 7, 'Q': 7 },
    }
]