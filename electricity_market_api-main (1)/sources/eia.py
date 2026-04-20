import pandas as pd
from datetime import datetime

BRENT_SOURCE = 'https://www.eia.gov/dnav/pet/hist_xls/RBRTEd.xls'

def getBrentPrice(dateStart: datetime, dateEnd: datetime) -> list:
    df = pd.read_excel(BRENT_SOURCE, sheet_name=1, skiprows=2)

    df = df[(df.Date >= dateStart) & (df.Date <= dateEnd)]
    values = []
    for row in df.values:
        values.append({
            'DispatchDay': row[0],
            'Value': row[1],
            'Product': 'SPOT'
        })

    return values
            