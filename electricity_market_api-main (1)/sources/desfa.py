from collections import defaultdict
import pandas as pd
import math
import numpy as np
import re
from datetime import datetime
from helpers.log_helper import logException
from helpers.metadata_helper import *
from helpers.date_helper import getInfoFromDate
from helpers.file_helper import getVirtualFileMetadataFromUrl
from models.metadata import getMetadata, getVirtualFileMetadata
from interface import envelope

import requests


QUANTITY_ENDPOINT = 'http://www.xn--mxafd0dp.gr/scada/search_quantity.php'
QUALITY_ENDPOINT = 'http://www.xn--mxafd0dp.gr/scada/search_quality.php'
NOMINATIONS_ENDPOINT = 'http://www.xn--mxafd0dp.gr/scada/search_nominations.php'

def formatExitPointName(exitPointName: str):
    return exitPointName.strip().replace('\n', ' ').replace('( ', '(').replace(' )', ')')

def getPhysicalFlows(dateFrom: datetime, dateTo: datetime, aggregation: bool = False):
    envelope_response = []

    try:
        response = requests.post(QUANTITY_ENDPOINT, data={
            'Date_From': dateFrom.strftime('%Y-%m-%d'),
            'Date_To': dateTo.strftime('%Y-%m-%d')
            })
        
        fileName = 'DESFA_QUANTITY'
        file = getVirtualFileMetadataFromUrl(QUANTITY_ENDPOINT, fileName, f'{fileName}_{dateFrom.strftime("%Y%m%d")}_{dateTo.strftime("%Y%m%d")}', dateFrom)
        
        quoted = re.compile("'[^']*'")

        data = str(response.content)
        values = quoted.findall(data)[0]
        rows = values.replace("'", "").split('\\n')

        results = []
        for row in rows:
            row_data = row.split('_')
            if len(row_data) <= 2:
                continue
            
            date = row_data[0]
            dispatchPeriod = int(row_data[1].split(':')[0])

            exit_point_data = row.split(':')[1].split('_')

            for exit_point_row in exit_point_data:
                exit_point_row_data = exit_point_row.split('-')
                if len(exit_point_row_data) <= 2:
                    continue
                
                exit_point = exit_point_row_data[0] + '-' + exit_point_row_data[1]
                exit_point_value = exit_point_row_data[2]

                if aggregation:
                    date_time = datetime.strptime(date, '%Y-%m-%d') + timedelta(hours=dispatchPeriod - 8) # To GasDate
                else:
                    date_time = datetime.strptime(date, '%Y-%m-%d') + timedelta(hours=dispatchPeriod - 2) # To CET
                
                results.append({
                    'dispatchDay': date_time.date(),
                    'dispatchPeriod': date_time.hour + 1,
                    'exitPointName': exit_point,
                    'validationVersion': 3,
                    'value': float(exit_point_value.replace('\\r','') if exit_point_value != '' else 0) / 1000
                })

        if aggregation:
            results = getFivePeriodsAggregatedPhysicalFlows(results)

        mapping = { x['Code'] : x['Name'] for x in pd.read_csv('exit_point_mapping.csv').to_dict('records') }
        results = list(filter(lambda x: x['exitPointName'] in mapping, results))
        for result in results:
            result['exitPointName'] = mapping[result['exitPointName']]

        if aggregation:
            return file, results

        envelope_response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
    except Exception as e:
        message = f'Failed to parse {file.Url}: {e}'
        logException(e)
        envelope_response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))

    return envelope.getSuccessEnvelope(envelope_response)

def getFivePeriodsAggregatedPhysicalFlows(data):
    # Group periods by dispatchDay
    groups = defaultdict(list)
    for entry in data:
        key = (entry['dispatchDay'],entry['exitPointName'],entry['validationVersion'])
        groups[key].append(entry)

    # Group and sum periods within each dispatchDay
    grouped_payload = []
    for day, day_data in groups.items():
        grouped_day_data = group_periods(day_data)
        grouped_payload.extend(grouped_day_data)

    return grouped_payload

def group_periods(period_data):
    grouped_data = []
    periods_mapping = [
        (1, 6),
        (2, 11),
        (3, 14),
        (4, 18),
        (5, 24)
    ]

    for period, end_period in periods_mapping:
        period_sum = sum([entry['value'] for entry in period_data if entry['dispatchPeriod'] <= end_period])
        grouped_data.append({
            'dispatchDay': period_data[0]['dispatchDay'],
            'dispatchPeriod': period,
            'exitPointName': formatExitPointName(period_data[0]['exitPointName']),
            'validationVersion': period_data[0]['validationVersion'],
            'value': period_sum
        })

    return grouped_data


def getNominations(dateFrom: datetime, dateTo: datetime):
    envelope_response = []

    try:
        response = requests.post(NOMINATIONS_ENDPOINT, data={
            'Date_From': dateFrom.strftime('%Y-%m-%d'),
            'Date_To': dateTo.strftime('%Y-%m-%d')
            })
        
        fileName = 'DESFA_NOMINATIONS'
        file = getVirtualFileMetadataFromUrl(NOMINATIONS_ENDPOINT, fileName, f'{fileName}_{dateFrom.strftime("%Y%m%d")}_{dateTo.strftime("%Y%m%d")}', dateFrom)
        
        quoted = re.compile("'[^']*'")

        data = str(response.content)
        values = quoted.findall(data)[0]

        rows = values.replace("'", "").split('\\n')

        results = []
        for row in rows:
            row_data = row.split(':')
            if len(row_data) < 2:
                continue
            
            date = row_data[0]

            exit_point_data = row.split(':')[1].split('_')

            for exit_point_row in exit_point_data:
                exit_point_row_data = exit_point_row.split('-')
                if len(exit_point_row_data) <= 2:
                    continue
                
                exit_point = exit_point_row_data[0] + '-' + exit_point_row_data[1]
                exit_point_value = exit_point_row_data[2]

                date_time = datetime.strptime(date, '%Y-%m-%d')
                
                results.append({
                    'dispatchDay': date_time.date(),
                    'exitPointName': formatExitPointName(exit_point),
                    'validationVersion': 3,
                    'value': float(exit_point_value.replace('\\r','')) / 1000
                })
        
        mapping = { x['Code'] : x['Name'] for x in pd.read_csv('exit_point_mapping.csv').to_dict('records') }
        results = list(filter(lambda x: x['exitPointName'] in mapping, results))
        for result in results:
            result['exitPointName'] = mapping[result['exitPointName']]

        envelope_response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
    except Exception as e:
        message = f'Failed to parse {file.Url}: {e}'
        logException(e)
        envelope_response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
    
    return envelope.getSuccessEnvelope(envelope_response)

def NominationsAllocations(dateFrom: datetime, dateTo: datetime):
    envelope_response = []
    try:

        file, results = get_verified_nominations(dateFrom, dateTo)
        envelope_response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))

        
        file, results = parse_nominations_from_power_bi(dateFrom, dateTo)
        envelope_response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
    except Exception as e:
        message = f'Failed to parse {file.Url}: {e}'
        logException(e)
        envelope_response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
    
    return envelope.getSuccessEnvelope(envelope_response)

def get_verified_nominations(dateFrom, dateTo):
    NominationsData = pd.DataFrame()
    if(dateTo >= datetime(2023, 1, 1).date()):
        Nominations = "https://www.desfa.gr/userfiles/pdflist/DERY/TS/Nominations-Allocations/Nominations%20(from%2001.01.2023).xlsx"
        NominationsData = pd.read_excel(Nominations, sheet_name=0, index_col=0, skiprows=2, verbose = True)
    if(dateFrom < datetime(2023, 1, 1).date()):
        Nominations = "https://www.desfa.gr/userfiles/pdflist/DERY/TS/Nominations-Allocations/Nominations%20(01.06.2017_31.12.2022).xlsx"
        NominationsData = pd.concat([NominationsData, pd.read_excel(Nominations, sheet_name=0, index_col=0, skiprows=2, verbose = True)])
    NominationsData = NominationsData.sort_index()[dateFrom:dateTo]

    fileName = 'DESFA_NOMINATIONS'
    file = getVirtualFileMetadataFromUrl(Nominations, fileName, f'{fileName}_{dateFrom.strftime("%Y%m%d")}_{dateTo.strftime("%Y%m%d")}', dateFrom)
        
    results = []

        # Iterate through the rows of the dataframe
    for date, row in NominationsData.iterrows():
            # Iterate through the columns of the row
        for exit_point, exit_point_value in row.iteritems():
                # Check if the value is NaN
            if math.isnan(exit_point_value): continue

                # Create the object with the specified structure
            obj = {
                    'dispatchDay': date.date(),
                    'exitPointName': formatExitPointName(exit_point),
                    'validationVersion': 3,
                    'value': None if math.isnan(exit_point_value) else float(exit_point_value) / 1000 
                }
                # Append the object to the list
            results.append(obj)
    return file,results

def parse_nominations_from_power_bi(dateFrom: datetime, dateTo: datetime):

    url = "https://wabi-europe-north-b-api.analysis.windows.net/public/reports/querydata?synchronous=true"
    fileName = 'DESFA_POWER_BI_NOMINATIONS'
    file = getVirtualFileMetadataFromUrl(url, fileName, f'{fileName}_{dateFrom.strftime("%Y%m%d")}_{dateTo.strftime("%Y%m%d")}', dateFrom)

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "el,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
        "ActivityId": "c0aeb80c-021f-40fb-826d-d3bd496875dc",
        "Connection": "keep-alive",
        "Content-Type": "application/json;charset=UTF-8",
        "DNT": "1",
        "Origin": "https://app.powerbi.com",
        "Referer": "https://app.powerbi.com/",
        "RequestId": "b6fb2295-48ce-13a3-61df-01fdfb8bb563",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.58",
        "X-PowerBI-ResourceKey": "4784e920-a826-42e7-93c4-878cefdf39b8",
        "sec-ch-ua": "\"Chromium\";v=\"112\", \"Microsoft Edge\";v=\"112\", \"Not:A-Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }

    result = []
    if dateFrom < datetime.now().date() - timedelta(days=3):
        dateFrom = datetime.now().date() - timedelta(days=3)
        
    if dateTo > datetime.now().date() + timedelta(days=2):
        dateTo = datetime.now().date() + timedelta(days=2)

    for date in pd.date_range(dateFrom, dateTo):
        data = {"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"n","Entity":"NominationMessageRows","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"n"}},"Property":"Point"},"Name":"NominationMessageRows.Point"},{"Column":{"Expression":{"SourceRef":{"Source":"n"}},"Property":"Data.Direction"},"Name":"NominationMessageRows.Data.Direction"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"n"}},"Property":"Quantity"}},"Function":0},"Name":"Sum(NominationMessageRows.Quantity)"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"n"}},"Property":"SlicerDate"}}],
                "Values":[[{"Literal":{"Value":f"'{date.strftime('%d/%m/%Y')}'"}}]]}}}],"OrderBy":[{"Direction":1,"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"n"}},"Property":"Data.Direction"}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[1],"Subtotal":1},{"Projections":[0,2],"Subtotal":1}],"Expansion":{"From":[{"Name":"n","Entity":"NominationMessageRows","Type":0}],"Levels":[{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"n"}},"Property":"Data.Direction"}}],"Default":0}],"Instances":{"Children":[{"Values":[{"Literal":{"Value":"'Entry'"}}]},{"Values":[{"Literal":{"Value":"'Exit'"}}]}]}}},"DataReduction":{"DataVolume":3,"Primary":{"Window":{"Count":500}}},"Version":1},"ExecutionMetricsKind":1}}]},"QueryId":"","ApplicationContext":{"DatasetId":"1c4b9658-af5f-4872-a454-fd0290677bf2","Sources":[{"ReportId":"8ae6b875-4848-43fd-b3af-6da528cd755e","VisualId":"b5eb8180945d0d983830"}]}}],"cancelQueries":[],"modelId":6842352}
        
        response = requests.post(url, headers=headers, json=data)
        response_data = response.json()

        if 'ValueDicts' not in response_data['results'][0]['result']['data']['dsr']['DS'][0] and date >= datetime.now().date():
            data = {"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"n","Entity":"NominationMessageRows","Type":0}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"n"}},"Property":"Point"},"Name":"NominationMessageRows.Point"},{"Column":{"Expression":{"SourceRef":{"Source":"n"}},"Property":"Data.Direction"},"Name":"NominationMessageRows.Data.Direction"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"n"}},"Property":"Quantity"}},"Function":0},"Name":"Sum(NominationMessageRows.Quantity)"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"n"}},"Property":"SlicerDate"}}],
                "Values":[[{"Literal":{"Value":"'Latest'"}}]]}}}],"OrderBy":[{"Direction":1,"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"n"}},"Property":"Data.Direction"}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[1],"Subtotal":1},{"Projections":[0,2],"Subtotal":1}],"Expansion":{"From":[{"Name":"n","Entity":"NominationMessageRows","Type":0}],"Levels":[{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"n"}},"Property":"Data.Direction"}}],"Default":0}],"Instances":{"Children":[{"Values":[{"Literal":{"Value":"'Entry'"}}]},{"Values":[{"Literal":{"Value":"'Exit'"}}]}]}}},"DataReduction":{"DataVolume":3,"Primary":{"Window":{"Count":500}}},"Version":1},"ExecutionMetricsKind":1}}]},"QueryId":"","ApplicationContext":{"DatasetId":"1c4b9658-af5f-4872-a454-fd0290677bf2","Sources":[{"ReportId":"8ae6b875-4848-43fd-b3af-6da528cd755e","VisualId":"b5eb8180945d0d983830"}]}}],"cancelQueries":[],"modelId":6842352}
        
            response = requests.post(url, headers=headers, json=data)
            response_data = response.json()

        names = response_data['results'][0]['result']['data']['dsr']['DS'][0]['ValueDicts']['D0']

        values_exit = response_data['results'][0]['result']['data']['dsr']['DS'][0]['PH'][1]['DM1'][1]['M']

        mapped_values_exit = map_indexes(values_exit)

        for index in mapped_values_exit:
            data = mapped_values_exit[index]
            name = names[index]
            obj = {
                    'dispatchDay': date.date(),
                    'exitPointName': formatExitPointName(name),
                    'validationVersion': 1,
                    'value': None if math.isnan(data) else float(data) / 1000 
                }

            result.append(obj)
    return file, result


def map_indexes(values_entry):
  c_values = {}

  for item in values_entry:
      for key, value_list in item.items():
          for value in value_list:
              if 'C' in value:
                  c_values[value['C'][0]] = value['C'][1]
  return c_values

def DeliveriesOfftakes(dateFrom: datetime, dateTo: datetime):
    envelope_response = []
    try:
        # if dateFrom < datetime(2023, 1, 1).date():
        #     return
        #     raise Exception("DateFrom must be greater than 2023-01-01. Prior to this date, the data is available in the DeliveriesOfftakesVerified endpoint.")
        
        if dateFrom <= datetime(2022, 8, 13).date():
            monthly_ranges = generate_monthly_ranges(dateFrom, dateTo)
            for start_date, end_date in monthly_ranges: 
                file, results = getPhysicalFlows(start_date, end_date + timedelta(days=1), aggregation = True)
                envelope_response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))

        if dateFrom >= datetime(2023, 1, 1).date() or dateTo >= datetime(2023, 1, 1).date():
            if dateFrom <= datetime(2023, 1, 1).date():
                dateFrom_temp = datetime(2023, 1, 1).date()
            else:
                dateFrom_temp = dateFrom
            file, results = parse_raw(dateFrom_temp, dateTo)
            envelope_response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
        
        # Define the target range
        target_start = datetime(2022, 8, 18).date()
        target_end = datetime(2023, 1, 1).date()
        # Check if the range between dateFrom and dateTo includes the range between 2022-08-13 and 2023-01-01
        if (dateFrom <= target_start and dateTo >= target_start) or (dateFrom <= target_end and dateTo >= target_end) or (dateFrom >= target_start and dateTo <= target_end):   
            if dateFrom < target_start:
                dateFrom_temp = target_start
            else:
                dateFrom_temp = dateFrom
            if dateTo > target_end:
                dateTo_temp = target_end
            else:
                dateTo_temp = dateTo
            
            file, results = parse_raw(dateFrom_temp, dateTo_temp, old = True)
            envelope_response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
        
        file, results = parse_verified(dateFrom, dateTo)
        envelope_response.append(getVirtualFileMetadata(results, getMetadata(True, file.Id), file))
    except Exception as e:
        message = f'Failed to parse {file.Url}: {e}'
        logException(e)
        envelope_response.append(getVirtualFileMetadata([], getMetadata(False, file.Id, message), file))
    
    return envelope.getSuccessEnvelope(envelope_response)

def generate_monthly_ranges(date_from, date_to):
    monthly_ranges = []
    current_start = date_from

    while current_start <= date_to:
        current_end = current_start + timedelta(days=30)  # Assuming 30 days per month
        if current_end > date_to:
            current_end = date_to
        monthly_ranges.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    return monthly_ranges

def parse_verified(dateFrom, dateTo):
    DeliveriesOfftakesData = pd.DataFrame()
    DeliveriesOfftakes = "https://www.desfa.gr/userfiles/pdflist/DDRA/Flows.xlsx"
    DeliveriesOfftakesData = pd.read_excel(DeliveriesOfftakes, sheet_name=0, index_col=0, skiprows=4, verbose = True)
        #Drop cols with name "Unnamed"
    DeliveriesOfftakesData = DeliveriesOfftakesData.loc[:, ~DeliveriesOfftakesData.columns.str.startswith('Unnamed')]
        #Remove last row
    DeliveriesOfftakesData = DeliveriesOfftakesData.iloc[:-1]
        #Remove all rows with index cotains "ΣΥΝΟΛΟ"
    mask = pd.Series(DeliveriesOfftakesData.index).str.contains("ΣΥΝΟΛΟ")
    mask = mask.fillna(False)
        # Invert the boolean mask using ~ operator
    mask = ~mask

        # Apply the mask to filter the DataFrame
    DeliveriesOfftakesData = DeliveriesOfftakesData[mask.values]
        
        #Make index datetime
    DeliveriesOfftakesData.index = pd.to_datetime(DeliveriesOfftakesData.index)
    DeliveriesOfftakesData = DeliveriesOfftakesData.sort_index()[dateFrom:dateTo]

    fileName = 'DESFA_Deliveries_Offtakes'
    file = getVirtualFileMetadataFromUrl(DeliveriesOfftakes, fileName, f'{fileName}_{dateFrom.strftime("%Y%m%d")}_{dateTo.strftime("%Y%m%d")}', dateFrom)
        
    results = []

        # Iterate through the rows of the dataframe
    for date, row in DeliveriesOfftakesData.iterrows():
            # Iterate through the columns of the row
        for exit_point, exit_point_value in row.iteritems():
            if  math.isnan(exit_point_value):
                    continue

                # Create the object with the specified structure
            obj = {
                    'dispatchDay': date.date(),
                    'dispatchPeriod': 5,
                    'exitPointName': exit_point,
                    'validationVersion': 3,
                    'value': float(exit_point_value) / 1000 
                }
                # Append the object to the list
            results.append(obj)
    results = [result for result in results if result['dispatchDay'] >= dateFrom and result['dispatchDay'] <= dateTo]

    return file,results

def parse_raw(dateFrom, dateTo, old = False):
    DeliveriesOfftakesData = pd.DataFrame()
    if old:
         DeliveriesOfftakes = "https://www.desfa.gr/userfiles/pdflist/DDRA/Hourly_Flows_since_13_08_2022.xlsx"
    else:
        firstOftheYear = datetime(dateFrom.year, 1, 1)
        DeliveriesOfftakes = f"https://www.desfa.gr/userfiles/pdflist/DDRA/Hourly_Flows_since_{firstOftheYear.strftime('%d_%m_%Y')}.xlsx"

        if requests.get(DeliveriesOfftakes, verify=False).status_code == 404:
            DeliveriesOfftakes = f"https://www.desfa.gr/userfiles/pdflist/DDRA/Hourly_Flows_since_{(firstOftheYear - timedelta(years=1)).strftime('%d_%m_%Y')}.xlsx"
        

    fileName = 'DESFA_Deliveries_Offtakes'
    file = getVirtualFileMetadataFromUrl(DeliveriesOfftakes, fileName, f'{fileName}_{dateFrom.strftime("%Y%m%d")}_{dateTo.strftime("%Y%m%d")}', dateFrom)
        
        #Create a list with all sheets in the excel file. Each sheet is a date in the format "dd.mm.yyyy". Iterate between dateFrom ad dateTo
    DeliveriesOfftakesSheets = [datetime.strftime(date, "%d.%m.%Y") for date in pd.date_range(dateFrom, dateTo)]
        
    DeliveriesOfftakesData = pd.read_excel(DeliveriesOfftakes, sheet_name=DeliveriesOfftakesSheets, skiprows=2)
    results = []
    for sheet_name, df in DeliveriesOfftakesData.items():
        df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

            # Parse the dispatchDay from the sheet_name
        dispatchDay = pd.to_datetime(sheet_name, format="%d.%m.%Y").date()

            # Skip the first two columns
        df = df.iloc[:, 2:]

            # Get the value in the sixth row
        value_rows = [0,1,2,3,5]  # Row index 5 corresponds to the sixth row

        index = 0
        for row in value_rows:
            index += 1
                
                # Iterate through the columns and create objects
            for exit_point, exit_point_value in df.iloc[row].items():
                if  math.isnan(exit_point_value):
                    continue

                    # Create the object with the specified structure
                obj = {
                        'dispatchDay': dispatchDay,
                        'dispatchPeriod': index,
                        'exitPointName': formatExitPointName(exit_point),
                        'validationVersion': 1,
                        'value':  float(exit_point_value) / 1000 
                    }
                results.append(obj)
    #filter results by dateFrom and dateTo
    results = [result for result in results if result['dispatchDay'] >= dateFrom and result['dispatchDay'] <= dateTo]
    return file,results