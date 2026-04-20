from sources.seecaoapi import *
from sources.jao import *   
from datetime import datetime
from helpers.date_helper import parseDateTimeFromArgs, dst_periods
import pytest


date_DST_summer, date_DST_winter = dst_periods(datetime.now())

#ignore the following function for test case
@pytest.mark.skip(reason="Not implemented yet")
def test_assertion(func_1, dispatch_range, dateFrom, dateTo, horizon = None, filterFunc = None):

    json = func_1(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), horizon)

    assert json.State == 1

    for row in json.Data:
        
        assert len(row.Results[0].Results)  == dispatch_range



  

#Seecao api
@pytest.mark.fast
def test_seecao_api_DST_summer():
    test_assertion(getSeecaoapi,  23, date_DST_summer, date_DST_summer, horizon = 'daily')
@pytest.mark.fast
def test_seecao_api_DST_winter():
    test_assertion(getSeecaoapi, 25, date_DST_winter, date_DST_winter, horizon = 'daily')

#jao api
@pytest.mark.fast
def test_jao_daily_DST_summer():
    test_assertion(getAuctions,  23, date_DST_summer , date_DST_summer, horizon = 'daily')
@pytest.mark.fast
def test_jao_daily_DST_winter():
    test_assertion(getAuctions, 25, date_DST_winter , date_DST_winter, horizon = 'daily')

