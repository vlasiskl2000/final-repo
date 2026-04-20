from sources.entsoe import *
import pytest
from helpers.date_helper import dst_periods
from datetime import datetime



dst_date = dst_periods(datetime.now())

date_DST_summer = datetime.strptime(dst_date[0], '%Y-%m-%d')
date_DST_winter = datetime.strptime(dst_date[1], '%Y-%m-%d')

#ignore the following function for test case
@pytest.mark.skip(reason="Not implemented yet")
def test_assertion(func, hour_length, dateFrom, dateTo, filterFunc = None):
    response = func(dateFrom, dateTo)
    data = response.Data 

    for row in data:
        data_per_country = row.Results 
        
        if filterFunc is not None:
            data_per_country = list(filter(filterFunc, data_per_country))

        length = len(data_per_country)
        if length == 0:
            continue

        assert len(data_per_country) == hour_length


#getEntsoeDamPrice
# @pytest.mark.fast
def test_entsoe_Dam_DST_summer():
    test_assertion(getEntsoeDamPrice, 92, date_DST_summer, date_DST_summer)

# @pytest.mark.fast
def test_entsoe_Dam_DST_winter():       
    test_assertion(getEntsoeDamPrice, 100, date_DST_winter, date_DST_winter)

#getENTSOEActualLoad
@pytest.mark.fast
def test_entsoe_Actual_Load_DST_summer():
    test_assertion(getENTSOEActualLoad, 96, date_DST_summer, date_DST_summer)

# @pytest.mark.fast
def test_entsoe_Actual_Load_winter():       
    test_assertion(getENTSOEActualLoad, 96, date_DST_winter, date_DST_winter)

#getENTSOEDayAheadPredictedLoad
@pytest.mark.fast
def test_entsoe_Day_Ahead_Predicted_Load_summer():       
    test_assertion(getENTSOEDayAheadPredictedLoad, 92, date_DST_summer, date_DST_summer)

# @pytest.mark.fast
def test_entsoe_Day_Ahead_Predicted_Load_winter():       
    test_assertion(getENTSOEDayAheadPredictedLoad, 100, date_DST_winter, date_DST_winter)

#getScheduledCommercialExchanges
def test_entsoe_schedule_commercial_Exchanges_summer():       
    test_assertion(getScheduledCommercialExchanges, 92, date_DST_summer, date_DST_summer)


def test_entsoe_schedule_commercial_Exchanges_winter():       
    test_assertion(getScheduledCommercialExchanges, 100, date_DST_winter, date_DST_winter)

#getCrossBorderPhysicalFlow
def test_entsoe_cross_border_physical_flow_summer():       
    test_assertion(getCrossBorderPhysicalFlow, 92, date_DST_summer, date_DST_summer)


def test_entsoe_cross_border_physical_flow_winter():       
    test_assertion(getCrossBorderPhysicalFlow, 100, date_DST_winter, date_DST_winter)

#getForecastedCapacity
def test_entsoe_get_forecasted_capacity_summer():       
    test_assertion(getForecastedCapacity, 92, date_DST_summer, date_DST_summer)


def test_entsoe_get_forecasted_capacity_winter():       
    test_assertion(getForecastedCapacity, 100, date_DST_winter, date_DST_winter) 

#getAggregatedHydroFillingRate #returns weekly data
def test_entsoe_get_aggregated_hydro_filling_rate_summer():       
    test_assertion(getAggregatedHydroFillingRate, 1, date_DST_summer, date_DST_summer)


def test_entsoe_get_aggregated_hydro_filling_rate_winter():       
    test_assertion(getAggregatedHydroFillingRate, 1, date_DST_winter, date_DST_winter) 
    
#getProductionPerCategory #returns yearly data for every month
def test_entsoe_get_production_per_category_summer():       
    test_assertion(getProductionPerCategory, 92, date_DST_summer, date_DST_summer, lambda x: x['countryName'] == '10YCA-BULGARIA-R' and x['productionCategory'] == 16)


def test_entsoe_get_production_per_category_winter():       
    test_assertion(getProductionPerCategory, 100, date_DST_winter, date_DST_winter, lambda x: x['countryName'] == '10YCA-BULGARIA-R' and x['productionCategory'] == 16)

#getWindAndSolarForecast
def test_entsoe_get_wind_and_solar_forecast_summer():       
    test_assertion(getWindAndSolarForecast, 92, date_DST_summer, date_DST_summer)


def test_entsoe_get_wind_and_solar_forecast_winter():       
    test_assertion(getWindAndSolarForecast, 100, date_DST_winter, date_DST_winter)


#getDayAheadGenerationForecast # api call also returns next day for each countr
def test_entsoe_get_day_ahead_generation_forecast_summer():       
    test_assertion(getDayAheadGenerationForecast, 2 * 92 + 2 * 96, date_DST_summer, date_DST_summer, lambda x: x['CountryName'] == "10YGR-HTSO-----Y")


def test_entsoe_get_day_ahead_generation_forecast_winter():       
    test_assertion(getDayAheadGenerationForecast, 2 * 96 + 2 * 100, date_DST_winter, date_DST_winter, lambda x: x['CountryName']== "10YGR-HTSO-----Y")
    
#getAggregatedBids #returns two collumns
def test_entsoe_get_aggregated_bids_summer():       
    test_assertion(getAggregatedBids, 2 * 92, date_DST_summer, date_DST_summer)


def test_entsoe_get_aggregated_bids_winter():       
    test_assertion(getAggregatedBids, 2 * 100, date_DST_winter, date_DST_winter)


#getActivatedEnergyPrices
def test_entsoe_get_activated_energy_prices_summer():       
    test_assertion(getActivatedEnergyPrices, 2 * 92, date_DST_summer, date_DST_summer)


def test_entsoe_get_activated_energy_prices_winter():       
    test_assertion(getActivatedEnergyPrices, 2 * 100, date_DST_winter, date_DST_winter)




#getUnavailabilityOfInterconnections
def test_entsoe_get_unavailability_of_interconnections_summer():       
    test_assertion(getUnavailabilityOfInterconnections, 92, date_DST_summer, date_DST_summer)


def test_entsoe_get_unavailability_of_interconnections_winter():       
    test_assertion(getUnavailabilityOfInterconnections, 100, date_DST_winter, date_DST_winter)


#getImplicitAllocationsDayAhead
def test_entsoe_get_implicit_allocations_day_ahead_summer():       
    test_assertion(getImplicitAllocationsDayAhead, 92, date_DST_summer, date_DST_summer)


def test_entsoe_get_implicit_allocations_day_ahead_winter():       
    test_assertion(getImplicitAllocationsDayAhead, 100, date_DST_winter, date_DST_winter)
    


