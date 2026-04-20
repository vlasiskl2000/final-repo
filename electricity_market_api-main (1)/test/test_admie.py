from sources.entsoe import *
import pytest
from helpers.date_helper import dst_periods
from datetime import datetime
from sources.admie_download import *
from sources.admie import *



dst_date = dst_periods(datetime.now())

date_DST_summer = datetime.strptime(dst_date[0], '%Y-%m-%d')
date_DST_winter = datetime.strptime(dst_date[1], '%Y-%m-%d')
non_dst_date = datetime.strptime('2023-09-23', '%Y-%m-%d')



#ignore the following function for test case
@pytest.mark.skip(reason="Not implemented yet")
def test_assertion(func_1, func_2, dispatch_range, dateFrom, dateTo, filterFunc = None, period_columns = 'DispatchPeriod'):

    json = func_1(parseDateTimeFromArgs(dateFrom.strftime('%Y-%m-%d')), parseDateTimeFromArgs(dateTo.strftime('%Y-%m-%d')))

    assert json.State == 1

    for file in json.Data:
        
        results = func_2(file)
        
        data_results = results.Data.Results
        if filterFunc is not None:
            data_results = list(filter(filterFunc,data_results))

        assert max(data_results, key= lambda x: x[period_columns])[period_columns] == dispatch_range

#['DispatchPeriod']



#downloadISPResults
#getIspActivatedBeActualFromFile
@pytest.mark.fast
def test_isp_ActivatedBeActual_summer():
    test_assertion(downloadISPResults, getIspActivatedBeActualFromFile, 92, date_DST_summer, date_DST_summer) 
@pytest.mark.fast
def test_isp_ActivatedBeActual_winter():
    test_assertion(downloadISPResults, getIspActivatedBeActualFromFile, 100, date_DST_winter, date_DST_winter) 

def test_isp_ActivatedBeActual():
    test_assertion(downloadISPResults, getIspActivatedBeActualFromFile, 96, non_dst_date, non_dst_date) 

#getIspScheduleActualFromFile problem with endpoint check other time


#getISPBalancingEnergyPricesFromFile
def test_isp_balancing_energy_prices_summer():
    test_assertion(downloadISPResults, getISPBalancingEnergyPricesFromFile, 92, date_DST_summer, date_DST_summer) 

def test_isp_balancing_energy_prices_winter():
    test_assertion(downloadISPResults, getISPBalancingEnergyPricesFromFile, 100, date_DST_winter, date_DST_winter) 

def test_isp_balancing_energy_prices():
    test_assertion(downloadISPResults, getISPBalancingEnergyPricesFromFile, 96, non_dst_date, non_dst_date) 

#getISPSystemImbalanceFromFile
def test_isp_system_imbalance__summer():
    test_assertion(downloadISPResults, getISPSystemImbalanceFromFile, 92, date_DST_summer, date_DST_summer) 

def test_isp_system_imbalance__winter():
    test_assertion(downloadISPResults, getISPSystemImbalanceFromFile, 100, date_DST_winter, date_DST_winter)

def test_isp_system_imbalance():
    test_assertion(downloadISPResults, getISPSystemImbalanceFromFile, 96, non_dst_date, non_dst_date)

#getIspReserveAwardsAndPricesActualFromFile
def test_isp_reserve_awards_and_prices_actual__summer():
    test_assertion(downloadISPResults, getIspReserveAwardsAndPricesActualFromFile, 92, date_DST_summer, date_DST_summer) 

def test_isp_reserve_awards_and_prices_actual__winter():
    test_assertion(downloadISPResults, getIspReserveAwardsAndPricesActualFromFile, 100, date_DST_winter, date_DST_winter)

def test_isp_reserve_awards_and_prices_actual():
    test_assertion(downloadISPResults, getIspReserveAwardsAndPricesActualFromFile, 96, non_dst_date, non_dst_date)
    
#getISPReservePricesFromFile
def test_isp_reserve_prices_actual_summer():
    test_assertion(downloadISPResults, getISPReservePricesFromFile, 92, date_DST_summer, date_DST_summer)

def test_isp_reserve_prices_actual__winter():
    test_assertion(downloadISPResults, getISPReservePricesFromFile, 100, date_DST_winter, date_DST_winter)

def test_isp_reserve_prices_actual():
    test_assertion(downloadISPResults, getISPReservePricesFromFile, 96, non_dst_date, non_dst_date)
    
#getIspVirtualScheduleConfigurationFromFile
def test_isp_virtual_schedule_config_summer():
    test_assertion(downloadISPResults, getIspVirtualScheduleConfigurationFromFile, 92, date_DST_summer, date_DST_summer) 

def test_isp_virtual_schedule_config_winter():
    test_assertion(downloadISPResults, getIspVirtualScheduleConfigurationFromFile, 100, date_DST_winter, date_DST_winter)

def test_isp_virtual_schedule_config():
    test_assertion(downloadISPResults, getIspVirtualScheduleConfigurationFromFile, 96, non_dst_date, non_dst_date)

#getIspSystemLoadFromFile
def test_isp_system_load_summer():
    test_assertion(downloadISPResults, getIspSystemLoadFromFile, 92, date_DST_summer, date_DST_summer) 

def test_isp_system_load__winter():
    test_assertion(downloadISPResults, getIspSystemLoadFromFile, 100, date_DST_winter, date_DST_winter)

def test_isp_system_load():
    test_assertion(downloadISPResults, getIspSystemLoadFromFile, 96, non_dst_date, non_dst_date)

#getIspCommissioningScheduleFromFile
def test_isp_commissioning_schedule_summer():
    test_assertion(downloadIspRequirements, getIspCommissioningScheduleFromFile, 92, date_DST_summer, date_DST_summer, period_columns = 'dispatchPeriod') 

def test_isp_commissioning_schedule_winter():
    test_assertion(downloadIspRequirements, getIspCommissioningScheduleFromFile, 100, date_DST_winter, date_DST_winter, period_columns = 'dispatchPeriod')

def test_isp_commissioning_schedule():
    test_assertion(downloadIspRequirements, getIspCommissioningScheduleFromFile, 96, non_dst_date, non_dst_date, period_columns = 'dispatchPeriod')


#---------------------------------------
#downloadIspRequirements
        

def test_mandatory_Hydro_summer():
    test_assertion(downloadIspRequirements, getMandatoryHydroFromFile, 92, date_DST_summer ,date_DST_summer) 

def test_mandatory_Hydro_winter():
    test_assertion(downloadIspRequirements, getMandatoryHydroFromFile, 100, date_DST_winter ,date_DST_winter) 

def test_mandatory_Hydro():
    test_assertion(downloadIspRequirements, getMandatoryHydroFromFile, 96, non_dst_date, non_dst_date)

#getCommissiongFromFile
@pytest.mark.fast
def test_commissioning_summer():
    test_assertion(downloadIspRequirements, getCommissiongFromFile, 92, date_DST_summer, date_DST_summer) 
@pytest.mark.fast
def test_commissioning_winter():
    test_assertion(downloadIspRequirements, getCommissiongFromFile, 100, date_DST_winter, date_DST_winter) 

def test_commissioning():
    test_assertion(downloadIspRequirements, getCommissiongFromFile, 96, non_dst_date, non_dst_date)

#getIspSystemLossesFromFile   
def test_system_losses_summer():
    test_assertion(downloadIspRequirements, getIspSystemLossesFromFile, 92, date_DST_summer, date_DST_summer, period_columns = 'dispatchPeriod') 

def test_system_losses_winter():
    test_assertion(downloadIspRequirements, getIspSystemLossesFromFile, 100, date_DST_winter, date_DST_winter, period_columns = 'dispatchPeriod') 

def test_system_losses():
    test_assertion(downloadIspRequirements, getIspSystemLossesFromFile, 96, non_dst_date, non_dst_date, period_columns = 'dispatchPeriod')
  
#getReserveRequirementsFromFile
def test_reserve_requirements_summer():
    test_assertion(downloadIspRequirements, getReserveRequirementsFromFile, 92, date_DST_summer, date_DST_summer) 

def test_reserve_requirements_winter():
    test_assertion(downloadIspRequirements, getReserveRequirementsFromFile, 100, date_DST_winter, date_DST_winter) 

def test_reserve_requirements():
    test_assertion(downloadIspRequirements, getReserveRequirementsFromFile, 96, non_dst_date, non_dst_date)
  
#downloadISPResults
#getReserveRequirementsFromISPResultsFromFile
def test_reserve_requirements_isp_results_summer():
    test_assertion(downloadISPResults, getReserveRequirementsFromISPResultsFromFile, 92, date_DST_summer, date_DST_summer) 

def test_reserve_requirements_isp_results_winter():
    test_assertion(downloadISPResults, getReserveRequirementsFromISPResultsFromFile, 100, date_DST_winter, date_DST_winter)

def test_reserve_requirements_isp_results():
    test_assertion(downloadISPResults, getReserveRequirementsFromISPResultsFromFile, 96, non_dst_date, non_dst_date)
  

#downloadIspLoadForecasts
#getIspLoadForecastsFromFile
def test_isp_load_forecast_summer():
    test_assertion(downloadIspLoadForecasts, getIspLoadForecastsFromFile, 96, date_DST_summer, date_DST_summer, period_columns = 'dispatchPeriod') 

#96 is the expected value for the winter dst date
@pytest.mark.fast
def test_isp_load_forecast_winter():
    test_assertion(downloadIspLoadForecasts, getIspLoadForecastsFromFile, 96, date_DST_winter, date_DST_winter, period_columns = 'dispatchPeriod') 

def test_isp_load_forecast_results():
    test_assertion(downloadIspLoadForecasts, getIspLoadForecastsFromFile, 96, non_dst_date, non_dst_date, period_columns = 'dispatchPeriod')

#downloadIspRequirements
#getIspLoadForecastsFromISPRequirementsFile
@pytest.mark.fast
def test_isp_load_forecast_isp_requirements_summer():
    test_assertion(downloadIspRequirements, getIspLoadForecastsFromISPRequirementsFile, 92, date_DST_summer, date_DST_summer, period_columns = 'dispatchPeriod') 
@pytest.mark.fast
def test_isp_load_forecast_isp_requirements_winter():
    test_assertion(downloadIspRequirements, getIspLoadForecastsFromISPRequirementsFile, 100, date_DST_winter, date_DST_winter, period_columns = 'dispatchPeriod') 

def test_isp_load_forecast_isp_requirements():
    test_assertion(downloadIspRequirements, getIspLoadForecastsFromISPRequirementsFile, 96, non_dst_date, non_dst_date, period_columns = 'dispatchPeriod')

#downloadIspResForecasts
#getIspResForecastsFromFile
@pytest.mark.fast
def test_isp_res_forecast_forecasts_summer():
    test_assertion(downloadIspResForecasts, getIspResForecastsFromFile, 96, date_DST_summer, date_DST_summer, period_columns = 'dispatchPeriod') 
@pytest.mark.fast
def test_isp_res_forecast_forecasts_winter():
    test_assertion(downloadIspResForecasts, getIspResForecastsFromFile, 96, date_DST_winter, date_DST_winter, period_columns = 'dispatchPeriod') 

def test_isp_res_forecast_forecasts():
    test_assertion(downloadIspResForecasts, getIspResForecastsFromFile, 96, non_dst_date, non_dst_date, period_columns = 'dispatchPeriod')


#downloadIspRequirements
#getIspResForecastsFromISPRequirementsFile
@pytest.mark.fast
def test_isp_res_forecast_forecasts_isp_requirements_summer():
    test_assertion(downloadIspRequirements, getIspResForecastsFromISPRequirementsFile, 92, date_DST_summer, date_DST_summer, period_columns = 'dispatchPeriod') 
@pytest.mark.fast
def test_isp_res_forecast_forecasts_isp_requirements_winter():
    test_assertion(downloadIspRequirements, getIspResForecastsFromISPRequirementsFile, 100, date_DST_winter, date_DST_winter, period_columns = 'dispatchPeriod') 

def test_isp_res_forecast_forecasts_isp_requirements():
    test_assertion(downloadIspRequirements, getIspResForecastsFromISPRequirementsFile, 96, non_dst_date, non_dst_date, period_columns = 'dispatchPeriod')

#downloadIspWeekAheadLoadForecasts
#getIspWeekAheadLoadForecastsFromFile
@pytest.mark.fast
def test_isp_week_ahead_forecast_forecasts_summer():
    test_assertion(downloadIspWeekAheadLoadForecasts, getIspWeekAheadLoadForecastsFromFile, 96, date_DST_summer, date_DST_summer, period_columns = 'dispatchPeriod') 
@pytest.mark.fast
def test_isp_week_ahead_forecasts_winter():
    test_assertion(downloadIspWeekAheadLoadForecasts, getIspWeekAheadLoadForecastsFromFile, 96, date_DST_winter, date_DST_winter, period_columns = 'dispatchPeriod') 

def test_isp_week_ahead_forecasts():
    test_assertion(downloadIspWeekAheadLoadForecasts, getIspWeekAheadLoadForecastsFromFile, 96, non_dst_date, non_dst_date, period_columns = 'dispatchPeriod')



#downloadBalancingEnergySettlements
#getBalancingEnergySettlementsFromFile
# @pytest.mark.fast
def test_balancing_energy_settlements_summer():
    test_assertion(downloadBalancingEnergySettlements, getBalancingEnergySettlementsFromFile, 92, date_DST_summer, date_DST_summer, filterFunc = lambda x: x['DispatchDay'] == datetime.date(date_DST_summer))
# @pytest.mark.fast
def test_balancing_energy_settlements_winter():
    test_assertion(downloadBalancingEnergySettlements, getBalancingEnergySettlementsFromFile, 100, date_DST_winter, date_DST_winter)

def test_balancing_energy_settlements():
    test_assertion(downloadBalancingEnergySettlements, getBalancingEnergySettlementsFromFile, 96, non_dst_date, non_dst_date)

#96 is the expected value for the winter dst date
#downloadScada
#getImportExportFromSCADAFromFile
@pytest.mark.fast
def test_imports_exports_scada_summer():
    test_assertion(downloadScada, getImportExportFromSCADAFromFile, 88, date_DST_summer, date_DST_summer, filterFunc = lambda x: x['DispatchDay'] == datetime.date(date_DST_summer))
@pytest.mark.fast
def test_imports_exports_scada_winter():
    test_assertion(downloadScada, getImportExportFromSCADAFromFile, 96, date_DST_winter, date_DST_winter) 

def test_imports_exports_scada():
    test_assertion(downloadScada, getImportExportFromSCADAFromFile, 96, non_dst_date, non_dst_date)

    
#getScadaEntityProductionFromFile
def test_scada_entity_production_summer():
    test_assertion(downloadScada, getScadaEntityProductionFromFile, 88, date_DST_summer, date_DST_summer, filterFunc = lambda x: x['DispatchDay'] == date_DST_summer.strftime('%Y-%m-%d'))

def test_scada_entity_production_winter():
    test_assertion(downloadScada, getScadaEntityProductionFromFile, 96, date_DST_winter, date_DST_winter) 

def test_scada_entity_production():
    test_assertion(downloadScada, getScadaEntityProductionFromFile, 96, non_dst_date, non_dst_date)

#getScadaResProductionFromFile
def test_scada_res_production_summer():
    test_assertion(downloadScada, getScadaResProductionFromFile, 88, date_DST_summer, date_DST_summer, filterFunc = lambda x: x['DispatchDay'].strftime('%Y-%m-%d') == date_DST_summer.strftime('%Y-%m-%d'))

def test_scada_res_production_winter():
    test_assertion(downloadScada, getScadaResProductionFromFile, 96, date_DST_winter, date_DST_winter) 

def test_scada_res_production():
    test_assertion(downloadScada, getScadaResProductionFromFile, 96, non_dst_date, non_dst_date)

    
#getScadaHydroProductionFromFile
def test_scada_hydro_production_summer():
    test_assertion(downloadScada, getScadaHydroProductionFromFile, 88, date_DST_summer, date_DST_summer, filterFunc = lambda x: x['DispatchDay'] == date_DST_summer.strftime('%Y-%m-%d')) 

def test_scada_hydro_production_winter():
    test_assertion(downloadScada, getScadaHydroProductionFromFile, 96, date_DST_winter, date_DST_winter)

def test_scada_hydro_production():
    test_assertion(downloadScada, getScadaHydroProductionFromFile, 96, non_dst_date, non_dst_date)

    
#getScadaNaturalGasProductionFromFile
def test_scada_natural_gas_production_summer():
    test_assertion(downloadScada, getScadaNaturalGasProductionFromFile, 88, date_DST_summer, date_DST_summer, filterFunc = lambda x: x['DispatchDay'] == date_DST_summer.strftime('%Y-%m-%d'))

def test_scada_natural_gas_production_winter():
    test_assertion(downloadScada, getScadaNaturalGasProductionFromFile, 96, date_DST_winter, date_DST_winter) 

def test_scada_natural_gas_production():
    test_assertion(downloadScada, getScadaNaturalGasProductionFromFile, 96, non_dst_date, non_dst_date)
    
    
#getScadaThermoProductionFromFile
def test_scada_thermo_production_summer():
    test_assertion(downloadScada, getScadaThermoProductionFromFile, 88, date_DST_summer, date_DST_summer, filterFunc = lambda x: x['DispatchDay'] == date_DST_summer.strftime('%Y-%m-%d')) 

def test_scada_thermo_production_winter():
    test_assertion(downloadScada, getScadaThermoProductionFromFile, 96, date_DST_winter, date_DST_winter) 
    
def test_scada_thermo_production():
    test_assertion(downloadScada, getScadaThermoProductionFromFile, 96, non_dst_date, non_dst_date)
   
#getScadaHVLoadFromFile
def test_scada_hv_load_summer():
    test_assertion(downloadScada, getScadaHVLoadFromFile, 88, date_DST_summer, date_DST_summer, filterFunc = lambda x: x['DispatchDay'].strftime('%Y-%m-%d') == date_DST_summer.strftime('%Y-%m-%d'))

def test_scada_hv_load_winter():
    test_assertion(downloadScada, getScadaHVLoadFromFile, 96, date_DST_winter, date_DST_winter) 

def test_scada_hv_load():
    test_assertion(downloadScada, getScadaHVLoadFromFile, 96, non_dst_date, non_dst_date)
   
    
#getScadaSystemLoadRealizationFromFile
def test_scada_system_load_realization_summer():
    test_assertion(downloadScada, getScadaSystemLoadRealizationFromFile, 88, date_DST_summer, date_DST_summer, filterFunc = lambda x: x['DispatchDay'].strftime('%Y-%m-%d') == date_DST_summer.strftime('%Y-%m-%d'))

def test_scada_system_load_realization_winter():
    test_assertion(downloadScada, getScadaSystemLoadRealizationFromFile, 96, date_DST_winter, date_DST_winter) 

def test_scada_system_load_realization():
    test_assertion(downloadScada, getScadaSystemLoadRealizationFromFile, 96, non_dst_date, non_dst_date)
   

#downloadRealTimeScadaRes
#getRealTimeScadaResFromFile
@pytest.mark.fast
def test_scada_real_time_scada_res_summer():
    test_assertion(downloadRealTimeScadaRes, getRealTimeScadaResFromFile, 88, date_DST_summer, date_DST_summer, filterFunc = lambda x: x['DispatchDay'] == datetime.date(date_DST_summer)) 
@pytest.mark.fast
def test_scada_real_time_scada_res_winter():
    test_assertion(downloadRealTimeScadaRes, getRealTimeScadaResFromFile, 96, date_DST_winter, date_DST_winter) 

def test_scada_real_time_scada_res():
    test_assertion(downloadRealTimeScadaRes, getRealTimeScadaResFromFile, 96, non_dst_date, non_dst_date)


#downloadResMvAdmie
#getRESActualMVInjectionsFromFile
@pytest.mark.fast
def test_res_actual_mv_injections_summer():
    test_assertion(downloadResMvAdmie, getRESActualMVInjectionsFromFile, 88, date_DST_summer, date_DST_summer, filterFunc = lambda x: x['DispatchDay'] == datetime.date(date_DST_summer))
@pytest.mark.fast
def test_res_actual_mv_injections():
    test_assertion(downloadResMvAdmie, getRESActualMVInjectionsFromFile, 96, non_dst_date, non_dst_date)

#no files available for winter dst day
# def test_res_actual_mv_injections_winter():
#     test_assertion(downloadResMvAdmie, getRESActualMVInjectionsFromFile, 96, date_DST_winter, date_DST_winter) 
        

#downloadLtNominations
#getLtNominationsFromFile
@pytest.mark.fast
def test_lt_nominations_summer():
    test_assertion(downloadLtNominations, getLtNominationsFromFile, 92, date_DST_summer, date_DST_summer) 
@pytest.mark.fast
def test_lt_nominations_winter():
    test_assertion(downloadLtNominations, getLtNominationsFromFile, 100, date_DST_winter, date_DST_winter) 

def test_lt_nominations():
    test_assertion(downloadLtNominations, getLtNominationsFromFile, 96, non_dst_date, non_dst_date)
      

    
#downloadBalancingEnergyProducts
#getBalancingEnergyProductsFromFile
# @pytest.mark.fast
def test_balancing_energy_products_summer():
    test_assertion(downloadBalancingEnergyProducts, getBalancingEnergyProductsFromFile, 92, date_DST_summer, date_DST_summer, lambda x: x['DispatchDay'] == datetime.date(date_DST_summer))  
# @pytest.mark.fast
def test_balancing_energy_products_winter():
    test_assertion(downloadBalancingEnergyProducts, getBalancingEnergyProductsFromFile, 100, date_DST_winter, date_DST_winter) 

def test_balancing_energy_products():
    test_assertion(downloadBalancingEnergyProducts, getBalancingEnergyProductsFromFile, 96, non_dst_date, non_dst_date)
        


#downloadBalancingCapacityProducts
#getBalancingCapacityProductsFromFile
# @pytest.mark.fast # 2025 dst yields 0 results
def test_balancing_capacity_products_summer():
    test_assertion(downloadBalancingCapacityProducts, getBalancingCapacityProductsFromFile, 92, date_DST_summer, date_DST_summer, filterFunc = lambda x: x['DispatchDay'] == datetime.date(date_DST_summer)) 
# @pytest.mark.fast
def test_balancing_capacity_products_winter():
    test_assertion(downloadBalancingCapacityProducts, getBalancingCapacityProductsFromFile, 100, date_DST_winter, date_DST_winter) 

def test_balancing_capacity_products():
    test_assertion(downloadBalancingCapacityProducts, getBalancingCapacityProductsFromFile, 96, non_dst_date, non_dst_date)


    
#downloadAvailableTransferCapacity
#getAvailableTransferCapacityFromFile
@pytest.mark.fast
def test_available_transfer_capacity_summer():
    test_assertion(downloadAvailableTransferCapacity, getAvailableTransferCapacityFromFile, 92, date_DST_summer, date_DST_summer) 
@pytest.mark.fast
def test_available_transfer_capacity_winter():
    test_assertion(downloadAvailableTransferCapacity, getAvailableTransferCapacityFromFile, 100, date_DST_winter, date_DST_winter) 

def test_available_transfer_capacity():
    test_assertion(downloadAvailableTransferCapacity, getAvailableTransferCapacityFromFile, 96, non_dst_date, non_dst_date)

