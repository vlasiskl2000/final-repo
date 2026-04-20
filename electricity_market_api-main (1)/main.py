from datetime import date
from date_helper import parseDateTimeFromArgs
import logging
from helpers.date_helper import *
from fastapi import BackgroundTasks, FastAPI, File, UploadFile, Form, Depends
from typing import List, Optional
from interface.envelope import Envelope

from dotenv import load_dotenv

from sources.the_ice_metadata import Price
load_dotenv()

from models.entsoe_models import EntsoeDayAheadAggregatedForecastModel
from models.metadata import VirtualFileMetadataPayload
import pytz
import sources.admie as admie
import sources.admie_download as admie_download
import sources.entsoe as entsoe
import sources.exchange as exchange
import sources.eia as eia
import sources.quandl as quantl
import sources.desfa as desfa
import sources.enexgroup as enexgroup
import sources.noaa as noaa
import sources.meteologica as meteologica
import sources.dapeep as dapeep
import sources.enexgroup_download as enexgroup_download
import sources.dapeep_download as dapeep_download
import sources.theice_api as theice
import sources.meteologica_download as meteologica_download
import sources.jao as jao
import sources.seecao as seecao
import sources.tsoc_forecast as tsoc_forecast
import sources.eex as eex
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
import sources.tsoc as tsoc
import sources.seecaoapi as seecaoapi
import os

from models.file import CustomFile, as_form

from helpers.download_helper import importFiles

sentry_enabled = os.getenv("SentryEnabled", '0') == '1'
if sentry_enabled:
    sentry_sdk.init(
        dsn=os.getenv("SentryDSN", "https://1080d3128ca24cd3b542d2ac1ba0ffec@sentry.stellarblue.eu/37"),

        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production,
        traces_sample_rate=1.0,
        attach_stacktrace=True,
        send_default_pii=True,
        integrations=[
            LoggingIntegration(
                level=logging.INFO,        # Capture info and above as breadcrumbs
                event_level=logging.ERROR  # Send errors as events
            ),
        ],
    )

app = FastAPI()

@app.post("/ImportFile")
def create_upload_file(file: UploadFile = File(...), custom_file: CustomFile = Depends(as_form)) -> Envelope[CustomFile]:
    return importFiles(custom_file, file)

@app.get("/UnitAvailability/{dateFrom}/{dateTo}")
def getUnitAvailabilities(dateFrom : str, dateTo : str, isp : int = None, version: int = None) -> Envelope:
    return admie.getUnitAvailabilities(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), isp, version)
    
@app.post("/UnitAvailability")
def getUnitAvailabilities(file: CustomFile) -> Envelope:
    return admie.getUnitAvailabilitiesFromFile(file)
    
@app.get("/UnitAvailability/Download/{dateFrom}/{dateTo}")
def downloadUnitAvailabilities(dateFrom: str, dateTo: str, isp : int = None, version: int = None, force: bool = False) -> Envelope:
    return admie_download.downloadUnitAvailabilities(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), isp, version, force)

@app.get("/IspBeOffersActual/{dateFrom}/{dateTo}")
def getISPEnergyOffers(dateFrom : str, dateTo : str):
    return admie.getISPEnergyOffers(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspBeOffersActual")
def getISPEnergyOffersFromFile(file: CustomFile) -> Envelope:
    return admie.getISPEnergyOffersFromFile(file)

@app.get("/IspBeOffersActual/Download/{dateFrom}/{dateTo}")
def downloadIspBeOffersActual(dateFrom: str, dateTo: str, force: bool = False) -> Envelope:
    return admie_download.downloadISPEnergyOfferFiles(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.get("/IspReserveOffersActual/{dateFrom}/{dateTo}")
def getISPReserveOffers(dateFrom : str, dateTo : str, failOnError: bool = False) -> Envelope:
    return admie.getISPCapacityOffers(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), failOnError)
    
@app.get("/IspReserveOffersActual/Download/{dateFrom}/{dateTo}")
def downloadISPReserveOffers(dateFrom : str, dateTo : str, force: bool = False) -> Envelope:
    return admie_download.downloadISPCapacityOffers(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.post("/IspReserveOffersActual")
def getISPEnergyOffersFromFile(file: CustomFile) -> Envelope:
    return admie.getISPCapacityOffersFromFile(file)

## ISP Results
@app.get("/IspResults/Download/{dateFrom}/{dateTo}")
def downloadISPResults(dateFrom : str, dateTo : str, isp : int = None, adhoc: bool = None, force: bool = None) -> Envelope:
    return admie_download.downloadISPResults(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), isp, adhoc, force)

@app.get("/IspActivatedBeActual/{dateFrom}/{dateTo}")
def getActivatedEnergy(dateFrom : str, dateTo : str, isp : int = None, adhoc: int = None) -> Envelope:
    return admie.getActivatedEnergyDataFromISPResults(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), isp, adhoc)

@app.post("/IspActivatedBeActual")
def getIspActivatedBeActualFromFile(file: CustomFile) -> Envelope:
    return admie.getIspActivatedBeActualFromFile(file)

@app.get("/IspScheduleActual/{dateFrom}/{dateTo}")
def getIspSchedules(dateFrom : str, dateTo : str, isp : int = None, adhoc: int = None) -> Envelope:
    return admie.getISPScheduleFromISPResults(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), isp , adhoc)

@app.post("/IspScheduleActual")
def getIspScheduleActualFromFile(file: CustomFile) -> Envelope:
    return admie.getIspScheduleActualFromFile(file)
    
@app.get("/IspBePricesActual/{dateFrom}/{dateTo}")
def getIspSchedules(dateFrom : str, dateTo : str):
    return admie.getISPBalancingEnergyPrices(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspBePricesActual")
def IspBePricesActualFromFile(file: CustomFile) -> Envelope:
    return admie.getISPBalancingEnergyPricesFromFile(file)
    
@app.get("/IspSystemImbalance/{dateFrom}/{dateTo}")
def getIspSchedules(dateFrom : str, dateTo : str):
    return admie.getISPSystemImbalance(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspSystemImbalance")
def IspBePricesActualFromFile(file: CustomFile) -> Envelope:
    return admie.getISPSystemImbalanceFromFile(file)

@app.get("/IspReserveAwardsAndPricesActual/{dateFrom}/{dateTo}")
def getIspReserveAwardsAndPricesActual(dateFrom : str, dateTo : str):
    return admie.getIspReserveAwardsAndPricesActual(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))
    
@app.post("/IspReserveAwardsAndPricesActual")
def IspReserveAwardsAndPricesActualFromFile(file: CustomFile) -> Envelope:
    return admie.getIspReserveAwardsAndPricesActualFromFile(file)

@app.get("/IspGenericConstraints/{dateFrom}/{dateTo}")
def getIspGenericConstraints(dateFrom : str, dateTo : str):
    return admie.getISPGenericConstraints(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspGenericConstraints")
def IspGenericConstraintsFromFile(file: CustomFile) -> Envelope:
    return admie.getISPGenericConstraintsFromFile(file)

@app.get("/IspReservePricesActual/{dateFrom}/{dateTo}")
def getIspSchedules(dateFrom : str, dateTo : str):
    return admie.getISPReservePrices(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspReservePricesActual")
def getIspReservePricesActualFromFile(file: CustomFile) -> Envelope:
    return admie.getISPReservePricesFromFile(file)

@app.get("/IspVirtualScheduleConfiguration/{dateFrom}/{dateTo}")
def getIspVirtualScheduleConfiguration(dateFrom : str, dateTo : str):
    return admie.getIspVirtualScheduleConfiguration(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspVirtualScheduleConfiguration")
def getIspVirtualScheduleConfigurationFromFile(file: CustomFile) -> Envelope:
    return admie.getIspVirtualScheduleConfigurationFromFile(file)

@app.get("/IspSystemLoad/{dateFrom}/{dateTo}")
def getIspSystemLoadFromISPResults(dateFrom : str, dateTo : str, isp : int = None, adhoc: int = None) -> Envelope:
    return admie.getIspSystemLoadFromISPResults(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), isp, adhoc)

@app.post("/IspSystemLoad")
def getIspSystemLoadFromFile(file: CustomFile) -> Envelope:
    return admie.getIspSystemLoadFromFile(file)

@app.get("/IspCommissioningSchedule/{dateFrom}/{dateTo}")
def getIspCommissioningSchedule(dateFrom : str, dateTo : str):
    return admie.getIspCommissioningScheduleFromRequirements(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspCommissioningSchedule")
def getIspCommissioningScheduleFromFile(file: CustomFile) -> Envelope:
    return admie.getIspCommissioningScheduleFromFile(file)

@app.post("/IspEnergySurplus")
def getIspEnergySurplusFromFile(file: CustomFile) -> Envelope:
    return admie.getIspEnergySurplusFromFile(file)


##
    
## ISP Requirements

@app.get("/IspRequirements/Download/{dateFrom}/{dateTo}")
def downloadIspRequirements(dateFrom : str, dateTo : str, isp : int = None, generateLinks: bool = False, force: bool = None) -> Envelope:
    return admie_download.downloadIspRequirements(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), isp, generateLinks, force)

@app.get("/IspMandatoryHydros/{dateFrom}/{dateTo}")
def getMandatoryHydro(dateFrom : str, dateTo : str):
    return admie.getMandatoryHydroFromISPRequirements(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspMandatoryHydros")
def getIspMandatoryHydrosFromFile(file: CustomFile) -> Envelope:
    return admie.getMandatoryHydroFromFile(file)
    
@app.get("/IspThermalCommissioning/{dateFrom}/{dateTo}")
def getCommissiongFromISPRequirements(dateFrom : str, dateTo : str):
    return admie.getCommissiongFromISPRequirements(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspThermalCommissioning")
def getCommissiongFromFile(file: CustomFile) -> Envelope:
    return admie.getCommissiongFromFile(file)

@app.get("/IspSystemLossesForecast/{dateFrom}/{dateTo}")
def getIspSystemLossesFromRequirements(dateFrom : str, dateTo : str):
    return admie.getIspSystemLossesFromRequirements(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))
    
@app.post("/IspSystemLossesForecast")
def getIspSystemLossesFromFile(file: CustomFile) -> Envelope:
    return admie.getIspSystemLossesFromFile(file)

@app.get("/IspReserveRequirements/{dateFrom}/{dateTo}")
def getIspReserveRequirements(dateFrom : str, dateTo : str):
    return admie.getReserveRequirementsFromISPRequirements(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspReserveRequirements")
def getIspReserveRequirementsFromFile(file: CustomFile) -> Envelope:
    return admie.getReserveRequirementsFromFile(file)

@app.post("/IspReserveRequirementsISPResults")
def getReserveRequirementsFromISPResults(file: CustomFile) -> Envelope:
    return admie.getReserveRequirementsFromISPResultsFromFile(file)

## ISP Deviations

@app.get("/IspItalyDev/Download/{dateFrom}/{dateTo}")
def downloadIspItalyDev(dateFrom: str, dateTo: str, force: bool = False) -> Envelope:
    return admie_download.downloadIspItalyDev(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.get("/IspItalyDev/{dateFrom}/{dateTo}")
def getIspItalyDev(dateFrom : str, dateTo : str):
    return admie.getIspItalyDev(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspItalyDev")
def getIspItalyDevFromFile(file: CustomFile) -> Envelope:
    return admie.getIspItalyDevFromFile(file)

@app.get("/IspNorthDev/Download/{dateFrom}/{dateTo}")
def downloadIspNorthDev(dateFrom: str, dateTo: str, force: bool = False) -> Envelope:
    return admie_download.downloadIspNorthDev(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.get("/IspNorthDev/{dateFrom}/{dateTo}")
def getIspNorthDev(dateFrom : str, dateTo : str):
    return admie.getIspNorthDev(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspNorthDev")
def getIspNorthDevFromFile(file: CustomFile) -> Envelope:
    return admie.getIspNorthDevFromFile(file)

##


## ISP Forecasts
@app.get("/IspLoadForecasts/Download/{dateFrom}/{dateTo}")
def downloadIspLoadForecasts(dateFrom: str, dateTo: str, isp: int = None, version: int = None, force: bool = False) -> Envelope:
    return admie_download.downloadIspLoadForecasts(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), isp, version, force)

@app.get("/IspLoadForecasts/{dateFrom}/{dateTo}")
def getIspLoadForecasts(dateFrom : str, dateTo : str, isp: int = None, version: int = None):
    return admie.getIspLoadForecasts(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspLoadForecasts")
def getIspLoadForecastsFromFile(file: CustomFile) -> Envelope:
    return admie.getIspLoadForecastsFromFile(file)

@app.post("/IspLoadForecastsISPRequirements")
def getIspResForecastsFromFile(file: CustomFile) -> Envelope:
    return admie.getIspLoadForecastsFromISPRequirementsFile(file)
    
@app.get("/IspResForecasts/Download/{dateFrom}/{dateTo}")
def downloadIspResForecasts(dateFrom: str, dateTo: str, isp: int = None, version: int = None, force: bool = False) -> Envelope:
    return admie_download.downloadIspResForecasts(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), isp, version, force)

@app.get("/IspResForecasts/{dateFrom}/{dateTo}")
def getIspResForecasts(dateFrom : str, dateTo : str):
    return admie.getIspResForecasts(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspResForecasts")
def getIspResForecastsFromFile(file: CustomFile) -> Envelope:
    return admie.getIspResForecastsFromFile(file)

@app.post("/IspResForecastsISPRequirements")
def getIspResForecastsFromFile(file: CustomFile) -> Envelope:
    return admie.getIspResForecastsFromISPRequirementsFile(file)

@app.get("/IspWeekAheadLoadForecasts/Download/{dateFrom}/{dateTo}")
def downloadIspWeekAheadLoadForecasts(dateFrom: str, dateTo: str, force: bool = False) -> Envelope:
    return admie_download.downloadIspWeekAheadLoadForecasts(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.get("/IspWeekAheadLoadForecasts/{dateFrom}/{dateTo}")
def getIspWeekAheadLoadForecasts(dateFrom : str, dateTo : str):
    return admie.getIspWeekAheadLoadForecasts(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/IspWeekAheadLoadForecasts")
def getIspWeekAheadLoadForecastsFromFile(file: CustomFile) -> Envelope:
    return admie.getIspWeekAheadLoadForecastsFromFile(file)


##

@app.get("/BalancingMarketSettlement/Download/{dateFrom}/{dateTo}")
def downloadIspBeOffersActual(dateFrom: str, dateTo: str, force: bool = False) -> Envelope:
    return admie_download.downloadBalancingEnergySettlements(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.get("/BalancingMarketSettlement/{dateFrom}/{dateTo}")
def getBalancingEnergySettlements(dateFrom : str, dateTo : str):
    return admie.getBalancingEnergySettlements(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/BalancingMarketSettlement")
def getBalancingEnergySettlementsFromFile(file: CustomFile) -> Envelope:
    return admie.getBalancingEnergySettlementsFromFile(file)

## SCADA

@app.get("/Scada/Download/{dateFrom}/{dateTo}")
def downloadScada(dateFrom: str, dateTo: str, force: bool = False) -> Envelope:
    return admie_download.downloadScada(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.get("/ScadaImportsExports/{dateFrom}/{dateTo}")
def getImportExportFromSCADA(dateFrom : str, dateTo : str):
    return admie.getImportExportFromSCADA(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/ScadaAggregatedProduction")
def getAggregatedScada(file: CustomFile):
    return admie.getSCADAAggregatedFromFile(file)
    
@app.post("/ScadaImportsExports")
def getImportExportFromSCADAFromFile(file: CustomFile) -> Envelope:
    return admie.getImportExportFromSCADAFromFile(file)

@app.post("/ScadaEntityProduction")
def getScadaEntityProductionFromFile(file: CustomFile) -> Envelope:
    return admie.getScadaEntityProductionFromFile(file)
    
@app.get("/ScadaEntityProduction/{dateFrom}/{dateTo}")
def getScadaEntityProduction(dateFrom : str, dateTo : str):
    return admie.getScadaEntityProduction(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/ScadaResProduction/{dateFrom}/{dateTo}")
def getScadaResProduction(dateFrom : str, dateTo : str):
    return admie.getScadaResProduction(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/ScadaResProduction")
def getScadaResProductionFromSCADA(file: CustomFile) -> Envelope:
    return admie.getScadaResProductionFromFile(file)

@app.get("/ScadaHydroProduction/{dateFrom}/{dateTo}")
def getScadaHydroProduction(dateFrom : str, dateTo : str):
    return admie.getScadaHydroProduction(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/ScadaHydroProduction")
def getScadaHydroProductionFromFile(file: CustomFile) -> Envelope:
    return admie.getScadaHydroProductionFromFile(file)

@app.get("/ScadaNaturalGasProduction/{dateFrom}/{dateTo}")
def getScadaNaturalGasProduction(dateFrom : str, dateTo : str):
    return admie.getScadaNaturalGasProduction(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/ScadaNaturalGasProduction")
def getScadaNaturalGasProductionFromFile(file: CustomFile) -> Envelope:
    return admie.getScadaNaturalGasProductionFromFile(file)

@app.get("/ScadaThermoProduction/{dateFrom}/{dateTo}")
def getScadaThermoProduction(dateFrom : str, dateTo : str):
    return admie.getScadaThermoProduction(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))
    
@app.post("/ScadaThermoProduction")
def getScadaThermoProductionFromFile(file: CustomFile) -> Envelope:
    return admie.getScadaThermoProductionFromFile(file)

@app.get("/ScadaHvload/{dateFrom}/{dateTo}")
def getScadaHVLoad(dateFrom : str, dateTo : str):
    return admie.getScadaHVLoad(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/ScadaHvload")
def getScadaHvloadFromFile(file: CustomFile) -> Envelope:
    return admie.getScadaHVLoadFromFile(file)

@app.get("/ScadaSystemLoadRealization/{dateFrom}/{dateTo}")
def getScadaSystemLoadRealization(dateFrom : str, dateTo : str):
    return admie.getScadaSystemLoadRealization(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/ScadaSystemLoadRealization")
def getScadaSystemLoadRealizationFromFile(file: CustomFile) -> Envelope:
    return admie.getScadaSystemLoadRealizationFromFile(file)

@app.get("/RealTimeScadaRes/Download/{dateFrom}/{dateTo}")
def downloadRealTimeScadaRes(dateFrom: str, dateTo: str, force: bool = False) -> Envelope:
    return admie_download.downloadRealTimeScadaRes(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.get("/RealTimeScadaRes/{dateFrom}/{dateTo}")
def getRealTimeScadaRes(dateFrom : str, dateTo : str):
    return admie.getRealTimeScadaRes(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))
    
@app.post("/RealTimeScadaRes")
def getRealTimeScadaResFromFile(file: CustomFile) -> Envelope:
    return admie.getRealTimeScadaResFromFile(file)

## RES MV
@app.get("/ResMvAdmie/Download/{dateFrom}/{dateTo}")
def downloadResMvAdmie(dateFrom: str, dateTo: str, force: bool = False) -> Envelope:
    return admie_download.downloadResMvAdmie(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.get("/ResMvAdmie/{dateFrom}/{dateTo}")
def getResMvAdmie(dateFrom : str, dateTo : str):
    return admie.getRESActualMVInjections(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))
    
@app.post("/ResMvAdmie")
def getResMvAdmieFromFile(file: CustomFile) -> Envelope:
    return admie.getRESActualMVInjectionsFromFile(file)

## LT Nominations
@app.get("/LtNominations/Download/{dateFrom}/{dateTo}")
def downloaLtNominations(dateFrom: str, dateTo: str, force: bool = False) -> Envelope:
    return admie_download.downloadLtNominations(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.get("/LtNominations/{dateFrom}/{dateTo}")
def getLtNominations(dateFrom : str, dateTo : str):
    return admie.getLtNominations(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/LtNominations")
def getLtNominationsFromFile(file: CustomFile) -> Envelope:
    return admie.getLtNominationsFromFile(file)
    
## Reservoir Filling Rate
@app.get("/ReservoirFillingRate/Download/{dateFrom}/{dateTo}")
def downloadReservoirFillingRate(dateFrom: str, dateTo: str, force: bool = False) -> Envelope:
    return admie_download.downloadReservoirFillingRate(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.get("/ReservoirFillingRate/{dateFrom}/{dateTo}")
def getReservoirFillingRate(dateFrom : str, dateTo : str):
    return admie.getReservoirFillingRate(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/ReservoirFillingRate")
def getReservoirFillingRateFromFile(file: CustomFile) -> Envelope:
    return admie.getReservoirFillingRateFromFile(file)

## Balancing Market Products

@app.get("/BalancingEnergyProduct/Download/{dateFrom}/{dateTo}")
def downloadBalancingEnergyProduct(dateFrom: str, dateTo: str, force: bool = False) -> Envelope:
    return admie_download.downloadBalancingEnergyProducts(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.post("/BalancingEnergyProduct")
def getBalancingEnergyProductsFromFile(file: CustomFile) -> Envelope:
    return admie.getBalancingEnergyProductsFromFile(file)

@app.get("/BalancingCapacityProduct/Download/{dateFrom}/{dateTo}")
def downloadBalancingReserveProduct(dateFrom: str, dateTo: str, force: bool = False) -> Envelope:
    return admie_download.downloadBalancingCapacityProducts(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)
    
@app.post("/BalancingCapacityProduct")
def getBalancingEnergyProductsFromFile(file: CustomFile) -> Envelope:
    return admie.getBalancingCapacityProductsFromFile(file)

## ATC
@app.get("/AvailableTransferCapacity/Download/{dateFrom}/{dateTo}")
def downloadAvailableTransferCapacity(dateFrom: str, dateTo: str, force: bool = False) -> Envelope:
    return admie_download.downloadAvailableTransferCapacity(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.get("/AvailableTransferCapacity/{dateFrom}/{dateTo}")
def getAvailableTransferCapacity(dateFrom : str, dateTo : str):
    return admie.getAvailableTransferCapacity(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/AvailableTransferCapacity")
def getAvailableTransferCapacityFromFile(file: CustomFile) -> Envelope:
    return admie.getAvailableTransferCapacityFromFile(file)

## ENTSOE
@app.get("/EntsoeParties")
def getEntsoeParties() -> Envelope:
    return entsoe.getEntsoeParties()

@app.get("/EntsoeAreas")
def getEntsoeAreas() -> Envelope:
    return entsoe.getEntsoeAreas()

@app.get("/EntsoeLoadForecast/{dateFrom}/{dateTo}")
def getENTSOEDayAheadPredictedLoad(dateFrom : str, dateTo : str):
    return entsoe.getENTSOEDayAheadPredictedLoad(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/EntsoeLoadForecastPerCountry/{dateFrom}/{dateTo}")
def getENTSOEDayAheadPredictedLoadPerCountry(dateFrom : str, dateTo : str):
    return entsoe.getENTSOEDayAheadPredictedLoadPerCountry(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/EntsoeDamPrice/{dateFrom}/{dateTo}")
def getEntsoeDamPrice(dateFrom : str, dateTo : str, countryShortCode: bool = True):
    return entsoe.getEntsoeDamPrice(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), countryShortCode)

@app.get("/EntsoeActualLoad/{dateFrom}/{dateTo}")
def getENTSOEActualLoad(dateFrom : str, dateTo : str):
    return entsoe.getENTSOEActualLoad(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/EntsoeActualLoadPerCountry/{dateFrom}/{dateTo}")
def getENTSOEActualLoadPerCountry(dateFrom : str, dateTo : str):
    return entsoe.getENTSOEActualLoadPerCountry(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/EntsoeScheduledCommercialExchange/{dateFrom}/{dateTo}")
def getScheduledCommercialExchanges(dateFrom : str, dateTo : str):
    return entsoe.getScheduledCommercialExchanges(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/EntsoeCrossBorderPhysicalFlow/{dateFrom}/{dateTo}")
def getCrossBorderPhysicalFlow(dateFrom : str, dateTo : str):
    return entsoe.getCrossBorderPhysicalFlow(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))
    
@app.get("/EntsoeForecastedCapacity/{dateFrom}/{dateTo}")
def getForecastedCapacity(dateFrom : str, dateTo : str):
    return entsoe.getForecastedCapacity(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/EntsoeAggregatedHydroFillingRate/{dateFrom}/{dateTo}")
def getAggregatedHydroFillingRate(dateFrom : str, dateTo : str):
    return entsoe.getAggregatedHydroFillingRate(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/EntsoeProductionPerCategory/{dateFrom}/{dateTo}")
def getEntsoeProductionPerCategory(dateFrom : str, dateTo : str):
    return entsoe.getProductionPerCategory(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/EntsoeWindAndSolarForecast/{dateFrom}/{dateTo}")
def getWindAndSolarForecast(dateFrom : str, dateTo : str):
    return entsoe.getWindAndSolarForecast(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/EntsoeProductionPerGeneratingUnit/{dateFrom}/{dateTo}")
def getProductionPerGeneratingUnit(dateFrom : str, dateTo : str):
    return entsoe.getProductionPerGeneratingUnit(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/EntsoeDayAheadGenerationForecast/{dateFrom}/{dateTo}")
def getDayAheadGenerationForecast(dateFrom : str, dateTo : str):
    return entsoe.getDayAheadGenerationForecast(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))
    
@app.get("/EntsoeAggregatedBids/{dateFrom}/{dateTo}")
def getEntsoeAggregatedBids(dateFrom : str, dateTo : str):
    return entsoe.getAggregatedBids(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))
    
@app.get("/EntsoeActivatedEnergyPrices/{dateFrom}/{dateTo}")
def getEntsoeActivatedEnergyPrices(dateFrom : str, dateTo : str):
    return entsoe.getActivatedEnergyPrices(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/BalancingEnergyBids/{dateFrom}/{dateTo}")
def getBalancingEnergyBids(dateFrom : str, dateTo : str):
    return entsoe.getBalancingEnergyBids(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/CurrentBalancingState/{dateFrom}/{dateTo}")
def getCurrentBalancingState(dateFrom : str, dateTo : str):
    return entsoe.getCurrentBalancingState(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/AFRRCBMP/{dateFrom}/{dateTo}")
def getAFRRCBMP(dateFrom : str, dateTo : str):
    return entsoe.getAFRRCBMP(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/EntsoeUnavailabilityOfUnits/{dateFrom}/{dateTo}")
def getEntsoeUnavailabilityOfUnits(dateFrom : str, dateTo : str):
    return entsoe.getUnavailabilityOfUnits(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))
    
@app.get("/EntsoeUnavailabilityOfInterconnections/{dateFrom}/{dateTo}")
def getEntsoeUnavailabilityOfUnits(dateFrom : str, dateTo : str):
    return entsoe.getUnavailabilityOfInterconnections(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/EntsoeImplicitAllocationsDayAhead/{dateFrom}/{dateTo}")
def getImplicitAllocationsDayAhead(dateFrom : str, dateTo : str):
    return entsoe.getImplicitAllocationsDayAhead(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/EntsoeDayAheadAggregatedGenerationForecast/{dateFrom}/{dateTo}")
def getDayAheadAggregatedGenerationForecast(dateFrom : str, dateTo : str) ->  Envelope[List[VirtualFileMetadataPayload[List[EntsoeDayAheadAggregatedForecastModel]]]]:
    return entsoe.getDayAheadAggregatedGenerationForecast(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))
    
# Unit Variable Cost
@app.get("/ExchangeRate/{dateFrom}/{dateTo}")
def getExchangeRate(dateFrom : str, dateTo : str, currencyFrom = 'EUR', currencyTo = 'USD') -> Envelope:
    return exchange.getExchangeRate(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), currencyFrom, currencyTo)
    
@app.get("/BrentPrice/{dateFrom}/{dateTo}")
def getBrent(dateFrom : str, dateTo : str):
    return theice.getBRENT(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/TtfPrice")
def getTTF() -> Envelope:
    return theice.getTTFFutures()

@app.get("/Co2Price")
def getCo2Price() -> Envelope:
    return theice.getCO2()
    
@app.get("/BrentPriceIntra")
def getBrentIntra() -> Envelope:
    return theice.getBRENTIntra()

@app.get("/TtfPriceIntra")
def getTTFIntra() -> Envelope:
    return theice.getTTFFuturesIntra()

@app.get("/Co2PriceIntra")
def getCo2PriceIntra() -> Envelope:
    return theice.getCO2Intra()

@app.get("/THEICE/Futures/Power/{dateFrom}/{dateTo}")
def getTheIceFuturesPower(dateFrom : date, dateTo : date) ->  Envelope[List[VirtualFileMetadataPayload[List[Price]]]]:
    return theice.getTheICEFuturesPower(dateFrom, dateTo)

@app.get("/THEICE/Futures/Gas/{dateFrom}/{dateTo}")
def getTHEIceFuturesNatGas(dateFrom : date, dateTo : date) ->  Envelope[List[VirtualFileMetadataPayload[List[Price]]]]:
    return theice.getTHEICEFuturesNatGas(dateFrom, dateTo)

@app.get("/THEICE/Futures/Environmental/{dateFrom}/{dateTo}")
def getTHEIceFuturesEnvironmental(dateFrom : date, dateTo : date) ->  Envelope[List[VirtualFileMetadataPayload[List[Price]]]]:
    return theice.getTHEICEFuturesEnvironmental(dateFrom, dateTo)

@app.get("/THEICE/Futures/Oil/{dateFrom}/{dateTo}")
def getTHEIceFuturesOil(dateFrom : date, dateTo : date) ->  Envelope[List[VirtualFileMetadataPayload[List[Price]]]]:
    return theice.getTHEICEFuturesOil(dateFrom, dateTo)

# Physical Flows
@app.get("/PhysicalFlows/{dateFrom}/{dateTo}")
def getPhysicalFlows(dateFrom : str, dateTo : str):
    return desfa.getPhysicalFlows(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

# Nominations
@app.get("/Nominations/{dateFrom}/{dateTo}")
def getNominations(dateFrom : str, dateTo : str):
    return desfa.getNominations(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.get("/NominationsAllocations/{dateFrom}/{dateTo}")
def getNominations(dateFrom : date, dateTo : date) -> Envelope:
    return desfa.NominationsAllocations(dateFrom, dateTo)

@app.get("/DeliveriesOfftakes/{dateFrom}/{dateTo}")
def DeliveriesOfftakes(dateFrom : date, dateTo : date) -> Envelope:
    return desfa.DeliveriesOfftakes(dateFrom, dateTo)

# GasToVolumeRate
@app.get("/GasVolumeRate/{dateFrom}/{dateTo}")
def getGasToVolumeRates(dateFrom: str, dateTo: str) -> Envelope:
    raise NotImplementedError

# HENEX
@app.get("/MarketResults/Download/{dateFrom}/{dateTo}")
def getMarketResults(dateFrom : str, dateTo : str, market = None) -> Envelope:
    return enexgroup_download.downloadResults(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), market)

@app.post("/MarketResults")
def getMarketResultsFromFile(file: CustomFile) -> Envelope:
    return enexgroup.getResultsFromFile(file)

@app.get("/MarketResultsSummary/Download/{dateFrom}/{dateTo}")
def getMarketResults(dateFrom : str, dateTo : str):
    return enexgroup_download.downloadResultsSummary(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/MarketResultsSummary")
def getMarketResultsFromFile(file: CustomFile) -> Envelope:
    return enexgroup.getResultsSummaryFromFile(file)

@app.get("/MarketData/Download/{dateFrom}/{dateTo}")
def getMarketResults(dateFrom : str, dateTo : str):
    return enexgroup_download.downloadData(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

@app.post("/MarketData")
def getMarketData(file: CustomFile) -> Envelope:
    return enexgroup.getDAMDataFromFile(file)

@app.get("/MarketAggregatedCurves/Download/{dateFrom}/{dateTo}")
def getMarketAggregatedCurves(dateFrom : str, dateTo : str, market: str = None, force: bool = False) -> Envelope:
    return enexgroup_download.downloadAggregatedCurves(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), market, force)

@app.post("/MarketAggregatedCurves")
def getMarketAggregatedCurvesFromFile(file: CustomFile) -> Envelope:
    return enexgroup.getMarketAggregatedCurvesFromFile(file)

@app.get("/MarketBlockOrderAcceptanceStatus/Download/{dateFrom}/{dateTo}")
def getMarketBlockOrderAcceptanceStatus(dateFrom : str, dateTo : str, force: bool = False) -> Envelope:
    return enexgroup_download.downloadMarketBlockOrderAcceptanceStatus(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), force)

@app.post("/MarketBlockOrderAcceptanceStatus")
def getMarketBlockOrderAcceptanceStatusFromFile(file: CustomFile) -> Envelope:
    return enexgroup.getMarketBlockOrderAcceptanceStatusFromFile(file)

# DAPEEP
@app.get("/DAMHybdridOffers/Download/{dateFrom}/{dateTo}")
def getDAMHybdridOffers(dateFrom: str, dateTo: str, market: int = None, force: bool = False) -> Envelope:
    return dapeep_download.downloadDAMHybridOffers(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), market, force)

@app.post("/DAMHybdridOffers")
def getMDAMHybdridOffers(file: CustomFile) -> Envelope:
    return dapeep.getDAMHybridOffersFromFile(file)

# Meteologica
@app.post("/MeteologicaHydroForecast")
def getMeteologicaHydroForecast(file: CustomFile) -> Envelope:
    return meteologica.getHydroForecast(file)

@app.get("/MeteologicaHydroForecast/Download/{dateFrom}/{dateTo}")
def getMeteologicaHydroForecast(dateFrom: date, dateTo: date) -> Envelope:
    return meteologica_download.getHydroForecastFiles(dateFrom, dateTo)
    
@app.post("/MeteologicaPowerDemand")
def getMeteologicaPowerDemand(file: CustomFile) -> Envelope:
    return meteologica.getPowerDemand(file)

@app.get("/MeteologicaPowerDemand/Download/{dateFrom}/{dateTo}")
def getMeteologicaPowerDemand(dateFrom: date, dateTo: date) -> Envelope:
    return meteologica_download.getPowerDemandFiles(dateFrom, dateTo)
    
@app.post("/MeteologicaPvProduction")
def getMeteologicaPvProduction(file: CustomFile) -> Envelope:
    return meteologica.getPVProduction(file)

@app.get("/MeteologicaPvProduction/Download/{dateFrom}/{dateTo}")
def getMeteologicaPvProduction(dateFrom: date, dateTo: date) -> Envelope:
    return meteologica_download.getPVProductionFiles(dateFrom, dateTo)
    
@app.post("/MeteologicaWindProduction")
def getMeteologicaWindProduction(file: CustomFile) -> Envelope:
    return meteologica.getWindProduction(file)

@app.get("/MeteologicaWindProduction/Download/{dateFrom}/{dateTo}")
def getMeteologicaWindProduction(dateFrom: date, dateTo: date) -> Envelope:
    return meteologica_download.getWindProductionFiles(dateFrom, dateTo)

# ECMWF
@app.get("/ECMWF/Download/{dateFrom}/{dateTo}")
def getECMWF(dateFrom: str, dateTo: str, version:int = None, background_tasks: BackgroundTasks = None) -> Envelope:
    return ecmwf.scheduleBackgroundJob(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), version, background_tasks)

# JAO
@app.get("/JAO/Auctions/{dateFrom}/{dateTo}")
def getJAOAuctions(dateFrom: str, dateTo: str, horizon:str = None, out_area: str = None, in_area: str = None, type: str = None) -> Envelope:
    return jao.getAuctions(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), horizon)

# SEECAO
@app.get("/SEECAO/{dateFrom}/{dateTo}")
def getSEECAO(dateFrom: str, dateTo: str) -> Envelope:
    return seecao.getAuctions(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo))

# EEX
@app.get("/EEX/PowerFutures/{date}")
def getEEXPowerFutures(date: str) -> Envelope:
    return eex.getPowerFutures(parseDateTimeFromArgs(date))

#TSOC
@app.get("/TSOC/{dateFrom}/{dateTo}")
def getTsoc(dateFrom: date, dateTo: date) -> Envelope:
    return tsoc.getTsoc(dateFrom, dateTo)

@app.get("/TSOCForecast/{dateFrom}/{dateTo}")
def getTsocForecast(dateFrom: date, dateTo: date) -> Envelope:
    return tsoc_forecast.getTsocForecast(dateFrom, dateTo)


@app.get("/TSOCISPForecast/Download/{dateFrom}/{dateTo}")
def getTsocISPForecast(dateFrom: date, dateTo: date) -> Envelope:
    return tsoc_forecast.getTsocForecast(dateFrom, dateTo, "ISP")

@app.get("/TSOCDAMForecast/Download/{dateFrom}/{dateTo}")
def getTsocDAMForecast(dateFrom: date, dateTo: date) -> Envelope:
    return tsoc_forecast.getTsocForecast(dateFrom, dateTo, "DAM")

#SEECAOAPI
@app.get("/SEECAOapi/{dateFrom}/{dateTo}")
def getSeecaoApi(dateFrom: str, dateTo: str, horizon : str = None) -> Envelope:
    return seecaoapi.getSeecaoapi(parseDateTimeFromArgs(dateFrom), parseDateTimeFromArgs(dateTo), horizon)




