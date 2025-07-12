# --- IMPORTS ---
import concurrent.futures
import logging
import multiprocessing
import random
import re
import threading
import time
from datetime import datetime
from functools import partial

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
import requests

# --- CORE CONFIGURATION ---
SECTOR_TO_PROCESS = "Utilities"
SCHEMA_NAME = "fs"
# This User-Agent is specifically for the SEC EDGAR API as required by their policy.
EDGAR_USER_AGENT = "Alex McMahon wmcmahon234@gmail.com"
FRED_API_KEY = "8c139d78ce5feb22bb1ba38e5b9ff196"  # Your FRED API Key

# --- DATABASE AND CONCURRENCY CONFIGURATION ---
DB_PARAMS = {
    "host": "localhost",
    "database": "utilities_cap_intel",
    "user": "postgres",
    "password": "Mondo@Mil!Pie19",  # Replace with your actual password
    "port": "5433"
}
MAX_WORKERS = 15  # IMPORTANT: Reduced to a safer number for multiple rate-limited APIs
# Specific headers for EDGAR, using the required User-Agent format.
EDGAR_HEADERS = {"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate", "Host": "data.sec.gov"}

# --- RATE LIMITING CONFIGURATION (REVISED) ---
# The SEC's limit is 10 requests/sec. We'll set ours to 9 to be safe.
EDGAR_API_CONFIG = {
    "REQUESTS_PER_UNIT": 9,
    "UNIT_IN_SECONDS": 1
}
STOCKANALYSIS_API_CONFIG = {
    "REQUESTS_PER_UNIT": 30,
    "UNIT_IN_SECONDS": 60  # A safe, conservative limit for stockanalysis.com
}

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')

# <<< FIX #1: ADD THIS DICTIONARY >>>
STATEMENT_ORDER_MAP = {
    "Balance Sheet": 1, "Income Statement": 2, "Comprehensive Income": 3,
    "Cash Flow Statement": 4, "Equity Changes": 5, "Supplemental Disclosures": 6,
    "Supplemental Cash Flow": 7
}

# <<< ADD THIS SNIPPET >>>
# --- MASTER HEADER LISTS FOR SORTING ---
# Generate master lists to enforce a perfect chronological sort, ignoring messy period_dates.

# --- DATA MAPPINGS ---
FRED_SERIES_ID_MAP = {
    "EUR": {"id": "DEXUSEU", "invert": False},
    "JPY": {"id": "DEXJPUS", "invert": True},
    "GBP": {"id": "DEXUSUK", "invert": False},
    "CAD": {"id": "DEXCAUS", "invert": True},
    "CHF": {"id": "DEXSZUS", "invert": True},
    "AUD": {"id": "DEXUSAL", "invert": False},
    "CNY": {"id": "DEXCHUS", "invert": True},
    "HKD": {"id": "DEXHKUS", "invert": True},
    "INR": {"id": "DEXINUS", "invert": True},
    "KRW": {"id": "DEXKOUS", "invert": True},
    "SGD": {"id": "DEXSIUS", "invert": True},
    "BRL": {"id": "DEXBZUS", "invert": True},
    "MXN": {"id": "DEXMXUS", "invert": True},
    "ZAR": {"id": "DEXSLUS", "invert": True},
    "RUB": {"id": "CCUSMA02RUM618N", "invert": True},
    "TWD": {"id": "DEXTAUS", "invert": True},
    "IDR": {"id": "CCUSMA02IDM618N", "invert": True},
    "PHP": {"id": "RBPHBIS", "invert": True},
    "ARS": {"id": "ARGCCUSMA02STM", "invert": True},
    "SEK": {"id": "EXSDUS", "invert": True},
    "NOK": {"id": "DEXNOUS", "invert": True},
    "DKK": {"id": "DEXDNUS", "invert": True},  # Danish Krone
    "PLN": {"id": "CCUSMA02PLM618N", "invert": True},
    "THB": {"id": "DEXTHUS", "invert": True},
    "HUF": {"id": "CCUSMA02HUM618N", "invert": True},
    "CZK": {"id": "CCUSMA02CZM618N", "invert": True},
    "COP": {"id": "COLCCUSMA02STM", "invert": True},
    "ILS": {"id": "CCUSMA02ILM618N", "invert": True},
    "CLP": {"id": "CCUSMA02CLM618N", "invert": True},
    "TRY": {"id": "CCUSMA02TRQ618N", "invert": True},
}

# --- XBRL Concept Mappings (IFRS & US-GAAP) ---
# NOTE: Mappings are extensive and are kept as they were.
IFRS_CONCEPT_MAPPING = {
    # --- Balance Sheet (Assets) ---
    "CashAndCashEquivalents": {"statement_type": "Balance Sheet", "item": "Cash and cash equivalents", "sort_order_item": 9},
    "BalancesWithBanks": {"statement_type": "Balance Sheet", "item": "Cash and cash equivalents", "sort_order_item": 9},  # Alias
    "RestrictedCashAndCashEquivalents": {"statement_type": "Balance Sheet", "item": "Restricted cash and cash equivalents", "sort_order_item": 12},
    "CurrentRestrictedCashAndCashEquivalents": {"statement_type": "Balance Sheet", "item": "Restricted cash and cash equivalents", "sort_order_item": 12},  # Alias
    "MarketableSecurities": {"statement_type": "Balance Sheet", "item": "Marketable securities", "sort_order_item": 20},
    "CurrentInvestments": {"statement_type": "Balance Sheet", "item": "Current Investments", "sort_order_item": 21},  # Alias
    "FinancialAssetsAtFairValueThroughOtherComprehensiveIncome": {"statement_type": "Balance Sheet", "item": "Financial assets at FVOCI", "sort_order_item": 22},
    "FinancialAssetsAvailableforsale": {"statement_type": "Balance Sheet", "item": "Available-for-sale financial assets", "sort_order_item": 24},
    "TradeAndOtherReceivables": {"statement_type": "Balance Sheet", "item": "Accounts receivable, net", "sort_order_item": 30},
    "TradeAndOtherCurrentReceivables": {"statement_type": "Balance Sheet", "item": "Accounts receivable, net", "sort_order_item": 30},  # Alias
    "TradeReceivables": {"statement_type": "Balance Sheet", "item": "Trade receivables, net", "sort_order_item": 31},
    "CurrentTradeReceivables": {"statement_type": "Balance Sheet", "item": "Trade receivables, net", "sort_order_item": 31},  # Alias
    "OtherReceivables": {"statement_type": "Balance Sheet", "item": "Other receivables", "sort_order_item": 32},
    "OtherCurrentReceivables": {"statement_type": "Balance Sheet", "item": "Other receivables", "sort_order_item": 32},  # Alias
    "ContractAssetsCurrent": {"statement_type": "Balance Sheet", "item": "Contract assets, current", "sort_order_item": 33},
    "CurrentFinanceLeaseReceivables": {"statement_type": "Balance Sheet", "item": "Finance lease receivables, current", "sort_order_item": 34},
    "Inventories": {"statement_type": "Balance Sheet", "item": "Inventories, net", "sort_order_item": 35},
    "FinishedGoods": {"statement_type": "Balance Sheet", "item": "Inventory, Finished Goods", "sort_order_item": 36},
    "WorkInProgress": {"statement_type": "Balance Sheet", "item": "Inventory, Work in Process", "sort_order_item": 37},
    "CurrentRawMaterialsAndCurrentProductionSupplies": {"statement_type": "Balance Sheet", "item": "Inventory, Raw Materials", "sort_order_item": 38},
    "OtherCurrentFinancialAssets": {"statement_type": "Balance Sheet", "item": "Other current financial assets", "sort_order_item": 38},
    "CurrentDerivativeFinancialAssets": {"statement_type": "Balance Sheet", "item": "Derivative financial assets, current", "sort_order_item": 39},
    "OtherCurrentAssets": {"statement_type": "Balance Sheet", "item": "Prepaid expenses and other current assets", "sort_order_item": 40},
    "CurrentPrepaymentsAndOtherCurrentAssets": {"statement_type": "Balance Sheet", "item": "Prepaid expenses and other current assets", "sort_order_item": 40},  # Alias
    "CurrentTaxAssetsCurrent": {"statement_type": "Balance Sheet", "item": "Current tax assets", "sort_order_item": 42},
    "CurrentAssets": {"statement_type": "Balance Sheet", "item": "Total current assets", "sort_order_item": 49},
    "InvestmentAccountedForUsingEquityMethod": {"statement_type": "Balance Sheet", "item": "Long-Term Investments", "sort_order_item": 61},
    "InvestmentsInAssociatesAndJointVentures": {"statement_type": "Balance Sheet", "item": "Long-Term Investments", "sort_order_item": 61},  # Alias
    "NoncurrentFinanceLeaseReceivables": {"statement_type": "Balance Sheet", "item": "Finance lease receivables, non-current", "sort_order_item": 63},
    "OtherNoncurrentFinancialAssets": {"statement_type": "Balance Sheet", "item": "Other non-current financial assets", "sort_order_item": 65},
    "PropertyPlantAndEquipment": {"statement_type": "Balance Sheet", "item": "Property and equipment, net", "sort_order_item": 70},
    "LandAndBuildings": {"statement_type": "Balance Sheet", "item": "Buildings", "sort_order_item": 71},
    "MachineryAndEquipment": {"statement_type": "Balance Sheet", "item": "Machinery", "sort_order_item": 72},
    "ConstructionInProgress": {"statement_type": "Balance Sheet", "item": "Construction In Progress", "sort_order_item": 73},
    "LeaseholdImprovements": {"statement_type": "Balance Sheet", "item": "Leasehold Improvements", "sort_order_item": 74},
    "RightOfUseAssets": {"statement_type": "Balance Sheet", "item": "Operating lease right-of-use assets", "sort_order_item": 80},
    "Goodwill": {"statement_type": "Balance Sheet", "item": "Goodwill", "sort_order_item": 90},
    "IntangibleAssetsOtherThanGoodwill": {"statement_type": "Balance Sheet", "item": "Intangible assets, net", "sort_order_item": 95},
    "CopyrightsPatentsAndOtherIndustrialPropertyRightsServiceAndOperatingRights": {"statement_type": "Balance Sheet", "item": "Intangible assets, net", "sort_order_item": 95},  # Alias
    "IntangibleAssetsAndGoodwill": {"statement_type": "Balance Sheet", "item": "Goodwill and intangible assets", "sort_order_item": 97},
    "DeferredTaxAssets": {"statement_type": "Balance Sheet", "item": "Deferred tax assets", "sort_order_item": 98},
    "NetDeferredTaxAssets": {"statement_type": "Balance Sheet", "item": "Deferred tax assets, net", "sort_order_item": 98},  # Alias
    "OtherNoncurrentAssets": {"statement_type": "Balance Sheet", "item": "Other non-current assets", "sort_order_item": 100},
    "OtherNoncurrentReceivables": {"statement_type": "Balance Sheet", "item": "Other non-current receivables", "sort_order_item": 101},
    "NoncurrentAssets": {"statement_type": "Balance Sheet", "item": "Total non-current assets", "sort_order_item": 105},
    "Assets": {"statement_type": "Balance Sheet", "item": "Total assets", "sort_order_item": 110},
    "FinancialAssets": {"statement_type": "Balance Sheet", "item": "Total financial assets", "sort_order_item": 111},

    # --- Balance Sheet (Liabilities & Equity) ---
    "TradeAndOtherPayables": {"statement_type": "Balance Sheet", "item": "Accounts payable", "sort_order_item": 120},
    "TradeAndOtherCurrentPayables": {"statement_type": "Balance Sheet", "item": "Accounts payable, current", "sort_order_item": 120},  # Alias
    "CurrentLeaseLiabilities": {"statement_type": "Balance Sheet", "item": "Operating lease liabilities, current", "sort_order_item": 130},
    "ContractLiabilities": {"statement_type": "Balance Sheet", "item": "Contract liabilities", "sort_order_item": 135},
    "ContractLiabilitiesCurrent": {"statement_type": "Balance Sheet", "item": "Contract liabilities, current", "sort_order_item": 135},  # Alias
    "CurrentProvisions": {"statement_type": "Balance Sheet", "item": "Provisions, current", "sort_order_item": 136},
    "OtherCurrentFinancialLiabilities": {"statement_type": "Balance Sheet", "item": "Other current financial liabilities", "sort_order_item": 138},
    "CurrentDerivativeFinancialLiabilities": {"statement_type": "Balance Sheet", "item": "Derivative financial liabilities, current", "sort_order_item": 139},
    "OtherCurrentLiabilities": {"statement_type": "Balance Sheet", "item": "Accrued expenses and other current liabilities", "sort_order_item": 140},
    "CurrentTaxLiabilitiesCurrent": {"statement_type": "Balance Sheet", "item": "Income tax payable", "sort_order_item": 143},
    "ShorttermBorrowings": {"statement_type": "Balance Sheet", "item": "Short-term borrowings", "sort_order_item": 144},
    "CurrentPortionOfLongtermBorrowings": {"statement_type": "Balance Sheet", "item": "Long-term debt, current", "sort_order_item": 145},
    "CurrentLiabilities": {"statement_type": "Balance Sheet", "item": "Total current liabilities", "sort_order_item": 150},
    "NoncurrentLeaseLiabilities": {"statement_type": "Balance Sheet", "item": "Operating lease liabilities, non-current", "sort_order_item": 160},
    "NoncurrentContractLiabilities": {"statement_type": "Balance Sheet", "item": "Contract liabilities, non-current", "sort_order_item": 162},
    "LeaseLiabilities": {"statement_type": "Balance Sheet", "item": "Total lease liabilities", "sort_order_item": 165},
    "Borrowings": {"statement_type": "Balance Sheet", "item": "Total borrowings", "sort_order_item": 171},
    "LongTermBorrowings": {"statement_type": "Balance Sheet", "item": "Total borrowings", "sort_order_item": 171},  # Alias
    "DeferredIncome": {"statement_type": "Balance Sheet", "item": "Deferred income", "sort_order_item": 184},
    "DeferredTaxLiabilities": {"statement_type": "Balance Sheet", "item": "Deferred tax liabilities", "sort_order_item": 185},
    "NetDeferredTaxLiabilities": {"statement_type": "Balance Sheet", "item": "Deferred tax liabilities, net", "sort_order_item": 185},  # Alias
    "NoncurrentProvisions": {"statement_type": "Balance Sheet", "item": "Provisions, non-current", "sort_order_item": 188},
    "OtherNoncurrentLiabilities": {"statement_type": "Balance Sheet", "item": "Other non-current liabilities", "sort_order_item": 190},
    "OtherNoncurrentPayables": {"statement_type": "Balance Sheet", "item": "Other non-current payables", "sort_order_item": 191},
    "NoncurrentLiabilities": {"statement_type": "Balance Sheet", "item": "Total non-current liabilities", "sort_order_item": 195},
    "Liabilities": {"statement_type": "Balance Sheet", "item": "Total liabilities", "sort_order_item": 200},
    "IssuedCapital": {"statement_type": "Balance Sheet", "item": "Common stock", "sort_order_item": 220},
    "SharePremium": {"statement_type": "Balance Sheet", "item": "Additional paid-in capital", "sort_order_item": 230},
    "TreasuryShares": {"statement_type": "Balance Sheet", "item": "Treasury stock", "sort_order_item": 235},
    "ReserveOfSharebasedPayments": {"statement_type": "Balance Sheet", "item": "Share-based payments reserve", "sort_order_item": 238},
    "AccumulatedOtherComprehensiveIncome": {"statement_type": "Balance Sheet", "item": "Accumulated other comprehensive income", "sort_order_item": 245},
    "RetainedEarnings": {"statement_type": "Balance Sheet", "item": "Retained earnings (deficit)", "sort_order_item": 250},
    "OtherReserves": {"statement_type": "Balance Sheet", "item": "Other reserves", "sort_order_item": 252},
    "EquityAttributableToOwnersOfParent": {"statement_type": "Balance Sheet", "item": "Total parent stockholders' equity", "sort_order_item": 258},
    "NoncontrollingInterests": {"statement_type": "Balance Sheet", "item": "Noncontrolling interests", "sort_order_item": 259},
    "Equity": {"statement_type": "Balance Sheet", "item": "Total stockholders' equity", "sort_order_item": 260},
    "EquityAndLiabilities": {"statement_type": "Balance Sheet", "item": "Total liabilities and stockholders' equity", "sort_order_item": 270},
    "LiabilitiesAndEquity": {"statement_type": "Balance Sheet", "item": "Total liabilities and stockholders' equity", "sort_order_item": 270},  # Alias

    # --- Income Statement ---
    "Revenue": {"statement_type": "Income Statement", "item": "Revenue", "sort_order_item": 10},
    "RevenueFromSaleOfGoods": {"statement_type": "Income Statement", "item": "Revenue from sale of goods", "sort_order_item": 11},
    "RevenueFromRenderingOfAdvertisingServices": {"statement_type": "Income Statement", "item": "Revenue from advertising services", "sort_order_item": 11},
    "RevenueFromRenderingOfTelecommunicationServices": {"statement_type": "Income Statement", "item": "Revenue from telecommunication services", "sort_order_item": 12},
    "CostOfSales": {"statement_type": "Income Statement", "item": "Cost of revenue", "sort_order_item": 20},
    "CostOfGoodsSold": {"statement_type": "Income Statement", "item": "Cost of goods sold", "sort_order_item": 21},
    "GrossProfit": {"statement_type": "Income Statement", "item": "Gross Profit", "sort_order_item": 29},
    "OtherIncome": {"statement_type": "Income Statement", "item": "Other operating income", "sort_order_item": 35},
    "GovernmentGrants": {"statement_type": "Income Statement", "item": "Government grants income", "sort_order_item": 36},
    "ResearchAndDevelopmentExpense": {"statement_type": "Income Statement", "item": "Research and development", "sort_order_item": 40},
    "EmployeeBenefitsExpense": {"statement_type": "Income Statement", "item": "Employee benefits expense", "sort_order_item": 42},
    "WagesAndSalaries": {"statement_type": "Income Statement", "item": "Wages and salaries", "sort_order_item": 43},
    "SocialSecurityContributions": {"statement_type": "Income Statement", "item": "Social security contributions", "sort_order_item": 44},
    "DepreciationAndAmortisationExpense": {"statement_type": "Income Statement", "item": "Depreciation and amortization expense", "sort_order_item": 47},
    "DepreciationPropertyPlantAndEquipment": {"statement_type": "Income Statement", "item": "Depreciation of property, plant and equipment", "sort_order_item": 48},
    "DepreciationRightofuseAssets": {"statement_type": "Income Statement", "item": "Depreciation of right-of-use assets", "sort_order_item": 49},
    "SalesAndMarketingExpense": {"statement_type": "Income Statement", "item": "Sales and marketing expense", "sort_order_item": 50},
    "AdvertisingExpense": {"statement_type": "Income Statement", "item": "Advertising expense", "sort_order_item": 51},
    "SellingGeneralAndAdministrativeExpense": {"statement_type": "Income Statement", "item": "Selling, general and administrative expense", "sort_order_item": 55},
    "DistributionAndAdministrativeExpense": {"statement_type": "Income Statement", "item": "Distribution and administrative expense", "sort_order_item": 56},
    "AdministrativeExpense": {"statement_type": "Income Statement", "item": "Administrative expense", "sort_order_item": 58},
    "AuditorsRemuneration": {"statement_type": "Income Statement", "item": "Auditor's remuneration", "sort_order_item": 59},
    "RentalExpense": {"statement_type": "Income Statement", "item": "Rental expense", "sort_order_item": 61},
    "OtherGainsLosses": {"statement_type": "Income Statement", "item": "Other gains (losses), net", "sort_order_item": 67},
    "OtherOperatingExpense": {"statement_type": "Income Statement", "item": "Other operating expense", "sort_order_item": 68},
    "OperatingExpense": {"statement_type": "Income Statement", "item": "Total operating expenses", "sort_order_item": 70},  # Alias
    "OperatingIncomeLoss": {"statement_type": "Income Statement", "item": "Income from operations", "sort_order_item": 80},
    "ProfitLossFromOperatingActivities": {"statement_type": "Income Statement", "item": "Income from operations", "sort_order_item": 80},  # Alias
    "FinanceIncome": {"statement_type": "Income Statement", "item": "Finance income", "sort_order_item": 82},
    "FinanceCosts": {"statement_type": "Income Statement", "item": "Finance costs", "sort_order_item": 84},
    # <<< ADD THESE NEW IFRS ENTRIES >>>
    "GainLossOnInvestments": {"statement_type": "Income Statement", "item": "Gain (Loss) on Investments", "sort_order_item": 85},
    "OtherGainsLossesOnFinancialInstrumentsAtFairValueThroughProfitOrLoss": {"statement_type": "Income Statement", "item": "Gain (Loss) on Investments", "sort_order_item": 85},
    "GainsLossesOnDisposalOfFinancialInvestments": {"statement_type": "Income Statement", "item": "Gain (Loss) on Investments", "sort_order_item": 85},
    "FairValueGainLossOnFinancialInstruments": {"statement_type": "Income Statement", "item": "Gain (Loss) on Investments", "sort_order_item": 85},
    "InterestExpenseOnLeaseLiabilities": {"statement_type": "Income Statement", "item": "Interest expense on lease liabilities", "sort_order_item": 86},
    "FinanceIncomeCost": {"statement_type": "Income Statement", "item": "Finance income (cost), net", "sort_order_item": 90},
    "ShareOfProfitLossOfAssociatesAndJointVenturesAccountedForUsingEquityMethod": {"statement_type": "Income Statement", "item": "Share of profit/loss of associates & JVs", "sort_order_item": 95},
    "ProfitLossBeforeTax": {"statement_type": "Income Statement", "item": "Income before tax", "sort_order_item": 100},
    "IncomeTaxExpenseBenefit": {"statement_type": "Income Statement", "item": "Income tax expense", "sort_order_item": 110},
    "CurrentTaxExpenseIncome": {"statement_type": "Income Statement", "item": "Current tax expense", "sort_order_item": 111},
    "DeferredTaxExpenseIncome": {"statement_type": "Income Statement", "item": "Deferred tax expense", "sort_order_item": 112},
    "ProfitLoss": {"statement_type": "Income Statement", "item": "Net income (loss)", "sort_order_item": 120},
    "ProfitLossAttributableToOwnersOfParent": {"statement_type": "Income Statement", "item": "Net income attributable to parent", "sort_order_item": 122},
    "ProfitLossAttributableToNoncontrollingInterests": {"statement_type": "Income Statement", "item": "Net income attributable to noncontrolling interests", "sort_order_item": 124},
    "EarningsPerShareBasic": {"statement_type": "Income Statement", "item": "Earnings per share: Basic", "sort_order_item": 130},
    "BasicEarningsLossPerShare": {"statement_type": "Income Statement", "item": "Earnings per share: Basic", "sort_order_item": 130},  # Alias
    "EarningsPerShareDiluted": {"statement_type": "Income Statement", "item": "Earnings per share: Diluted", "sort_order_item": 140},
    "DilutedEarningsLossPerShare": {"statement_type": "Income Statement", "item": "Earnings per share: Diluted", "sort_order_item": 140},  # Alias
    "WeightedAverageNumberOfSharesOutstandingBasic": {"statement_type": "Income Statement", "item": "Weighted-average shares: Basic", "sort_order_item": 150},
    "WeightedAverageShares": {"statement_type": "Income Statement", "item": "Weighted-average shares: Basic", "sort_order_item": 150},  # Alias
    "WeightedAverageNumberOfDilutedSharesOutstanding": {"statement_type": "Income Statement", "item": "Weighted-average shares: Diluted", "sort_order_item": 160},
    "AdjustedWeightedAverageShares": {"statement_type": "Income Statement", "item": "Weighted-average shares: Diluted", "sort_order_item": 160},  # Alias
    "LitigationSettlementExpense": {"statement_type": "Income Statement", "item": "Restructuring charges", "sort_order_item": 72},

    # --- Comprehensive Income ---
    "ComprehensiveIncome": {"statement_type": "Comprehensive Income", "item": "Total comprehensive income", "sort_order_item": 10},
    "OtherComprehensiveIncomeNetOfTaxExchangeDifferencesOnTranslation": {"statement_type": "Comprehensive Income", "item": "Exchange differences on translation, net of tax", "sort_order_item": 20},
    "OtherComprehensiveIncomeNetOfTaxFinancialAssetsMeasuredAtFairValueThroughOtherComprehensiveIncome": {"statement_type": "Comprehensive Income", "item": "Gains/losses on financial assets at FVOCI, net of tax", "sort_order_item": 30},
    "OtherComprehensiveIncome": {"statement_type": "Comprehensive Income", "item": "Other comprehensive income (loss)", "sort_order_item": 40},
    "ComprehensiveIncomeAttributableToOwnersOfParent": {"statement_type": "Comprehensive Income", "item": "Comprehensive income attributable to parent", "sort_order_item": 50},
    "ComprehensiveIncomeAttributableToNoncontrollingInterests": {"statement_type": "Comprehensive Income", "item": "Comprehensive income attributable to non-controlling interests", "sort_order_item": 60},

    # --- Cash Flow Statement ---
    "CashFlowsFromUsedInOperatingActivities": {"statement_type": "Cash Flow Statement", "item": "Net cash from operating activities", "sort_order_item": 10},
    "CashFlowsFromUsedInOperations": {"statement_type": "Cash Flow Statement", "item": "Net cash from operating activities", "sort_order_item": 10},  # Alias
    "AdjustmentsForDepreciationAndAmortisationExpense": {"statement_type": "Cash Flow Statement", "item": "Depreciation and amortization", "sort_order_item": 30},
    "AdjustmentsForSharebasedPayments": {"statement_type": "Cash Flow Statement", "item": "Share-based compensation (CF)", "sort_order_item": 40},
    "AdjustmentsForInterestIncome": {"statement_type": "Cash Flow Statement", "item": "Adjustment for interest income", "sort_order_item": 45},
    "AdjustmentsForInterestExpense": {"statement_type": "Cash Flow Statement", "item": "Adjustment for interest expense", "sort_order_item": 46},
    "AdjustmentsForUnrealisedForeignExchangeLossesGains": {"statement_type": "Cash Flow Statement", "item": "Adjustment for unrealized FX gain/loss", "sort_order_item": 62},
    "AdjustmentsForGainLossOnDisposalOfInvestmentsInSubsidiariesJointVenturesAndAssociates": {"statement_type": "Cash Flow Statement", "item": "Adjustment for gain/loss on disposal of investments", "sort_order_item": 64},
    "AdjustmentsForDecreaseIncreaseInTradeAccountReceivable": {"statement_type": "Cash Flow Statement", "item": "Changes in trade receivables", "sort_order_item": 81},
    "AdjustmentsForDecreaseIncreaseInInventories": {"statement_type": "Cash Flow Statement", "item": "Changes in inventories", "sort_order_item": 85},
    "AdjustmentsForDecreaseIncreaseInOtherAssets": {"statement_type": "Cash Flow Statement", "item": "Changes in other assets", "sort_order_item": 91},
    "AdjustmentsForIncreaseDecreaseInTradeAccountPayable": {"statement_type": "Cash Flow Statement", "item": "Changes in trade payables", "sort_order_item": 111},
    "AdjustmentsForProvisions": {"statement_type": "Cash Flow Statement", "item": "Changes in provisions", "sort_order_item": 115},
    "IncomeTaxesPaidRefundClassifiedAsOperatingActivities": {"statement_type": "Cash Flow Statement", "item": "Income taxes paid (Operating)", "sort_order_item": 135},
    "CashFlowsFromUsedInInvestingActivities": {"statement_type": "Cash Flow Statement", "item": "Net cash from investing activities", "sort_order_item": 140},
    "PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities": {"statement_type": "Cash Flow Statement", "item": "Purchases of property and equipment", "sort_order_item": 150},
    "PurchaseOfFinancialInstrumentsClassifiedAsInvestingActivities": {"statement_type": "Cash Flow Statement", "item": "Purchases of financial instruments", "sort_order_item": 160},
    "ProceedsFromSalesOrMaturityOfFinancialInstrumentsClassifiedAsInvestingActivities": {"statement_type": "Cash Flow Statement", "item": "Proceeds from sale of financial instruments", "sort_order_item": 170},
    "PurchaseOfIntangibleAssetsClassifiedAsInvestingActivities": {"statement_type": "Cash Flow Statement", "item": "Purchases of intangible assets", "sort_order_item": 180},
    "CashFlowsUsedInObtainingControlOfSubsidiariesOrOtherBusinessesClassifiedAsInvestingActivities": {"statement_type": "Cash Flow Statement", "item": "Cash paid for acquisitions", "sort_order_item": 190},
    "CashFlowsFromLosingControlOfSubsidiariesOrOtherBusinessesClassifiedAsInvestingActivities": {"statement_type": "Cash Flow Statement", "item": "Cash from disposal of subsidiaries", "sort_order_item": 191},
    "DividendsReceivedClassifiedAsInvestingActivities": {"statement_type": "Cash Flow Statement", "item": "Dividends received (Investing)", "sort_order_item": 195},
    "OtherInflowsOutflowsOfCashClassifiedAsInvestingActivities": {"statement_type": "Cash Flow Statement", "item": "Other investing activities", "sort_order_item": 198},
    "CashFlowsFromUsedInFinancingActivities": {"statement_type": "Cash Flow Statement", "item": "Net cash from financing activities", "sort_order_item": 200},
    "ProceedsFromIssuingShares": {"statement_type": "Cash Flow Statement", "item": "Proceeds from issuance of stock", "sort_order_item": 215},
    "ProceedsFromExerciseOfOptions": {"statement_type": "Cash Flow Statement", "item": "Proceeds from exercise of options", "sort_order_item": 216},
    "PaymentsToAcquireOrRedeemEntitysShares": {"statement_type": "Cash Flow Statement", "item": "Payments for repurchase of stock", "sort_order_item": 220},
    "DividendsPaidToEquityHoldersOfParentClassifiedAsFinancingActivities": {"statement_type": "Cash Flow Statement", "item": "Dividends paid to parent equity holders", "sort_order_item": 225},
    "DividendsPaidToNoncontrollingInterestsClassifiedAsFinancingActivities": {"statement_type": "Cash Flow Statement", "item": "Dividends paid to non-controlling interests", "sort_order_item": 226},
    "ProceedsFromBorrowingsClassifiedAsFinancingActivities": {"statement_type": "Cash Flow Statement", "item": "Proceeds from borrowings", "sort_order_item": 240},
    "RepaymentsOfBorrowings": {"statement_type": "Cash Flow Statement", "item": "Repayments of borrowings", "sort_order_item": 245},
    "PaymentsOfLeaseLiabilitiesClassifiedAsFinancingActivities": {"statement_type": "Cash Flow Statement", "item": "Payments on lease liabilities", "sort_order_item": 250},
    "InterestPaidClassifiedAsFinancingActivities": {"statement_type": "Cash Flow Statement", "item": "Interest paid (Financing)", "sort_order_item": 255},
    "EffectOfExchangeRateChangesOnCashAndCashEquivalents": {"statement_type": "Cash Flow Statement", "item": "Effect of exchange rate changes on cash", "sort_order_item": 270},
    "IncreaseDecreaseInCashAndCashEquivalentsBeforeEffectOfExchangeRateChanges": {"statement_type": "Cash Flow Statement", "item": "Net inc/dec in cash before FX", "sort_order_item": 275},
    "IncreaseDecreaseInCashAndCashEquivalents": {"statement_type": "Cash Flow Statement", "item": "Net increase in cash", "sort_order_item": 280},
    "CashAndCashEquivalentsBeginningOfPeriod": {"statement_type": "Cash Flow Statement", "item": "Cash at beginning of period", "sort_order_item": 290},
    "CashAndCashEquivalentsEndOfPeriod": {"statement_type": "Cash Flow Statement", "item": "Cash at end of period", "sort_order_item": 300},

    # --- Equity Changes ---
    "IssueOfEquity": {"statement_type": "Equity Changes", "item": "Issuance of equity", "sort_order_item": 10},
    "IncreaseDecreaseThroughSharebasedPaymentTransactions": {"statement_type": "Equity Changes", "item": "Share-based compensation", "sort_order_item": 30},
    "DividendsRecognisedAsDistributionsToOwnersOfParent": {"statement_type": "Equity Changes", "item": "Dividends paid", "sort_order_item": 40},
    "IncreaseDecreaseThroughAcquisitionOfSubsidiary": {"statement_type": "Equity Changes", "item": "Change from acquisition of subsidiary", "sort_order_item": 70},
    "IncreaseDecreaseThroughDisposalOfSubsidiary": {"statement_type": "Equity Changes", "item": "Change from disposal of subsidiary", "sort_order_item": 75},
    "ChangesInEquity": {"statement_type": "Equity Changes", "item": "Total change in equity", "sort_order_item": 90},

    # --- Supplemental Disclosures ---
    "CapitalCommitments": {"statement_type": "Supplemental Disclosures", "item": "Capital commitments", "sort_order_item": 10},
    "NumberOfSharesAuthorised": {"statement_type": "Supplemental Disclosures", "item": "Number of shares authorized", "sort_order_item": 40},
    "NumberOfSharesIssued": {"statement_type": "Supplemental Disclosures", "item": "Number of shares issued", "sort_order_item": 41},
    "ParValuePerShare": {"statement_type": "Supplemental Disclosures", "item": "Par value per share", "sort_order_item": 42},
    "AcquisitionsThroughBusinessCombinationsPropertyPlantAndEquipment": {"statement_type": "Supplemental Cash Flow", "item": "PPE acquired through business combinations", "sort_order_item": 60},
    "AcquisitionsThroughBusinessCombinationsIntangibleAssetsOtherThanGoodwill": {"statement_type": "Supplemental Cash Flow", "item": "Intangibles acquired through business combinations", "sort_order_item": 61},
    "AdditionalRecognitionGoodwill": {"statement_type": "Supplemental Cash Flow", "item": "Goodwill from business combinations", "sort_order_item": 62},
    # In IFRS_CONCEPT_MAPPING, add these new entries:
    "OtherGainsLossesOnFinancialInstrumentsAtFairValueThroughProfitOrLoss": {"statement_type": "Income Statement", "item": "Gain (Loss) on Investments, Net", "sort_order_item": 85},
    "GainsLossesOnDisposalOfFinancialInvestments": {"statement_type": "Income Statement", "item": "Gain (Loss) on Investments, Net", "sort_order_item": 85},
    "ForeignExchangeGainLoss": {"statement_type": "Income Statement", "item": "Foreign Exchange Gain (Loss)", "sort_order_item": 87},
}
US_GAAP_CONCEPT_MAPPING = {
    "CashAndCashEquivalentsAtCarryingValue": {"statement_type": "Balance Sheet", "item": "Cash and cash equivalents", "sort_order_item": 9},
    "RestrictedCashAndCashEquivalentsAtCarryingValue": {"statement_type": "Balance Sheet", "item": "Restricted cash and cash equivalents, current", "sort_order_item": 15},
    "MarketableSecuritiesCurrent": {"statement_type": "Balance Sheet", "item": "Marketable securities, current", "sort_order_item": 19},
    "AccountsReceivableNetCurrent": {"statement_type": "Balance Sheet", "item": "Accounts receivable, net", "sort_order_item": 30},
    "AccountsReceivableNet": {"statement_type": "Balance Sheet", "item": "Accounts receivable, net", "sort_order_item": 30},
    "FinancingReceivable": {"statement_type": "Balance Sheet", "item": "Financing receivables, net", "sort_order_item": 32},
    "NotesReceivableNet": {"statement_type": "Balance Sheet", "item": "Financing receivables, net", "sort_order_item": 32},
    "InventoryNet": {"statement_type": "Balance Sheet", "item": "Inventory, net", "sort_order_item": 35},
    "InventoryFinishedGoodsNetOfReserves": {"statement_type": "Balance Sheet", "item": "Inventory, Finished Goods", "sort_order_item": 36},
    "InventoryWorkInProcessNetOfReserves": {"statement_type": "Balance Sheet", "item": "Inventory, Work in Process", "sort_order_item": 37},
    "InventoryRawMaterialsNetOfReserves": {"statement_type": "Balance Sheet", "item": "Inventory, Raw Materials", "sort_order_item": 38},
    "PrepaidExpenseAndOtherAssetsCurrent": {"statement_type": "Balance Sheet", "item": "Prepaid expenses and other current assets", "sort_order_item": 40},
    "PrepaidExpensesAndOtherCurrentAssets": {"statement_type": "Balance Sheet", "item": "Prepaid expenses and other current assets", "sort_order_item": 40},
    "AssetsCurrent": {"statement_type": "Balance Sheet", "item": "Total current assets", "sort_order_item": 49},
    "MarketableSecuritiesNoncurrent": {"statement_type": "Balance Sheet", "item": "Marketable securities, non-current", "sort_order_item": 60},
    "PropertyPlantAndEquipmentGross": {"statement_type": "Balance Sheet", "item": "Property and equipment, gross", "sort_order_item": 68},
    "AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment": {"statement_type": "Balance Sheet", "item": "Accumulated depreciation", "sort_order_item": 69},
    "PropertyPlantAndEquipmentNet": {"statement_type": "Balance Sheet", "item": "Property and equipment, net", "sort_order_item": 70},
    "OperatingLeaseRightOfUseAsset": {"statement_type": "Balance Sheet", "item": "Operating lease right-of-use assets", "sort_order_item": 80},
    "OperatingLeaseRightOfUseAssets": {"statement_type": "Balance Sheet", "item": "Operating lease right-of-use assets", "sort_order_item": 80},
    "FinanceLeaseRightOfUseAsset": {"statement_type": "Balance Sheet", "item": "Finance Lease, Right-of-Use Asset", "sort_order_item": 81},
    "Goodwill": {"statement_type": "Balance Sheet", "item": "Goodwill", "sort_order_item": 90},
    "IntangibleAssetsNetExcludingGoodwill": {"statement_type": "Balance Sheet", "item": "Intangible assets, net", "sort_order_item": 95},
    "FiniteLivedIntangibleAssetsNet": {"statement_type": "Balance Sheet", "item": "Intangible assets, net (Finite-Lived)", "sort_order_item": 96},
    "DeferredTaxAssetsNet": {"statement_type": "Balance Sheet", "item": "Deferred tax assets, net", "sort_order_item": 98},
    "DeferredIncomeTaxAssetsNet": {"statement_type": "Balance Sheet", "item": "Deferred tax assets, net", "sort_order_item": 98},
    "OtherAssetsNoncurrent": {"statement_type": "Balance Sheet", "item": "Other assets, non-current", "sort_order_item": 100},
    "NoncurrentAssets": {"statement_type": "Balance Sheet", "item": "Total non-current assets", "sort_order_item": 105},
    "Assets": {"statement_type": "Balance Sheet", "item": "Total assets", "sort_order_item": 110},
    "AccountsPayableCurrent": {"statement_type": "Balance Sheet", "item": "Accounts payable, current", "sort_order_item": 120},
    "AccountsPayable": {"statement_type": "Balance Sheet", "item": "Accounts payable, current", "sort_order_item": 120},
    "OperatingLeaseLiabilityCurrent": {"statement_type": "Balance Sheet", "item": "Operating lease liabilities, current", "sort_order_item": 130},
    "OperatingLeaseLiabilitiesCurrent": {"statement_type": "Balance Sheet", "item": "Operating lease liabilities, current", "sort_order_item": 130},
    "FinanceLeaseLiabilityCurrent": {"statement_type": "Balance Sheet", "item": "Finance lease liabilities, current", "sort_order_item": 131},
    "ContractWithCustomerLiabilityCurrent": {"statement_type": "Balance Sheet", "item": "Contract liabilities, current", "sort_order_item": 135},
    "DeferredRevenueCurrent": {"statement_type": "Balance Sheet", "item": "Contract liabilities, current", "sort_order_item": 135},
    "CustomerAdvancesAndDeposits": {"statement_type": "Balance Sheet", "item": "Customer advances and deposits", "sort_order_item": 136},
    "AccruedLiabilitiesCurrent": {"statement_type": "Balance Sheet", "item": "Accrued expenses and other current liabilities", "sort_order_item": 140},
    "EmployeeRelatedLiabilitiesCurrent": {"statement_type": "Balance Sheet", "item": "Employee-related liabilities, current", "sort_order_item": 141},
    "ShortTermBorrowings": {"statement_type": "Balance Sheet", "item": "Short-term borrowings", "sort_order_item": 142},
    "LongTermDebtCurrent": {"statement_type": "Balance Sheet", "item": "Long-term debt, current", "sort_order_item": 144},
    "LiabilitiesCurrent": {"statement_type": "Balance Sheet", "item": "Total current liabilities", "sort_order_item": 150},
    "OperatingLeaseLiabilityNoncurrent": {"statement_type": "Balance Sheet", "item": "Operating lease liabilities, non-current", "sort_order_item": 160},
    "OperatingLeaseLiabilitiesNoncurrent": {"statement_type": "Balance Sheet", "item": "Operating lease liabilities, non-current", "sort_order_item": 160},
    "FinanceLeaseLiabilityNoncurrent": {"statement_type": "Balance Sheet", "item": "Finance lease liabilities, non-current", "sort_order_item": 161},
    "DeferredRevenueNoncurrent": {"statement_type": "Balance Sheet", "item": "Contract liabilities, non-current", "sort_order_item": 162},
    "LongTermDebtNoncurrent": {"statement_type": "Balance Sheet", "item": "Long-term debt, non-current", "sort_order_item": 170},
    "LongTermDebt": {"statement_type": "Balance Sheet", "item": "Long-term debt, non-current", "sort_order_item": 170},
    "DeferredIncomeTaxLiabilitiesNoncurrent": {"statement_type": "Balance Sheet", "item": "Long-term income taxes payable", "sort_order_item": 180},
    "DeferredIncomeTaxLiabilitiesNet": {"statement_type": "Balance Sheet", "item": "Deferred tax liabilities, net", "sort_order_item": 182},
    "OtherLiabilitiesNoncurrent": {"statement_type": "Balance Sheet", "item": "Other non-current liabilities", "sort_order_item": 190},
    "Liabilities": {"statement_type": "Balance Sheet", "item": "Total liabilities", "sort_order_item": 200},
    "CommitmentsAndContingencies": {"statement_type": "Balance Sheet", "item": "Commitments and contingencies", "sort_order_item": 210},
    "CommonStockValue": {"statement_type": "Balance Sheet", "item": "Common stock, value", "sort_order_item": 220},
    "AdditionalPaidInCapital": {"statement_type": "Balance Sheet", "item": "Additional paid-in capital", "sort_order_item": 230},
    "TreasuryStockValue": {"statement_type": "Balance Sheet", "item": "Treasury stock, value", "sort_order_item": 235},
    "AccumulatedOtherComprehensiveIncomeLossNetOfTax": {"statement_type": "Balance Sheet", "item": "Accumulated other comprehensive income (loss)", "sort_order_item": 240},
    "RetainedEarningsAccumulatedDeficit": {"statement_type": "Balance Sheet", "item": "Retained earnings (deficit)", "sort_order_item": 250},
    "RetainedEarningsAppropriated": {"statement_type": "Balance Sheet", "item": "Retained earnings, appropriated", "sort_order_item": 251},
    "RetainedEarningsUnappropriated": {"statement_type": "Balance Sheet", "item": "Retained earnings, unappropriated", "sort_order_item": 252},
    "StockholdersEquity": {"statement_type": "Balance Sheet", "item": "Total stockholders' equity", "sort_order_item": 260},
    "LiabilitiesAndStockholdersEquity": {"statement_type": "Balance Sheet", "item": "Total liabilities and stockholders' equity", "sort_order_item": 270},
    "Revenues": {"statement_type": "Income Statement", "item": "Revenue", "sort_order_item": 10},
    "RevenueFromContractWithCustomerExcludingAssessedTax": {"statement_type": "Income Statement", "item": "Revenue", "sort_order_item": 10},
    "SalesRevenueGoodsNet": {"statement_type": "Income Statement", "item": "Sales Revenue, Products", "sort_order_item": 11},
    "SalesRevenueServicesNet": {"statement_type": "Income Statement", "item": "Sales Revenue, Services", "sort_order_item": 12},
    "CostOfRevenue": {"statement_type": "Income Statement", "item": "Cost of revenue", "sort_order_item": 20},
    "CostOfGoodsAndServicesSold": {"statement_type": "Income Statement", "item": "Cost of revenue", "sort_order_item": 20},
    "ProvisionForLoanAndLeaseLosses": {"statement_type": "Income Statement", "item": "Provision for loan and lease losses", "sort_order_item": 25},
    "GrossProfit": {"statement_type": "Income Statement", "item": "Gross Profit", "sort_order_item": 29},
    "ResearchAndDevelopmentExpense": {"statement_type": "Income Statement", "item": "Research and development expense", "sort_order_item": 40},
    "TechnologyAndContentExpense": {"statement_type": "Income Statement", "item": "Technology and content expense", "sort_order_item": 41},
    "FulfillmentExpense": {"statement_type": "Income Statement", "item": "Fulfillment expense", "sort_order_item": 50},
    "SellingAndMarketingExpense": {"statement_type": "Income Statement", "item": "Selling and marketing expense", "sort_order_item": 51},
    "MarketingExpense": {"statement_type": "Income Statement", "item": "Marketing expense", "sort_order_item": 52},
    "AdvertisingExpense": {"statement_type": "Income Statement", "item": "Advertising expense", "sort_order_item": 53},
    "MarketingAndAdvertisingExpense": {"statement_type": "Income Statement", "item": "Marketing and advertising expense", "sort_order_item": 54},
    "ShippingHandlingAndTransportationCosts": {"statement_type": "Income Statement", "item": "Shipping and handling expense", "sort_order_item": 55},
    "SellingGeneralAndAdministrativeExpense": {"statement_type": "Income Statement", "item": "Selling, General & Administrative Expense", "sort_order_item": 60},
    "GeneralAndAdministrativeExpense": {"statement_type": "Income Statement", "item": "General and administrative expense", "sort_order_item": 61},
    "LeaseAndRentalExpense": {"statement_type": "Income Statement", "item": "Lease and rental expense", "sort_order_item": 62},
    "OtherOperatingIncomeExpenseNet": {"statement_type": "Income Statement", "item": "Other operating income (expense), net", "sort_order_item": 68},
    "OperatingExpenses": {"statement_type": "Income Statement", "item": "Total operating expenses", "sort_order_item": 70},
    "RestructuringCharges": {"statement_type": "Income Statement", "item": "Restructuring charges", "sort_order_item": 72},
    "AssetImpairmentCharges": {"statement_type": "Income Statement", "item": "Asset impairment charges", "sort_order_item": 73},
    "GoodwillAndIntangibleAssetImpairment": {"statement_type": "Income Statement", "item": "Goodwill and intangible asset impairment", "sort_order_item": 74},
    "CostsAndExpenses": {"statement_type": "Income Statement", "item": "Total costs and expenses", "sort_order_item": 75},
    "OperatingIncomeLoss": {"statement_type": "Income Statement", "item": "Income (loss) from operations", "sort_order_item": 80},
    "IncomeLossFromOperations": {"statement_type": "Income Statement", "item": "Income (loss) from operations", "sort_order_item": 80},
    "InterestAndOtherIncomeExpenseNet": {"statement_type": "Income Statement", "item": "Interest and other income (expense), net", "sort_order_item": 90},
    "EquitySecuritiesFvNiGainLoss": {"statement_type": "Income Statement", "item": "Gain (Loss) on Equity Securities, Net", "sort_order_item": 91},
    "InterestExpense": {"statement_type": "Income Statement", "item": "Interest expense", "sort_order_item": 92},
    "InterestIncomeInterestEarningAsset": {"statement_type": "Income Statement", "item": "Interest income from financing", "sort_order_item": 93},
    "InvestmentIncomeInterest": {"statement_type": "Income Statement", "item": "Interest income (from investments)", "sort_order_item": 94},
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments": {"statement_type": "Income Statement", "item": "Income before taxes & equity investments", "sort_order_item": 99},
    "IncomeBeforeProvisionForIncomeTaxes": {"statement_type": "Income Statement", "item": "Income before provision for income taxes", "sort_order_item": 100},
    "IncomeTaxExpenseBenefit": {"statement_type": "Income Statement", "item": "Provision for income taxes", "sort_order_item": 110},
    "EffectiveIncomeTaxRateContinuingOperations": {"statement_type": "Income Statement", "item": "Effective Income Tax Rate (Continuing Operations)", "sort_order_item": 115},
    "NetIncomeLoss": {"statement_type": "Income Statement", "item": "Net income (loss)", "sort_order_item": 120},
    "NetIncomeLossAvailableToCommonStockholdersBasic": {"statement_type": "Income Statement", "item": "Net income available to common stockholders", "sort_order_item": 121},
    "NetIncomeLossAttributableToNoncontrollingInterest": {"statement_type": "Income Statement", "item": "Net income attributable to noncontrolling interest", "sort_order_item": 122},
    "EarningsPerShareBasic": {"statement_type": "Income Statement", "item": "Earnings per share: Basic", "sort_order_item": 130},
    "EarningsPerShareDiluted": {"statement_type": "Income Statement", "item": "Earnings per share: Diluted", "sort_order_item": 140},
    "WeightedAverageNumberOfSharesOutstandingBasic": {"statement_type": "Income Statement", "item": "Weighted-average shares: Basic", "sort_order_item": 150},
    "WeightedAverageNumberOfDilutedSharesOutstanding": {"statement_type": "Income Statement", "item": "Weighted-average shares: Diluted", "sort_order_item": 160},
    "WeightedAverageNumberOfSharesOutstandingDiluted": {"statement_type": "Income Statement", "item": "Weighted-average shares: Diluted", "sort_order_item": 160},
    "ComprehensiveIncomeNetOfTax": {"statement_type": "Comprehensive Income", "item": "Comprehensive income", "sort_order_item": 10},
    "ComprehensiveIncomeNetOfTaxIncludingPortionAttributableToNoncontrollingInterest": {"statement_type": "Comprehensive Income", "item": "Comprehensive income (incl. noncontrolling interest)", "sort_order_item": 11},
    "OtherComprehensiveIncomeLossNetOfTax": {"statement_type": "Comprehensive Income", "item": "Other comprehensive income (loss), net of tax", "sort_order_item": 15},
    "ForeignCurrencyTranslationAdjustmentNetOfTax": {"statement_type": "Comprehensive Income", "item": "Change in foreign currency translation adjustment, net of tax", "sort_order_item": 20},
    "UnrealizedGainLossOnAvailableForSaleSecuritiesNetOfTax": {"statement_type": "Comprehensive Income", "item": "Change in unrealized gain/loss on investments, net of tax", "sort_order_item": 30},
    "IssuanceOfCommonStock": {"statement_type": "Equity Changes", "item": "Issuance of common stock", "sort_order_item": 10},
    "SharesWithheldRelatedToNetShareSettlement": {"statement_type": "Equity Changes", "item": "Shares withheld related to net share settlement", "sort_order_item": 20},
    "ShareBasedCompensation": {"statement_type": "Equity Changes", "item": "Share-based compensation", "sort_order_item": 30},
    "CommonStockRepurchased": {"statement_type": "Equity Changes", "item": "Share repurchases", "sort_order_item": 40},
    "StockRepurchasedDuringPeriodValue": {"statement_type": "Equity Changes", "item": "Share repurchases", "sort_order_item": 40},
    "DividendsDeclaredPerShare": {"statement_type": "Equity Changes", "item": "Dividends declared (per share)", "sort_order_item": 70},
    "CommonStockDividendsPerShareDeclared": {"statement_type": "Equity Changes", "item": "Dividends declared (per share)", "sort_order_item": 70},
    "NetCashProvidedByUsedInOperatingActivities": {"statement_type": "Cash Flow Statement", "item": "Net cash from operating activities", "sort_order_item": 10},
    "Depreciation": {"statement_type": "Cash Flow Statement", "item": "Depreciation", "sort_order_item": 25},
    "DepreciationDepletionAndAmortization": {"statement_type": "Cash Flow Statement", "item": "Depreciation and amortization", "sort_order_item": 30},
    "AmortizationOfIntangibleAssets": {"statement_type": "Cash Flow Statement", "item": "Amortization of intangible assets", "sort_order_item": 35},
    "AmortizationOfLeasedAsset": {"statement_type": "Cash Flow Statement", "item": "Amortization of leased assets", "sort_order_item": 36},
    "AllocatedShareBasedCompensationExpense": {"statement_type": "Cash Flow Statement", "item": "Share-based compensation (CF)", "sort_order_item": 40},
    "ExcessTaxBenefitFromShareBasedCompensationOperatingActivities": {"statement_type": "Cash Flow Statement", "item": "Excess tax benefit from stock comp (CF)", "sort_order_item": 41},
    "DeferredIncomeTaxExpenseBenefit": {"statement_type": "Cash Flow Statement", "item": "Deferred income taxes", "sort_order_item": 50},
    "ImpairmentOfLongLivedAssets": {"statement_type": "Cash Flow Statement", "item": "Impairment charges", "sort_order_item": 60},
    "GoodwillImpairmentLoss": {"statement_type": "Cash Flow Statement", "item": "Goodwill Impairment Loss", "sort_order_item": 61},
    "GainsLossesOnExtinguishmentOfDebt": {"statement_type": "Cash Flow Statement", "item": "Gains/Losses on Extinguishment of Debt", "sort_order_item": 70},
    "OtherNoncashIncomeExpense": {"statement_type": "Cash Flow Statement", "item": "Other non-cash adjustments", "sort_order_item": 78},
    "OtherOperatingActivitiesNet": {"statement_type": "Cash Flow Statement", "item": "Other operating activities, net", "sort_order_item": 80},
    "IncreaseDecreaseInAccountsReceivable": {"statement_type": "Cash Flow Statement", "item": "Changes in Accounts receivable", "sort_order_item": 85},
    "AccountsReceivableNetIncreaseDecrease": {"statement_type": "Cash Flow Statement", "item": "Changes in Accounts receivable", "sort_order_item": 85},
    "IncreaseDecreaseInInventories": {"statement_type": "Cash Flow Statement", "item": "Changes in Inventories", "sort_order_item": 86},
    "IncreaseDecreaseInPrepaidDeferredExpenseAndOtherAssets": {"statement_type": "Cash Flow Statement", "item": "Changes in Prepaid expenses & other assets", "sort_order_item": 90},
    "PrepaidExpensesAndOtherCurrentAssetsIncreaseDecrease": {"statement_type": "Cash Flow Statement", "item": "Changes in Prepaid expenses & other assets", "sort_order_item": 90},
    "OtherAssetsNoncurrentIncreaseDecrease": {"statement_type": "Cash Flow Statement", "item": "Changes in Other non-current assets", "sort_order_item": 110},
    "IncreaseDecreaseInAccountsPayableAndAccruedLiabilities": {"statement_type": "Cash Flow Statement", "item": "Changes in Accounts payable & accrued liabilities", "sort_order_item": 115},
    "AccountsPayableAndAccruedLiabilitiesCurrentIncreaseDecrease": {"statement_type": "Cash Flow Statement", "item": "Changes in Accounts payable & accrued liabilities", "sort_order_item": 115},
    "IncreaseDecreaseInContractWithCustomerLiability": {"statement_type": "Cash Flow Statement", "item": "Changes in contract liabilities", "sort_order_item": 116},
    "AccruedLiabilitiesCurrentIncreaseDecrease": {"statement_type": "Cash Flow Statement", "item": "Changes in Accrued liabilities", "sort_order_item": 130},
    "OtherLiabilitiesNoncurrentIncreaseDecrease": {"statement_type": "Cash Flow Statement", "item": "Changes in Other non-current liabilities", "sort_order_item": 140},
    "NetCashProvidedByUsedInInvestingActivities": {"statement_type": "Cash Flow Statement", "item": "Net cash from investing activities", "sort_order_item": 150},
    "PaymentsToAcquireProductiveAssets": {"statement_type": "Cash Flow Statement", "item": "Purchases of productive assets", "sort_order_item": 159},
    "PaymentsToAcquirePropertyPlantAndEquipment": {"statement_type": "Cash Flow Statement", "item": "Purchases of property and equipment", "sort_order_item": 160},
    "PurchasesOfMarketableSecurities": {"statement_type": "Cash Flow Statement", "item": "Purchases of marketable securities", "sort_order_item": 170},
    "PaymentsToAcquireAvailableForSaleSecuritiesDebt": {"statement_type": "Cash Flow Statement", "item": "Purchases of marketable securities (AFS, Debt)", "sort_order_item": 171},
    "SalesMaturitiesAndOtherDisposalsOfMarketableSecurities": {"statement_type": "Cash Flow Statement", "item": "Sales and maturities of marketable securities", "sort_order_item": 180},
    "ProceedsFromSaleAndMaturityOfMarketableSecurities": {"statement_type": "Cash Flow Statement", "item": "Sales and maturities of marketable securities", "sort_order_item": 180},
    "PaymentsToAcquireInvestments": {"statement_type": "Cash Flow Statement", "item": "Purchases of investments", "sort_order_item": 185},
    "PaymentsToAcquireIntangibleAssets": {"statement_type": "Cash Flow Statement", "item": "Purchases of intangible assets", "sort_order_item": 188},
    "PaymentsToAcquireBusinessesNetOfCashAcquired": {"statement_type": "Cash Flow Statement", "item": "Acquisitions of businesses, net", "sort_order_item": 190},
    "OtherInvestingActivitiesNet": {"statement_type": "Cash Flow Statement", "item": "Other investing activities", "sort_order_item": 200},
    "NetCashProvidedByUsedInFinancingActivities": {"statement_type": "Cash Flow Statement", "item": "Net cash from financing activities", "sort_order_item": 210},
    "PaymentsRelatedToTaxWithholdingForShareBasedCompensation": {"statement_type": "Cash Flow Statement", "item": "Taxes paid related to net share settlement of equity awards", "sort_order_item": 215},
    "PaymentsOfTaxesRelatedToShareBasedPaymentArrangements": {"statement_type": "Cash Flow Statement", "item": "Taxes paid related to net share settlement of equity awards", "sort_order_item": 215},
    "PaymentsForRepurchaseOfCommonStock": {"statement_type": "Cash Flow Statement", "item": "Repurchases of Class A common stock", "sort_order_item": 220},
    "PaymentsOfDividends": {"statement_type": "Cash Flow Statement", "item": "Payments for dividends and dividend equivalents (CF)", "sort_order_item": 230},
    "ProceedsFromIssuanceOfCommonStock": {"statement_type": "Cash Flow Statement", "item": "Proceeds from issuance of common stock", "sort_order_item": 240},
    "ProceedsFromShortTermDebt": {"statement_type": "Cash Flow Statement", "item": "Proceeds from short-term debt", "sort_order_item": 242},
    "RepaymentsOfShortTermDebt": {"statement_type": "Cash Flow Statement", "item": "Repayments of short-term debt", "sort_order_item": 243},
    "ProceedsFromIssuanceOfLongTermDebt": {"statement_type": "Cash Flow Statement", "item": "Proceeds from issuance of long-term debt, net", "sort_order_item": 245},
    "RepaymentsOfLongTermDebt": {"statement_type": "Cash Flow Statement", "item": "Repayments of long-term debt", "sort_order_item": 246},
    "PrincipalPaymentsForFinanceLeaseLiabilities": {"statement_type": "Cash Flow Statement", "item": "Principal payments on finance leases", "sort_order_item": 250},
    "FinanceLeasePrincipalPayments": {"statement_type": "Cash Flow Statement", "item": "Principal payments on finance leases", "sort_order_item": 250},
    "OtherFinancingActivitiesNet": {"statement_type": "Cash Flow Statement", "item": "Other financing activities", "sort_order_item": 260},
    "EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents": {"statement_type": "Cash Flow Statement", "item": "Effect of exchange rate changes on cash", "sort_order_item": 270},
    "EffectOfExchangeRateChangesOnCashAndCashEquivalents": {"statement_type": "Cash Flow Statement", "item": "Effect of exchange rate changes on cash", "sort_order_item": 270},
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect": {"statement_type": "Cash Flow Statement", "item": "Net increase in cash", "sort_order_item": 280},
    "CashAndCashEquivalentsPeriodIncreaseDecrease": {"statement_type": "Cash Flow Statement", "item": "Net increase in cash", "sort_order_item": 280},
    "CashAndCashEquivalentsAtBeginningOfPeriod": {"statement_type": "Cash Flow Statement", "item": "Cash at beginning of period", "sort_order_item": 290},
    "CashAndCashEquivalentsAtEndOfPeriod": {"statement_type": "Cash Flow Statement", "item": "Cash at end of period", "sort_order_item": 300},
    "RestrictedCashAndCashEquivalentsIncludedInPrepaidExpensesAndOtherCurrentAssets": {"statement_type": "Cash Flow Statement", "item": "Restricted cash in prepaid expenses", "sort_order_item": 320},
    "RestrictedCashAndCashEquivalentsIncludedInOtherAssets": {"statement_type": "Cash Flow Statement", "item": "Restricted cash in other assets", "sort_order_item": 330},
    "IncomeTaxesPaidNet": {"statement_type": "Supplemental Cash Flow", "item": "Cash paid for income taxes, net", "sort_order_item": 10},
    "InterestPaidNet": {"statement_type": "Supplemental Cash Flow", "item": "Cash paid for interest, net", "sort_order_item": 20},
    "InterestPaid": {"statement_type": "Supplemental Cash Flow", "item": "Cash paid for interest", "sort_order_item": 20},
    "PropertyPlantAndEquipmentInAccountsPayableAndAccruedLiabilities": {"statement_type": "Supplemental Cash Flow", "item": "Property and equipment in accounts payable", "sort_order_item": 30},
    "AcquisitionOfBusinessesNetOfCashAcquiredIncludedInAccountsPayableAndAccruedLiabilities": {"statement_type": "Supplemental Cash Flow", "item": "Acquisition of businesses in accrued expenses", "sort_order_item": 40},
    "CommonStockRepurchasedDuringPeriodIncludedInAccountsPayableAndAccruedLiabilities": {"statement_type": "Supplemental Cash Flow", "item": "Repurchases of stock in accrued expenses", "sort_order_item": 50},
    # In US_GAAP_CONCEPT_MAPPING, add these new entries if they don't exist:
    "GainsLossesOnExtinguishmentOfDebt": {"statement_type": "Income Statement", "item": "Gain (Loss) on Debt Extinguishment", "sort_order_item": 91},
    "GainsLossesOnSaleOfEquityInvestments": {"statement_type": "Income Statement", "item": "Gain (Loss) on Investments, Net", "sort_order_item": 85},
    "EquitySecuritiesFvNiGainLoss": {"statement_type": "Income Statement", "item": "Gain (Loss) on Equity Securities, Net", "sort_order_item": 91},
    "ForeignCurrencyTransactionGainLoss": {"statement_type": "Income Statement", "item": "Foreign Exchange Gain (Loss)", "sort_order_item": 93},
}
DEI_CONCEPT_MAPPING = {
    # --- Supplemental Disclosures & Entity Information ---
    "EntityCommonStockSharesOutstanding": {
        "statement_type": "Supplemental Disclosures",
        "item": "Entity common stock shares outstanding",
        "sort_order_item": 50
    },
    "EntityPublicFloat": {
        "statement_type": "Supplemental Disclosures",
        "item": "Entity public float",
        "sort_order_item": 55
    },
    "EntityRegistrantName": {
        "statement_type": "Supplemental Disclosures",
        "item": "Entity registrant name",
        "sort_order_item": 1
    },
}

# --- MASTER CONCEPT MAPPING (Combines all previous mappings) ---
MASTER_CONCEPT_MAPPING = {**US_GAAP_CONCEPT_MAPPING, **IFRS_CONCEPT_MAPPING, **DEI_CONCEPT_MAPPING}
STOCKANALYSIS_TO_STANDARD_MAP = {
    # --- Income Statement Mappings ---
    "revenue": "Revenue", "operating revenue": "Revenue", "revenue as reported": "Revenue",
    "cost of revenue": "Cost of revenue", "gross profit": "Gross Profit",
    "research and development": "Research and development expense", "research & development": "Research and development expense",
    "selling general and admin": "Selling, General & Administrative Expense", "advertising expenses": "Advertising expense",
    "other operating expenses": "Other operating expense", "operating expenses": "Total operating expenses",
    "operating income": "Income (loss) from operations", "interest expense": "Interest expense",
    "pretax income": "Income before provision for income taxes", "income tax expense": "Provision for income taxes",
    "provision for income taxes": "Provision for income taxes",
    "earnings from continuing operations": "Net income (loss)", "net income": "Net income (loss)",
    "net income to company": "Net income (loss)",
    "net income to common": "Net income available to common stockholders",
    "provision for credit losses": "Provision for loan and lease losses",
    "shipping and handling": "Shipping and handling expense", "product sales": "Sales Revenue, Products",
    "services sales": "Sales Revenue, Services", "technology and content": "Technology and content expense",
    "fulfillment": "Fulfillment expense", "interest & investment income": "Interest income (from investments)",
    "earnings from equity investments": "Share of profit/loss of associates & JVs",
    "ebt excluding unusual items": "Income before provision for income taxes",
    "minority interest in earnings": "Net income attributable to noncontrolling interest",
    "other revenue": "Other operating income",
    "other non operating income (expenses)": "Interest and other income (expense), net",
    "currency exchange gain (loss)": "Foreign Exchange Gain (Loss)",
    "gain (loss) on sale of investments": "Gain (Loss) on Investments, Net",
    "loss (gain) on equity investments": "Gain (Loss) on Equity Securities, Net",
    "earnings from discontinued operations": "Net income (loss)",
    "provision & write-off of bad debts": "Other operating expense",
    "merger & restructuring charges": "Restructuring charges",
    "impairment of goodwill": "Goodwill and intangible asset impairment",
    "gain (loss) on sale of assets": "Other non-cash adjustments",
    "loss (gain) from sale of assets": "Other non-cash adjustments",
    "asset writedown": "Asset impairment charges",
    "legal settlements": "Restructuring charges",
    "other unusual items": "Restructuring charges",
    "preferred dividends & other adjustments": "Other non-cash adjustments",

    # --- Balance Sheet Mappings ---
    "cash and equivalents": "Cash and cash equivalents", "cash & equivalents": "Cash and cash equivalents",
    "short term investments": "Marketable securities", "cash and short term investments": "Current Investments",
    "trading asset securities": "Marketable securities, current",
    "accounts receivable": "Accounts receivable, net", "receivables": "Accounts receivable, net",
    "other receivables": "Other receivables", "inventory": "Inventories, net",
    "prepaid expenses": "Prepaid expenses and other current assets",
    "restricted cash": "Restricted cash and cash equivalents", "other current assets": "Prepaid expenses and other current assets",
    "total current assets": "Total current assets", "property, plant & equipment": "Property and equipment, net",
    "net ppe": "Property and equipment, net", "land": "Property and equipment, net",
    "long term investments": "Long-Term Investments", "goodwill": "Goodwill",
    "other intangible assets": "Intangible assets, net", "long term deferred tax assets": "Deferred tax assets, net",
    "long-term deferred charges": "Other non-current assets",
    "other long term assets": "Other non-current assets", "total assets": "Total assets",
    "accounts payable": "Accounts payable, current", "accrued expenses": "Accrued expenses and other current liabilities",
    "short term debt": "Short-term borrowings", "current portion of long term debt": "Long-term debt, current",
    "current portion of leases": "Operating lease liabilities, current", "long term leases": "Operating lease liabilities, non-current",
    "current income taxes payable": "Income tax payable", "current unearned revenue": "Contract liabilities, current",
    "long term unearned revenue": "Contract liabilities, non-current",
    "employee related liabilities": "Employee-related liabilities, current", "rent and lease expense": "Rental expense",
    "auditor remuneration": "Auditor's remuneration", "other current liabilities": "Accrued expenses and other current liabilities",
    "total current liabilities": "Total current liabilities", "long term debt": "Long-term debt, non-current",
    "total debt": "Total borrowings", "long term deferred tax liabilities": "Deferred tax liabilities, net",
    "other long term liabilities": "Other non-current liabilities", "total liabilities": "Total liabilities",
    "common stock": "Common stock", "additional paid in capital": "Additional paid-in capital",
    "retained earnings": "Retained earnings (deficit)", "retearn": "Retained earnings (deficit)",
    "comprehensive income and other": "Accumulated other comprehensive income (loss)", "other equity": "Accumulated other comprehensive income (loss)",
    "treasury stock": "Treasury stock, value", "minority interest": "Noncontrolling interests",
    "minority interest bs": "Noncontrolling interests",
    "total common equity": "Total parent stockholders' equity", "shareholders equity": "Total stockholders' equity",
    "equity": "Total stockholders' equity",
    "total liabilities and equity": "Total liabilities and stockholders' equity",
    "working capital": "Working Capital", "net cash / debt": "Net Cash (Debt)",
    "tangible book value": "Tangible Book Value", "tangible book value per share": "Tangible Book Value Per Share",
    "net cash per share": "Net Cash Per Share", "net cash growth": "Net Cash Growth",
    "book value per share": "Book Value Per Share",
    "buildings": "Buildings", "machinery": "Machinery", "construction in progress": "Construction In Progress",
    "leasehold improvements": "Leasehold Improvements",

    # --- Cash Flow Mappings ---
    "depreciation & amortization": "Depreciation and amortization", "depreciation and amortization": "Depreciation and amortization",
    "other amortization": "Depreciation and amortization",
    "stock based compensation": "Share-based compensation (CF)",
    "other operating activities": "Other operating activities, net", "change in accounts receivable": "Changes in Accounts receivable",
    "change in inventory": "Changes in Inventories", "change in accounts payable": "Changes in Accounts payable & accrued liabilities",
    "change in unearned revenue": "Changes in contract liabilities", "change in income taxes": "Deferred income taxes",
    "change in other net operating assets": "Changes in Prepaid expenses & other assets",
    "operating cash flow": "Net cash from operating activities", "capital expenditures": "Purchases of property and equipment",
    "sale purchase of intangibles": "Purchases of intangible assets", "sale (purchase) of intangibles": "Purchases of intangible assets",
    "investment in securities": "Purchases of marketable securities",
    "other investing activities": "Other investing activities", "investing cash flow": "Net cash from investing activities",
    "cash acquisitions": "Acquisitions of businesses, net", "divestitures": "Proceeds from sale of business",
    "issuance of common stock": "Proceeds from issuance of common stock", "repurchase of common stock": "Repurchases of Class A common stock",
    "short term debt issued": "Proceeds from short-term debt", "long term debt issued": "Proceeds from issuance of long-term debt, net",
    "total debt issued": "Proceeds from borrowings", "short term debt repaid": "Repayments of short-term debt",
    "long term debt repaid": "Repayments of long-term debt", "total debt repaid": "Repayments of borrowings",
    "common dividends paid": "Payments for dividends and dividend equivalents (CF)",
    "other financing activities": "Other financing activities", "financing cash flow": "Net cash from financing activities",
    "foreign exchange rate adjustments": "Effect of exchange rate changes on cash", "net cash flow": "Net increase in cash",
    "change in net working capital": "Changes in Accounts payable & accrued liabilities", 
    "cash interest paid": "Cash paid for interest, net", "cash income tax paid": "Cash paid for income taxes, net",
    "net debt issued (repaid)": "Other financing activities",
    "levered free cash flow": "Free Cash Flow", "unlevered free cash flow": "Free Cash Flow",
    "sale of property, plant & equipment": "Proceeds from sale of financial instruments",

    # --- Shares & EPS ---
    "shares outstanding (basic)": "Weighted-average shares: Basic", "shares outstanding (diluted)": "Weighted-average shares: Diluted",
    "eps (basic)": "Earnings per share: Basic", "eps (diluted)": "Earnings per share: Diluted",
    "filing date shares outstanding": "Entity common stock shares outstanding", "total common shares outstanding": "Entity common stock shares outstanding"
}
METRIC_SORTING_MAP = {
    # --- Balance Sheet Metrics ---
    "cash growth": {"parent": "Cash and cash equivalents", "order": 1, "name": "Cash Growth"},
    "working capital": {"parent": "Total current assets", "order": 1, "name": "Working Capital"},
    "asset turnover": {"parent": "Total assets", "order": 1, "name": "Asset Turnover"},
    "a/r turnover": {"parent": "Accounts receivable, net", "order": 1, "name": "Accounts Receivable Turnover"},
    "current ratio": {"parent": "Total current liabilities", "order": 1, "name": "Current Ratio"},
    "quick ratio": {"parent": "Current Ratio", "order": 2, "name": "Quick Ratio"}, # Child of Current Ratio
    "cash ratio": {"parent": "Current Ratio", "order": 3, "name": "Cash Ratio"},    # Child of Current Ratio
    "return on equity": {"parent": "Total stockholders' equity", "order": 1, "name": "Return on Equity (ROE)"},
    "return on assets": {"parent": "Total stockholders' equity", "order": 2, "name": "Return on Assets (ROA)"},
    "debt to equity": {"parent": "Total stockholders' equity", "order": 3, "name": "Debt to Equity Ratio"},
    "debt to assets": {"parent": "Total stockholders' equity", "order": 4, "name": "Debt to Assets Ratio"},
    "net cash debt": {"parent": "Total stockholders' equity", "order": 5, "name": "Net Cash (Debt)"},
    "net cash per share": {"parent": "Net Cash (Debt)", "order": 1, "name": "Net Cash Per Share"},
    "net cash growth": {"parent": "Net Cash (Debt)", "order": 2, "name": "Net Cash Growth"},
    "book value per share": {"parent": "Total stockholders' equity", "order": 6, "name": "Book Value Per Share"},
    "tangible book value": {"parent": "Total stockholders' equity", "order": 7, "name": "Tangible Book Value"},
    "tangible book value per share": {"parent": "Tangible Book Value", "order": 1, "name": "Tangible Book Value Per Share"},

    # --- Income Statement Metrics ---
    "revenue growth": {"parent": "Revenue", "order": 1, "name": "Revenue Growth (YoY)"},
    "gross margin": {"parent": "Gross Profit", "order": 1, "name": "Gross Margin"},
    "operating margin": {"parent": "Income (loss) from operations", "order": 1, "name": "Operating Margin"},
    "ebit": {"parent": "Income (loss) from operations", "order": 2, "name": "EBIT"},
    "ebit margin": {"parent": "EBIT", "order": 1, "name": "EBIT Margin"},
    "ebitda": {"parent": "Income (loss) from operations", "order": 3, "name": "EBITDA"},
    "ebitda margin": {"parent": "EBITDA", "order": 1, "name": "EBITDA Margin"},
    "d&a for ebitda": {"parent": "EBITDA", "order": 2, "name": "D&A For EBITDA"},
    "interest coverage ratio": {"parent": "Interest expense", "order": 1, "name": "Interest Coverage Ratio"},
    "net income growth": {"parent": "Net income (loss)", "order": 1, "name": "Net Income Growth"},
    "profit margin": {"parent": "Net income (loss)", "order": 2, "name": "Profit Margin"},
    "eps growth": {"parent": "Earnings per share: Diluted", "order": 1, "name": "EPS Growth"},
    "dividend per share": {"parent": "Earnings per share: Diluted", "order": 2, "name": "Dividend Per Share"},
    "dividend growth": {"parent": "Dividend Per Share", "order": 1, "name": "Dividend Growth"},
    "effective tax rate": {"parent": "Provision for income taxes", "order": 1, "name": "Effective Tax Rate"},
    "shares change": {"parent": "Weighted-average shares: Diluted", "order": 1, "name": "Shares Change (YoY)"},

    # --- Cash Flow Statement Metrics ---
    "operating cash flow growth": {"parent": "Net cash from operating activities", "order": 1, "name": "Operating Cash Flow Growth"},
    "free cash flow": {"parent": "Net cash from operating activities", "order": 2, "name": "Free Cash Flow"},
    "free cash flow per share": {"parent": "Free Cash Flow", "order": 1, "name": "Free Cash Flow Per Share"},
    "free cash flow margin": {"parent": "Free Cash Flow", "order": 2, "name": "Free Cash Flow Margin"},
    "free cash flow growth": {"parent": "Free Cash Flow", "order": 3, "name": "Free Cash Flow Growth"},
}
# <<< ADD THIS NEW HELPER DICTIONARY >>>
# Creates a lookup map with normalized keys for fast and reliable translation of scraped item names.

# --- GLOBAL SYNONYM SETS FOR ROBUST CALCULATIONS ---
# Built by scanning all mapping dictionaries to find every possible name for a concept.
def normalize_item_name(item_name: str) -> str:
    if not isinstance(item_name, str): return ""
    name = item_name.lower()
    name = re.sub(r'\(.*?\)', '', name)
    name = name.replace('&', 'and').replace('/', ' ').replace('-', ' ')
    name = re.sub(r'[^\w\s]', '', name)
    name = ' '.join(name.split())
    return name

NORMALIZED_STOCKANALYSIS_MAP = {normalize_item_name(k): v for k, v in STOCKANALYSIS_TO_STANDARD_MAP.items()}

# --- Build Reverse Mapping and Synonym Sets ---
NORMALIZED_REVERSE_MAPPING = {}
for concept, mapping in MASTER_CONCEPT_MAPPING.items():
    name = mapping['item']
    normalized = normalize_item_name(name)
    if normalized not in NORMALIZED_REVERSE_MAPPING:
        statement_type = mapping['statement_type']
        NORMALIZED_REVERSE_MAPPING[normalized] = {
            'sort_order_item': mapping['sort_order_item'],
            'statement_type': statement_type,
            'official_item_name': name,
            'statement_sort_order': STATEMENT_ORDER_MAP.get(statement_type, 99)
        }

def resolve_item_metadata(item_name: str) -> dict:
    if not isinstance(item_name, str):
        return {'sort_order_item': 9999, 'statement_type': "Unknown", 'official_item_name': item_name, 'statement_sort_order': 99}

    normalized_sa_name = normalize_item_name(item_name)
    
    # CORRECTED: Using the dictionary with normalized keys.
    standard_name = NORMALIZED_STOCKANALYSIS_MAP.get(normalized_sa_name)

    if standard_name:
        normalized_standard_name = normalize_item_name(standard_name)
    else:
        normalized_standard_name = normalized_sa_name
    
    if normalized_standard_name in NORMALIZED_REVERSE_MAPPING:
        return NORMALIZED_REVERSE_MAPPING[normalized_standard_name]

    for metric_key, metric_info in METRIC_SORTING_MAP.items():
        if normalize_item_name(metric_info['name']) == normalized_standard_name:
            parent_meta = resolve_item_metadata(metric_info['parent'])
            if parent_meta:
                return {
                    'sort_order_item': parent_meta.get('sort_order_item', 9999),
                    'statement_type': parent_meta.get('statement_type', 'Unknown'),
                    'official_item_name': metric_info['name'],
                    'statement_sort_order': parent_meta.get('statement_sort_order', 99)
                }

    return {'sort_order_item': 9999, 'statement_type': "Unknown", 'official_item_name': item_name, 'statement_sort_order': 99}


def build_synonym_set(standard_names):
    synonyms = set()
    for concept, mapping in MASTER_CONCEPT_MAPPING.items():
        if mapping['item'] in standard_names:
            synonyms.add(normalize_item_name(mapping['item']))
            synonyms.add(normalize_item_name(concept))
    for alias, standard in STOCKANALYSIS_TO_STANDARD_MAP.items():
        if standard in standard_names:
            synonyms.add(normalize_item_name(alias))
    return synonyms


REVENUE_KEYS = build_synonym_set({"Revenue", "Sales Revenue, Products", "Sales Revenue, Services"})
COGS_KEYS = build_synonym_set({"Cost of revenue", "Cost of goods sold"})
OP_INCOME_KEYS = build_synonym_set({"Income (loss) from operations", "Income from operations"})
INTEREST_EXPENSE_KEYS = build_synonym_set({"Interest expense", "Interest expense on lease liabilities"})
DA_KEYS = build_synonym_set({"Depreciation and amortization", "Depreciation", "DepreciationDepletionAndAmortization"})
NET_INCOME_KEYS = build_synonym_set({"Net income (loss)"})
TOTAL_ASSETS_KEYS = build_synonym_set({"Total assets"})
TOTAL_EQUITY_KEYS = build_synonym_set({"Total stockholders' equity"})
ACCOUNTS_RECEIVABLE_KEYS = build_synonym_set({"Accounts receivable, net"})
CURRENT_ASSETS_KEYS = build_synonym_set({"Total current assets"})
CURRENT_LIABILITIES_KEYS = build_synonym_set({"Total current liabilities"})
INVENTORY_KEYS = build_synonym_set({"Inventories, net", "Inventory, net"})
CASH_KEYS = build_synonym_set({"Cash and cash equivalents"})
TOTAL_DEBT_KEYS = build_synonym_set({"Total borrowings", "Long-term debt, non-current", "Short-term borrowings"})
NET_CASH_FROM_OPS_KEYS = build_synonym_set({"Net cash from operating activities"})
CAPEX_KEYS = build_synonym_set({"Purchases of property and equipment", "Capital Expenditures", "Purchases of productive assets"})

# Defines metrics that are percentages or ratios and should not have forex conversion applied.
PERCENTAGE_METRICS = {
    "Revenue Growth (YoY)", "Gross Margin", "Operating Margin", "Net Income Growth", "Profit Margin",
    "Shares Change (YoY)", "EPS Growth", "Free Cash Flow Margin", "Dividend Growth", "EBITDA Margin",
    "EBIT Margin", "Effective Tax Rate", "Cash Growth", "Operating Cash Flow Growth", "Free Cash Flow Growth",
    "Return on Assets (ROA)", "Return on Equity (ROE)", "Current Ratio", "Quick Ratio", "Cash Ratio",
    "Debt to Equity Ratio", "Debt to Assets Ratio", "Interest Coverage Ratio", "Asset Turnover", "Accounts Receivable Turnover"
}




# ==============================================================================
# RATE LIMITER CLASS
# ==============================================================================
class RateLimiter:
    """
    A simple, thread-safe rate limiter that enforces a consistent delay
    between consume calls, preventing request bursts.
    """
    def __init__(self, requests_per_unit: int, unit_in_seconds: int = 60):
        # Calculate the minimum delay required between requests.
        self.delay = unit_in_seconds / requests_per_unit
        self._lock = threading.Lock()
        self.last_request_time = 0

    def consume(self):
        """
        Blocks until the required delay has passed since the last call,
        then proceeds.
        """
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_request_time

            # If the delay has not yet passed, sleep for the remaining time.
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)

            # Update the last request time to the current time.
            self.last_request_time = time.monotonic()


# ==============================================================================
# HELPER AND DATA PROCESSING FUNCTIONS (FINALIZED)
# ==============================================================================
def get_period_sort_key(header_str):
    """
    Converts a standardized period header string into a sortable tuple.
    Example: 'Q1_2023' -> (2023, 1), 'FY_2023' -> (2023, 5)
    """
    if not isinstance(header_str, str) or '_' not in header_str:
        return (0, 0) # Sort malformed headers last

    try:
        period, year_str = header_str.split('_')
        year = int(year_str)
        
        # This map defines the chronological order within a year
        period_map = {'Q1': 1, 'H1': 2, 'Q2': 3, 'Q3': 4, 'H2': 5, 'Q4': 6, 'FY': 7}
        
        period_order = period_map.get(period, 99) # Sort unknown periods last
        
        return (year, period_order)
    except (ValueError, IndexError):
        return (0, 0) # Handle cases where splitting or conversion fails


def apply_final_sorting(df):
    """
    Applies the definitive, multi-level sorting to the final DataFrame.
    """
    if df.empty:
        return df
        
    logging.info(f"Applying final sorting to the DataFrame for {df['symbol'].iloc[0]}...")

    # Step 1: Create the dynamic chronological sort key from the 'header' column.
    df['period_sort_key'] = df['header'].apply(get_period_sort_key)
    
    # Step 2: Define the full sorting hierarchy.
    # This ensures statements are grouped, items are in order, metrics follow their parents,
    # and all periods are chronological.
    final_sort_columns = [
        'statement_sort_order',
        'sort_order_item',
        'sort_order_metric',
        'period_sort_key'
    ]

    # Step 3: Apply the sort and clean up.
    df.sort_values(by=final_sort_columns, ascending=True, inplace=True, na_position='last')
    df.drop(columns=['period_sort_key'], inplace=True)
    
    # Step 4: Create the final sort_key and extracted_order for the database.
    df.reset_index(drop=True, inplace=True)
    df['sort_key'] = df.index
    df['extracted_order'] = df.groupby(['symbol', 'item']).cumcount() + 1
    
    return df



def get_val(data_map: dict, keys: set) -> float | None:
    """Safely sums values from a dictionary for a given set of keys."""
    if not data_map or not keys:
        return None

    total = sum(v for k, v in data_map.items() if k in keys and pd.notna(v))
    return total if total is not None else None
def calculate_and_insert_subtotals(df):
    """
    Calculates key sub-totals and their related metrics.
    FIX: Deletes any pre-existing subtotals to avoid conflicts and uses a smarter template row.
    """
    if df.empty:
        return df

    symbol = df['symbol'].iloc[0]
    logging.info(f"Calculating subtotals for {symbol}...")
    
    # --- FIX 1: Delete pre-existing metrics to avoid conflicts ---
    # Get a set of all possible metric names we are about to calculate
    subtotal_and_metric_names = {metric['name'] for metric in METRIC_SORTING_MAP.values()}
    # Filter the DataFrame to keep only the rows that are NOT metrics we're about to add
    df = df[~df['item'].isin(subtotal_and_metric_names)].copy()

    all_new_rows = []
    for (current_symbol, header), group in df.groupby(['symbol', 'header']):
        
        inc_map = {normalize_item_name(r['item']): r['original_value'] for _, r in group[group['statement_type'] == "Income Statement"].iterrows()}
        cf_map = {normalize_item_name(r['item']): r['original_value'] for _, r in group[group['statement_type'] == "Cash Flow Statement"].iterrows()}
        
        # --- FIX 2: Use a smarter template row that has a monetary currency ---
        monetary_rows = group[group['original_currency'] != 'SHARES']
        if monetary_rows.empty:
            # If for some reason no monetary rows exist for this period, skip calculations
            continue 
        template_row = monetary_rows.iloc[0].to_dict()

        # Helper function to create and append new rows
        def add_item(metric_key, value):
            if pd.isna(value) or np.isinf(value): return
            metric_info = METRIC_SORTING_MAP.get(metric_key)
            if not metric_info: return
            parent_meta = resolve_item_metadata(metric_info['parent'])
            
            new_row_data = template_row.copy()
            new_row_data.update({
                'item': metric_info['name'],
                'original_value': round(value, 4),
                'filing_type': 'Derived-Calc',
                'statement_type': parent_meta['statement_type'],
                'sort_order_item': parent_meta['sort_order_item'],
                'sort_order_metric': metric_info['order'],
                'statement_sort_order': parent_meta.get('statement_sort_order', 99)
            })
            all_new_rows.append(new_row_data)

        # --- Calculations ---
        # Gross Profit
        if 'gross profit' not in inc_map:
            revenue = get_val(inc_map, REVENUE_KEYS)
            cogs = get_val(inc_map, COGS_KEYS)
            if revenue is not None and cogs is not None:
                gross_profit = revenue - cogs
                gp_meta = resolve_item_metadata('Gross Profit')
                all_new_rows.append({**template_row, 'item': 'Gross Profit', 
                                     'original_value': gross_profit, 'filing_type': 'Derived-Calc', 
                                     'sort_order_metric': 0, **gp_meta})
                inc_map['gross profit'] = gross_profit

        # Operating Income / EBIT / EBITDA
        operating_income = get_val(inc_map, OP_INCOME_KEYS)
        revenue = get_val(inc_map, REVENUE_KEYS)
        gross_profit = get_val(inc_map, {'gross profit'})
        
        if operating_income is not None:
            add_item('ebit', operating_income) # EBIT is the same as Operating Income
            if revenue and revenue != 0:
                add_item('operating margin', operating_income / revenue)
            
            # D&A can come from the Cash Flow Statement or Income Statement
            d_and_a = get_val(cf_map, DA_KEYS) or get_val(inc_map, DA_KEYS) or 0
            add_item('ebitda', operating_income + d_and_a)
        
        if gross_profit is not None and revenue and revenue != 0:
            add_item('gross margin', gross_profit / revenue)
    
    if all_new_rows:
        return pd.concat([df, pd.DataFrame(all_new_rows)], ignore_index=True)
    return df

def calculate_and_insert_analytical_ratios(df):
    if df.empty: return df
    
    symbol = df['symbol'].iloc[0]
    logging.info(f"Calculating analytical ratios for {symbol}...")
    
    # --- FIX 1: Delete pre-existing metrics to avoid conflicts ---
    analytical_ratio_names = {
        "Return on Equity (ROE)", "Return on Assets (ROA)", "Debt to Equity Ratio", "Debt to Assets Ratio",
        "Current Ratio", "Quick Ratio", "Cash Ratio", "Asset Turnover", "Accounts Receivable Turnover",
        "Interest Coverage Ratio"
    }
    df = df[~df['item'].isin(analytical_ratio_names)].copy()

    df['original_value'] = pd.to_numeric(df['original_value'], errors='coerce')
    df['period_date'] = pd.to_datetime(df['period_date'])

    all_new_rows = []
    for header, group in df.groupby('header'):
        
        # --- FIX 2: Use a smarter template row that has a monetary currency ---
        monetary_rows = group[group['original_currency'] != 'SHARES']
        if monetary_rows.empty:
            continue
        template_row = monetary_rows.iloc[0].to_dict()

        # Create maps for easy lookup for the current period
        bs_map = {normalize_item_name(r['item']): r['original_value'] for _, r in group[group['statement_type'] == "Balance Sheet"].iterrows()}
        inc_map = {normalize_item_name(r['item']): r['original_value'] for _, r in group[group['statement_type'] == "Income Statement"].iterrows()}

        # Find the previous year's balance sheet data for calculating averages
        current_date = group['period_date'].iloc[0]
        prev_date = current_date - pd.DateOffset(years=1)
        prev_group = df[df['period_date'] == prev_date]
        prev_bs_map = {normalize_item_name(r['item']): r['original_value'] for _, r in prev_group[prev_group['statement_type'] == "Balance Sheet"].iterrows()} if not prev_group.empty else {}
        
        # Helper to add new rows
        def add_ratio(metric_key, value):
            if value is None or pd.isna(value) or np.isinf(value): return
            metric_info = METRIC_SORTING_MAP[metric_key]
            parent_meta = resolve_item_metadata(metric_info['parent'])
            new_row_data = template_row.copy()
            new_row_data.update({
                'item': metric_info['name'], 'original_value': round(value, 4), 'filing_type': 'Derived-Ratio',
                'statement_type': parent_meta['statement_type'], 'sort_order_item': parent_meta['sort_order_item'],
                'sort_order_metric': metric_info['order'], 'statement_sort_order': parent_meta.get('statement_sort_order', 99)
            })
            all_new_rows.append(new_row_data)

        # --- Gather Components ---
        # Flow statement items (use current period's value)
        net_income = get_val(inc_map, NET_INCOME_KEYS)
        revenue = get_val(inc_map, REVENUE_KEYS)
        operating_income = get_val(inc_map, OP_INCOME_KEYS)
        interest_expense = get_val(inc_map, INTEREST_EXPENSE_KEYS)

        # Balance sheet items (get current and prior period values for averaging)
        total_assets, prev_assets = get_val(bs_map, TOTAL_ASSETS_KEYS), get_val(prev_bs_map, TOTAL_ASSETS_KEYS)
        total_equity, prev_equity = get_val(bs_map, TOTAL_EQUITY_KEYS), get_val(prev_bs_map, TOTAL_EQUITY_KEYS)
        accounts_receivable, prev_ar = get_val(bs_map, ACCOUNTS_RECEIVABLE_KEYS), get_val(prev_bs_map, ACCOUNTS_RECEIVABLE_KEYS)
        current_assets, current_liabilities = get_val(bs_map, CURRENT_ASSETS_KEYS), get_val(bs_map, CURRENT_LIABILITIES_KEYS)
        inventory, cash, total_debt = get_val(bs_map, INVENTORY_KEYS), get_val(bs_map, CASH_KEYS), get_val(bs_map, TOTAL_DEBT_KEYS)
        
        # --- Calculate Averages (helper function to avoid repetition) ---
        def get_avg(current, previous):
            if current is not None and previous is not None:
                return (current + previous) / 2
            return current # Fallback to current value if no previous value exists
        
        avg_assets = get_avg(total_assets, prev_assets)
        avg_equity = get_avg(total_equity, prev_equity)
        avg_ar = get_avg(accounts_receivable, prev_ar)

        # --- Calculate and Add Ratios ---
        if net_income and avg_assets and avg_assets != 0: add_ratio('return on assets', net_income / avg_assets)
        if net_income and avg_equity and avg_equity != 0: add_ratio('return on equity', net_income / avg_equity)
        if revenue and avg_assets and avg_assets != 0: add_ratio('asset turnover', revenue / avg_assets)
        if revenue and avg_ar and avg_ar != 0: add_ratio('a/r turnover', revenue / avg_ar)
        if operating_income and interest_expense and interest_expense != 0: add_ratio('interest coverage ratio', operating_income / interest_expense)
        
        # Liquidity Ratios
        if current_liabilities and current_liabilities != 0:
            if current_assets: add_ratio('current ratio', current_assets / current_liabilities)
            if current_assets and inventory: add_ratio('quick ratio', (current_assets - inventory) / current_liabilities)
            if cash: add_ratio('cash ratio', cash / current_liabilities)
        
        # Leverage Ratios
        if total_debt and total_equity and total_equity != 0: add_ratio('debt to equity', total_debt / total_equity)
        if total_debt and total_assets and total_assets != 0: add_ratio('debt to assets', total_debt / total_assets)

    if all_new_rows:
        # ** FIX: Add ignore_index=True to prevent a broken/duplicated index **
        return pd.concat([df, pd.DataFrame(all_new_rows)], ignore_index=True)
    return df

def format_period_header(fiscal_year, fiscal_period):
    """Generates a consistent period header (e.g., Q1_2023, FY_2023)"""
    return f"{fiscal_period}_{fiscal_year}"


def has_sufficient_quarterly_data(df, threshold=10):
    """
    Checks if the EDGAR data for a US-GAAP company has enough discrete quarterly
    data points to stand alone, or if it needs to be supplemented.
    """
    if df.empty:
        return False
    flow_statements = ["Income Statement", "Cash Flow Statement", "Comprehensive Income"]
    df_flow = df[df['statement_type'].isin(flow_statements)]
    if df_flow.empty:
        logging.warning("No flow statements found in EDGAR data; triggering hybrid fetch.")
        return False
    quarterly_counts = df_flow['fiscal_period'].value_counts()
    q1_count = quarterly_counts.get('Q1', 0)
    q2_count = quarterly_counts.get('Q2', 0)
    q3_count = quarterly_counts.get('Q3', 0)
    logging.info(f"EDGAR data contains: Q1 items: {q1_count}, Q2 items: {q2_count}, Q3 items: {q3_count}.")
    if q1_count < threshold or q2_count < threshold or q3_count < threshold:
        logging.warning(f"Quarterly data below threshold of {threshold}. Recommending hybrid approach.")
        return False
    logging.info("Sufficient quarterly data found in EDGAR filings.")
    return True

def process_quarterly_data(df):
    """
    Converts cumulative YTD flow statement values from EDGAR into discrete quarters.
    This function does NOT derive Q4; it only cleans existing Q2 and Q3 data.
    """
    if df.empty:
        return df

    logging.info(f"Converting cumulative YTD data to discrete quarters for {df['symbol'].iloc[0]}...")
    
    flow_statements = ["Income Statement", "Cash Flow Statement", "Comprehensive Income"]
    df_flow = df[df['statement_type'].isin(flow_statements)].copy()
    df_balance_sheet = df[~df['statement_type'].isin(flow_statements)].copy()

    if df_flow.empty:
        return df_balance_sheet

    # Ensure values are numeric before doing arithmetic
    df_flow['original_value'] = pd.to_numeric(df_flow['original_value'], errors='coerce')
    df_flow.dropna(subset=['original_value'], inplace=True)

    processed_groups = []
    # Group by each line item for each year to perform the conversion
    for (item, year), group in df_flow.groupby(['item', 'fiscal_year']):
        
        # Get the values for each period if they exist
        period_values = group.set_index('fiscal_period')['original_value'].to_dict()
        q1_val = period_values.get('Q1')
        q2_ytd_val = period_values.get('Q2')
        q3_ytd_val = period_values.get('Q3')
        
        group_copy = group.copy()
        
        # Convert Q2: Discrete Q2 = YTD Q2 - Q1
        if pd.notna(q1_val) and pd.notna(q2_ytd_val):
            group_copy.loc[group_copy['fiscal_period'] == 'Q2', 'original_value'] = q2_ytd_val - q1_val
        
        # Convert Q3: Discrete Q3 = YTD Q3 - YTD Q2
        if pd.notna(q2_ytd_val) and pd.notna(q3_ytd_val):
            group_copy.loc[group_copy['fiscal_period'] == 'Q3', 'original_value'] = q3_ytd_val - q2_ytd_val
            
        processed_groups.append(group_copy)

    if not processed_groups:
        return df

    # Combine the processed flow statements with the original balance sheet data
    df_flow_processed = pd.concat(processed_groups, ignore_index=True)
    return pd.concat([df_balance_sheet, df_flow_processed], ignore_index=True)

def relabel_semi_annual_periods(df):
    """
    Detects semi-annual filers and standardizes period headers.
    - For semi-annual filers: Q2 -> H1, Q4 -> FY.
    - For quarterly filers: Duplicates Q4 BS as FY, AND duplicates FY BS as Q4
      to ensure both periods are always present for Balance Sheet statements.
    """
    if df.empty:
        return df

    symbol = df['symbol'].iloc[0]
    logging.info(f"Relabeling periods and aligning Q4/FY for {symbol}...")

    flow_statements = ['Income Statement', 'Cash Flow Statement', 'Comprehensive Income']
    df_flow = df[df['statement_type'].isin(flow_statements)]
    
    is_semi_annual = False
    if not df_flow.empty:
        present_periods = set(df_flow['fiscal_period'].dropna().unique())
        if 'Q2' in present_periods and 'Q4' in present_periods and 'Q1' not in present_periods and 'Q3' not in present_periods:
            is_semi_annual = True
            logging.info(f"{symbol} detected as a SEMI-ANNUAL filer.")

    if is_semi_annual:
        # For semi-annual filers, Q2 is H1 and Q4 is the final FY report
        df['header'] = df['header'].str.replace(r'Q2_', 'H1_', regex=True)
        df['header'] = df['header'].str.replace(r'Q4_', 'FY_', regex=True)
    else:
        # For standard quarterly filers, ensure both Q4 and FY exist for Balance Sheets
        bs_df = df[df['statement_type'] == 'Balance Sheet'].copy()
        other_stmts_df = df[df['statement_type'] != 'Balance Sheet'].copy()
        
        # --- LOGIC 1: Create FY rows from existing Q4 rows ---
        q4_rows = bs_df[bs_df['header'].str.startswith('Q4_')].copy()
        if not q4_rows.empty:
            fy_from_q4 = q4_rows.copy()
            fy_from_q4['header'] = fy_from_q4['header'].str.replace('Q4_', 'FY_', regex=True)
            fy_from_q4['fiscal_period'] = 'FY'
            fy_from_q4['filing_type'] = 'Derived-FY-BS'
            bs_df = pd.concat([bs_df, fy_from_q4], ignore_index=True)

        # --- NEW LOGIC 2: Create Q4 rows from existing FY rows (for EDGAR data) ---
        fy_rows = bs_df[bs_df['header'].str.startswith('FY_')].copy()
        if not fy_rows.empty:
            q4_from_fy = fy_rows.copy()
            q4_from_fy['header'] = q4_from_fy['header'].str.replace('FY_', 'Q4_', regex=True)
            q4_from_fy['fiscal_period'] = 'Q4'
            q4_from_fy['filing_type'] = 'Derived-Q4-BS'
            bs_df = pd.concat([bs_df, q4_from_fy], ignore_index=True)
        
        # Recombine the statements and remove any true duplicates this process created
        df = pd.concat([other_stmts_df, bs_df], ignore_index=True)
        df.drop_duplicates(subset=['symbol', 'item', 'header'], keep='first', inplace=True)
            
    return df

def derive_and_align_fy_data(df):
    """
    For quarterly filers, derives a Fiscal Year (FY) total for flow statements
    if a full set of four quarters exists but an official FY report does not.
    """
    if df.empty:
        return df

    logging.info(f"Checking if FY derivation is needed for {df['symbol'].iloc[0]}...")
    flow_statements = ['Income Statement', 'Cash Flow Statement', 'Comprehensive Income']
    df_flow = df[df['statement_type'].isin(flow_statements)]

    derived_fy_rows = []
    # Group by item and year to check for complete quarters
    for (item, year), group in df_flow.groupby(['item', 'fiscal_year']):
        present_periods = set(group['fiscal_period'].dropna().unique())
        
        # Condition: All 4 quarters must exist AND an FY must NOT exist.
        if {'Q1', 'Q2', 'Q3', 'Q4'}.issubset(present_periods) and 'FY' not in present_periods:
            q4_row = group[group['fiscal_period'] == 'Q4'].iloc[0]
            
            # Sum the values of the four quarters for that item
            fy_value = group[group['fiscal_period'].isin(['Q1','Q2','Q3','Q4'])]['original_value'].sum()
            
            # Create a new row for the derived FY data
            new_fy_row = q4_row.copy()
            new_fy_row['original_value'] = fy_value
            new_fy_row['header'] = f"FY_{year}"
            new_fy_row['fiscal_period'] = 'FY'
            new_fy_row['filing_type'] = 'Derived-FY'
            
            derived_fy_rows.append(new_fy_row)

    if derived_fy_rows:
        logging.info(f"Derived {len(derived_fy_rows)} new FY data rows.")
        return pd.concat([df, pd.DataFrame(derived_fy_rows)], ignore_index=True)

    return df



def derive_q4_from_fy(df):
    """
    For quarterly filers, derives a discrete Q4 value for flow statements
    if a full set of Q1, Q2, Q3, and FY exists but Q4 does not.
    """
    if df.empty:
        return df

    symbol = df['symbol'].iloc[0]
    logging.info(f"Deriving Q4 data from FY totals for {symbol}...")
    
    flow_statements = ["Income Statement", "Cash Flow Statement", "Comprehensive Income"]
    df_flow = df[df['statement_type'].isin(flow_statements)].copy()
    df_balance_sheet = df[~df['statement_type'].isin(flow_statements)].copy()
    
    if df_flow.empty:
        return df_balance_sheet

    derived_q4_rows = []
    # Group by each item and year to check for the required periods
    for (item, year), group in df_flow.groupby(['item', 'fiscal_year']):
        present_periods = set(group['fiscal_period'].dropna().unique())
        
        # Condition: Q1, Q2, Q3, and FY must exist, and Q4 must NOT exist.
        if {'Q1', 'Q2', 'Q3', 'FY'}.issubset(present_periods) and 'Q4' not in present_periods:
            
            period_values = group.set_index('fiscal_period')['original_value'].to_dict()
            q1_val = period_values.get('Q1', 0)
            q2_val = period_values.get('Q2', 0)
            q3_val = period_values.get('Q3', 0)
            fy_val = period_values.get('FY', 0)

            # Derive Q4: FY - Q1 - Q2 - Q3
            q4_value = fy_val - q1_val - q2_val - q3_val

            # Use the FY row as a template for the new Q4 row
            fy_row = group[group['fiscal_period'] == 'FY'].iloc[0].copy()
            fy_row['original_value'] = q4_value
            fy_row['header'] = f"Q4_{year}"
            fy_row['fiscal_period'] = 'Q4'
            fy_row['filing_type'] = 'Derived-Q4'
            fy_row['period_date'] = datetime(year, 12, 31).date()
            
            derived_q4_rows.append(fy_row)

    if derived_q4_rows:
        logging.info(f"Derived {len(derived_q4_rows)} new Q4 data rows for {symbol}.")
        return pd.concat([df, pd.DataFrame(derived_q4_rows)], ignore_index=True)

    return df




# ==============================================================================
# DATA FETCHING FUNCTIONS (EDGAR, STOCKANALYSIS, FRED)
# ==============================================================================
def get_stockanalysis_headers():
    """
    Returns a consistent, browser-like header set for all requests to stockanalysis.com
    to ensure proper activation and avoid using the EDGAR agent by mistake.
    """
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Referer": "https://stockanalysis.com/stocks/",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "sec-ch-ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin"
    }


def fetch_companies_by_sector(sector_name: str) -> pd.DataFrame:
    """
    Fetches a list of all companies from stockanalysis.com.
    This version uses the standardized header function.
    """
    logging.info(f"--- STAGE 1: Dynamic company discovery for sector: '{sector_name}' ---")
    metrics_to_fetch = ['name', 'sector', 'industry', 'marketCapCategory', 'country', 'cik']
    rename_map = {'name': 'company_name', 'sector': 'sector', 'industry': 'industry', 'marketCapCategory': 'market_cap_group', 'country': 'country', 'cik': 'cik_code'}
    batch_string = "+".join(metrics_to_fetch)
    url = f"https://stockanalysis.com/api/screener/s/bd/{batch_string}.json"

    try:
        logging.info(f"Requesting screener data for {len(metrics_to_fetch)} metrics...")
        # Use the correct, consistent headers for stockanalysis.com
        response = requests.get(url, headers=get_stockanalysis_headers(), timeout=90)
        response.raise_for_status()

        json_data = response.json().get('data', {}).get('data', {})
        if not json_data:
            logging.error("No data returned from the screener API.")
            return pd.DataFrame()

        df = pd.DataFrame.from_dict(json_data, orient='index')
        df.index.name = 'symbol'
        df.reset_index(inplace=True)

        initial_count = len(df)
        df_filtered = df[df['sector'] == sector_name].copy()
        logging.info(f"Filtered {initial_count} total companies down to {len(df_filtered)} in the '{sector_name}' sector.")

        if df_filtered.empty: return pd.DataFrame()

        df_filtered.rename(columns=rename_map, inplace=True)
        df_filtered['cik_code'] = df_filtered['cik_code'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(10)

        final_columns = ['symbol', 'company_name', 'cik_code', 'sector', 'industry', 'market_cap_group', 'country']
        for col in final_columns:
            if col not in df_filtered.columns:
                df_filtered[col] = None

        return df_filtered[final_columns]
    except Exception as e:
        logging.error(f"An unexpected error occurred during company discovery: {e}", exc_info=True)
        return pd.DataFrame()


def fetch_historical_monthly_forex_rates_fred(api_key: str, currency_series_map: dict, start_year: int = 2008) -> pd.DataFrame:
    """
    Fetches historical forex rates from FRED. This function is NOT rate-limited as FRED is more permissive.
    """
    # This function remains unchanged as it's not the source being rate-limited.
    if not api_key or api_key == "YOUR_API_KEY_HERE":
        logging.error("FRED API key is not set. Cannot fetch forex rates.")
        return pd.DataFrame(columns=['year_month', 'currency_code', 'rate_to_usd'])

    all_forex_data = []
    base_fred_url = "https://api.stlouisfed.org/fred/series/observations"
    observation_start = f"{start_year}-01-01"

    for currency_code, series_info in currency_series_map.items():
        if currency_code == "USD":
            continue
        if not series_info or 'id' not in series_info:
            logging.warning(f"Missing series ID for currency {currency_code}. Skipping FRED fetch for this currency.")
            continue

        series_id = series_info['id']
        invert_rate = series_info.get('invert', False)
        daily_data_processed = False

        # --- Attempt 1: Fetch Daily data and resample to Monthly (Preferred method) ---
        params_daily = {
            "series_id": series_id, "api_key": api_key, "file_type": "json",
            "frequency": "d", "observation_start": observation_start,
        }
        try:
            logging.info(f"Attempting to fetch DAILY series: {series_id} for {currency_code}")
            response_d = requests.get(base_fred_url, params=params_daily, timeout=60)
            response_d.raise_for_status()
            data_d = response_d.json()
            observations_daily = data_d.get("observations", [])

            if observations_daily and observations_daily[0].get("value") != ".":
                logging.info(f"Success: Fetched {len(observations_daily)} daily observations for {series_id}. Resampling to monthly average.")
                daily_df = pd.DataFrame(observations_daily)
                daily_df = daily_df[daily_df['value'] != "."].copy()
                if not daily_df.empty:
                    daily_df['date'] = pd.to_datetime(daily_df['date'])
                    daily_df['value'] = pd.to_numeric(daily_df['value'])
                    daily_df = daily_df.set_index('date')
                    if not daily_df.empty:
                        monthly_avg_df = daily_df['value'].resample('M').mean().to_frame()
                        for idx, row_data in monthly_avg_df.iterrows():
                            rate = row_data['value']
                            if pd.isna(rate) or rate == 0: continue
                            rate_to_usd = round((1 / rate) if invert_rate else rate, 4)
                            all_forex_data.append({
                                "year_month": idx.strftime('%Y-%m'),
                                "currency_code": currency_code,
                                "rate_to_usd": rate_to_usd
                            })
                        daily_data_processed = True
                    else:
                        logging.info(f"Daily data for {series_id} was empty after filtering non-numeric. Will try other frequencies.")
                else:
                    logging.info(f"Daily data for {series_id} was empty after initial filter. Will try other frequencies.")
            else:
                logging.info(f"No usable daily data for {series_id} (empty or '.' values). Will try monthly/quarterly.")
        except (requests.exceptions.RequestException, ValueError, KeyError) as e:
            logging.warning(f"Could not fetch/process daily data for {series_id}: {e}. Will try other frequencies.")

        if daily_data_processed:
            time.sleep(random.uniform(0.34, 0.5))  # Short polite pause between FRED calls
            continue

        # --- Fallback to Monthly or Quarterly if Daily fails or yields no data ---
        observations_fallback = []
        fetched_frequency = None
        for freq, agg_method in [("m", "avg"), ("q", "avg")]:
            params_fallback = {
                "series_id": series_id, "api_key": api_key, "file_type": "json",
                "frequency": freq, "aggregation_method": agg_method,
                "observation_start": observation_start,
            }
            try:
                logging.info(f"Fetching FRED series ({freq} {agg_method}): {series_id} for {currency_code}")
                response_fallback = requests.get(base_fred_url, params=params_fallback, timeout=60)
                response_fallback.raise_for_status()
                data_fallback = response_fallback.json()
                obs_list = data_fallback.get("observations", [])
                if obs_list and (len(obs_list) > 0 and obs_list[0].get("value") != "."):
                    observations_fallback = obs_list
                    fetched_frequency = "quarterly" if freq == "q" else "monthly"
                    logging.info(f"Successfully fetched {fetched_frequency} data for {series_id}. Count: {len(observations_fallback)}")
                    break
            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching {freq} data for {series_id}: {e}")
            except Exception as e_gen:
                logging.error(f"Unexpected error during {freq} FRED fetch for {series_id} ({currency_code}): {e_gen}", exc_info=False)

        if observations_fallback:
            for obs_data in observations_fallback:
                obs_date_str = obs_data.get("date")
                obs_value_str = obs_data.get("value")
                if obs_date_str and obs_value_str and obs_value_str != ".":
                    try:
                        rate_val = float(obs_value_str)
                        if rate_val == 0: continue
                        rate_to_usd = (1 / rate_val) if invert_rate else rate_val
                        obs_datetime_obj = datetime.strptime(obs_date_str, '%Y-%m-%d')

                        if fetched_frequency == "quarterly":
                            year_val = obs_datetime_obj.year
                            q_month_val = obs_datetime_obj.month
                            if q_month_val == 1:
                                months_in_q = [1, 2, 3]
                            elif q_month_val == 4:
                                months_in_q = [4, 5, 6]
                            elif q_month_val == 7:
                                months_in_q = [7, 8, 9]
                            elif q_month_val == 10:
                                months_in_q = [10, 11, 12]
                            else:
                                continue

                            for m_val in months_in_q:
                                all_forex_data.append({
                                    "year_month": f"{year_val}-{str(m_val).zfill(2)}",
                                    "currency_code": currency_code, "rate_to_usd": rate_to_usd
                                })
                        else:  # Monthly
                            all_forex_data.append({
                                "year_month": obs_datetime_obj.strftime('%Y-%m'),
                                "currency_code": currency_code, "rate_to_usd": rate_to_usd
                            })
                    except ValueError as e:
                        logging.warning(f"Could not parse observation for {series_id}: {obs_data}. Error: {e}")
        time.sleep(random.uniform(0.34, 0.5))  # Short polite pause between FRED calls

    if not all_forex_data:
        logging.warning("No forex data successfully fetched from FRED after all attempts.")
        return pd.DataFrame(columns=['year_month', 'currency_code', 'rate_to_usd'])

    forex_df = pd.DataFrame(all_forex_data)
    if not forex_df.empty:
        forex_df = forex_df.drop_duplicates(subset=['year_month', 'currency_code'], keep='last')
    logging.info(f"Successfully fetched and processed {len(forex_df)} unique monthly forex rates from FRED.")
    return forex_df


def get_company_facts_data(cik_code, edgar_rate_limiter):
    """
    Fetches data from EDGAR. THIS FUNCTION IS NOW RATE-LIMITED to prevent 403 errors.
    """
    if not cik_code or not isinstance(cik_code, str) or not cik_code.isdigit() or len(cik_code) != 10:
        logging.warning(f"Invalid CIK provided: '{cik_code}'. Cannot fetch from EDGAR.")
        return pd.DataFrame(), None

    # Consume a token from the EDGAR rate limiter before making the request.
    edgar_rate_limiter.consume()
    logging.info(f"EDGAR token acquired. Fetching all facts for CIK {cik_code}...")

    company_facts_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_code}.json"

    try:
        # This call now correctly uses the EDGAR-specific headers.
        response = requests.get(company_facts_url, headers=EDGAR_HEADERS, timeout=60)
        response.raise_for_status()
        data = response.json()

        # ... (rest of the function logic is unchanged) ...
        facts_by_end_date = {}
        processed_taxonomies = set()
        for taxonomy_name, taxonomy_facts in data.get('facts', {}).items():
            for concept, data_points in taxonomy_facts.items():
                if concept in MASTER_CONCEPT_MAPPING:
                    mapped_info = MASTER_CONCEPT_MAPPING[concept]
                    processed_taxonomies.add(taxonomy_name)
                    for currency, facts_list in data_points.get('units', {}).items():
                        for fact in facts_list:
                            form = fact.get('form', 'N/A').upper()
                            if form in ('10-K', '20-F', '40-F', '10-Q', '10-K/A', '20-F/A', '40-F/A', '10-Q/A'):
                                group_key = (mapped_info["item"], fact.get('end'))
                                if group_key not in facts_by_end_date:
                                    facts_by_end_date[group_key] = []
                                fact_tuple = (fact, mapped_info, currency.upper())
                                facts_by_end_date[group_key].append(fact_tuple)
        final_facts = []
        for group_key, fact_group in facts_by_end_date.items():
            best_fact_tuple = sorted(fact_group, key=lambda x: (0 if '/A' in x[0]['form'] else 1, x[0]['filed']))[0]
            final_facts.append(best_fact_tuple)

        primary_taxonomy = 'us-gaap' if 'us-gaap' in processed_taxonomies else 'ifrs-full'
        if not final_facts:
            return pd.DataFrame(), primary_taxonomy

        df_data = []
        for fact_tuple in final_facts:
            fact, mapped_info, currency = fact_tuple
            df_data.append({
                "item": mapped_info['item'],
                "statement_type": mapped_info.get("statement_type", "N/A"),
                "sort_order_item": mapped_info.get("sort_order_item", 999),
                "sort_order_metric": 0,
                "fiscal_year": fact['fy'],
                "fiscal_period": fact['fp'],
                "period_date": datetime.strptime(fact['end'], '%Y-%m-%d').date(),
                "original_value": fact['val'],
                "original_currency": currency,
                "filing_type": fact['form'],
                "filed_date": datetime.strptime(fact['filed'], '%Y-%m-%d').date(),
                "header": format_period_header(fact['fy'], fact['fp'])
            })

        final_df = pd.DataFrame(df_data)
        return final_df.sort_values(by='filed_date').drop_duplicates(subset=['item', 'header'], keep='first'), primary_taxonomy

    except requests.exceptions.RequestException as e:
        logging.error(f"Request error for CIK {cik_code}: {e}")
        return pd.DataFrame(), None
    except Exception as e:
        logging.error(f"Error processing CIK {cik_code}: {e}", exc_info=True)
        return pd.DataFrame(), None


def fetch_company_profile_currency(symbol, sa_rate_limiter):
    """
    Fetches profile currency from StockAnalysis.
    This version uses the standardized header function.
    """
    sa_rate_limiter.consume()
    logging.info(f"SA token acquired. Fetching profile currency for {symbol}.")

    json_target_url = f"https://stockanalysis.com/stocks/{symbol.lower()}/company/__data.json"

    try:
        # Use the correct, consistent headers for stockanalysis.com
        response = requests.get(json_target_url, headers=get_stockanalysis_headers(), timeout=35)
        response.raise_for_status()
        json_data = response.json()

        # ... (rest of the function logic is unchanged) ...
        for node in json_data.get('nodes', []):
            if node.get('type') == 'data' and isinstance(node.get('data'), list):
                data_section = node.get('data')
                info_map = None
                for item in data_section:
                    if isinstance(item, dict) and 'currency' in item and 'symbol' in item:
                        info_map = item
                        break
                if info_map:
                    currency_index = info_map.get('currency')
                    if currency_index is not None and isinstance(currency_index, int) and currency_index < len(data_section):
                        currency = data_section[currency_index]
                        if isinstance(currency, str) and currency:
                            logging.info(f"Success: Found currency '{currency}' for {symbol}.")
                            return currency.upper()
                    logging.warning(f"Found currency map for {symbol}, but index was invalid.")
                    return None
        logging.warning(f"Could not find currency info for {symbol}.")
        return None
    except (requests.exceptions.RequestException, ValueError) as e:
        logging.error(f"Critical error fetching profile currency for {symbol}: {e}", exc_info=False)
        return None
def fetch_stockanalysis_data(symbol, sa_rate_limiter, periods_to_fetch='ALL'):
    """
    Fetches financial statements from StockAnalysis.com for specified period types.
    This version correctly parses the nested JSON structure to get real values.
    """
    all_rows = []
    fetch_modes = []
    if periods_to_fetch in ['ALL', 'A_ONLY']:
        fetch_modes.append({'param': '', 'label': 'Annual'})
    if periods_to_fetch in ['ALL', 'Q_ONLY']:
        fetch_modes.append({'param': 'quarterly', 'label': 'Quarterly'})

    statement_map = [
        {"slug": "financials", "display_name": "Income Statement"},
        {"slug": "financials/balance-sheet", "display_name": "Balance Sheet"},
        {"slug": "financials/cash-flow-statement", "display_name": "Cash Flow Statement"}
    ]

    for mode in fetch_modes:
        for stmt_info in statement_map:
            url = f"https://stockanalysis.com/stocks/{symbol.lower()}/{stmt_info['slug']}/__data.json?p={mode['param']}"
            try:
                sa_rate_limiter.consume()
                logging.info(f"SA token acquired. Fetching {mode['label']} {stmt_info['display_name']} for {symbol}.")
                response = requests.get(url, headers=get_stockanalysis_headers(), timeout=30)
                response.raise_for_status()
                json_data = response.json()

                data_section = next((node['data'] for node in json_data.get('nodes', []) if node.get('type') == 'data' and isinstance(node.get('data'), list) and len(node['data']) > 0 and 'financialData' in node['data'][0]), None)
                if not data_section: continue

                key_map = data_section[0]
                financial_data_map = data_section[key_map.get('financialData')]
                headers = []

                column_map_index = key_map.get('column')
                if column_map_index and isinstance(data_section[column_map_index], dict):
                    columns_list_index = data_section[column_map_index].get('columns')
                    if columns_list_index: headers = [data_section[i]['t'] for i in data_section[columns_list_index]]
                
                if not headers:
                    fy_index, fq_index = financial_data_map.get('fiscalYear'), financial_data_map.get('fiscalQuarter')
                    if fy_index and fq_index:
                        years = [data_section[i] for i in data_section[fy_index]]
                        quarters = [data_section[i] for i in data_section[fq_index]]
                        if len(quarters) == 1 and len(years) > 1: quarters *= len(years)
                        headers = [f"{q} {y}" for q, y in zip(quarters, years)]

                if not headers: continue

                item_titles = {data_section[e['id']]: data_section[e['title']] for e in data_section if isinstance(e, dict) and 'id' in e and 'title' in e}
                
                for item_id, data_pointer in financial_data_map.items():
                    scraped_item_name = item_titles.get(item_id)
                    if not scraped_item_name or "ttm" in scraped_item_name.lower(): continue

                    if not (isinstance(data_pointer, int) and data_pointer < len(data_section)): continue
                    value_pointers = data_section[data_pointer]
                    if not isinstance(value_pointers, list): continue

                    for i, period_header_text in enumerate(headers):
                        if i >= len(value_pointers): continue
                        
                        try:
                            # *** THIS IS THE CRITICAL FIX ***
                            # 1. Get the index that points to the actual value
                            value_index = value_pointers[i]
                            if not (isinstance(value_index, int) and value_index < len(data_section)): continue

                            # 2. Use the index to get the value object from the main data list
                            value_obj = data_section[value_index]
                            if value_obj is None: continue

                            # 3. Extract the float value
                            numeric_value = float(value_obj[0] if isinstance(value_obj, list) else value_obj)
                            if pd.isna(numeric_value): continue
                            # *** END CRITICAL FIX ***

                            match = re.search(r'([A-Z0-9]+)\s(\d{4})', period_header_text)
                            if not match: continue
                            
                            fiscal_period, fiscal_year_str = match.groups()
                            all_rows.append({
                                'item': scraped_item_name, 'header': f"{fiscal_period}_{int(fiscal_year_str)}",
                                'original_value': numeric_value, 'fiscal_year': int(fiscal_year_str),
                                'fiscal_period': fiscal_period, 'statement_type': stmt_info['display_name'],
                                'filing_type': 'Scraped-JSON'
                            })
                        except (ValueError, TypeError, IndexError):
                            continue
            except Exception as e:
                logging.error(f"Critical error fetching {stmt_info['display_name']} for {symbol}: {e}", exc_info=False)

    return pd.DataFrame(all_rows)
# ==============================================================================
# DATABASE FUNCTIONS
# ==============================================================================
def create_and_insert_data(df_to_insert, symbol, company_name, company_metadata):
    """
    Drops and recreates the table for a single company, then inserts the processed data.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema};").format(schema=sql.Identifier(SCHEMA_NAME)))

        table_name_str = symbol.lower().replace('.', '_')
        full_table_name = sql.Identifier(SCHEMA_NAME, table_name_str)

        logging.debug(f"Dropping table {full_table_name.as_string(conn)} if it exists to ensure fresh schema.")
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {table_name} CASCADE;").format(table_name=full_table_name))

        # --- REVISED SECTION: New column order and added 'sort_order_metric' ---
        create_table_query = sql.SQL("""
            CREATE TABLE {table_name} (
                symbol TEXT,
                company_name TEXT,
                sector TEXT,
                industry TEXT,
                market_cap_group TEXT,
                statement_type TEXT,
                item TEXT,
                header TEXT,
                original_value NUMERIC,
                original_currency TEXT,
                forex_rate_vs_usd NUMERIC,
                value NUMERIC,
                sort_order_item INTEGER,
                sort_order_metric INTEGER,
                sort_key INTEGER,
                extracted_order INTEGER,
                period_date DATE,
                filing_type TEXT,
                country TEXT,
                statement_sort_order INTEGER, -- <-- ADD THIS LINE,
                PRIMARY KEY (symbol, statement_type, item, header)
            );
        """).format(table_name=full_table_name)
        cur.execute(create_table_query)

        df_to_insert.drop_duplicates(subset=['symbol', 'statement_type', 'item', 'header'], keep='first', inplace=True)

        insert_data = []
        for _, row in df_to_insert.iterrows():
            original_val = pd.to_numeric(row.get('original_value'), errors='coerce')
            final_val_usd = pd.to_numeric(row.get('value'), errors='coerce')
            forex_rate = pd.to_numeric(row.get('forex_rate_vs_usd'), errors='coerce')
            symbol_lower = str(row.get('symbol', '')).lower()

            # Reorder the tuple to match the new table structure
            insert_data.append((
                symbol_lower,
                row.get('company_name'),
                company_metadata.get('sector'),
                company_metadata.get('industry'),
                company_metadata.get('market_cap_group'),
                row.get('statement_type'),
                row.get('item'),
                row.get('header'),
                None if pd.isna(original_val) else original_val,
                row.get('original_currency'),
                None if pd.isna(forex_rate) else forex_rate,
                None if pd.isna(final_val_usd) else final_val_usd,
                row.get('sort_order_item'),
                row.get('sort_order_metric', 0),  # Default to 0 if not present
                row.get('sort_key'),
                row.get('extracted_order'),
                row.get('period_date'),
                row.get('filing_type'),
                company_metadata.get('country'),
                row.get('statement_sort_order')
            ))

        # Update column list in INSERT query to match the new order
        insert_query = sql.SQL("""
            INSERT INTO {table_name} (
                symbol, company_name, sector, industry, market_cap_group,
                statement_type, item, header, original_value, original_currency,
                forex_rate_vs_usd, value, sort_order_item, sort_order_metric,
                sort_key, extracted_order, period_date, filing_type, country, statement_sort_order
            ) VALUES %s;
        """).format(table_name=full_table_name)
        # --- END REVISED SECTION ---

        if insert_data:
            execute_values(cur, insert_query, insert_data, page_size=500)
            logging.info(f"Successfully inserted {len(insert_data)} rows into {full_table_name.as_string(conn)}.")
        else:
            logging.info(f"No data to insert for {full_table_name.as_string(conn)}.")

    except Exception as e:
        logging.error(f"Database error for {symbol}: {e}", exc_info=True)
    finally:
        if conn:
            if 'cur' in locals() and cur: cur.close()
            conn.close()


def create_aggregate_table(conn, cursor, processed_symbols_info):
    """
    Drops and recreates the main public aggregate table from all individual company tables.
    This version is adapted from the stable standalone script.
    """
    aggregate_table_name = f"aggregate_table_{SCHEMA_NAME}"
    full_aggregate_table_id = sql.Identifier("public", aggregate_table_name)
    logging.info(f"\n--- Recreating aggregate table: {full_aggregate_table_id.as_string(conn)} ---")

    # --- Step 1: Drop all dependent materialized views first ---
    logging.info("Dropping all potentially dependent materialized views...")

    # Drop the main public MVs
    yoy_change_mv_name = sql.Identifier("public", f"{SCHEMA_NAME}_financial_statement_yoy_changes")
    pop_change_mv_name = sql.Identifier("public", f"{SCHEMA_NAME}_financial_statement_changes")
    cursor.execute(sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {mv};").format(mv=yoy_change_mv_name))
    cursor.execute(sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {mv};").format(mv=pop_change_mv_name))

    # Drop the company-specific wide MVs
    stmt_slugs = ["bs_wide", "inc_wide", "comp_inc_wide", "cf_wide", "eq_chg_wide", "supp_disc_wide", "supp_cf_wide"]
    for company in processed_symbols_info:
        ticker_clean = company['symbol'].lower().replace('.', '_')
        for slug in stmt_slugs:
            mv_name = sql.Identifier(SCHEMA_NAME, f"{ticker_clean}_{slug}")
            cursor.execute(sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {mv};").format(mv=mv_name))

    conn.commit()
    logging.info("Finished dropping all dependent MVs.")

    # --- Step 2: Drop and recreate the aggregate table ---
    logging.info("Recreating the aggregate table structure.")
    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {table_name} CASCADE;").format(table_name=full_aggregate_table_id))

    union_queries = []
    for company in processed_symbols_info:
        individual_table_name = company['symbol'].lower().replace('.', '_')
        individual_table_id = sql.Identifier(SCHEMA_NAME, individual_table_name)
        union_queries.append(sql.SQL("SELECT * FROM {individual_table}").format(individual_table=individual_table_id))

    if not union_queries:
        logging.error("No individual company tables found to aggregate.")
        return False

    full_union_query = sql.SQL(" UNION ALL ").join(union_queries)
    create_table_sql = sql.SQL("CREATE TABLE {agg_table_name} AS ({union_query});").format(
        agg_table_name=full_aggregate_table_id,
        union_query=full_union_query
    )
    cursor.execute(create_table_sql)
    conn.commit()
    logging.info(f"Aggregate table '{full_aggregate_table_id.as_string(conn)}' created successfully.")
    return True


def create_materialized_views(conn, cursor, processed_symbols_info):
    """
    Orchestrates creation of all materialized views.
    1. Creates company-specific wide MVs in parallel.
    2. Creates sequential (period-over-period) change MV.
    3. Creates year-over-year change MV.
    """
    aggregate_table_id = sql.Identifier("public", f"aggregate_table_{SCHEMA_NAME}")

    # --- Part 1: Parallel creation of company-specific wide MVs ---
    logging.info(f"\n--- Starting PARALLEL creation of {len(processed_symbols_info)} sets of company-wide MVs ---")
    num_processes = max(1, multiprocessing.cpu_count() - 2)
    logging.info(f"Creating a pool of {num_processes} worker processes...")

    # Use partial to pre-fill arguments for the worker function
    task_func = partial(create_wide_views_for_ticker_worker, db_params=DB_PARAMS, schema_name=SCHEMA_NAME)

    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(task_func, processed_symbols_info)
    logging.info("--- Finished parallel creation of company-specific wide MVs. ---")

    # --- Part 2: Sequential (Period-over-Period) Percent Change MV ---
    logging.info(f"\n--- Creating Sequential (Period-over-Period) Change MV for {SCHEMA_NAME} ---")
    pop_mv_name = sql.Identifier("public", f"{SCHEMA_NAME}_financial_statement_changes")
    try:
        cursor.execute(sql.SQL('DROP MATERIALIZED VIEW IF EXISTS {mv_name};').format(mv_name=pop_mv_name))

        create_pop_sql = sql.SQL("""
            CREATE MATERIALIZED VIEW {mv_name} AS
            WITH lagged_values AS (
                SELECT
                    t.*,
                    lag(t.value, 1) OVER (
                        PARTITION BY t.symbol, t.statement_type, t.item
                        ORDER BY t.period_date
                    ) AS previous_period_value
                FROM {agg_table} t
                WHERE t.statement_type IN ('Income Statement', 'Cash Flow Statement', 'Comprehensive Income')
            )
            SELECT
                symbol, company_name, sector, industry, statement_type, item AS financial_metric,
                header AS period, period_date, value AS current_value, previous_period_value,
                round(((value - previous_period_value) / abs(NULLIF(previous_period_value, 0))) * 100, 4) AS sequential_percent_change,
                sort_order_item,
                sort_order_metric,
                sort_key,
                extracted_order,
                statement_sort_order
            FROM lagged_values
            WHERE previous_period_value IS NOT NULL AND value IS NOT NULL
            ORDER BY symbol, item, period_date;
        """).format(mv_name=pop_mv_name, agg_table=aggregate_table_id)
        cursor.execute(create_pop_sql)
        logging.info(f"Sequential PoP MV '{pop_mv_name.as_string(conn)}' created.")
    except Exception as e:
        logging.error(f"Error creating Sequential PoP MV: {e}", exc_info=True)

    # --- Part 3: Year-over-Year Percent Change MV ---
    logging.info(f"\n--- Creating Year-over-Year (YoY) Change MV for {SCHEMA_NAME} ---")
    yoy_mv_name = sql.Identifier("public", f"{SCHEMA_NAME}_financial_statement_yoy_changes")
    try:
        cursor.execute(sql.SQL('DROP MATERIALIZED VIEW IF EXISTS {mv_name};').format(mv_name=yoy_mv_name))

        create_yoy_sql = sql.SQL("""
            CREATE MATERIALIZED VIEW {mv_name} AS
            WITH parsed_periods AS (
                SELECT t.*, SUBSTRING(t.header FROM '^[A-Z0-9]+') AS period_type
                FROM {agg_table} t
                WHERE t.statement_type IN ('Income Statement', 'Cash Flow Statement', 'Comprehensive Income')
            ),
            lagged_values AS (
                SELECT
                    p.*,
                    lag(p.value, 1) OVER (
                        PARTITION BY p.symbol, p.statement_type, p.item, p.period_type
                        ORDER BY p.period_date
                    ) AS previous_year_value
                FROM parsed_periods p
            )
            SELECT
                symbol, company_name, sector, industry, statement_type, item AS financial_metric,
                header AS period, period_date, value AS current_value, previous_year_value,
                round(((value - previous_year_value) / abs(NULLIF(previous_year_value, 0))) * 100, 4) AS yoy_percent_change,
                sort_order_item,
                sort_order_metric,
                sort_key,
                extracted_order,
                statement_sort_order
            FROM lagged_values
            WHERE previous_year_value IS NOT NULL AND value IS NOT NULL
            ORDER BY symbol, item, period_date;
        """).format(mv_name=yoy_mv_name, agg_table=aggregate_table_id)
        cursor.execute(create_yoy_sql)
        logging.info(f"YoY MV '{yoy_mv_name.as_string(conn)}' created.")
    except Exception as e:
        logging.error(f"Error creating YoY MV: {e}", exc_info=True)

    conn.commit()


def create_wide_views_for_ticker_worker(ticker_info, db_params, schema_name):
    """
    Worker function to create wide-format materialized views for a single company.
    This function is designed to be called by a multiprocessing Pool.
    It connects to the database, generates, and executes the SQL for the MVs.
    """
    # --- CHANGE 1: Unpack the new metadata from the ticker_info dictionary ---
    symbol = ticker_info['symbol']
    company_name = ticker_info['company_name']
    sector = ticker_info.get('sector', 'N/A')
    industry = ticker_info.get('industry', 'N/A')
    market_cap_group = ticker_info.get('market_cap_group', 'N/A')

    conn = None
    try:
        # Each worker process must establish its own database connection.
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        cursor = conn.cursor()

        logging.info(f"WORKER for '{symbol}': Starting wide MV creation.")

        aggregate_table_id = sql.Identifier("public", f"aggregate_table_{schema_name}")
        ticker_clean_str = symbol.lower().replace('.', '_')

        # Define the financial statements to create views for.
        statement_types_for_mv = [
            {"display_name": "Balance Sheet", "slug": "bs_wide"},
            {"display_name": "Income Statement", "slug": "inc_wide"},
            {"display_name": "Comprehensive Income", "slug": "comp_inc_wide"},
            {"display_name": "Cash Flow Statement", "slug": "cf_wide"},
            {"display_name": "Equity Changes", "slug": "eq_chg_wide"},
            {"display_name": "Supplemental Disclosures", "slug": "supp_disc_wide"},
            {"display_name": "Supplemental Cash Flow", "slug": "supp_cf_wide"}
        ]

        for stmt_info in statement_types_for_mv:
            statement_type_display = stmt_info['display_name']
            mv_name_str = f"{ticker_clean_str}_{stmt_info['slug']}"
            full_mv_name = sql.Identifier(schema_name, mv_name_str)

            # Fetch the headers (e.g., Q1_2023, Q2_2023) for the current statement type
            header_query = sql.SQL("""
                SELECT DISTINCT header FROM {agg_table}
                WHERE lower(symbol) = %s AND statement_type = %s;
            """).format(agg_table=aggregate_table_id)
            cursor.execute(header_query, (symbol.lower(), statement_type_display))

            headers = [row[0] for row in cursor.fetchall()]
            if not headers:
                logging.debug(f"WORKER for '{symbol}': No data for '{statement_type_display}', skipping MV.")
                continue

            # Sort headers chronologically (most recent first)
            def sort_key_func(h):
                parts = h.split('_')
                if len(parts) != 2 or not parts[1].isdigit(): return (0, 0)
                year = int(parts[1])
                period_map = {'Q1': 1, 'Q2': 2, 'H1': 2.5, 'Q3': 3, 'Q4': 4, 'H2': 4.5, 'FY': 5}
                period = period_map.get(parts[0], 99)
                return (year, period)

            ordered_headers = sorted(headers, key=sort_key_func, reverse=True)

            # Define the columns for the crosstab function (pivoted table)
            crosstab_defs_list = ["item TEXT", "sort_order_item INTEGER", "sort_order_metric INTEGER"] + \
                                 [sql.SQL("{h} NUMERIC").format(h=sql.Identifier(h)).as_string(conn) for h in ordered_headers]
            crosstab_defs_sql = sql.SQL(", ").join(map(sql.SQL, crosstab_defs_list))

            # SQL to get the source data for pivoting
            source_sql_str = cursor.mogrify(sql.SQL("""
                SELECT item, sort_order_item, sort_order_metric, header, value
                FROM {agg_table}
                WHERE lower(symbol) = %s AND statement_type = %s
                ORDER BY 1, 2, 3;
            """).format(agg_table=aggregate_table_id), (symbol.lower(), statement_type_display)).decode('utf-8')

            # SQL to get the category columns for pivoting
            category_sql_str = cursor.mogrify("SELECT unnest(%s::text[])", (ordered_headers,)).decode('utf-8')

            # --- CHANGE 2: Add the new columns to the SELECT statement ---
            final_query = sql.SQL("""
                CREATE MATERIALIZED VIEW {mv_name} AS
                SELECT
                    %s AS symbol,
                    %s AS company_name,
                    %s AS sector,
                    %s AS industry,
                    %s AS market_cap_group,
                    ct.item,
                    ct.sort_order_item,
                    ct.sort_order_metric,
                    {header_cols}
                FROM crosstab(
                    {source_sql},
                    {category_sql}
                ) AS ct({crosstab_defs})
                ORDER BY ct.sort_order_item, ct.sort_order_metric;
            """).format(
                mv_name=full_mv_name,
                header_cols=sql.SQL(', ').join(map(sql.Identifier, ordered_headers)),
                source_sql=sql.Literal(source_sql_str),
                category_sql=sql.Literal(category_sql_str),
                crosstab_defs=crosstab_defs_sql
            )

            # Drop the old MV if it exists and create the new one
            cursor.execute(sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {mv_name};").format(mv_name=full_mv_name))

            # --- CHANGE 3: Pass the new values as parameters to the execute call ---
            cursor.execute(final_query, (symbol, company_name, sector, industry, market_cap_group))

        logging.info(f"WORKER: Successfully created all wide MVs for {symbol}.")

    except psycopg2.Error as e:
        logging.error(f"WORKER DB ERROR for ticker {symbol}: {e}")
    except Exception as e:
        logging.error(f"WORKER unhandled exception for {symbol}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


# ==============================================================================
# MAIN WORKER FUNCTION (FINALIZED)
# ==============================================================================
def process_company_worker(company_info, forex_rates_df, sa_rate_limiter, edgar_rate_limiter, failed_ciks_lock, failed_ciks_list):
    symbol = company_info['symbol']
    company_name = company_info['company_name']
    cik_code = company_info.get('cik_code')
    logging.info(f"--- Starting process for {symbol} ---")

    # Step 1: Fetch and Combine Data
    edgar_df, taxonomy = get_company_facts_data(cik_code, edgar_rate_limiter)
    
    scraped_df = pd.DataFrame()
    # Logic to decide the data strategy
    if not edgar_df.empty and taxonomy == 'us-gaap' and has_sufficient_quarterly_data(edgar_df.copy()):
        final_df = edgar_df
        logging.info(f"Strategy for {symbol}: ALL EDGAR.")
    else:
        fetch_period = 'Q_ONLY' if not edgar_df.empty else 'ALL'
        log_msg = 'HYBRID' if not edgar_df.empty else 'FULL FALLBACK'
        logging.info(f"Strategy for {symbol}: {log_msg}.")
        scraped_df = fetch_stockanalysis_data(symbol, sa_rate_limiter, periods_to_fetch=fetch_period)
        
        # FIX: Add symbol to scraped data before concat to prevent KeyError
        if not scraped_df.empty:
            scraped_df['symbol'] = symbol 
            scraped_df['original_currency'] = fetch_company_profile_currency(symbol, sa_rate_limiter) or 'USD'
        
        combined_df = pd.concat([edgar_df, scraped_df], ignore_index=True)
        if not combined_df.empty:
            combined_df['source_priority'] = combined_df['filing_type'].apply(lambda x: 1 if str(x) != 'Scraped-JSON' else 2)
            final_df = combined_df.sort_values(by='source_priority').drop_duplicates(subset=['symbol', 'item', 'header'], keep='first')
            final_df.drop(columns=['source_priority'], inplace=True)
        else:
            final_df = pd.DataFrame()
    
    if final_df.empty:
        logging.warning(f"No data could be retrieved for {symbol}. Skipping.")
        return None

    # Step 2: Main Processing Pipeline
    final_df.reset_index(drop=True, inplace=True)
    
    # FIX: Ensure company name and symbol are always present on the DataFrame
    final_df['symbol'] = symbol
    final_df['company_name'] = company_name

    # --- 2a. Normalize and Attach Metadata ---
    final_df['item'] = final_df['item'].apply(lambda x: STOCKANALYSIS_TO_STANDARD_MAP.get(normalize_item_name(x), x))
    meta_df = final_df['item'].apply(resolve_item_metadata).apply(pd.Series)
    
    cols_from_meta = ['official_item_name', 'statement_type', 'sort_order_item', 'statement_sort_order']
    cols_to_drop = ['item'] + [col for col in cols_from_meta if col in final_df.columns and col != 'item']
    final_df = pd.concat([final_df.drop(columns=cols_to_drop, errors='ignore'), meta_df], axis=1).rename(columns={'official_item_name': 'item'})
    
    # --- FIX: Ensure sort_order_metric is initialized and cleaned ---
    if 'sort_order_metric' not in final_df.columns:
        final_df['sort_order_metric'] = 0
    final_df['sort_order_metric'] = final_df['sort_order_metric'].fillna(0)

    # --- 2b. Derive Dates & Process Periods ---
    final_df['period_date'] = final_df.apply(lambda r: pd.to_datetime(r.get('period_date')) if pd.notna(r.get('period_date')) else datetime(int(r['fiscal_year']), {'Q1':3,'Q2':6,'Q3':9,'Q4':12,'H1':6,'H2':12,'FY':12}.get(r['fiscal_period'], 12), {'Q1':31,'Q2':30,'Q3':30,'Q4':31,'H1':30,'H2':31,'FY':31}.get(r['fiscal_period'], 31)).date(), axis=1)
    final_df.dropna(subset=['period_date', 'item'], inplace=True)
    if final_df.empty: return None

    # --- 2c. Run Calculations & Final Processing ---
    final_df = process_quarterly_data(final_df)
    final_df = relabel_semi_annual_periods(final_df)
    # ** NEW STEP **: Derive Q4 data before calculating ratios
    final_df = derive_q4_from_fy(final_df) 
    final_df = derive_and_align_fy_data(final_df)
    final_df = calculate_and_insert_subtotals(final_df)
    final_df = calculate_and_insert_analytical_ratios(final_df)
    
    # --- 2d. Forex Conversion & Share Count Fix ---
    share_count_keys = build_synonym_set({"Weighted-average shares: Basic", "Weighted-average shares: Diluted", "Entity common stock shares outstanding"})
    final_df.loc[final_df['item'].apply(normalize_item_name).isin(share_count_keys), 'original_currency'] = 'SHARES'
    
    if not forex_rates_df.empty:
        final_df['year_month'] = pd.to_datetime(final_df['period_date']).dt.strftime('%Y-%m')
        final_df = pd.merge(final_df, forex_rates_df, left_on=['year_month', 'original_currency'], right_on=['year_month', 'currency_code'], how='left')
    
    final_df['forex_rate_vs_usd'] = final_df.apply(lambda r: 1.0 if r.get('original_currency') == 'USD' else r.get('rate_to_usd', 1.0), axis=1).fillna(1.0)
    final_df['value'] = final_df.apply(lambda r: r['original_value'] if (r.get('item') in PERCENTAGE_METRICS or r.get('original_currency') == 'SHARES') else round(pd.to_numeric(r['original_value'], errors='coerce') * r['forex_rate_vs_usd'], -1) if pd.notna(r['original_value']) else None, axis=1)
    
    # --- 2e. Final Sort ---
    final_df = apply_final_sorting(final_df)
    
    # Step 3: Load to Database
    create_and_insert_data(final_df, symbol, company_name, company_info)
    return symbol

def main():
    # --- Stage 1: Initial Setup ---
    companies_df = fetch_companies_by_sector(SECTOR_TO_PROCESS)
    if companies_df.empty:
        logging.error(f"No companies found for sector '{SECTOR_TO_PROCESS}'. Halting.")
        return

    companies_to_process = companies_df.to_dict('records')

    # --- CORRECTED FOREX LOGIC ---
    all_known_currencies = FRED_SERIES_ID_MAP.keys()
    currency_map_for_fred = {
        currency: FRED_SERIES_ID_MAP.get(currency)
        for currency in all_known_currencies if currency != 'USD'
    }
    forex_rates_df = fetch_historical_monthly_forex_rates_fred(FRED_API_KEY, currency_map_for_fred)
    if forex_rates_df.empty:
        logging.warning("Could not fetch any forex data from FRED. All values will be treated as USD.")

    # --- Stage 2: Concurrent Processing with Rate Limiters ---
    failed_ciks_lock = threading.Lock()
    failed_ciks_list = []
    processed_symbols = []

    sa_rate_limiter = RateLimiter(STOCKANALYSIS_API_CONFIG['REQUESTS_PER_UNIT'], STOCKANALYSIS_API_CONFIG['UNIT_IN_SECONDS'])
    edgar_rate_limiter = RateLimiter(EDGAR_API_CONFIG['REQUESTS_PER_UNIT'], EDGAR_API_CONFIG['UNIT_IN_SECONDS'])

    task_func = partial(process_company_worker,
                          forex_rates_df=forex_rates_df,
                          sa_rate_limiter=sa_rate_limiter,
                          edgar_rate_limiter=edgar_rate_limiter,
                          failed_ciks_lock=failed_ciks_lock,
                          failed_ciks_list=failed_ciks_list)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='FS_Worker') as executor:
        futures = {executor.submit(task_func, company): company for company in companies_to_process}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            company_info = futures[future]
            symbol = company_info['symbol']
            logging.info(f"--- Main thread processing result for {symbol} ({i + 1}/{len(companies_to_process)}) ---")
            try:
                # Check the return value from the worker
                result = future.result()
                if result is not None:
                    # Only append to the list if the worker returned a success signal
                    processed_symbols.append(symbol)
            except Exception as e:
                logging.error(f"--- Top-level error for {symbol} in worker thread: {e} ---", exc_info=True)

    # --- Stage 3 & 4: Final Database Operations & Reporting ---
    # Now this list will ONLY contain symbols for which a table was actually created
    processed_symbols_info = [
        info for info in companies_to_process
        if info['symbol'] in processed_symbols
    ]

    if not processed_symbols_info:
        logging.warning("No companies were successfully processed. Skipping final aggregation.")
    else:
        conn = None
        try:
            logging.info("\n--- STAGE 3: All fetching complete. Starting final aggregation and view creation. ---")
            conn = psycopg2.connect(**DB_PARAMS)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            if create_aggregate_table(conn, cursor, processed_symbols_info):
                create_materialized_views(conn, cursor, processed_symbols_info)
            else:
                logging.error("Aggregation failed. Halting MV creation.")
        except Exception as e:
            logging.error(f"An error occurred during final database operations: {e}", exc_info=True)
        finally:
            if conn:
                if 'cursor' in locals() and cursor and not cursor.closed:
                    cursor.close()
                conn.close()

    if failed_ciks_list:
        logging.warning("\n" + "=" * 80)
        logging.warning("--- EDGAR CIK FETCH FAILURE REPORT ---")
        logging.warning("The following companies could not be found on EDGAR using the discovered CIK:")
        for failure in sorted(failed_ciks_list, key=lambda x: x['symbol']):
            logging.warning(f" - Symbol: {failure['symbol']:<8} | Company: {failure['company_name']:<40} | Failed CIK: {failure['cik_code']}")
        logging.warning("=" * 80)
    else:
        logging.info("\n--- All discovered CIKs successfully queried against EDGAR. ---")

    logging.info("\nScript finished. All resources closed.")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()