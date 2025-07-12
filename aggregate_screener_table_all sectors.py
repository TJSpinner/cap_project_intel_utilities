import requests
import pandas as pd
import time
import logging
import concurrent.futures
import threading
import psycopg2
import psycopg2.extras
import numpy as np

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_PARAMS = {
    "host": "localhost",
    "database": "utilities_cap_intel",
    "user": "postgres",
    "password": "Mondo@Mil!Pie19",
    "port": "5433"
}

# --- Table and Schema Names ---
SCHEMA_NAME = "public"
WIDE_TABLE_NAME = "aggregate_screener_table"
VERTICAL_TABLE_NAME = "agg_screener_table_vertical"
MATERIALIZED_VIEW_NAME = "screener_analytics_summary"

# --- Fetching Configuration ---
METRICS_PER_BATCH = 24
REQUESTS_PER_MINUTE = 12
MAX_WORKERS = 10

# --- Data Configuration ---
METRIC_ABBREVIATIONS = [
    'name', 'sector', 'industry', 'peRatio', 'marketCapCategory', 'enterpriseValue', 'marketCap', 'close', 'change', 
    'volume', 'dividendYield', 'dps', 'lastDividend', 'dividendGrowth', 'dividendGrowthYears', 'divCAGR3', 'divCAGR5', 
    'ch1w', 'ch1m', 'ch3m', 'peForward', 'exchange', 'ch6m', 'chYTD', 'ch1y', 'ch3y', 'ch5y', 'ch10y', 'ch15y', 'ch20y', 
    'tr1w', 'tr1m', 'tr3m', 'tr6m', 'trYTD', 'tr1y', 'tr3y', 'tr5y', 'tr10y', 'tr15y', 'tr20y', 'cagr1y', 'cagr3y', 
    'cagr5y', 'cagr10y', 'cagr15y', 'cagr20y', 'low52ch', 'high52ch', 'allTimeHigh', 'allTimeHighChange', 
    'allTimeHighDate', 'allTimeLow', 'allTimeLowChange', 'allTimeLowDate', 'dollarVolume', 'analystRatings', 
    'analystCount', 'priceTarget', 'priceTargetChange', 'open', 'low', 'high', 'premarketPrice', 
    'premarketChangePercent', 'premarketVolume', 'preClose', 'postmarketPrice', 'postmarketChangePercent', 'postClose', 
    'low52', 'high52', 'country', 'employees', 'employeesChange', 'employeesChangePercent', 'founded', 'ipoDate', 
    'lastReportDate', 'fiscalYearEnd', 'last10kFilingDate', 'revenue', 'revenueGrowth', 'revenueGrowthQ', 
    'revenueGrowth3Y', 'revenueGrowth5Y', 'revenueGrowthYears', 'revenueGrowthQuarters', 'grossProfit', 
    'grossProfitGrowth', 'grossProfitGrowthQ', 'grossProfitGrowth3Y', 'grossProfitGrowth5Y', 'operatingIncome', 
    'operatingIncomeGrowth', 'operatingIncomeGrowthQ', 'operatingIncomeGrowth3Y', 'operatingIncomeGrowth5Y', 
    'netIncome', 'netIncomeGrowth', 'netIncomeGrowthQ', 'netIncomeGrowth3Y', 'netIncomeGrowth5Y', 
    'netIncomeGrowthYears', 'netIncomeGrowthQuarters', 'incomeTax', 'eps', 'epsGrowth', 'epsGrowthQ', 'epsGrowth3Y', 
    'epsGrowth5Y', 'epsGrowthYears', 'epsGrowthQuarters', 'ebit', 'ebitda', 'operatingCF', 'shareBasedComp', 
    'sbcByRevenue', 'investingCF', 'financingCF', 'netCF', 'capex', 'fcf', 'adjustedFCF', 'fcfPerShare', 'fcfGrowth', 
    'fcfGrowthQ', 'fcfGrowth3Y', 'fcfGrowth5Y', 'cash', 'debt', 'debtGrowth', 'debtGrowthQoQ', 'debtGrowth3Y', 
    'debtGrowth5Y', 'netCash', 'netCashGrowth', 'netCashByMarketCap', 'assets', 'liabilities', 'grossMargin', 
    'operatingMargin', 'pretaxMargin', 'profitMargin', 'fcfMargin', 'ebitdaMargin', 'ebitMargin', 
    'researchAndDevelopment', 'rndByRevenue', 'psRatio', 'psForward', 'pbRatio', 'pFcfRatio', 'pOcfRatio', 'pegRatio', 
    'evSales', 'evSalesForward', 'evEarnings', 'evEbitda', 'evEbit', 'evFcf', 'priceEbitda', 'earningsYield', 
    'fcfYield', 'fcfEvYield', 'payoutRatio', 'payoutFrequency', 'buybackYield', 'totalReturn', 'averageVolume', 
    'relativeVolume', 'daysGap', 'changeFromOpen', 'beta', 'rsi', 'rsiWeekly', 'rsiMonthly', 'atr', 'shortFloat', 
    'shortShares', 'shortRatio', 'sharesOut', 'float', 'sharesYoY', 'sharesQoQ', 'sharesInsiders', 
    'sharesInstitutions', 'earningsDate', 'lastEarningsDate', 'nextEarningsDate', 'earningsTime', 
    'earningsRevenueEstimate', 'earningsRevenueEstimateGrowth', 'earningsEpsEstimate', 'earningsEpsEstimateGrowth', 
    'roe', 'roa', 'roic', 'roce', 'roe5y', 'roa5y', 'roic5y', 'revPerEmployee', 'profitPerEmployee', 
    'assetTurnover', 'inventoryTurnover', 'currentRatio', 'quickRatio', 'debtEquity', 'debtEbitda', 'debtFcf', 
    'taxRate', 'taxByRevenue', 'equity', 'bvPerShare', 'tangibleBookValue', 'tangibleBookValuePerShare', 'ptbvRatio', 
    'peRatio3Y', 'peRatio5Y', 'workingCapital', 'netWorkingCapital', 'workingCapitalTurnover', 'lastSplitType', 
    'lastSplitDate', 'zScore', 'fScore', 'epsThisQuarter', 'epsNextQuarter', 'epsThisYear', 'epsNextYear', 
    'eps5y', 'revenue5y', 'inIndex', 'tags', 'ipr', 'iprfo', 'ma20', 'ma50', 'ma150', 'ma200', 'ma20ch', 'ma50ch', 'ma150ch', 'ma200ch', 
    'ma50vs200', 'sic', 'cik', 'isin', 'cusip', 'website', 'lynchUpside', 'grahamUpside'
]
METRIC_ABBREVIATIONS = sorted(list(set(METRIC_ABBREVIATIONS)))

METRIC_MAP = {
    'name':'company_name','sector':'sector','industry':'industry','marketCapCategory':'market_cap_group',
    'country':'country','exchange':'exchange','sic':'sic_code','cik':'cik_code','isin':'isin','cusip':'cusip','website':'website',
    'founded':'founded','ipoDate':'ipo_date','inIndex':'in_index','marketCap':'market_cap','enterpriseValue':'enterprise_value',
    'peRatio':'price_to_earnings_ratio','peForward':'fwd_pe_ratio','psRatio':'price_to_sales_ratio','psForward':'fwd_ps_ratio',
    'pbRatio':'price_to_book_ratio','priceEbitda':'price_to_ebitda_ratio','pegRatio':'peg_ratio','evSales':'ev_to_sales',
    'evSalesForward':'fwd_ev_to_sales','evEarnings':'ev_to_earnings','evEbitda':'ev_to_ebitda','evEbit':'ev_to_ebit',
    'evFcf':'ev_to_fcf','ptbvRatio':'price_to_tbv_ratio','peRatio3Y':'pe_ratio_3y_avg','peRatio5Y':'pe_ratio_5y_avg',
    'close':'price','change':'price_change_1d','ch1w':'price_change_1w','ch1m':'price_change_1m','ch3m':'price_change_3m',
    'ch6m':'price_change_6m','chYTD':'price_change_ytd','ch1y':'price_change_1y','ch3y':'price_change_3y','ch5y':'price_change_5y',
    'ch10y':'price_change_10y','ch15y':'price_change_15y','ch20y':'price_change_20y',
    'tr1w':'total_return_1w','tr1m':'total_return_1m','tr3m':'total_return_3m','tr6m':'total_return_6m',
    'trYTD':'total_return_ytd','tr1y':'total_return_1y','tr3y':'total_return_3y','tr5y':'total_return_5y',
    'tr10y':'total_return_10y','tr15y':'total_return_15y','tr20y':'total_return_20y',
    'cagr1y':'cagr_1y','cagr3y':'cagr_3y','cagr5y':'cagr_5y','cagr10y':'cagr_10y','cagr15y':'cagr_15y','cagr20y':'cagr_20y',
    'ipr':'return_from_ipo','iprfo':'return_from_ipo_open','open':'open_price','high':'high_price','low':'low_price','preClose':'prev_close',
    'low52':'low_52w','high52':'high_52w','low52ch':'low_52w_change_pct','high52ch':'high_52w_change_pct',
    'allTimeHigh':'all_time_high','allTimeHighChange':'ath_change_pct','allTimeHighDate':'ath_date',
    'allTimeLow':'all_time_low','allTimeLowChange':'atl_change_pct','allTimeLowDate':'atl_date',
    'volume':'volume','dollarVolume':'dollar_volume','averageVolume':'avg_volume','relativeVolume':'relative_volume',
    'ma20':'ma_20d','ma50':'ma_50d','ma150':'ma_150d','ma200':'ma_200d','ma20ch':'ma_20d_change_pct',
    'ma50ch':'ma_50d_change_pct','ma150ch':'ma_150d_change_pct','ma200ch':'ma_200d_change_pct','ma50vs200':'ma50_vs_200d',
    'beta':'beta_5y','rsi':'rsi_14d','rsiWeekly':'rsi_14w','rsiMonthly':'rsi_14m','atr':'atr_14',
    'dividendYield':'dividend_yield','dps':'dividend_per_share','lastDividend':'last_dividend','dividendGrowth':'dividend_growth',
    'dividendGrowthYears':'div_growth_years','divCAGR3':'div_cagr_3y','divCAGR5':'div_cagr_5y','payoutRatio':'payout_ratio',
    'payoutFrequency':'payout_frequency','buybackYield':'buyback_yield','totalReturn':'shareholder_yield',
    'revenueGrowth':'revenue_growth_yoy','revenueGrowthQ':'revenue_growth_qoq','revenueGrowth3Y':'revenue_growth_3y_cagr',
    'revenueGrowth5Y':'revenue_growth_5y_cagr','revenueGrowthYears':'revenue_growth_years_cons','revenueGrowthQuarters':'revenue_growth_qtrs_cons',
    'grossProfitGrowth':'gp_growth_yoy','grossProfitGrowthQ':'gp_growth_qoq','grossProfitGrowth3Y':'gp_growth_3y_cagr',
    'grossProfitGrowth5Y':'gp_growth_5y_cagr','operatingIncomeGrowth':'op_income_growth_yoy','operatingIncomeGrowthQ':'op_income_growth_qoq',
    'operatingIncomeGrowth3Y':'op_income_growth_3y_cagr','operatingIncomeGrowth5Y':'op_income_growth_5y_cagr',
    'netIncomeGrowth':'net_income_growth_yoy','netIncomeGrowthQ':'net_income_growth_qoq','netIncomeGrowth3Y':'net_income_growth_3y_cagr',
    'netIncomeGrowth5Y':'net_income_growth_5y_cagr','netIncomeGrowthYears':'net_income_growth_years_cons',
    'netIncomeGrowthQuarters':'net_income_growth_qtrs_cons','epsGrowth':'eps_growth_yoy','epsGrowthQ':'eps_growth_qoq',
    'epsGrowth3Y':'eps_growth_3y_cagr','epsGrowth5Y':'eps_growth_5y_cagr','epsGrowthYears':'eps_growth_years_cons',
    'epsGrowthQuarters':'eps_growth_qtrs_cons','fcfGrowth':'fcf_growth_yoy','fcfGrowthQ':'fcf_growth_qoq',
    'fcfGrowth3Y':'fcf_growth_3y_cagr','fcfGrowth5Y':'fcf_growth_5y_cagr','debtGrowth':'debt_growth_yoy',
    'debtGrowthQoQ':'debt_growth_qoq','debtGrowth3Y':'debt_growth_3y_cagr','debtGrowth5Y':'debt_growth_5y_cagr',
    'netCashGrowth':'net_cash_growth','employeesChange':'employees_change','employeesChangePercent':'employees_growth_pct',
    'revenue':'revenue','grossProfit':'gross_profit','operatingIncome':'operating_income','netIncome':'net_income',
    'ebitda':'ebitda','ebit':'ebit','eps':'eps','operatingCF':'operating_cf','investingCF':'investing_cf',
    'financingCF':'financing_cf','netCF':'net_cf','capex':'capex','fcf':'fcf','adjustedFCF':'fcf_adjusted_for_sbc',
    'fcfPerShare':'fcf_per_share','shareBasedComp':'sbc','incomeTax':'income_tax','researchAndDevelopment':'r_and_d',
    'assets':'total_assets','liabilities':'total_liabilities','equity':'shareholder_equity','cash':'total_cash',
    'debt':'total_debt','netCash':'net_cash_debt','workingCapital':'working_capital','netWorkingCapital':'net_working_capital',
    'tangibleBookValue':'tangible_book_value','tangibleBookValuePerShare':'tbv_per_share','bvPerShare':'book_value_per_share',
    'grossMargin':'gross_margin','operatingMargin':'operating_margin','pretaxMargin':'pretax_margin',
    'profitMargin':'profit_margin','fcfMargin':'fcf_margin','ebitdaMargin':'ebitda_margin','ebitMargin':'ebit_margin',
    'sbcByRevenue':'sbc_to_revenue_pct','rndByRevenue':'randd_to_revenue_pct','taxByRevenue':'tax_to_revenue_pct','taxRate':'effective_tax_rate',
    'roe':'roe','roa':'roa','roic':'roic','roce':'roce','roe5y':'roe_5y_avg','roa5y':'roa_5y_avg','roic5y':'roic_5y_avg',
    'debtEquity':'debt_to_equity','debtEbitda':'debt_to_ebitda','debtFcf':'debt_to_fcf','currentRatio':'current_ratio',
    'quickRatio':'quick_ratio','assetTurnover':'asset_turnover','inventoryTurnover':'inventory_turnover',
    'workingCapitalTurnover':'wc_turnover','revPerEmployee':'revenue_per_employee','profitPerEmployee':'profit_per_employee',
    'earningsYield':'earnings_yield','fcfYield':'fcf_yield','fcfEvYield':'fcf_to_ev_yield',
    'zScore':'altman_z_score','fScore':'piotroski_f_score','sharesOut':'shares_outstanding','float':'shares_float',
    'sharesYoY':'shares_change_yoy','sharesQoQ':'shares_change_qoq','sharesInsiders':'shares_insiders_pct',
    'sharesInstitutions':'shares_institutions_pct','shortFloat':'short_float_pct','shortShares':'short_shares_pct',
    'shortRatio':'short_ratio','analystRatings':'analyst_rating','analystCount':'analyst_count','priceTarget':'price_target',
    'priceTargetChange':'price_target_upside_pct','earningsDate':'earnings_date','lastEarningsDate':'last_earnings_date',
    'nextEarningsDate':'next_earnings_date','earningsTime':'earnings_time','earningsRevenueEstimate':'est_revenue',
    'earningsRevenueEstimateGrowth':'est_revenue_growth_pct','earningsEpsEstimate':'est_eps','earningsEpsEstimateGrowth':'est_eps_growth_pct',
    'epsThisQuarter':'est_eps_growth_this_q_pct','epsNextQuarter':'est_eps_growth_next_q_pct','epsThisYear':'est_eps_growth_this_y_pct',
    'epsNextYear':'est_eps_growth_next_y_pct','eps5y':'est_eps_growth_next_5y_pa','revenueThisQuarter':'est_revenue_growth_this_q_pct',
    'revenueNextQuarter':'est_revenue_growth_next_q_pct','revenueThisYear':'est_revenue_growth_this_y_pct',
    'revenueNextYear':'est_revenue_growth_next_y_pct','revenue5y':'est_revenue_growth_next_5y_pa','grahamUpside':'graham_upside_pct',
    'lynchUpside':'lynch_upside_pct','changeFromOpen':'change_from_open','daysGap':'days_gap','employees':'employees','fiscalYearEnd':'fiscal_year_end','last10kFilingDate':'last_10k_filing_date',
    'lastReportDate':'last_report_date','lastSplitDate':'last_split_date','lastSplitType':'last_split_type','netCashByMarketCap':'net_cash_by_market_cap','pFcfRatio':'price_to_fcf_ratio',
    'pOcfRatio':'price_to_ocf_ratio','postClose':'postmarket_close','postmarketChangePercent':'postmarket_change_percent','postmarketPrice':'postmarket_price',
    'premarketChangePercent':'premarket_change_percent','premarketPrice':'premarket_price','premarketVolume':'premarket_volume','tags':'tags'
}


class RateLimiter:
    def __init__(self, requests_per_minute: int):
        self._lock = threading.Lock()
        self.rate = requests_per_minute / 60.0
        self.capacity = float(requests_per_minute)
        self.tokens = self.capacity
        self.last_refill_time = time.monotonic()
    def consume(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill_time
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_refill_time = now
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate
                logging.info(f"Rate limit reached. Waiting for {wait_time:.2f} seconds.")
                time.sleep(wait_time)
                self.tokens += ((time.monotonic() - self.last_refill_time) * self.rate)
                self.last_refill_time = time.monotonic()
            self.tokens -= 1


def get_browser_headers() -> dict:
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Referer": "https://stockanalysis.com/screener/",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "sec-ch-ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin"
    }


def fetch_screener_batch(metric_batch: list[str], rate_limiter: RateLimiter) -> pd.DataFrame | None:
    if not metric_batch: return None
    batch_string = "+".join(metric_batch)
    url = f"https://stockanalysis.com/api/screener/s/bd/{batch_string}.json"
    try:
        rate_limiter.consume()
        logging.info(f"Requesting batch of {len(metric_batch)} metrics starting with '{metric_batch[0]}'...")
        response = requests.get(url, headers=get_browser_headers(), timeout=90)
        response.raise_for_status()
        data = response.json().get('data', {}).get('data', {})
        if not data:
            logging.warning(f"No data returned for batch starting with '{metric_batch[0]}'.")
            return None
        df = pd.DataFrame.from_dict(data, orient='index')
        df.index.name = 'symbol'
        return df
    except Exception as e:
        logging.error(f"An unexpected error occurred processing batch '{metric_batch[0]}': {e}")
        return None


def create_and_load_vertical_table(df_wide: pd.DataFrame, conn):
    """
    Melts the wide-format DataFrame into a long-format vertical table and loads it to the database.
    Identifier columns (id_vars) are explicitly defined to ensure stability.
    """
    if df_wide.empty:
        logging.warning("Wide DataFrame is empty. Skipping vertical table creation.")
        return

    full_vertical_table_name = f'"{SCHEMA_NAME}"."{VERTICAL_TABLE_NAME}"'
    logging.info(f"Starting creation of vertical table: {full_vertical_table_name}")

    id_vars = [
        'symbol', 'company_name', 'sector', 'industry', 'market_cap_group', 'analyst_rating', 'ma50_vs_200d',
        'country', 'exchange', 'sic_code', 'cik_code', 'isin', 'cusip', 'website', 'founded', 'ipo_date',
        'in_index', 'ath_date', 'atl_date', 'payout_frequency', 'earnings_date', 'last_earnings_date',
        'next_earnings_date', 'earnings_time', 'last_split_date', 'last_split_type', 'tags'
    ]
    id_vars = [col for col in id_vars if col in df_wide.columns]
    
    logging.info(f"Using {len(id_vars)} explicitly defined columns as identifiers for the melt operation.")

    df_long = pd.melt(df_wide, id_vars=id_vars, var_name='screener_metric', value_name='metric_value')
    
    df_long.dropna(subset=['metric_value'], inplace=True)
    df_long['metric_value'] = pd.to_numeric(df_long['metric_value'], errors='coerce')
    df_long.dropna(subset=['metric_value'], inplace=True)

    final_column_order = [
        'analyst_rating','symbol', 'company_name', 'sector', 'industry', 'market_cap_group', 
        'screener_metric', 'metric_value','ma50_vs_200d', 'country', 'founded', 'ipo_date', 
        'ath_date', 'atl_date', 'exchange', 'in_index', 'payout_frequency', 
        'earnings_date', 'last_earnings_date', 'next_earnings_date', 'earnings_time', 
        'last_split_date', 'last_split_type', 'tags', 'sic_code', 'cik_code', 'isin', 
        'cusip', 'website'
    ]
    ordered_cols = [col for col in final_column_order if col in df_long.columns]
    df_long = df_long[ordered_cols]

    logging.info(f"Melted DataFrame to long format with {len(df_long)} rows and reordered columns.")

    try:
        with conn.cursor() as cursor:
            logging.info(f"Dropping and recreating vertical table: {full_vertical_table_name}")
            cursor.execute(f"DROP TABLE IF EXISTS {full_vertical_table_name} CASCADE;")
            
            final_pg_cols = []
            for col in df_long.columns:
                if col == 'metric_value':
                    final_pg_cols.append(f'"{col}" NUMERIC')
                else:
                    final_pg_cols.append(f'"{col}" TEXT')

            create_sql = f"CREATE TABLE {full_vertical_table_name} ({', '.join(final_pg_cols)});"
            cursor.execute(create_sql)

            insert_cols = [f'"{c}"' for c in df_long.columns]
            insert_sql = f"INSERT INTO {full_vertical_table_name} ({', '.join(insert_cols)}) VALUES %s"
            
            psycopg2.extras.execute_values(cursor, insert_sql, list(df_long.itertuples(index=False, name=None)), page_size=500)
            conn.commit()
            logging.info(f"Successfully inserted {len(df_long)} rows into {full_vertical_table_name}.")
    except Exception as e:
        logging.error(f"An error occurred during vertical table creation: {e}", exc_info=True)
        conn.rollback()

# --- MODIFIED: Function to create the summary materialized view ---
def create_summary_materialized_view(conn):
    """
    Creates a materialized view that calculates aggregate statistics based on the vertical table.
    This version uses GROUPING SETS for powerful, multi-dimensional analysis.
    """
    full_vertical_table_name = f'"{SCHEMA_NAME}"."{VERTICAL_TABLE_NAME}"'
    full_mv_name = f'"{SCHEMA_NAME}"."{MATERIALIZED_VIEW_NAME}"'
    
    logging.info(f"Creating summary materialized view: {full_mv_name}")

    # --- THIS IS THE NEW, COMPREHENSIVE SQL QUERY ---
    mv_sql = f"""
    CREATE MATERIALIZED VIEW {full_mv_name} AS
    WITH source_data AS (
        SELECT
            sector, industry, market_cap_group, country, exchange,
            analyst_rating, ma50_vs_200d, tags,
            screener_metric, metric_value
        FROM {full_vertical_table_name}
        WHERE metric_value IS NOT NULL
    )
    SELECT
        sector, industry, market_cap_group, country, exchange,
        analyst_rating, ma50_vs_200d, tags,
        screener_metric,
        count(metric_value) AS count_of_records,
        COALESCE(round(avg(metric_value), 3), 0::numeric) AS average,
        COALESCE(round(max(metric_value), 3), 0::numeric) AS max_value,
        COALESCE(round(min(metric_value), 3), 0::numeric) AS min_value,
        COALESCE(round(stddev_samp(metric_value), 3), 0::numeric) AS standard_deviation,
        COALESCE(round((percentile_cont(0.5) WITHIN GROUP (ORDER BY metric_value))::numeric, 3), 0::numeric) AS median
    FROM
        source_data
    GROUP BY
        GROUPING SETS (
            -- 1. Grand Total for each metric
            (screener_metric),

            -- 2. Every single dimension by itself
            (screener_metric, sector),
            (screener_metric, industry),
            (screener_metric, market_cap_group),
            (screener_metric, country),
            (screener_metric, exchange),
            (screener_metric, analyst_rating),
            (screener_metric, ma50_vs_200d),
            (screener_metric, tags),

            -- 3. Core Hierarchy
            (screener_metric, sector, industry),

            -- 4. Common & Requested PAIRS
            (screener_metric, sector, market_cap_group),
            (screener_metric, sector, analyst_rating),
            (screener_metric, sector, ma50_vs_200d),
            (screener_metric, sector, country),
            (screener_metric, industry, market_cap_group),
            (screener_metric, industry, analyst_rating),
            (screener_metric, industry, ma50_vs_200d),
            (screener_metric, market_cap_group, analyst_rating),
            (screener_metric, market_cap_group, ma50_vs_200d),
            (screener_metric, analyst_rating, ma50_vs_200d),
            (screener_metric, country, analyst_rating),

            -- 5. Common & Requested TRIPLETS
            (screener_metric, sector, industry, market_cap_group),
            (screener_metric, sector, industry, analyst_rating),
            (screener_metric, sector, industry, ma50_vs_200d),
            (screener_metric, sector, market_cap_group, analyst_rating),
            (screener_metric, sector, market_cap_group, ma50_vs_200d),
            (screener_metric, sector, analyst_rating, ma50_vs_200d),
            (screener_metric, industry, market_cap_group, analyst_rating),

            -- 6. Requested High-Complexity Sets
            (screener_metric, sector, industry, market_cap_group, analyst_rating),
            (screener_metric, sector, industry, market_cap_group, ma50_vs_200d),
            (screener_metric, sector, industry, analyst_rating, ma50_vs_200d),
            (screener_metric, sector, market_cap_group, analyst_rating, ma50_vs_200d),
            (screener_metric, sector, industry, market_cap_group, analyst_rating, ma50_vs_200d)
        )
    ORDER BY
        sector, industry, market_cap_group, country, exchange,
        analyst_rating, ma50_vs_200d, tags,
        screener_metric;
    """

    try:
        with conn.cursor() as cursor:
            logging.info(f"Dropping and recreating materialized view: {full_mv_name}")
            cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {full_mv_name} CASCADE;")
            cursor.execute(mv_sql)
            conn.commit()
            logging.info(f"Successfully created materialized view {full_mv_name}.")
    except Exception as e:
        logging.error(f"An error occurred during materialized view creation: {e}", exc_info=True)
        conn.rollback()


def main():
    metric_batches = [
        METRIC_ABBREVIATIONS[i:i + METRICS_PER_BATCH]
        for i in range(0, len(METRIC_ABBREVIATIONS), METRICS_PER_BATCH)
    ]
    logging.info(f"Split {len(METRIC_ABBREVIATIONS)} metrics into {len(metric_batches)} batches.")

    rate_limiter = RateLimiter(REQUESTS_PER_MINUTE)
    all_dfs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {executor.submit(fetch_screener_batch, batch, rate_limiter): batch for batch in metric_batches}
        for future in concurrent.futures.as_completed(future_to_batch):
            df_batch = future.result()
            if df_batch is not None and not df_batch.empty:
                all_dfs.append(df_batch)

    if not all_dfs:
        logging.error("No data was fetched. Exiting script.")
        return

    logging.info("Consolidating all fetched data...")
    final_df = all_dfs[0]
    if len(all_dfs) > 1:
        for df_batch in all_dfs[1:]:
            final_df = final_df.join(df_batch, how='outer')

    final_df = final_df.loc[:, ~final_df.columns.duplicated()]
    final_df.reset_index(inplace=True)
    
    # --- THIS IS THE CHANGE YOU REQUESTED ---
    # This line converts all characters in the 'symbol' column to lowercase.
    final_df['symbol'] = final_df['symbol'].str.lower()
    
    cleaned_metric_map = {key: value.strip() for key, value in METRIC_MAP.items()}
    final_df.rename(columns=cleaned_metric_map, inplace=True)
    
    final_column_order = [
        'symbol', 'company_name', 'sector', 'industry', 'market_cap_group', 'ma50_vs_200d', 'founded', 'market_cap', 
        'enterprise_value', 'price_to_earnings_ratio', 'fwd_pe_ratio', 'price_to_sales_ratio', 'fwd_ps_ratio', 
        'price_to_book_ratio', 'price_to_tbv_ratio', 'peg_ratio', 'price_to_ebitda_ratio', 'ev_to_sales', 
        'fwd_ev_to_sales', 'ev_to_ebitda', 'ev_to_ebit', 'ev_to_earnings', 'ev_to_fcf', 'net_cash_by_market_cap', 
        'earnings_yield', 'fcf_yield', 'fcf_to_ev_yield', 'pe_ratio_3y_avg', 'pe_ratio_5y_avg', 'graham_upside_pct', 
        'lynch_upside_pct', 'price', 'price_change_1d', 'price_change_1w', 'price_change_1m', 'price_change_3m', 
        'price_change_6m', 'price_change_ytd', 'price_change_1y', 'price_change_3y', 'price_change_5y', 
        'price_change_10y', 'price_change_15y', 'price_change_20y', 'open_price', 'high_price', 'low_price', 
        'prev_close', 'all_time_high', 'ath_change_pct', 'ath_date', 'all_time_low', 'atl_change_pct', 'atl_date', 
        'low_52w', 'high_52w', 'low_52w_change_pct', 'high_52w_change_pct', 'total_return_1w', 'total_return_1m', 
        'total_return_3m', 'total_return_6m', 'total_return_ytd', 'total_return_1y', 'total_return_3y', 
        'total_return_5y', 'total_return_10y', 'total_return_15y', 'total_return_20y', 'cagr_1y', 'cagr_3y', 
        'cagr_5y', 'cagr_10y', 'cagr_15y', 'cagr_20y', 'return_from_ipo', 'return_from_ipo_open', 'dividend_yield', 
        'dividend_per_share', 'last_dividend', 'dividend_growth', 'div_growth_years', 'div_cagr_3y', 'div_cagr_5y', 
        'payout_ratio', 'payout_frequency', 'buyback_yield', 'shareholder_yield', 'revenue_growth_yoy', 
        'revenue_growth_qoq', 'revenue_growth_3y_cagr', 'revenue_growth_5y_cagr', 'gp_growth_yoy', 'gp_growth_qoq', 
        'gp_growth_3y_cagr', 'gp_growth_5y_cagr', 'op_income_growth_yoy', 'op_income_growth_qoq', 
        'op_income_growth_3y_cagr', 'op_income_growth_5y_cagr', 'net_income_growth_yoy', 'net_income_growth_qoq', 
        'net_income_growth_3y_cagr', 'net_income_growth_5y_cagr', 'eps_growth_yoy', 'eps_growth_qoq', 
        'eps_growth_3y_cagr', 'eps_growth_5y_cagr', 'fcf_growth_yoy', 'fcf_growth_qoq', 'fcf_growth_3y_cagr', 
        'fcf_growth_5y_cagr', 'debt_growth_yoy', 'debt_growth_qoq', 'debt_growth_3y_cagr', 'debt_growth_5y_cagr', 
        'net_cash_growth', 'employees_growth_pct', 'employees_change', 'revenue_growth_years_cons', 
        'revenue_growth_qtrs_cons', 'net_income_growth_years_cons', 'net_income_growth_qtrs_cons', 
        'eps_growth_years_cons', 'eps_growth_qtrs_cons', 'roe', 'roa', 'roic', 'roce', 'roe_5y_avg', 'roa_5y_avg', 
        'roic_5y_avg', 'gross_margin', 'operating_margin', 'profit_margin', 'pretax_margin', 'fcf_margin', 
        'ebitda_margin', 'ebit_margin', 'sbc_to_revenue_pct', 'randd_to_revenue_pct', 'tax_to_revenue_pct', 
        'effective_tax_rate', 'debt_to_equity', 'debt_to_ebitda', 'debt_to_fcf', 'current_ratio', 'quick_ratio', 
        'asset_turnover', 'inventory_turnover', 'wc_turnover', 'altman_z_score', 'piotroski_f_score', 'revenue', 
        'gross_profit', 'operating_income', 'net_income', 'ebitda', 'ebit', 'eps', 'operating_cf', 'investing_cf', 
        'financing_cf', 'net_cf', 'capex', 'fcf', 'fcf_adjusted_for_sbc', 'fcf_per_share', 'sbc', 'income_tax', 
        'r_and_d', 'total_assets', 'total_liabilities', 'shareholder_equity', 'total_cash', 'total_debt', 
        'net_cash_debt', 'working_capital', 'net_working_capital', 'tangible_book_value', 'tbv_per_share', 
        'book_value_per_share', 'shares_outstanding', 'shares_float', 'shares_change_yoy', 'shares_change_qoq', 
        'shares_insiders_pct', 'shares_institutions_pct', 'short_float_pct', 'short_ratio', 'short_shares_pct', 
        'volume', 'dollar_volume', 'avg_volume', 'relative_volume', 'beta_5y', 'rsi_14d', 'rsi_14w', 'rsi_14m', 
        'atr_14', 'analyst_rating', 'analyst_count', 'price_target', 'price_target_upside_pct', 'earnings_date', 
        'last_earnings_date', 'next_earnings_date', 'earnings_time', 'est_revenue', 'est_revenue_growth_pct', 
        'est_eps', 'est_eps_growth_pct', 'est_eps_growth_this_y_pct', 'est_eps_growth_next_y_pct', 
        'est_eps_growth_next_5y_pa', 'est_revenue_growth_this_y_pct', 'est_revenue_growth_next_y_pct', 
        'est_revenue_growth_next_5y_pa', 'est_eps_growth_this_q_pct', 'est_eps_growth_next_q_pct', 
        'est_revenue_growth_this_q_pct', 'est_revenue_growth_next_q_pct','ipo_date','in_index', 'country', 
        'exchange', 'sic_code', 'cik_code', 'isin', 'cusip', 'website','change_from_open', 'days_gap', 'employees', 'fiscal_year_end', 'last_10k_filing_date',
        'last_report_date', 'last_split_date', 'last_split_type', 'price_to_fcf_ratio',
        'price_to_ocf_ratio', 'postmarket_close', 'postmarket_change_percent', 'postmarket_price', 'premarket_change_percent',
        'premarket_price', 'premarket_volume', 'tags',
    ]
        
    existing_cols = final_df.columns.tolist()
    ordered_cols = [col for col in final_column_order if col in existing_cols]
    remaining_cols = sorted([col for col in existing_cols if col not in ordered_cols])
    final_df = final_df[ordered_cols + remaining_cols]

    logging.info(f"Final table has {final_df.shape[0]} rows and {final_df.shape[1]} columns.")

    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        pg_cols = []
        unique_columns = final_df.columns.unique().tolist()
        
        for col_name in unique_columns:
            column_data = final_df[col_name]
            if isinstance(column_data, pd.DataFrame):
                column_series = column_data.iloc[:, 0]
            else:
                column_series = column_data

            dtype = column_series.dtype
            pg_type = "TEXT"
            if pd.api.types.is_numeric_dtype(dtype):
                pg_type = "NUMERIC"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                pg_type = "DATE"
            
            pg_cols.append(f'"{col_name}" {pg_type}')

        if pg_cols and '"symbol"' in pg_cols[0]:
                pg_cols[0] += " PRIMARY KEY"
        else:
                logging.warning("'symbol' was not the first column, PRIMARY KEY not set automatically.")


        full_table_name = f'"{SCHEMA_NAME}"."{WIDE_TABLE_NAME}"'
        logging.info(f"Dropping and recreating table: {full_table_name}")
        cursor.execute(f"DROP TABLE IF EXISTS {full_table_name} CASCADE;")
        
        create_table_sql = f"CREATE TABLE {full_table_name} ({', '.join(pg_cols)});"
        cursor.execute(create_table_sql)
        
        df_for_db = final_df.replace({np.nan: None, pd.NaT: None})
        
        df_for_db = df_for_db[unique_columns]

        db_cols = [f'"{col}"' for col in df_for_db.columns]
        
        insert_sql = (f"INSERT INTO {full_table_name} ({', '.join(db_cols)}) VALUES %s "
                        f"ON CONFLICT (symbol) DO UPDATE SET " + 
                        ", ".join([f'{col} = EXCLUDED.{col}' for col in db_cols if col != '"symbol"']))
        
        psycopg2.extras.execute_values(cursor, insert_sql, list(df_for_db.itertuples(index=False, name=None)), page_size=250)
        conn.commit()
        
        logging.info(f"Successfully inserted/updated {len(df_for_db)} rows into {full_table_name}.")
        
        create_and_load_vertical_table(final_df.copy(), conn)
        create_summary_materialized_view(conn)

    except Exception as e:
        logging.error(f"A database error occurred: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")


if __name__ == "__main__":
    main()