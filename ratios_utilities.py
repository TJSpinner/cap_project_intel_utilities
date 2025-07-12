# --- IMPORTS ---
import requests
import pandas as pd
import time
import re
from datetime import datetime
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
import logging
import random
import threading
import concurrent.futures
import multiprocessing
from functools import partial


# --- Core Configuration ---
SECTOR_TO_PROCESS = "Utilities"
SCHEMA_NAME = "r" # The database schema where tables will be created

# --- Database and API Configuration ---
DB_PARAMS = {
    "host": "localhost",
    "database": "utilities_cap_intel",
    "user": "postgres",
    "password": "Mondo@Mil!Pie19", # Replace with your actual password
    "port": "5433"
}

# --- Concurrency and Rate Limiting Configuration ---
MAX_WORKERS = 15
API_REQUESTS_PER_MINUTE = 32 # Rate limit for fetching the ratio data itself

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')


# ==============================================================================
# STAGE 1: DYNAMIC COMPANY DISCOVERY
# ==============================================================================

def get_screener_headers() -> dict:
    """Returns a complete set of browser headers to mimic a real browser request."""
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

def fetch_companies_by_sector(sector_name: str) -> pd.DataFrame:
    """
    Fetches a list of all companies from the stockanalysis.com screener,
    keeps essential metadata, and filters them by the specified sector.
    """
    logging.info(f"--- STAGE 1: Starting dynamic company discovery for sector: '{sector_name}' ---")

    metrics_to_fetch = ['name', 'sector', 'industry', 'marketCapCategory', 'country', 'analystRatings', 'ma50vs200']
    rename_map = {
        'name': 'company_name',
        'sector': 'sector',
        'industry': 'industry',
        'marketCapCategory': 'market_cap_group',
        'country': 'country',
        'analystRatings': 'analyst_rating',
        'ma50vs200': 'ma50_vs_200d'
    }

    batch_string = "+".join(metrics_to_fetch)
    url = f"https://stockanalysis.com/api/screener/s/bd/{batch_string}.json"

    try:
        logging.info(f"Requesting screener data for {len(metrics_to_fetch)} metrics...")
        response = requests.get(url, headers=get_screener_headers(), timeout=90)
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

        if df_filtered.empty:
            return pd.DataFrame()

        df_filtered.rename(columns=rename_map, inplace=True)
        
        final_columns = ['symbol'] + list(rename_map.values())
        for col in final_columns:
            if col not in df_filtered.columns:
                df_filtered[col] = None
        
        return df_filtered[final_columns]

    except Exception as e:
        logging.error(f"An unexpected error occurred during company discovery: {e}", exc_info=True)
        return pd.DataFrame()


# ==============================================================================
# STAGE 2: FINANCIAL RATIO FETCHING & PROCESSING
# ==============================================================================

class RateLimiter:
    """A thread-safe token bucket rate limiter."""
    def __init__(self, requests_per_minute: int):
        self._lock = threading.Lock()
        self.rate_per_second = requests_per_minute / 60.0
        self.capacity = float(requests_per_minute)
        self.tokens = self.capacity
        self.last_refill_time = time.monotonic()

    def consume(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill_time
            new_tokens = elapsed * self.rate_per_second
            if new_tokens > 0:
                self.tokens = min(self.capacity, self.tokens + new_tokens)
                self.last_refill_time = now
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate_per_second
                time.sleep(wait_time)
                self.tokens += ((time.monotonic() - self.last_refill_time) * self.rate_per_second)
                self.last_refill_time = time.monotonic()
            self.tokens -= 1

def get_period_sort_order(header_str):
    header_str = header_str.upper()
    if 'Q1' in header_str: return 1
    if 'Q2' in header_str: return 2
    if 'Q3' in header_str: return 3
    if 'Q4' in header_str: return 4
    if 'FY' in header_str: return 5
    if 'H1' in header_str or 'S1' in header_str: return 2.5
    if 'H2' in header_str or 'S2' in header_str: return 4.5
    return 99


def fetch_financial_ratios_stockanalysis(symbol, company_name):
    """
    Fetches financial ratio data for a given stock symbol from the stockanalysis.com JSON API.
    """
    all_ratio_data = []
    session_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"https://stockanalysis.com/stocks/{symbol}/financials/ratios/",
        "sec-ch-ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin"
    }
    session = requests.Session()
    session.headers.update(session_headers)

    url = f"https://stockanalysis.com/stocks/{symbol.lower()}/financials/ratios/__data.json?p=quarterly"

    time.sleep(random.uniform(2.1, 3.7))
    logging.info(f"Fetching Financial Ratios for {symbol} from SA-JSON...")

    try:
        response = session.get(url, timeout=20)
        response.raise_for_status()
        json_data = response.json()

        data_nodes_found = 0
        for node in json_data.get('nodes', []):
            if (node.get('type') == 'data' and
                isinstance(node.get('data'), list) and len(node['data']) > 0 and
                isinstance(node['data'][0], dict) and 'financialData' in node['data'][0]):

                data_nodes_found += 1
                data_section = node['data']
                key_map = data_section[0]
                financial_data_index = key_map.get('financialData')
                if financial_data_index is None: continue
                financial_data_map = data_section[financial_data_index]

                date_indices_pointer = financial_data_map.get('datekey')
                if date_indices_pointer is None: continue
                date_indices_list = data_section[date_indices_pointer]
                columns = [data_section[i] for i in date_indices_list]

                item_titles = {}
                for element in data_section:
                    if isinstance(element, dict) and 'id' in element and 'title' in element:
                        item_id_index, item_title_index = element.get('id'), element.get('title')
                        if (isinstance(item_id_index, int) and isinstance(item_title_index, int) and
                            item_id_index < len(data_section) and item_title_index < len(data_section) and
                            isinstance(data_section[item_id_index], str) and isinstance(data_section[item_title_index], str)):
                            item_titles[data_section[item_id_index]] = data_section[item_title_index]

                for item_id, data_pointer in financial_data_map.items():
                    if item_id not in item_titles or not item_titles[item_id]: continue
                    item_name = item_titles[item_id]
                    if item_name.lower() in ["ttm", "last 12 months"]: continue

                    target_obj = data_section[data_pointer]
                    is_indexed_series = isinstance(target_obj, list)

                    for i, period_header_date in enumerate(columns):
                        value = None
                        if is_indexed_series:
                            if i < len(target_obj):
                                value_index = target_obj[i]
                                if value_index is not None and value_index < len(data_section):
                                    value = data_section[value_index]
                        else:
                            value_index = data_pointer + i
                            if value_index < len(data_section):
                                value = data_section[value_index]

                        numeric_value = None
                        try:
                            if isinstance(value, list): value = value[0]
                            numeric_value = float(value)
                        except (ValueError, TypeError):
                            continue

                        match = re.search(r'(\d{4})-(\d{2})-(\d{2})', period_header_date)
                        if not match: continue
                        year, month_str, _ = match.groups()
                        year, month = int(year), int(month_str)

                        header = None
                        if month in [1, 2, 3]: header = f"Q1_{year}"
                        elif month in [4, 5, 6]: header = f"Q2_{year}"
                        elif month in [7, 8, 9]: header = f"Q3_{year}"
                        elif month in [10, 11, 12]: header = f"Q4_{year}"

                        if not header: continue

                        all_ratio_data.append({
                            'statement_type': 'Financial Ratios',
                            'item': item_name,
                            'header': header,
                            'value': numeric_value,
                            'period_date': datetime.strptime(period_header_date, '%Y-%m-%d').date(),
                            'filing_type': 'Scraped-JSON'
                        })

        if data_nodes_found == 0:
            logging.warning(f"No valid data nodes found in JSON for {symbol}")

    except Exception as e:
        logging.error(f"Critical error fetching ratios for {symbol}: {e}", exc_info=False)

    return all_ratio_data


def relabel_semi_annual_periods(df):
    """
    Detects if a company is a semi-annual filer and relabels periods.
    """
    if df.empty:
        return df

    symbol = df['symbol'].iloc[0]
    logging.debug(f"Analyzing filing frequency for {symbol} ratios...")

    df['period_type_temp'] = df['header'].str.split('_').str[0]
    unique_periods = set(df['period_type_temp'].unique())
    is_semi_annual_filer = False

    if 'Q1' in unique_periods or 'Q3' in unique_periods:
        is_semi_annual_filer = False
    elif unique_periods.issubset({'Q2', 'Q4', 'FY'}):
        is_semi_annual_filer = True
        logging.info(f"{symbol}: No Q1/Q3 data. Treating as SEMI-ANNUAL filer.")

    if is_semi_annual_filer:
        df.loc[df['header'].str.startswith('Q2_'), 'header'] = df['header'].str.replace('Q2_', 'H1_', regex=False)
        df.loc[df['header'].str.startswith('Q4_'), 'header'] = df['header'].str.replace('Q4_', 'FY_', regex=False)
    else:
        q4_mask = df['header'].str.startswith('Q4_')
        if q4_mask.any():
            df.loc[q4_mask, 'header'] = df.loc[q4_mask, 'header'].str.replace('Q4_', 'FY_', regex=False)

    df.drop(columns=['period_type_temp'], errors='ignore', inplace=True)
    return df


def process_company_worker(company_info, rate_limiter):
    """
    Worker function: fetches, processes, and returns ratio data for one company.
    """
    symbol = company_info['symbol']
    company_name = company_info['company_name']

    try:
        logging.debug(f"Requesting token for {symbol}...")
        rate_limiter.consume()
        logging.info(f"Token acquired. Starting ratio processing for {symbol}.")

        ratio_data_list = fetch_financial_ratios_stockanalysis(symbol, company_name)

        if not ratio_data_list:
            logging.warning(f"No ratio data returned for {symbol}. Worker finishing.")
            return symbol, None

        financial_df = pd.DataFrame(ratio_data_list)
        financial_df['symbol'] = symbol.lower()
        
        for key, value in company_info.items():
            if key != 'symbol': # Avoid overwriting the lowercased symbol
                financial_df[key] = value

        financial_df = relabel_semi_annual_periods(financial_df)

        financial_df['year'] = financial_df['header'].apply(lambda x: int(x.split('_')[-1]) if '_' in x else None)
        financial_df['period_sort_value'] = financial_df['header'].apply(get_period_sort_order)
        sort_by_cols = ['symbol', 'item', 'year', 'period_sort_value']
        financial_df = financial_df.sort_values(by=sort_by_cols).drop(columns=['year', 'period_sort_value'])

        financial_df = financial_df.reset_index(drop=True)
        financial_df['sort_key'] = financial_df.index
        financial_df['extracted_order'] = financial_df.groupby(['symbol', 'item']).cumcount() + 1

        logging.info(f"Successfully processed {len(financial_df)} ratio records for {symbol}.")
        return symbol, financial_df

    except Exception as e:
        logging.error(f"WORKER ERROR for {symbol}: Unhandled exception -> {e}", exc_info=True)
        return symbol, None


# ==============================================================================
# STAGE 3: DATABASE INTEGRATION & POST-PROCESSING
# ==============================================================================

def create_and_insert_ratio_data(df_to_insert, symbol):
    """
    Inserts processed ratio data into the database. Now drops the table first
    to ensure the schema is always up-to-date.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema};").format(schema=sql.Identifier(SCHEMA_NAME)))

        table_name_str = symbol.lower().replace('.', '_') + '_ratios'
        full_table_name = sql.Identifier(SCHEMA_NAME, table_name_str)

        logging.debug(f"Dropping table {full_table_name.as_string(conn)} if it exists...")
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {table_name} CASCADE;").format(table_name=full_table_name))

        create_table_query = sql.SQL("""
            CREATE TABLE {table_name} (
                symbol TEXT NOT NULL,
                company_name TEXT,
                sector TEXT,
                industry TEXT,
                market_cap_group TEXT,
                country TEXT,
                analyst_rating TEXT,
                ma50_vs_200d TEXT,
                statement_type TEXT,
                item TEXT,
                header TEXT,
                value NUMERIC,
                sort_key INTEGER,
                extracted_order INTEGER,
                period_date DATE,
                filing_type TEXT,
                PRIMARY KEY (symbol, item, header)
            );
        """).format(table_name=full_table_name)
        cur.execute(create_table_query)

        df_to_insert.drop_duplicates(subset=['symbol', 'item', 'header'], keep='last', inplace=True)

        insert_data = []
        for _, row in df_to_insert.iterrows():
            ratio_value = pd.to_numeric(row.get('value'), errors='coerce')
            if pd.isna(ratio_value): ratio_value = None

            insert_data.append((
                row['symbol'], row.get('company_name'), row.get('sector'), row.get('industry'),
                row.get('market_cap_group'), row.get('country'), row.get('analyst_rating'), row.get('ma50_vs_200d'),
                row['statement_type'], row['item'], row['header'], ratio_value,
                row['sort_key'], row['extracted_order'], row['period_date'], row['filing_type']
            ))

        insert_query = sql.SQL("""
            INSERT INTO {table_name} (
                symbol, company_name, sector, industry, market_cap_group, country, analyst_rating, ma50_vs_200d,
                statement_type, item, header, value,
                sort_key, extracted_order, period_date, filing_type
            ) VALUES %s;
        """).format(table_name=full_table_name)

        if insert_data:
            execute_values(cur, insert_query, insert_data, page_size=500)
            logging.info(f"Successfully inserted {len(insert_data)} ratio rows into {full_table_name.as_string(conn)}.")
        else:
            logging.info(f"No ratio data to insert for {symbol}.")
    except Exception as e:
        logging.error(f"Database error for {symbol} ratios: {e}", exc_info=True)
    finally:
        if conn:
            if 'cur' in locals() and cur: cur.close()
            conn.close()


def create_aggregate_ratio_table(conn, cursor, sector_name_param, symbols):
    """Creates the aggregate table for the given sector, dropping dependent views first."""
    aggregate_table_name = f"aggregate_table_{sector_name_param}_ratios"
    full_aggregate_table_id = sql.Identifier("public", aggregate_table_name)
    logging.info(f"\n--- Recreating aggregate table: {full_aggregate_table_id.as_string(conn)} ---")

    wide_mv_schema = sector_name_param
    percent_change_schema = "public"
    mvs_to_drop = [
        sql.Identifier(percent_change_schema, f"{sector_name_param}_ratios_changes"),
        sql.Identifier(percent_change_schema, f"{sector_name_param}_ratios_YOY_change")
    ]
    for symbol in symbols:
        mvs_to_drop.append(sql.Identifier(wide_mv_schema, f"{symbol.lower().replace('.', '_')}_ratios_wide"))

    for mv_name in mvs_to_drop:
        cursor.execute(sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {mv} CASCADE;").format(mv=mv_name))
    conn.commit()

    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {table_name} CASCADE;").format(table_name=full_aggregate_table_id))

    union_queries = []
    for symbol in symbols:
        table_name_str = symbol.lower().replace('.', '_') + '_ratios'
        individual_table_id = sql.Identifier(sector_name_param, table_name_str)
        union_queries.append(sql.SQL("SELECT * FROM {individual_table}").format(individual_table=individual_table_id))

    if not union_queries: return False

    full_union_query = sql.SQL(" UNION ALL ").join(union_queries)
    create_table_sql = sql.SQL("CREATE TABLE {agg_table_name} AS ({union_query});").format(
        agg_table_name=full_aggregate_table_id,
        union_query=full_union_query
    )
    cursor.execute(create_table_sql)
    conn.commit()
    logging.info("Aggregate table created successfully.")
    return True

def create_views_for_ticker_worker(symbol, sector_name_param):
    """Worker function for multiprocessing. Creates a wide MV for a single company."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.autocommit = False
        cursor = conn.cursor()
        
        cursor.execute("CREATE EXTENSION IF NOT EXISTS tablefunc;")
        conn.commit()

        aggregate_table_id = sql.Identifier("public", f"aggregate_table_{sector_name_param}_ratios")
        wide_mv_schema = sector_name_param
        mv_name_str = f"{symbol.lower().replace('.', '_')}_ratios_wide"
        full_mv_name = sql.Identifier(wide_mv_schema, mv_name_str)

        # FIXED: Use SQL composition instead of .format() to avoid errors with regex characters.
        query_part1 = sql.SQL("SELECT header FROM ( SELECT DISTINCT header, SUBSTRING(header FROM '_(\d{4})$')::int AS year_val, CASE WHEN header LIKE 'Q1%%' THEN 1 WHEN header LIKE 'Q2%%' THEN 2 WHEN header LIKE 'H1%%' THEN 2 WHEN header LIKE 'Q3%%' THEN 3 WHEN header LIKE 'Q4%%' THEN 4 WHEN header LIKE 'H2%%' THEN 4 WHEN header LIKE 'FY%%' THEN 5 ELSE 99 END AS period_val FROM ")
        query_part2 = sql.SQL(" WHERE lower(symbol) = %s ) AS sorted_headers ORDER BY year_val DESC, period_val DESC;")
        header_query = query_part1 + aggregate_table_id + query_part2
        
        cursor.execute(header_query, (symbol.lower(),))
        ordered_headers = [row[0] for row in cursor.fetchall()]
        if not ordered_headers: 
            logging.warning(f"WORKER: No headers found for {symbol}, skipping MV creation.")
            return

        crosstab_output_defs_list = ["item TEXT"] + [sql.SQL("{c} NUMERIC").format(c=sql.Identifier(h)).as_string(conn) for h in ordered_headers]
        crosstab_output_defs_sql = sql.SQL(", ").join(map(sql.SQL, crosstab_output_defs_list))

        source_sql = cursor.mogrify(sql.SQL("SELECT item, header, value FROM {agg_table} WHERE lower(symbol) = %s ORDER BY 1;").format(agg_table=aggregate_table_id), (symbol.lower(),)).decode('utf-8')
        category_sql = cursor.mogrify("SELECT unnest(%s::text[])", (ordered_headers,)).decode('utf-8')

        final_query = sql.SQL("""
            CREATE MATERIALIZED VIEW {mv_name} AS
            SELECT T1.symbol, T1.company_name, T1.sector, T1.industry, T1.market_cap_group, T1.country, T1.analyst_rating, T1.ma50_vs_200d, T2.*
            FROM (SELECT DISTINCT symbol, company_name, sector, industry, market_cap_group, country, analyst_rating, ma50_vs_200d FROM {agg_table} WHERE lower(symbol) = %s) AS T1,
                 (SELECT * FROM crosstab({source_sql}, {category_sql}) AS ct({crosstab_output_defs})) AS T2;
        """).format(
            mv_name=full_mv_name, 
            agg_table=aggregate_table_id, 
            source_sql=sql.Literal(source_sql), 
            category_sql=sql.Literal(category_sql), 
            crosstab_output_defs=crosstab_output_defs_sql
        )

        cursor.execute(sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {mv_name};").format(mv_name=full_mv_name))
        cursor.execute(final_query, (symbol.lower(),))
        conn.commit()
        logging.info(f"WORKER: Successfully created MV for '{symbol}'.")

    except psycopg2.Error as e:
        logging.error(f"WORKER ERROR for symbol {symbol}: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def create_materialized_views(conn, cursor, sector_name_param, symbols):
    """Orchestrates creation of all materialized views using multiprocessing for wide views."""
    logging.info(f"\n--- Starting PARALLEL creation of {len(symbols)} company-specific ratio MVs ---")
    num_processes = max(1, multiprocessing.cpu_count() - 1)
    # The worker function now handles its own DB connection, so we don't pass conn/cursor
    task_func = partial(create_views_for_ticker_worker, sector_name_param=sector_name_param)
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(task_func, symbols)
    logging.info("--- Finished parallel creation of company-specific wide MVs ---")

    # The rest of the MVs are created sequentially in the main process
    aggregate_table_id = sql.Identifier("public", f"aggregate_table_{sector_name_param}_ratios")

    logging.info(f"\n--- Creating Sequential Percent Change MV for {sector_name_param} ratios ---")
    pop_mv_name = sql.Identifier("public", f"{sector_name_param}_ratios_changes")
    try:
        cursor.execute(sql.SQL('DROP MATERIALIZED VIEW IF EXISTS {mv_name};').format(mv_name=pop_mv_name))
        create_pop_sql = sql.SQL("""
            CREATE MATERIALIZED VIEW {mv_name} AS
            WITH p AS (SELECT t.*, SUBSTRING(t.header FROM '^[A-Z]+') AS pt FROM {agg_table} t WHERE t.value IS NOT NULL AND t.value <> 0),
            l AS (SELECT p.*, lag(p.value, 1) OVER (PARTITION BY p.symbol, p.item, p.pt ORDER BY p.period_date) AS pv FROM p)
            SELECT symbol, company_name, sector, industry, market_cap_group, country, analyst_rating, ma50_vs_200d, item AS financial_ratio, header AS period, period_date, value AS current_value, pv AS previous_period_value,
                   round(((value - pv) / abs(NULLIF(pv, 0))) * 100, 4) AS sequential_percent_change
            FROM l WHERE pv IS NOT NULL ORDER BY symbol, item, period_date;
        """).format(mv_name=pop_mv_name, agg_table=aggregate_table_id)
        cursor.execute(create_pop_sql)
        logging.info(f"Sequential MV '{pop_mv_name.as_string(conn)}' created.")
    except Exception as e:
        logging.error(f"Error creating Sequential MV: {e}", exc_info=True)

    logging.info(f"\n--- Creating Year-over-Year (YoY) Percent Change MV for {sector_name_param} ratios ---")
    yoy_mv_name = sql.Identifier("public", f"{sector_name_param}_ratios_YOY_change")
    try:
        cursor.execute(sql.SQL('DROP MATERIALIZED VIEW IF EXISTS {mv_name};').format(mv_name=yoy_mv_name))
        create_yoy_sql = sql.SQL("""
            CREATE MATERIALIZED VIEW {mv_name} AS
            WITH p AS (SELECT t.*, CASE WHEN t.header LIKE 'Q1%%' THEN 'Q1' WHEN t.header LIKE 'Q2%%' THEN 'Q2' WHEN t.header LIKE 'Q3%%' THEN 'Q3' WHEN t.header LIKE 'Q4%%' THEN 'Q4' WHEN t.header LIKE 'H1%%' THEN 'H1' WHEN t.header LIKE 'H2%%' THEN 'H2' WHEN t.header LIKE 'FY%%' THEN 'FY' END AS sp FROM {agg_table} t WHERE t.value IS NOT NULL AND t.value <> 0),
            l AS (SELECT p.*, lag(p.value, 1) OVER (PARTITION BY p.symbol, p.item, p.sp ORDER BY p.period_date) as pvy FROM p)
            SELECT symbol, company_name, sector, industry, market_cap_group, country, analyst_rating, ma50_vs_200d, item AS financial_ratio, header AS period, period_date, value AS current_value, pvy AS previous_year_value,
                   round(((value - pvy) / abs(NULLIF(pvy, 0))) * 100, 4) AS yoy_percent_change
            FROM l WHERE pvy IS NOT NULL ORDER BY symbol, item, period_date;
        """).format(mv_name=yoy_mv_name, agg_table=aggregate_table_id)
        cursor.execute(create_yoy_sql)
        logging.info(f"YoY MV '{yoy_mv_name.as_string(conn)}' created.")
    except Exception as e:
        logging.error(f"Error creating YoY MV: {e}", exc_info=True)
    conn.commit()


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def main():
    """Main end-to-end execution function."""

    companies_df = fetch_companies_by_sector(SECTOR_TO_PROCESS)
    if companies_df.empty:
        logging.error(f"No companies found for sector '{SECTOR_TO_PROCESS}'. Halting script.")
        return

    companies_to_process = companies_df.to_dict('records')
    logging.info(f"Discovered {len(companies_to_process)} companies to process for ratio data.")


    logging.info(f"\n--- STAGE 2: Submitting {len(companies_to_process)} companies to a thread pool for ratio fetching ---")
    processed_symbols = []
    rate_limiter = RateLimiter(API_REQUESTS_PER_MINUTE)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='RatioWorker') as executor:
        futures = {executor.submit(process_company_worker, c, rate_limiter): c for c in companies_to_process}

        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            symbol, df_result = future.result()
            logging.info(f"--- RESULT RECEIVED ({i+1}/{len(companies_to_process)}) for {symbol} ---")
            if df_result is not None and not df_result.empty:
                create_and_insert_ratio_data(df_result, symbol)
                processed_symbols.append(symbol)
            else:
                logging.warning(f"Skipping DB insert for {symbol} due to missing/empty data.")

    if not processed_symbols:
        logging.warning("No companies were successfully processed. Halting before final aggregation.")
        return

    conn = None
    try:
        logging.info("\n--- STAGE 3: All fetching complete. Starting final aggregation and view creation. ---")
        conn = psycopg2.connect(**DB_PARAMS)
        conn.autocommit = True
        cursor = conn.cursor()

        if create_aggregate_ratio_table(conn, cursor, SCHEMA_NAME, processed_symbols):
            create_materialized_views(conn, cursor, SCHEMA_NAME, processed_symbols)
        else:
            logging.error("Halting MV creation due to aggregation failure.")

    except Exception as e:
        logging.error(f"An error occurred during final database operations: {e}", exc_info=True)
    finally:
        if conn:
            if 'cursor' in locals() and cursor: cursor.close()
            conn.close()

    logging.info("\nScript finished. All resources closed.")


if __name__ == "__main__":
    # This check is crucial for multiprocessing to work correctly on Windows/macOS
    # It prevents child processes from re-importing and re-executing the main script's code.
    multiprocessing.freeze_support()
    main()


