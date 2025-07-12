import requests
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras # Needed for execute_batch
import logging
import time
import numpy as np
import threading
import concurrent.futures
import re

# --- Core Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- API and Database Configuration ---
POLYGON_API_KEY = "QIlIIfr5YblqJpoVBv1BY3zV6JjmFUap" # YOUR POLYGON.IO API KEY
POLYGON_BASE_URL = "https://api.polygon.io/v2/aggs/ticker"
DB_PARAMS = {
    "host": "localhost",
    "database": "utilities_cap_intel",
    "user": "postgres",
    "password": "Mondo@Mil!Pie19",
    "port": "5433"
}

# --- Screener Configuration ---
SECTOR_TO_PROCESS = "Utilities" 
SCHEMA_NAME = "stock_price"

# --- Polygon API Concurrency Configuration (For Stage 2) ---
POLYGON_MAX_WORKERS = 55 
POLYGON_API_REQUESTS_PER_SECOND = 80


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
    Fetches a list of all companies from the screener, keeps only essential
    metadata, and filters them by the specified sector.
    """
    logging.info(f"Starting dynamic company discovery for sector: '{sector_name}'")
    
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

    # Since this function makes only one API call, a rate limiter is not needed here.
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
        logging.info(f"Filtered {initial_count} total companies down to {len(df_filtered)} in '{sector_name}' sector.")

        if df_filtered.empty: return pd.DataFrame()

        df_filtered.rename(columns=rename_map, inplace=True)
        final_columns = ['symbol'] + list(rename_map.values())
        return df_filtered[final_columns]

    except Exception as e:
        logging.error(f"An unexpected error occurred during company discovery: {e}", exc_info=True)
        return pd.DataFrame()


# ==============================================================================
# STAGE 2: HISTORICAL PRICE FETCHING
# ==============================================================================

class RateLimiter:
    """A thread-safe token bucket rate limiter for the Polygon API calls."""
    def __init__(self, requests_per_second: int):
        self._lock = threading.Lock()
        self.rate = float(requests_per_second)
        self.capacity = float(requests_per_second)
        self.tokens = self.capacity
        self.last_refill_time = time.monotonic()

    def consume(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill_time
            self.tokens = min(self.capacity, self.tokens + (elapsed * self.rate))
            self.last_refill_time = now
            if self.tokens < 1:
                time.sleep((1 - self.tokens) / self.rate)
            self.tokens -= 1

def fetch_historical_data_polygon(symbol, api_key, years_back=5):
    """Fetches up to 5 years of daily historical data for a symbol from Polygon.io."""
    today = datetime.now().date()
    from_date = (today - timedelta(days=years_back * 365 + 30)).strftime('%Y-%m-%d')
    to_date = today.strftime('%Y-%m-%d')
    url = f"{POLYGON_BASE_URL}/{symbol}/range/1/day/{from_date}/{to_date}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": api_key}
    all_results = []
    while url:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            results = data.get('results')
            if not results: break
            all_results.extend(results)
            url = data.get('next_url')
            if url: params = {"apiKey": api_key}
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error for {symbol}: {e}")
            return None
    if not all_results: return pd.DataFrame()
    df = pd.DataFrame(all_results)
    df.rename(columns={'t': 'Date', 'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'}, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'], unit='ms').dt.date
    numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in numeric_cols: df[col] = pd.to_numeric(df[col], errors='coerce')
    df['adjusted_close'] = df['Close']
    df.sort_values(by='Date', inplace=True)
    df['change'] = round(((df['Close'] - df['Close'].shift(1)) / df['Close'].shift(1)) * 100, 3)
    df['dollar_volume'] = round(df['adjusted_close'] * df['Volume'],-1)
    df['log_return'] = np.log(df['adjusted_close'] / df['adjusted_close'].shift(1))
    df['volatility_past_year'] = round(df['log_return'].rolling(window=252).std() * np.sqrt(252),4)
    
    final_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'adjusted_close', 'change', 'Volume', 'dollar_volume', 'volatility_past_year']
    return df[final_cols]

def process_stock_worker(stock_info: dict, rate_limiter: RateLimiter):
    """Worker function to fetch data for a single stock."""
    symbol = stock_info['symbol']
    logging.info(f"WORKER: Starting processing for {symbol}")
    try:
        rate_limiter.consume()
        df = fetch_historical_data_polygon(symbol.upper(), POLYGON_API_KEY)
        if df is not None and not df.empty:
            logging.info(f"WORKER: Successfully fetched {len(df)} records for {symbol}")
            return stock_info, df
        else:
            logging.warning(f"WORKER: No data returned for {symbol}")
            return stock_info, None
    except Exception as e:
        logging.error(f"WORKER: Unhandled exception for {symbol}: {e}")
        return stock_info, None

# ==============================================================================
# STAGE 3: MAIN EXECUTION AND DATABASE INTEGRATION
# ==============================================================================


def main():
    """Main execution block."""
    companies_df = fetch_companies_by_sector(SECTOR_TO_PROCESS)
    if companies_df.empty:
        logging.error(f"No companies found for sector '{SECTOR_TO_PROCESS}'. Exiting.")
        return
    companies_to_process = companies_df.to_dict('records')
    logging.info(f"Found {len(companies_to_process)} companies to process.")

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")
        
        agg_table_name = f'public."aggregate_table_{SCHEMA_NAME}"'
        cursor.execute(f"DROP TABLE IF EXISTS {agg_table_name} CASCADE;")
        
        cursor.execute(f"""
            CREATE TABLE {agg_table_name} (
                symbol TEXT NOT NULL, company_name TEXT, sector TEXT, industry TEXT, market_cap_group TEXT,
                country TEXT, analyst_rating TEXT, ma50_vs_200d TEXT, date DATE NOT NULL,
                open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, change NUMERIC, 
                volume NUMERIC, adjusted_close NUMERIC, dollar_volume NUMERIC, volatility_past_year NUMERIC,
                PRIMARY KEY (symbol, date)
            );
        """)
        conn.commit()
        logging.info(f"Database schema '{SCHEMA_NAME}' and table '{agg_table_name}' are ready.")

        rate_limiter = RateLimiter(POLYGON_API_REQUESTS_PER_SECOND)
        with concurrent.futures.ThreadPoolExecutor(max_workers=POLYGON_MAX_WORKERS) as executor:
            futures = [executor.submit(process_stock_worker, company, rate_limiter) for company in companies_to_process]
            
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                stock_info, df_historical = future.result()
                symbol = stock_info['symbol']
                logging.info(f"Processing result for {symbol} ({i+1}/{len(companies_to_process)})")

                if df_historical is not None and not df_historical.empty:
                    ind_table_name = f'"{SCHEMA_NAME}"."{symbol.lower()}"'
                    cursor.execute(f"DROP TABLE IF EXISTS {ind_table_name};")
                    cursor.execute(f"""
                        CREATE TABLE {ind_table_name} (
                            symbol TEXT, company_name TEXT, sector TEXT, industry TEXT, market_cap_group TEXT,
                            country TEXT, analyst_rating TEXT, ma50_vs_200d TEXT, date DATE,
                            open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, adjusted_close NUMERIC,
                            change NUMERIC, volume NUMERIC, dollar_volume NUMERIC, volatility_past_year NUMERIC,
                            PRIMARY KEY (symbol, date)
                        );
                    """)

                    # --- THE DEFINITIVE FIX IS HERE ---
                    # Explicitly convert all potential NaN values to None before insertion.
                    records = [(
                        None if pd.isna(stock_info.get('symbol')) else stock_info.get('symbol').lower(),
                        None if pd.isna(stock_info.get('company_name')) else stock_info.get('company_name'),
                        None if pd.isna(stock_info.get('sector')) else stock_info.get('sector'),
                        None if pd.isna(stock_info.get('industry')) else stock_info.get('industry'),
                        None if pd.isna(stock_info.get('market_cap_group')) else stock_info.get('market_cap_group'),
                        None if pd.isna(stock_info.get('country')) else stock_info.get('country'),
                        None if pd.isna(stock_info.get('analyst_rating')) else stock_info.get('analyst_rating'),
                        None if pd.isna(stock_info.get('ma50_vs_200d')) else stock_info.get('ma50_vs_200d'),
                        row['Date'],
                        None if pd.isna(row.get('Open')) else row.get('Open'),
                        None if pd.isna(row.get('High')) else row.get('High'),
                        None if pd.isna(row.get('Low')) else row.get('Low'),
                        None if pd.isna(row.get('Close')) else row.get('Close'),
                        None if pd.isna(row.get('adjusted_close')) else row.get('adjusted_close'),
                        None if pd.isna(row.get('change')) else row.get('change'),
                        None if pd.isna(row.get('Volume')) else row.get('Volume'),
                        None if pd.isna(row.get('dollar_volume')) else row.get('dollar_volume'),
                        None if pd.isna(row.get('volatility_past_year')) else row.get('volatility_past_year')
                    ) for _, row in df_historical.iterrows()]

                    try:
                        cols = [
                            'symbol', 'company_name', 'sector', 'industry', 'market_cap_group', 'country', 
                            'analyst_rating', 'ma50_vs_200d', 'date', 'open', 'high', 'low', 'close', 
                            'adjusted_close', 'change', 'volume', 'dollar_volume', 'volatility_past_year'
                        ]
                        insert_cols_str = ", ".join(cols)
                        update_cols_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in cols if col not in ['symbol', 'date']])
                        
                        insert_query = f"INSERT INTO {ind_table_name} ({insert_cols_str}) VALUES %s ON CONFLICT (symbol, date) DO UPDATE SET {update_cols_str};"
                        psycopg2.extras.execute_values(cursor, insert_query, records, page_size=250)

                        agg_insert_query = f"INSERT INTO {agg_table_name} ({insert_cols_str}) VALUES %s ON CONFLICT (symbol, date) DO UPDATE SET {update_cols_str};"
                        psycopg2.extras.execute_values(cursor, agg_insert_query, records, page_size=250)
                        
                        conn.commit()
                        logging.info(f"Successfully upserted {len(records)} records for {symbol}.")
                    except Exception as e:
                        logging.error(f"Database error during upsert for {symbol}: {e}", exc_info=True)
                        conn.rollback()
        
        mv_name = f'public.monthly_{SCHEMA_NAME}_summary'
        logging.info(f"Refreshing Materialized View: {mv_name}...")
        cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name};")
        cursor.execute(f"""
            CREATE MATERIALIZED VIEW {mv_name} AS
            SELECT
                symbol, company_name, sector, industry, market_cap_group, country,
                DATE_TRUNC('month', date) AS month_start_date,
                AVG(adjusted_close) AS average_adjusted_close,
                AVG(volume) AS average_volume,
                AVG(dollar_volume) AS average_dollar_volume,
                (MAX(high) - MIN(low)) AS monthly_high_low_range_volatility
            FROM {agg_table_name}
            GROUP BY symbol, company_name, sector, industry, market_cap_group, country, DATE_TRUNC('month', date)
            ORDER BY symbol ASC, month_start_date DESC;
        """)
        conn.commit()
        logging.info("Materialized view refreshed successfully.")

    except Exception as e:
        logging.error(f"An unexpected error occurred in main execution: {e}", exc_info=True)
    finally:
        if cursor: cursor.close()
        if conn:
            conn.close()
            logging.info("Script finished. Database connection closed.")
if __name__ == "__main__":
    main()