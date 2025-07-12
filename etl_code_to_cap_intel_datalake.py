import psycopg2
import psycopg2.extras
import time
import re
from psycopg2 import sql
import logging
import concurrent.futures

# --- 1. Configuration ---

# Setup basic logging to see the script's progress
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Concurrency Configuration
MAX_WORKERS = 25  # Number of tables to process in parallel

# Target Data Lake Database Connection
TARGET_DB_PARAMS = {
    "host": "localhost",
    "database": "cap_intel_datalake",  # <-- UPDATED
    "user": "etl_alex",
    "password": "pizzapie123",  # IMPORTANT: Replace with your actual password
    "port": "5433"
}

# Source Database Connection
SOURCE_DB_PARAMS = {
    "host": "localhost",
    "database": "utilities_cap_intel",  # <-- UPDATED
    "user": "etl_alex",
    "password": "pizzapie123",  # IMPORTANT: Replace with your actual password
    "port": "5433"
}

# Schemas to exclude from extraction (system schemas).
EXCLUDE_SCHEMAS = ['pg_catalog', 'information_schema']


# --- 2. Helper Functions ---

def _clean_name(name):
    """Cleans a string to be a valid, lowercased PostgreSQL identifier part."""
    clean = name.lower()
    clean = re.sub(r'[^a-zA-Z0-9_]+', '_', clean)
    clean = re.sub(r'_+', '_', clean)
    clean = clean.strip('_')
    return clean

def get_db_connection(params):
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to database '{params.get('database', 'N/A')}': {e}")
        return None

def get_tables_and_mvs(conn):
    """Retrieves a list of all regular tables and materialized views from the database."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        excluded_schemas_str = tuple(EXCLUDE_SCHEMAS)
        query = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN %s AND table_type = 'BASE TABLE'
            UNION ALL
            SELECT schemaname AS table_schema, matviewname AS table_name
            FROM pg_matviews
            WHERE schemaname NOT IN %s;
        """
        cursor.execute(query, (excluded_schemas_str, excluded_schemas_str))
        return cursor.fetchall()

def create_target_schema_if_not_exists(target_conn, schema_name):
    """Creates a schema in the target database if it doesn't exist."""
    with target_conn.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema};").format(schema=sql.Identifier(schema_name)))
    logging.info(f"Target schema '{schema_name}' ensured.")

def get_table_columns_and_types(conn, schema_name, table_name):
    """Retrieves column names and their data types for a given table/MV."""
    columns_info = []
    with conn.cursor() as cursor:
        try:
            query = sql.SQL("""
                SELECT a.attname AS column_name, pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum;
            """)
            cursor.execute(query, (schema_name, table_name))
            for col in cursor.fetchall():
                col_name, data_type = col[0], col[1]
                # Clean column names for the CREATE TABLE statement
                columns_info.append(f'"{_clean_name(col_name)}" {data_type}')
        except psycopg2.Error as e:
            logging.error(f"Could not get columns for \"{schema_name}\".\"{table_name}\": {e}")
    return columns_info
def extract_and_load_data(source_conn, target_conn, source_schema, source_table, target_schema, target_table):
    """Extracts data from source and loads into target."""
    logging.info(f"Processing: \"{source_schema}\".\"{source_table}\" -> \"{target_schema}\".\"{target_table}\"")
    try:
        columns_info = get_table_columns_and_types(source_conn, source_schema, source_table)
        if not columns_info:
            logging.warning(f"No columns found for \"{source_schema}\".\"{source_table}\". Skipping.")
            return

        cols_sql_def = ", ".join(columns_info)
        target_full_table_name = sql.SQL("{}.{}").format(sql.Identifier(target_schema), sql.Identifier(target_table))

        with target_conn.cursor() as target_cursor:
            target_cursor.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(target_full_table_name))
            target_cursor.execute(sql.SQL("CREATE TABLE {} ({});").format(target_full_table_name, sql.SQL(cols_sql_def)))

            # Use a server-side cursor for memory efficiency with large tables
            # We must turn autocommit OFF for the source connection for this to work
            source_conn.autocommit = False # <-- ADD THIS LINE
            source_cursor = source_conn.cursor(name="server_side_cursor", cursor_factory=psycopg2.extras.DictCursor)
            source_cursor.execute(sql.SQL('SELECT * FROM {}.{};').format(sql.Identifier(source_schema), sql.Identifier(source_table)))
            
            # Fetch and insert in batches to manage memory
            batch_size = 5000
            while True:
                rows = source_cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                # Clean the column names from the cursor description for the INSERT statement
                column_names = [_clean_name(desc.name) for desc in source_cursor.description]
                cols_sql_insert = sql.SQL(', ').join(map(sql.Identifier, column_names))
                placeholders = sql.SQL(', ').join(sql.Placeholder() * len(column_names))
                insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({});").format(target_full_table_name, cols_sql_insert, placeholders)
                
                list_of_tuples = [tuple(row) for row in rows]
                psycopg2.extras.execute_batch(target_cursor, insert_sql, list_of_tuples)
                logging.info(f"-> Inserted {len(list_of_tuples)} rows into {target_full_table_name.as_string(target_conn)}.")
        
        source_cursor.close()

    except Exception as e:
        logging.error(f"ETL failed for \"{source_schema}\".\"{source_table}\": {e}")
        raise

# --- 3. Main ETL Process ---

def process_single_object(task_info):
    """
    Worker function to process a single database object (table/MV).
    This function is executed by each thread in the pool.
    """
    obj = task_info
    source_schema = obj['table_schema']
    source_table = obj['table_name']
    
    # <-- NEW NAMING LOGIC
    target_schema = source_schema  # Target schema is the same as source
    
    if source_schema == 'public':
        # For 'public' schema, keep the original table name
        target_table = _clean_name(source_table)
    else:
        # For other schemas, prefix the table name with the schema name
        target_table = f"{_clean_name(source_schema)}_{_clean_name(source_table)}"

    source_conn = None
    target_conn = None
    try:
        source_conn = get_db_connection(SOURCE_DB_PARAMS)
        target_conn = get_db_connection(TARGET_DB_PARAMS)
        
        if not source_conn or not target_conn:
            raise ConnectionError(f"Could not establish DB connections for processing {source_table}.")

        create_target_schema_if_not_exists(target_conn, target_schema)
        extract_and_load_data(
            source_conn, target_conn,
            source_schema, source_table,
            target_schema, target_table
        )
    finally:
        if source_conn:
            source_conn.close()
        if target_conn:
            target_conn.close()

def run_etl():
    """Main function to orchestrate the concurrent ETL process."""
    start_time = time.time()
    logging.info("\n--- Starting Concurrent ETL Process ---")

    # --- Step 1: Gather all tasks from the source database ---
    logging.info(f"Connecting to '{SOURCE_DB_PARAMS['database']}' to gather list of tables/MVs...")
    
    initial_source_conn = get_db_connection(SOURCE_DB_PARAMS)
    if not initial_source_conn:
        logging.critical(f"Could not connect to '{SOURCE_DB_PARAMS['database']}'. Aborting ETL.")
        return
    
    tasks = get_tables_and_mvs(initial_source_conn)
    initial_source_conn.close()

    if not tasks:
        logging.critical("No tasks found in the database. Aborting ETL.")
        return

    # --- Step 2: Process all gathered tasks in a single thread pool ---
    logging.info(f"\n--- Found {len(tasks)} tasks. Starting thread pool with {MAX_WORKERS} workers. ---")

    success_count = 0
    failure_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {executor.submit(process_single_object, task): task for task in tasks}

        for future in concurrent.futures.as_completed(future_to_task):
            task = future_to_task[future]
            try:
                future.result()
                success_count += 1
            except Exception as exc:
                logging.error(f"ETL task for '{task['table_schema']}.{task['table_name']}' generated an exception: {exc}")
                failure_count += 1
    
    end_time = time.time()
    logging.info(f"\n--- ETL Process completed in {end_time - start_time:.2f} seconds ---")
    logging.info(f"Summary: {success_count} succeeded, {failure_count} failed.")

if __name__ == "__main__":
    run_etl()