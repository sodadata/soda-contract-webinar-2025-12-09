"""
Airflow DAG for Orders Data Pipeline with Soda Contract Verification

This DAG demonstrates how to integrate Soda data quality checks into an Airflow pipeline:
1. Load CSV data from source into pandas DataFrame
2. Verify contract using DuckDB in-memory (data quality gate before database write)
3. If verification passes, insert data to public.orders
4. Verify contract on PostgreSQL and publish results to Soda Cloud

The pipeline fails if any data quality check fails, preventing bad data from reaching production.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
import pandas as pd
import duckdb
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv

# Ensure venv's site-packages are in Python path (for Airflow)
# This helps when Airflow tasks run in a different environment
if 'VIRTUAL_ENV' in os.environ:
    venv_path = os.environ['VIRTUAL_ENV']
    site_packages = os.path.join(venv_path, 'lib', f'python{sys.version_info.major}.{sys.version_info.minor}', 'site-packages')
    if os.path.exists(site_packages) and site_packages not in sys.path:
        sys.path.insert(0, site_packages)
# Also try to find venv relative to project root
else:
    project_root = os.getenv("AIRFLOW_HOME") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    venv_path = os.path.join(project_root, 'venv')
    if os.path.exists(venv_path):
        site_packages = os.path.join(venv_path, 'lib', f'python{sys.version_info.major}.{sys.version_info.minor}', 'site-packages')
        if os.path.exists(site_packages) and site_packages not in sys.path:
            sys.path.insert(0, site_packages)

from soda_core.contracts import verify_contract_locally
from soda_duckdb import DuckDBDataSource
from soda_core import configure_logging
import tempfile
import yaml

# Load environment variables
load_dotenv()

# Get project root directory
# When running in Airflow, use AIRFLOW_HOME; otherwise use the directory containing this file's parent (dags folder)
PROJECT_ROOT = os.getenv("AIRFLOW_HOME") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Configuration - use absolute paths
SOURCE_DIR = os.path.join(PROJECT_ROOT, "source")
PUBLIC_SCHEMA = "public"
PUBLIC_TABLE = "orders"
CONTRACT_FILE = os.path.join(PROJECT_ROOT, "contracts/webinardb/postgres/public/orders.yaml")
DATA_SOURCE_FILE = os.path.join(PROJECT_ROOT, "soda-db-config.yaml")
SODA_CLOUD_FILE = os.path.join(PROJECT_ROOT, "soda-cloud-config.yaml")


def get_db_connection():
    """Create PostgreSQL connection from environment variables with timeout and lightweight settings."""
    import resource
    
    # Log memory usage before connection
    try:
        mem_info = resource.getrusage(resource.RUSAGE_SELF)
        print(f"Memory usage before connection: {mem_info.ru_maxrss / 1024:.2f} MB")
    except:
        pass
    
    print("Attempting to connect to PostgreSQL...")
    print(f"Host: {os.getenv('POSTGRES_HOST')}")
    print(f"Port: {os.getenv('POSTGRES_PORT')}")
    print(f"Database: {os.getenv('POSTGRES_DATABASE')}")
    sys.stdout.flush()
    
    # Validate required environment variables
    required_vars = ["POSTGRES_HOST", "POSTGRES_DATABASE", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    try:
        # Use lightweight connection parameters to reduce memory usage
        # Set a very short timeout to fail fast
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DATABASE"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            connect_timeout=3,  # Very short timeout - fail after 3 seconds
            # Use minimal SSL settings
            sslmode='prefer',
            # Keep connection lightweight
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        print("PostgreSQL connection established successfully")
        sys.stdout.flush()
        return conn
    except psycopg2.OperationalError as e:
        print(f"PostgreSQL connection failed (OperationalError): {e}")
        sys.stdout.flush()
        raise
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()
        raise


def load_csv_data(batch_id):
    """
    Load CSV data from source directory using pandas, with data types from contract.
    
    Note: This function is called in both load_and_verify_task and insert_to_public_task.
    While this means loading the CSV twice, it's a deliberate trade-off to avoid passing
    large DataFrames through XCom, which can cause memory issues and serialization problems.
    For large datasets, re-reading from disk is more memory-efficient than serializing/deserializing.
    """
    csv_path = os.path.join(SOURCE_DIR, f"batch_{batch_id}.csv")
    
    print(f"Looking for CSV file at: {csv_path}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"SOURCE_DIR: {SOURCE_DIR}")
    print(f"PROJECT_ROOT: {PROJECT_ROOT}")
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    print(f"Loading data from {csv_path}...")
    
    # Load contract to get data types
    dtype_map = {}
    parse_dates = []
    
    if os.path.exists(CONTRACT_FILE):
        with open(CONTRACT_FILE, 'r') as f:
            contract_data = yaml.safe_load(f)
        
        # Map Soda data types to pandas dtypes
        soda_to_pandas = {
            'text': 'string',  # Use 'string' for nullable strings
            'varchar': 'string',
            'char': 'string',
            'numeric': 'float64',
            'integer': 'Int64',  # Nullable integer
            'int': 'Int64',
            'bigint': 'Int64',
            'smallint': 'Int64',
            'decimal': 'float64',
            'double': 'float64',
            'real': 'float32',
            'date': None,  # Will be parsed as date
            'timestamp': None,  # Will be parsed as datetime
            'datetime': None,
            'boolean': 'boolean',
            'bool': 'boolean',
        }
        
        # Extract column definitions from contract
        if 'columns' in contract_data:
            for col_def in contract_data['columns']:
                col_name = col_def.get('name')
                soda_type = col_def.get('data_type', 'text').lower()
                
                if col_name:
                    pandas_type = soda_to_pandas.get(soda_type, 'string')
                    if pandas_type is None:
                        # Date/timestamp types need special handling
                        if soda_type in ['date', 'timestamp', 'datetime']:
                            parse_dates.append(col_name)
                        else:
                            dtype_map[col_name] = 'string'
                    else:
                        dtype_map[col_name] = pandas_type
        
        print(f"Applying data types from contract: {dtype_map}")
        if parse_dates:
            print(f"Parsing dates for columns: {parse_dates}")
    
    # Read CSV into pandas DataFrame with proper data types
    # Use engine='c' for faster parsing and low_memory=False for better type inference
    df = pd.read_csv(
        csv_path,
        dtype=dtype_map if dtype_map else None,
        parse_dates=parse_dates if parse_dates else None,
        engine='c',  # Use C engine for faster parsing
        low_memory=False  # Better type inference, but requires more memory upfront
    )
    
    # Convert string columns that should be nullable
    for col, dtype in dtype_map.items():
        if col in df.columns and dtype == 'string':
            df[col] = df[col].astype('string')
    
    print(f"Detected columns: {', '.join(df.columns.tolist())}")
    print(f"Column dtypes: {df.dtypes.to_dict()}")
    print(f"Loaded {len(df)} rows from CSV")
    
    return df


def insert_data(pg_conn, schema, table, df):
    """Insert data from pandas DataFrame into PostgreSQL table using psycopg2.extras.execute_values."""
    if df is None or len(df) == 0:
        print("No data to insert")
        return
    
    column_names = df.columns.tolist()
    print(f"Inserting columns: {', '.join(column_names)}")
    
    # Quote column names for PostgreSQL
    quoted_columns = [f'"{col}"' for col in column_names]
    
    # Build INSERT statement with ON CONFLICT to handle duplicates gracefully
    insert_sql = f"""
    INSERT INTO {schema}.{table} ({', '.join(quoted_columns)})
    VALUES %s
    ON CONFLICT (order_id) DO NOTHING
    """
    
    # Convert DataFrame to list of tuples for efficient insertion
    # Use to_records() which is more memory-efficient than df.values
    # Replace NaN with None for proper NULL handling in PostgreSQL
    # Convert numpy.datetime64 to Python datetime for psycopg2 compatibility
    print("Converting DataFrame to tuples (memory-efficient)...")
    sys.stdout.flush()
    
    import numpy as np
    
    # Use to_records() which avoids creating a full NumPy array copy
    # Convert to list of tuples, handling NaN values and datetime types
    data = []
    for record in df.to_records(index=False):
        # Convert numpy record to tuple, handling NaN and datetime types
        row_values = []
        for val in record:
            if pd.isna(val):
                row_values.append(None)
            elif isinstance(val, np.datetime64):
                # Convert numpy.datetime64 to Python datetime
                row_values.append(pd.Timestamp(val).to_pydatetime())
            elif isinstance(val, np.integer):
                # Convert numpy integer types to Python int
                row_values.append(int(val))
            elif isinstance(val, np.floating):
                # Convert numpy float types to Python float
                row_values.append(float(val))
            else:
                row_values.append(val)
        data.append(tuple(row_values))
    
    print(f"Converted {len(data)} rows to tuples")
    sys.stdout.flush()
    
    # Use context manager for cursor to ensure proper cleanup
    print("Executing bulk insert...")
    sys.stdout.flush()
    try:
        with pg_conn.cursor() as cur:
            extras.execute_values(
                cur,
                insert_sql,
                data,
                template=None,
                page_size=100  # Insert in batches of 100 rows
            )
            print("Bulk insert completed, committing...")
            sys.stdout.flush()
            pg_conn.commit()
            # Note: Some rows may have been skipped due to duplicates (ON CONFLICT DO NOTHING)
            print(f"Insert operation completed for {len(data)} rows into {schema}.{table} (duplicates skipped)")
            sys.stdout.flush()
    except Exception as e:
        print(f"Error during insert: {e}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()
        pg_conn.rollback()
        raise


def create_contract_for_schema(original_contract_path, schema, output_path):
    """Create a modified contract file for a specific schema."""
    with open(original_contract_path, 'r') as f:
        contract_data = yaml.safe_load(f)
    
    # Update the dataset path to use the specified schema
    contract_data['dataset'] = f"webinardb/postgres/{schema}/orders"
    
    with open(output_path, 'w') as f:
        yaml.dump(contract_data, f, default_flow_style=False, sort_keys=False)
    
    return output_path


def verify_contract_with_duckdb(df, contract_file, publish=False):
    """Verify contract using DuckDB in-memory with pandas DataFrame."""
    import sys
    sys.stdout.flush()
    
    print(f"\n{'='*60}", flush=True)
    print(f"Verifying contract with DuckDB in-memory...", flush=True)
    print(f"{'='*60}", flush=True)
    
    # Configure logging
    print("Configuring logging...", flush=True)
    configure_logging(verbose=False)
    print("Logging configured", flush=True)
    
    # Create DuckDB in-memory connection
    print("Creating DuckDB connection...", flush=True)
    conn = duckdb.connect(database=":memory:")
    cursor = conn.cursor()
    print("DuckDB connection created", flush=True)
    
    # Register pandas DataFrame as a view in DuckDB
    print("Registering DataFrame as view...", flush=True)
    cursor.register(view_name="orders_raw", python_object=df)
    print("DataFrame registered", flush=True)
    
    # Create a view with proper types: convert dates to timestamp_ns and ensure amount is numeric
    print("Creating typed view with proper DuckDB types...", flush=True)
    select_parts = []
    
    for col in df.columns:
        if col == 'order_date':
            select_parts.append("CAST(order_date AS DATE) AS order_date")
        elif col == 'shipping_date':
            select_parts.append("CAST(shipping_date AS DATE) AS shipping_date")
        elif col == 'amount':
            select_parts.append("CAST(amount AS NUMERIC) AS amount")
        else:
            # Keep other columns as-is
            select_parts.append(f'"{col}"')
    
    # Create the typed view
    create_view_sql = f"""
    CREATE VIEW orders AS
    SELECT {', '.join(select_parts)}
    FROM orders_raw
    """
    cursor.execute(create_view_sql)
    print("Typed view created with proper types", flush=True)
    
    # Create DuckDB data source
    print("Creating DuckDBDataSource...", flush=True)
    duckdb_data_source = DuckDBDataSource.from_existing_cursor(cursor, name="webinardb")
    print("DuckDBDataSource created", flush=True)
    
    try:
        # Update contract dataset path to use main schema instead of postgres
        with open(contract_file, 'r') as f:
            contract_data = yaml.safe_load(f)
        
        # Store original dataset path
        original_dataset = contract_data.get('dataset', '')
        
        # Update dataset to use main schema (e.g., webinardb/main/orders)
        # DuckDB uses "main" as the default schema
        if '/' in original_dataset:
            parts = original_dataset.split('/')
            # Replace postgres/public with main, keep data source name and table
            updated_dataset = f"{parts[0]}/main/{parts[-1]}"
            contract_data['dataset'] = updated_dataset
        else:
            updated_dataset = original_dataset
        
        # Create temporary contract file with updated dataset
        temp_contract = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
        temp_contract_path = temp_contract.name
        yaml.dump(contract_data, temp_contract, default_flow_style=False, sort_keys=False)
        temp_contract.close()
        
        print(f"Using dataset: {updated_dataset}")
        print(f"Contract file: {temp_contract_path}")
        print(f"Data source type: {type(duckdb_data_source)}")
        print("About to call verify_contract_locally...")
        import time
        start_time = time.time()
        
        # Verify contract using DuckDB data source
        result = verify_contract_locally(
            data_sources=[duckdb_data_source],
            contract_file_path=temp_contract_path,
            soda_cloud_file_path=SODA_CLOUD_FILE if publish else None,
            publish=publish
        )
        
        elapsed_time = time.time() - start_time
        print(f"verify_contract_locally completed in {elapsed_time:.2f} seconds")
        
        # Print summary
        print(f"\nVerification Summary:")
        print(f"  Total checks: {result.number_of_checks}")
        print(f"  Passed: {result.number_of_checks_passed}")
        print(f"  Failed: {result.number_of_checks_failed}")
        print(f"  Has errors: {result.has_errors}")
        
        if result.has_errors:
            print(f"\nErrors:")
            print(result.get_errors_str())
        
        # Check if verification passed
        if result.is_failed or result.has_errors:
            return False
        else:
            print(f"\n✅ Contract verification PASSED")
            return True
            
    except Exception as e:
        print(f"\n❌ Error during verification:")
        print(str(e))
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Clean up
        conn.close()
        if 'temp_contract_path' in locals() and os.path.exists(temp_contract_path):
            os.unlink(temp_contract_path)


def verify_contract(schema, contract_file=None, publish=False):
    """Verify contract for PostgreSQL schema."""
    import sys
    
    # Configure logging
    print("Configuring logging...")
    sys.stdout.flush()
    configure_logging(verbose=False)
    print("Logging configured")
    sys.stdout.flush()
    
    print(f"\n{'='*60}")
    print(f"Verifying contract for {schema}.orders...")
    print(f"Publish to Soda Cloud: {publish}")
    print(f"{'='*60}")
    sys.stdout.flush()
    
    # Create a temporary contract file for the schema
    if contract_file is None:
        print("Creating temporary contract file...")
        sys.stdout.flush()
        temp_contract = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
        contract_file = create_contract_for_schema(CONTRACT_FILE, schema, temp_contract.name)
        temp_contract.close()
        print(f"Temporary contract file created: {contract_file}")
        sys.stdout.flush()
    
    try:
        print(f"About to call verify_contract_locally...")
        print(f"  data_source_file_path: {DATA_SOURCE_FILE}")
        print(f"  contract_file_path: {contract_file}")
        print(f"  soda_cloud_file_path: {SODA_CLOUD_FILE if publish else None}")
        print(f"  publish: {publish}")
        sys.stdout.flush()
        
        result = verify_contract_locally(
            data_source_file_path=DATA_SOURCE_FILE,
            contract_file_path=contract_file,
            soda_cloud_file_path=SODA_CLOUD_FILE if publish else None,
            publish=publish
        )
        
        print("verify_contract_locally completed successfully")
        sys.stdout.flush()
        
        # Print summary
        print(f"\nVerification Summary:")
        print(f"  Total checks: {result.number_of_checks}")
        print(f"  Passed: {result.number_of_checks_passed}")
        print(f"  Failed: {result.number_of_checks_failed}")
        print(f"  Has errors: {result.has_errors}")
        
        if result.has_errors:
            print(f"\nErrors:")
            print(result.get_errors_str())
        
        # Check if verification passed
        if result.is_failed or result.has_errors:
            print(f"\n❌ Contract verification FAILED for {schema}.orders")
            return False
        else:
            print(f"\n✅ Contract verification PASSED for {schema}.orders")
            return True
            
    except Exception as e:
        print(f"\n❌ Error during verification:")
        print(str(e))
        return False
    finally:
        # Clean up temporary contract file if we created one
        if contract_file != CONTRACT_FILE and os.path.exists(contract_file):
            os.unlink(contract_file)


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'orders_pipeline_with_soda',
    default_args=default_args,
    description='Orders data pipeline with Soda contract verification',
    schedule=None,  # Manual trigger only - no automatic scheduling
    start_date=datetime(2025, 12, 2),  # Start date for the DAG
    catchup=False,  # Don't run backfill for past dates
    is_paused_upon_creation=True,  # Pause the DAG when first created
    tags=['soda', 'data-quality', 'orders', 'etl'],
)


def load_and_verify_task(**context):
    """Task to load CSV data and verify contract using DuckDB in-memory."""
    import sys
    import time
    
    # Immediate output to confirm function is called
    print("TASK STARTED", flush=True)
    print(f"Timestamp: {time.time()}", flush=True)
    print("=" * 60, flush=True)
    print("Starting load_and_verify_task", flush=True)
    print("=" * 60, flush=True)
    
    try:
        # Change to project root directory (AIRFLOW_HOME) to ensure paths resolve correctly
        airflow_home = os.getenv("AIRFLOW_HOME")
        if airflow_home:
            os.chdir(airflow_home)
            print(f"Changed working directory to: {os.getcwd()}")
            sys.stdout.flush()
        
        # Get batch_id from DAG run config or use default
        dag_run = context.get('dag_run')
        batch_id = dag_run.conf.get('batch_id', '1') if dag_run and dag_run.conf else '1'
        
        print(f"Loading CSV data for batch_{batch_id} and verifying contract...")
        print(f"Batch ID: {batch_id}")
        sys.stdout.flush()
        
        # Load CSV data into pandas DataFrame
        print("Calling load_csv_data...")
        sys.stdout.flush()
        df = load_csv_data(batch_id)
        print(f"Loaded DataFrame with {len(df)} rows")
        sys.stdout.flush()
        
        # Verify contract using DuckDB in-memory (without publishing)
        print("Calling verify_contract_with_duckdb...")
        sys.stdout.flush()
        passed = verify_contract_with_duckdb(df, CONTRACT_FILE, publish=False)
        print(f"Verification result: {passed}")
        sys.stdout.flush()
        
        if not passed:
            raise Exception(f"Contract verification failed. Pipeline stopped.")
        
        print("Task completed successfully")
        sys.stdout.flush()
        
        # Only return batch_id - next task will load CSV directly
        return {'batch_id': batch_id, 'verified': True}
    except Exception as e:
        print(f"ERROR in load_and_verify_task: {e}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()
        raise


def insert_to_public_task(**context):
    """Task to insert verified data to public.orders."""
    try:
        # Change to project root directory (AIRFLOW_HOME) to ensure paths resolve correctly
        airflow_home = os.getenv("AIRFLOW_HOME")
        if airflow_home:
            os.chdir(airflow_home)
            print(f"Changed working directory to: {os.getcwd()}")
            sys.stdout.flush()
        
        # Get batch_id from previous task or DAG run config
        ti = context['ti']
        result = ti.xcom_pull(task_ids='load_and_verify')
        if result:
            batch_id = result['batch_id']
        else:
            # Fallback to DAG run config
            dag_run = context.get('dag_run')
            batch_id = dag_run.conf.get('batch_id', '1') if dag_run and dag_run.conf else '1'
        
        print(f"Inserting verified data to {PUBLIC_SCHEMA}.{PUBLIC_TABLE} for batch_{batch_id}")
        sys.stdout.flush()
        
        # Load CSV data 
        print(f"Loading CSV data for batch_{batch_id}...")
        sys.stdout.flush()
        df = load_csv_data(batch_id)
        print(f"Loaded DataFrame with {len(df)} rows")
        sys.stdout.flush()
        
        print("Establishing Connection")
        sys.stdout.flush()
        
        # Get database connection - use context manager pattern for proper cleanup
        pg_conn = get_db_connection()
        print("Connection established")
        sys.stdout.flush()
        
        try:
            insert_data(pg_conn, PUBLIC_SCHEMA, PUBLIC_TABLE, df)
            print(f"Successfully inserted {len(df)} rows to {PUBLIC_SCHEMA}.{PUBLIC_TABLE}")
            sys.stdout.flush()
        finally:
            # Ensure connection is always closed, even on error
            print("Closing database connection...")
            sys.stdout.flush()
            pg_conn.close()
            print("Database connection closed")
            sys.stdout.flush()
        
        return {'batch_id': batch_id}
    except Exception as e:
        print(f"ERROR in insert_to_public_task: {e}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()
        raise


def verify_public_task(**context):
    """Task to verify contract on public.orders and publish to Soda Cloud."""
    ti = context['ti']
    result = ti.xcom_pull(task_ids='insert_to_public')
    batch_id = result['batch_id']
    
    print(f"Verifying contract on {PUBLIC_SCHEMA}.{PUBLIC_TABLE} for batch_{batch_id}")
    
    # Verify contract on public (with publishing to Soda Cloud)
    passed = verify_contract(PUBLIC_SCHEMA, publish=True)
    
    if not passed:
        raise Exception(f"Contract verification failed on {PUBLIC_SCHEMA}.{PUBLIC_TABLE}.")
    
    return {'batch_id': batch_id, 'public_verified': True}


# Define tasks
load_and_verify = PythonOperator(
    task_id='load_and_verify',
    python_callable=load_and_verify_task,
    dag=dag,
)

insert_to_public = PythonOperator(
    task_id='insert_to_public',
    python_callable=insert_to_public_task,
    dag=dag,
)

verify_public = PythonOperator(
    task_id='verify_public',
    python_callable=verify_public_task,
    dag=dag,
)

# Define task dependencies
# The pipeline flows: load_and_verify -> insert_to_public -> verify_public
# 1. Load CSV data and verify contract using DuckDB in-memory (before database write)
# 2. Insert to PostgreSQL if verification passes
# 3. Verify on PostgreSQL and publish to Soda Cloud
load_and_verify >> insert_to_public >> verify_public
