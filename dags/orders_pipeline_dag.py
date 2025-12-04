"""
Airflow DAG for Orders Data Pipeline with Soda Contract Verification

Pipeline steps:
1. Load CSV data and verify contract using DuckDB in-memory
2. Insert verified data to PostgreSQL
3. Verify contract on PostgreSQL and publish to Soda Cloud
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import pandas as pd
import duckdb
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
import tempfile
import yaml

# Setup venv path for Airflow
if 'VIRTUAL_ENV' in os.environ:
    venv_path = os.environ['VIRTUAL_ENV']
    site_packages = os.path.join(venv_path, 'lib', f'python{os.sys.version_info.major}.{os.sys.version_info.minor}', 'site-packages')
    if os.path.exists(site_packages) and site_packages not in os.sys.path:
        os.sys.path.insert(0, site_packages)
else:
    project_root = os.getenv("AIRFLOW_HOME") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    venv_path = os.path.join(project_root, 'venv')
    if os.path.exists(venv_path):
        site_packages = os.path.join(venv_path, 'lib', f'python{os.sys.version_info.major}.{os.sys.version_info.minor}', 'site-packages')
        if os.path.exists(site_packages) and site_packages not in os.sys.path:
            os.sys.path.insert(0, site_packages)

from soda_core.contracts import verify_contract_locally
from soda_duckdb import DuckDBDataSource
from soda_core import configure_logging

load_dotenv()

# Configuration
PROJECT_ROOT = os.getenv("AIRFLOW_HOME") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_DIR = os.path.join(PROJECT_ROOT, "source")
CONTRACT_FILE = os.path.join(PROJECT_ROOT, "contracts/webinardb/postgres/public/orders.yaml")
DATA_SOURCE_FILE = os.path.join(PROJECT_ROOT, "soda-db-config.yaml")
SODA_CLOUD_FILE = os.path.join(PROJECT_ROOT, "soda-cloud-config.yaml")


def load_csv(batch_id):
    """Load CSV data from source directory."""
    csv_path = os.path.join(SOURCE_DIR, f"batch_{batch_id}.csv")
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    print(f"📂 Loading CSV: batch_{batch_id}.csv")
    df = pd.read_csv(csv_path)
    print(f"✅ Loaded {len(df)} rows")
    
    return df


def insert_to_postgres(df, schema="public", table="orders"):
    """Insert data to PostgreSQL."""
    print(f"\n💾 Inserting data to {schema}.{table}...")
    
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    
    try:
        quoted_columns = [f'"{col}"' for col in df.columns]
        insert_sql = f"""
        INSERT INTO {schema}.{table} ({', '.join(quoted_columns)})
        VALUES %s
        ON CONFLICT (order_id) DO NOTHING
        """
        
        # Convert DataFrame to tuples
        import numpy as np
        data = []
        for record in df.to_records(index=False):
            row_values = []
            for val in record:
                if pd.isna(val):
                    row_values.append(None)
                elif isinstance(val, np.datetime64):
                    row_values.append(pd.Timestamp(val).to_pydatetime())
                elif isinstance(val, (np.integer, np.int64, np.int32, np.int16, np.int8)):
                    row_values.append(int(val))
                elif isinstance(val, (np.floating, np.float64, np.float32)):
                    row_values.append(float(val))
                elif isinstance(val, np.bool_):
                    row_values.append(bool(val))
                else:
                    row_values.append(val)
            data.append(tuple(row_values))
        
        with conn.cursor() as cur:
            extras.execute_values(cur, insert_sql, data, page_size=100)
            conn.commit()
        
        print(f"✅ Inserted {len(data)} rows")
    finally:
        conn.close()


# DAG Definition
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'orders_pipeline_with_soda',
    default_args=default_args,
    description='Orders data pipeline with Soda contract verification',
    schedule=None,
    start_date=datetime(2025, 12, 2),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['soda', 'data-quality', 'orders', 'etl'],
)


# Task Functions
def step1_load_and_verify(**context):
    """Step 1: Load CSV and verify contract with DuckDB."""
    print("=" * 60)
    print("STEP 1: Load CSV and Verify Contract (DuckDB)")
    print("=" * 60)
    
    # Get batch_id from DAG run config
    dag_run = context.get('dag_run')
    batch_id = dag_run.conf.get('batch_id', '1') if dag_run and dag_run.conf else '1'
    
    # Load CSV
    df = load_csv(batch_id)
    
    # Verify contract with DuckDB
    print(f"\n🔍 Verifying contract with DuckDB (in-memory)...")
    configure_logging(verbose=False)
    
    # Setup DuckDB
    conn = duckdb.connect(database=":memory:")
    cursor = conn.cursor()
    cursor.register(view_name="orders_raw", python_object=df)
    
    # Create typed view
    select_parts = []
    for col in df.columns:
        if col in ['order_date', 'shipping_date']:
            select_parts.append(f"CAST({col} AS DATE) AS {col}")
        elif col == 'amount':
            select_parts.append(f"CAST(amount AS NUMERIC) AS amount")
        else:
            select_parts.append(f'"{col}"')
    
    cursor.execute(f"CREATE VIEW orders AS SELECT {', '.join(select_parts)} FROM orders_raw")
    
    # Create data source
    duckdb_data_source = DuckDBDataSource.from_existing_cursor(cursor, name="webinardb")
    
    # Update contract for DuckDB schema
    with open(CONTRACT_FILE, 'r') as f:
        contract_data = yaml.safe_load(f)
    
    original_dataset = contract_data.get('dataset', '')
    if '/' in original_dataset:
        parts = original_dataset.split('/')
        contract_data['dataset'] = f"{parts[0]}/main/{parts[-1]}"
    
    temp_contract = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
    temp_contract_path = temp_contract.name
    yaml.dump(contract_data, temp_contract, default_flow_style=False)
    temp_contract.close()
    
    try:
        result = verify_contract_locally(
            data_sources=[duckdb_data_source],
            contract_file_path=temp_contract_path,
            publish=False
        )
        
        print(f"   Checks: {result.number_of_checks} | Passed: {result.number_of_checks_passed} | Failed: {result.number_of_checks_failed}")
        
        if result.is_failed or result.has_errors:
            print(f"❌ Contract verification FAILED")
            if result.has_errors:
                print(result.get_errors_str())
            raise Exception("Contract verification failed. Pipeline stopped.")
        else:
            print(f"✅ Contract verification PASSED")
    finally:
        conn.close()
        if os.path.exists(temp_contract_path):
            os.unlink(temp_contract_path)
    
    return {'batch_id': batch_id}


def step2_insert_to_postgres(**context):
    """Step 2: Insert verified data to PostgreSQL."""
    print("=" * 60)
    print("STEP 2: Insert Data to PostgreSQL")
    print("=" * 60)
    
    # Get batch_id from previous task
    ti = context['ti']
    result = ti.xcom_pull(task_ids='load_and_verify')
    batch_id = result['batch_id'] if result else '1'
    
    # Load CSV and insert
    df = load_csv(batch_id)
    insert_to_postgres(df)
    
    return {'batch_id': batch_id}


def step3_verify_on_postgres(**context):
    """Step 3: Verify contract on PostgreSQL and publish to Soda Cloud."""
    print("=" * 60)
    print("STEP 3: Verify Contract on PostgreSQL")
    print("=" * 60)
    
    # Get batch_id from previous task
    ti = context['ti']
    result = ti.xcom_pull(task_ids='insert_to_public')
    batch_id = result['batch_id'] if result else '1'
    
    # Verify contract on PostgreSQL
    schema = "public"
    publish = True
    print(f"\n🔍 Verifying contract on {schema}.orders...")
    print(f"   Publishing to Soda Cloud: Yes")
    
    configure_logging(verbose=False)
    
    # Create contract for schema
    with open(CONTRACT_FILE, 'r') as f:
        contract_data = yaml.safe_load(f)
    contract_data['dataset'] = f"webinardb/postgres/{schema}/orders"
    
    temp_contract = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
    temp_contract_path = temp_contract.name
    yaml.dump(contract_data, temp_contract, default_flow_style=False)
    temp_contract.close()
    
    try:
        result = verify_contract_locally(
            data_source_file_path=DATA_SOURCE_FILE,
            contract_file_path=temp_contract_path,
            soda_cloud_file_path=SODA_CLOUD_FILE if publish else None,
            publish=publish
        )
        
        print(f"   Checks: {result.number_of_checks} | Passed: {result.number_of_checks_passed} | Failed: {result.number_of_checks_failed}")
        
        if result.is_failed or result.has_errors:
            print(f"❌ Contract verification FAILED")
            if result.has_errors:
                print(result.get_errors_str())
            raise Exception("Contract verification failed on PostgreSQL.")
        else:
            print(f"✅ Contract verification PASSED")
    finally:
        if os.path.exists(temp_contract_path):
            os.unlink(temp_contract_path)
    
    print("\n" + "=" * 60)
    print("✅ Pipeline completed successfully!")
    print("=" * 60)
    
    return {'batch_id': batch_id}


# Define Tasks
load_and_verify = PythonOperator(
    task_id='load_and_verify',
    python_callable=step1_load_and_verify,
    dag=dag,
)

insert_to_public = PythonOperator(
    task_id='insert_to_public',
    python_callable=step2_insert_to_postgres,
    dag=dag,
)

verify_public = PythonOperator(
    task_id='verify_public',
    python_callable=step3_verify_on_postgres,
    dag=dag,
)

# Task Dependencies
load_and_verify >> insert_to_public >> verify_public
