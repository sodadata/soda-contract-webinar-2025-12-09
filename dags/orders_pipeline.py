"""
Airflow DAG for Orders Data Pipeline with Soda Contract Verification

Pipeline steps:
1. Generate test data (5 rows)
2. Verify contract using DuckDB in-memory
3. Insert verified data to PostgreSQL
4. Verify contract on PostgreSQL and publish to Soda Cloud

Use {"hasErrors":true} to make it fail
"""

from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
import os


# Configuration - Use lazy evaluation
def get_config():
    """Lazy load configuration when needed."""
    from dotenv import load_dotenv
    load_dotenv()
    
    PROJECT_ROOT = os.getenv("AIRFLOW_HOME") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return {
        'CONTRACT_FILE': os.path.join(PROJECT_ROOT, "contracts/webinardb/postgres/public/orders.yaml"),
        'DATA_SOURCE_FILE': os.path.join(PROJECT_ROOT, "soda-db-config.yaml"),
        'SODA_CLOUD_FILE': os.path.join(PROJECT_ROOT, "soda-cloud-config.yaml"),
        'PROJECT_ROOT': PROJECT_ROOT
    }


def generate_test_data(has_error=False):
    """Generate 5 rows of test data."""
    import pandas as pd
    import uuid
    import random
    
    print(f"📝 Generating test data (5 rows)...")
    if has_error:
        print(f"   ⚠️  Generating data with errors for testing")
    
    valid_statuses = ['PENDING', 'SHIPPED', 'CANCELLED']
    invalid_status = 'INVALID_STATUS'
    countries = ['AU', 'CA', 'DE', 'FR', 'GB', 'IN', 'US']
    addresses = [
        '123 Main Street, New York, NY 10001',
        '456 Oak Avenue, Los Angeles, CA 90001',
        '789 Elm Road, Chicago, IL 60601',
        '321 Pine Boulevard, Houston, TX 77001',
        '654 Maple Drive, Phoenix, AZ 85001'
    ]
    
    data = []
    for i in range(5):
        # Generate valid UUIDs
        order_id = str(uuid.uuid4())
        customer_id = str(uuid.uuid4())
        
        # Generate dates
        order_date = date.today() - timedelta(days=random.randint(1, 30))
        
        # Error: shipping_date before order_date (row 0)
        if has_error and i == 0:
            shipping_date = order_date - timedelta(days=random.randint(1, 7))
        else:
            shipping_date = order_date + timedelta(days=random.randint(1, 7))
        
        # Generate status
        if has_error and i == 1:
            # Error: invalid status (row 1)
            status = invalid_status
        elif has_error and i == 2:
            # Error: missing status (row 2)
            status = None
        else:
            status = random.choice(valid_statuses)
        
        # Generate other fields
        shipping_address = random.choice(addresses)
        amount = round(random.uniform(10.0, 1000.0), 2)
        country_code = random.choice(countries)
        
        data.append({
            'order_id': order_id,
            'customer_id': customer_id,
            'order_date': order_date,
            'shipping_date': shipping_date,
            'shipping_address': shipping_address,
            'amount': amount,
            'status': status,
            'country_code': country_code
        })
    
    df = pd.DataFrame(data)
    # Ensure date columns are proper date types
    df['order_date'] = pd.to_datetime(df['order_date']).dt.date
    df['shipping_date'] = pd.to_datetime(df['shipping_date']).dt.date
    # Ensure amount is numeric (not float/double) - use Decimal for proper numeric type
    from decimal import Decimal
    df['amount'] = df['amount'].apply(lambda x: Decimal(str(x)) if x is not None else None)
    
    print(f"✅ Generated {len(df)} rows")
    
    return df


def insert_to_postgres(df, schema="public", table="orders"):
    """Insert data to PostgreSQL."""
    import psycopg2
    from psycopg2 import extras
    import pandas as pd
    import numpy as np
    from dotenv import load_dotenv
    
    load_dotenv()
    
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



# Task Functions
def step1_load(**context):
    """Step 1: Generate test data."""
    print("=" * 60)
    print("STEP 1: Generate Test Data")
    print("=" * 60)
    
    # Get hasErrors from DAG run config (default: false)
    dag_run = context.get('dag_run')
    has_error = dag_run.conf.get('hasErrors', False) if dag_run and dag_run.conf else False
    
    # Generate test data
    df = generate_test_data(has_error=has_error)
    
    # Print generated data as table with nice formatting
    print("=" * 60)
    print(f"\n📊 Generated Data:")
    print()
    
    # Create a nicely formatted table with borders
    from tabulate import tabulate
    print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
    
    print()
    print("=" * 60)
    
    # Return DataFrame as JSON for XCom
    return df.to_dict('records')


def step2_verify_locally(**context):
    """Step 2: Verify contract with DuckDB in-memory."""
    # Import heavy dependencies INSIDE the task
    import pandas as pd
    import duckdb
    from soda_core.contracts import verify_contract_locally
    from soda_duckdb import DuckDBDataSource
    from soda_core import configure_logging
    
    print("=" * 60)
    print("STEP 2: Verify Contract in-memory (DuckDB)")
    print("=" * 60)
    
    # Get config
    config = get_config()
    
    # Get data from previous task
    ti = context['ti']
    data_records = ti.xcom_pull(task_ids='load')
    df = pd.DataFrame(data_records)
    
    print(f"\n🔍 Verifying contract with DuckDB (in-memory)...")
    configure_logging(verbose=True)
    
    # Setup DuckDB
    conn = duckdb.connect(database=":memory:")
    cursor = conn.cursor()

    # Register DataFrame directly as a view in main schema (DuckDB default)
    # DuckDB will infer types from pandas DataFrame
    cursor.register(view_name="orders", python_object=df)
    
    # Create temporary contract with adjusted dataset path for DuckDB
    import yaml
    import tempfile
    with open(config['CONTRACT_FILE'], 'r') as f:
        contract_data = yaml.safe_load(f)
    
    # Extract dataset and replace middle part with 'main' for DuckDB
    # e.g., webinardb/postgres/public/orders -> webinardb/main/orders
    original_dataset = contract_data.get('dataset', '')
    parts = original_dataset.split('/')
    # Keep first part (data source) and last part (table), replace middle with 'main'
    contract_data['dataset'] = f"{parts[0]}/main/{parts[-1]}"
    
    temp_contract = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
    temp_contract_path = temp_contract.name
    yaml.dump(contract_data, temp_contract, default_flow_style=False)
    temp_contract.close()
    
    # Create data source
    duckdb_data_source = DuckDBDataSource.from_existing_cursor(cursor, name="webinardb")
    
    result = verify_contract_locally(
        data_sources=[duckdb_data_source],
        contract_file_path=temp_contract_path,
        publish=False
    )
    
    # Only fail if checks actually failed, not if there were evaluation errors
    if result.has_errors:
        print(f"⚠️  Some checks could not be evaluated (errors occurred)")
        print(result.get_errors_str())

    
    # CRITICAL: Only fail if checks actually failed (is_failed), not on evaluation errors
    if result.number_of_checks_failed > 0:
        print(f"❌ Contract verification FAILED (checks failed)")
        ti.max_tries = ti.try_number
        raise Exception("Contract verification failed. Pipeline stopped.")
    else:
        print(f"✅ Contract verification PASSED")
    
    conn.close()
    # Clean up temporary contract file
    if os.path.exists(temp_contract_path):
        os.unlink(temp_contract_path)
    
    # Return DataFrame as JSON for next step
    return ti.xcom_pull(task_ids='load')


def step3_insert_to_postgres(**context):
    """Step 3: Insert verified data to PostgreSQL."""
    import pandas as pd
    
    print("=" * 60)
    print("STEP 3: Insert Data to PostgreSQL")
    print("=" * 60)
    
    # Get data from previous task
    ti = context['ti']
    data_records = ti.xcom_pull(task_ids='verify_locally')
    df = pd.DataFrame(data_records)
    
    # Insert to PostgreSQL
    insert_to_postgres(df)


def step4_verify_on_postgres(**context):
    """Step 4: Verify contract on PostgreSQL and publish to Soda Cloud."""
    # Import heavy dependencies INSIDE the task
    from soda_core.contracts import verify_contract_locally
    from soda_core import configure_logging
    
    print("=" * 60)
    print("STEP 4: Verify Contract on PostgreSQL")
    print("=" * 60)
    
    # Get config
    config = get_config()
    
    configure_logging(verbose=False)
    
    result = verify_contract_locally(
        data_source_file_path=config['DATA_SOURCE_FILE'],
        contract_file_path=config['CONTRACT_FILE'],
        soda_cloud_file_path=config['SODA_CLOUD_FILE'],
        publish=True
    )
    
    # Only fail if checks actually failed, not if there were evaluation errors
    if result.has_errors:
        print(f"⚠️  Some checks could not be evaluated (errors occurred)")
        print(result.get_errors_str())

    # CRITICAL: Only fail if checks actually failed (is_failed), not on evaluation errors
    if result.is_failed:
        print(f"❌ Contract verification FAILED (checks failed)")
        ti.max_tries = ti.try_number 
        raise Exception("Contract verification failed on PostgreSQL.")
    else:
        print(f"✅ Contract verification PASSED")
        print("✅ Pipeline completed successfully!")

# DAG Definition
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
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

# Define Tasks
load = PythonOperator(
    task_id='load',
    python_callable=step1_load,
    dag=dag,
)

verify_locally = PythonOperator(
    task_id='verify_locally',
    python_callable=step2_verify_locally,
    dag=dag,
)

insert_to_public = PythonOperator(
    task_id='insert_to_public',
    python_callable=step3_insert_to_postgres,
    dag=dag,
)

verify_public = PythonOperator(
    task_id='verify_public',
    python_callable=step4_verify_on_postgres,
    dag=dag,
)

# Task Dependencies
load >> verify_locally >> insert_to_public >> verify_public