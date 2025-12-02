#!/usr/bin/env python3
"""
Orders Pipeline Script

This script:
1. Loads CSV data from source/batch_{id}.csv
2. Pushes data to staging.orders
3. Verifies contract on staging.orders
4. If verification passes, publishes to public.orders
5. Verifies contract on public.orders
6. Fails if any step fails
"""

import sys
import os
import argparse
import csv
import duckdb
import psycopg2
from dotenv import load_dotenv
from soda_core.contracts import verify_contract_locally
from soda_core import configure_logging
import tempfile
import yaml
from pathlib import Path

# Load environment variables
load_dotenv()

# Configuration
SOURCE_DIR = "source"
STAGING_SCHEMA = "staging"
PUBLIC_SCHEMA = "public"
STAGING_TABLE = "orders"
PUBLIC_TABLE = "orders"
CONTRACT_FILE = "contracts/webinardb/postgres/public/orders.yaml"
DATA_SOURCE_FILE = "soda-db-config.yaml"
SODA_CLOUD_FILE = "soda-cloud-config.yaml"


def get_db_connection():
    """Create PostgreSQL connection from environment variables."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )


def load_csv_data(batch_id):
    """Load CSV data from source directory using DuckDB."""
    csv_path = os.path.join(SOURCE_DIR, f"batch_{batch_id}.csv")
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    print(f"Loading data from {csv_path}...")
    conn = duckdb.connect()
    
    # Read CSV header to get column names
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        column_names = next(reader)  # Read header row
    
    print(f"Detected columns: {', '.join(column_names)}")
    
    # Read CSV directly into DuckDB using read_csv_auto
    conn.execute(f"CREATE TABLE csv_data AS SELECT * FROM read_csv_auto('{csv_path}')")
    
    # Get row count
    row_count = conn.execute("SELECT COUNT(*) FROM csv_data").fetchone()[0]
    print(f"Loaded {row_count} rows from CSV")
    
    return conn

def insert_data(pg_conn, schema, table, duckdb_conn, batch_id):
    """Insert data from DuckDB into PostgreSQL table."""
    # Get PostgreSQL connection string
    pg_connection_string = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DATABASE')}"
    )
    
    # Attach PostgreSQL to DuckDB and insert data
    duckdb_conn.execute(f"ATTACH '{pg_connection_string}' AS pg (TYPE POSTGRES)")
    
    # Get column names directly from CSV file header (most reliable)
    csv_path = os.path.join(SOURCE_DIR, f"batch_{batch_id}.csv")
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        column_names = next(reader)  # Read header row
    
    # Get actual column names from DuckDB table to ensure they match
    try:
        info_result = duckdb_conn.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'csv_data'
            ORDER BY ordinal_position
        """).fetchall()
        duckdb_columns = [row[0] for row in info_result]
        # Use DuckDB's actual column names
        if duckdb_columns:
            column_names = duckdb_columns
    except:
        # If that fails, use the CSV header names
        pass
    
    # Quote column names for PostgreSQL (target table)
    quoted_columns = [f'"{col}"' for col in column_names]
    # Don't quote for DuckDB SELECT (source table)
    unquoted_columns = column_names
    
    # Insert data from DuckDB table to PostgreSQL
    insert_sql = f"""
    INSERT INTO pg.{schema}.{table} ({', '.join(quoted_columns)})
    SELECT {', '.join(unquoted_columns)} FROM csv_data
    """
    
    print(f"Inserting columns: {', '.join(column_names)}")
    duckdb_conn.execute(insert_sql)
    
    # Get row count
    row_count = duckdb_conn.execute("SELECT COUNT(*) FROM csv_data").fetchone()[0]
    print(f"Inserted {row_count} rows into {schema}.{table}")


def create_contract_for_schema(original_contract_path, schema, output_path):
    """Create a modified contract file for a specific schema."""
    with open(original_contract_path, 'r') as f:
        contract_data = yaml.safe_load(f)
    
    # Update the dataset path to use the specified schema
    contract_data['dataset'] = f"webinardb/postgres/{schema}/orders"
    
    with open(output_path, 'w') as f:
        yaml.dump(contract_data, f, default_flow_style=False, sort_keys=False)
    
    return output_path


def verify_contract(schema, contract_file=None, publish=False):
    """Verify contract for the specified schema."""
    # Configure logging
    configure_logging(verbose=False)
    
    print(f"\n{'='*60}")
    print(f"Verifying contract for {schema}.orders...")
    print(f"{'='*60}")
    
    # Create a temporary contract file for the schema
    if contract_file is None:
        temp_contract = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
        contract_file = create_contract_for_schema(CONTRACT_FILE, schema, temp_contract.name)
        temp_contract.close()
    
    try:
        result = verify_contract_locally(
            data_source_file_path=DATA_SOURCE_FILE,
            contract_file_path=contract_file,
            soda_cloud_file_path=SODA_CLOUD_FILE,
            publish=publish
        )
        
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


def copy_staging_to_public(conn):
    """Copy data from staging.orders to public.orders."""
    print(f"\n{'='*60}")
    print(f"Copying data from {STAGING_SCHEMA}.{STAGING_TABLE} to {PUBLIC_SCHEMA}.{PUBLIC_TABLE}...")
    print(f"{'='*60}")
    
    with conn.cursor() as cur:
        # Copy data
        copy_sql = f"""
        INSERT INTO {PUBLIC_SCHEMA}.{PUBLIC_TABLE}
        SELECT * FROM {STAGING_SCHEMA}.{STAGING_TABLE}
        """
        cur.execute(copy_sql)
        conn.commit()
        
        # Get row count
        cur.execute(f"SELECT COUNT(*) FROM {PUBLIC_SCHEMA}.{PUBLIC_TABLE}")
        count = cur.fetchone()[0]
        print(f"Copied {count} rows to {PUBLIC_SCHEMA}.{PUBLIC_TABLE}")


def main():
    parser = argparse.ArgumentParser(description='Orders Pipeline: Load CSV, verify, and publish to production')
    parser.add_argument('id', type=str, help='Batch ID (e.g., "1" for batch_1.csv)')
    args = parser.parse_args()
    
    batch_id = args.id
    pg_conn = None
    duckdb_conn = None
    
    try:
        print(f"\n{'='*60}")
        print(f"Starting Orders Pipeline for batch_{batch_id}")
        print(f"{'='*60}\n")
        
        # Load CSV data
        duckdb_conn = load_csv_data(batch_id)
        
        # Connect to database
        print("\nConnecting to PostgreSQL...")
        pg_conn = get_db_connection()
        print("Connected successfully")
        
        # Create staging table and load data
        print(f"\n{'='*60}")
        print(f"Loading data to {STAGING_SCHEMA}.{STAGING_TABLE}")
        print(f"{'='*60}")
        insert_data(pg_conn, STAGING_SCHEMA, STAGING_TABLE, duckdb_conn, batch_id)
        
        # Verify contract on staging
        print(f"\n{'='*60}")
        print(f"Verifying contract on {STAGING_SCHEMA}.{STAGING_TABLE}")
        print(f"{'='*60}")
        staging_verification_passed = verify_contract(STAGING_SCHEMA, publish=False)
        
        if not staging_verification_passed:
            print(f"\n❌ Pipeline FAILED: Contract verification failed on staging")
            sys.exit(1)
        
        # Copy to public if staging verification passed
        print(f"\n{'='*60}")
        print(f"Publishing data to {PUBLIC_SCHEMA}.{PUBLIC_TABLE}")
        print(f"{'='*60}")
        copy_staging_to_public(pg_conn)
        
        # Verify contract on public
        print(f"\n{'='*60}")
        print(f"Verifying contract on {PUBLIC_SCHEMA}.{PUBLIC_TABLE}")
        print(f"{'='*60}")
        public_verification_passed = verify_contract(PUBLIC_SCHEMA, publish=False)
        
        if not public_verification_passed:
            print(f"\n❌ Pipeline FAILED: Contract verification failed on public")
            sys.exit(1)
        
        # Success!
        print(f"\n{'='*60}")
        print(f"✅ Pipeline SUCCESS: All steps completed successfully")
        print(f"{'='*60}\n")
        
    except FileNotFoundError as e:
        print(f"\n❌ Pipeline FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline FAILED with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if pg_conn:
            pg_conn.close()
            print("PostgreSQL connection closed")
        if duckdb_conn:
            duckdb_conn.close()
            print("DuckDB connection closed")


if __name__ == "__main__":
    main()

