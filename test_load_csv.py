#!/usr/bin/env python3
"""Simple script to test load_csv_data and verify_contract_with_duckdb functions from CLI."""

import sys
import os

# Add dags directory to path (everything is now in orders_pipeline_dag.py)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dags'))

from orders_pipeline_dag import load_csv_data, verify_contract_with_duckdb, CONTRACT_FILE

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_load_csv.py <batch_id> [--verify]")
        print("Example: python test_load_csv.py 1")
        print("Example: python test_load_csv.py 1 --verify")
        sys.exit(1)
    
    batch_id = sys.argv[1]
    verify = '--verify' in sys.argv
    
    print(f"Loading CSV data for batch_{batch_id}...")
    
    try:
        df = load_csv_data(batch_id)
        print(f"\n✅ Successfully loaded {len(df)} rows")
        print(f"\nColumns: {', '.join(df.columns.tolist())}")
        print(f"\nFirst few rows:")
        print(df.head())
        
        if verify:
            print(f"\n{'='*60}")
            print("Verifying contract with DuckDB in-memory...")
            print(f"{'='*60}")
            passed = verify_contract_with_duckdb(df, CONTRACT_FILE, publish=False)
            if passed:
                print("\n✅ Contract verification PASSED")
            else:
                print("\n❌ Contract verification FAILED")
                sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

