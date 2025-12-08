# Running Orders Pipeline with Airflow

This guide explains how to run the orders pipeline with Apache Airflow, demonstrating how Soda data quality checks integrate into a production data pipeline.

## Overview

The Airflow DAG (`orders_pipeline.py`) orchestrates the following workflow:

1. **load**: Generate test data (5 rows)
2. **verify_locally**: Verify contract using DuckDB in-memory (data quality gate before database write)
3. **insert_to_public**: Insert verified data to PostgreSQL
4. **verify_public**: Verify contract on PostgreSQL and publish results to Soda Cloud

The pipeline **fails** if any data quality check fails, preventing bad data from reaching production.

## Setup

### 1. Install Dependencies

```bash
# Install Soda packages from private PyPI
pip install -i https://pypi.dev.sodadata.io/simple -U soda-postgres soda-duckdb

# Install other dependencies
pip install -r requirements.txt
```

### 2. Configure Airflow

Add to your `.env` file:
```
AIRFLOW_HOME=/path/to/soda-contract-webinar
```

### 3. Create Database Table

Run the DDL script to create the orders table:
```bash
psql -h <host> -U <user> -d <database> -f ddl/create_orders_table.sql
```

### 4. Ensure Environment Variables are Set

Make sure your `.env` file contains all required variables:
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DATABASE`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `SODA_CLOUD_ENV`
- `SODA_CLOUD_API_KEY`
- `SODA_CLOUD_API_SECRET`

## Running the DAG

### Start Airflow

```bash
# Runs webserver, scheduler, and triggerer in one process
# Automatically creates admin/admin user
airflow standalone
```

### Access Airflow UI

Open your browser to: http://localhost:8080

Login with the default credentials:
- **Username**: admin
- **Password**: admin

(If using standalone mode, these are created automatically)

### Trigger the DAG

1. Find the `orders_pipeline_with_soda` DAG in the Airflow UI
2. Toggle it ON to enable it
3. Click the "Play" button to trigger a DAG run
4. Optionally, pass `hasErrors` via the "Trigger DAG w/ config" option:
   ```json
   {
     "hasErrors": true
   }
   ```

## Understanding the Pipeline

### Task Flow

```
load → verify_locally → insert_to_public → verify_public
```

- **load**: Generates 5 rows of test data
- **verify_locally**: **Data Quality Gate** - Verifies contract with DuckDB in-memory. If this fails, the pipeline stops.
- **insert_to_public**: Only runs if local verification passes. Inserts data to PostgreSQL.
- **verify_public**: Final verification on PostgreSQL with publishing to Soda Cloud

### Data Quality Gates

The `verify_locally` task acts as a **data quality gate**:
- ✅ If verification passes → data proceeds to PostgreSQL
- ❌ If verification fails → pipeline stops, bad data never reaches the database

This demonstrates how Soda prevents bad data from contaminating your production tables.

### Testing with Errors

To test error handling, trigger the DAG with:
```json
{
  "hasErrors": true
}
```

This will generate data with:
- Row 0: shipping_date before order_date (data quality issue)
- Row 1: Invalid status value
- Row 2: Missing status value

The pipeline should fail at the `verify_locally` step.

## Monitoring

### View Task Logs

1. Click on a DAG run in the Airflow UI
2. Click on any task to see its logs
3. Check for Soda verification results in the logs

### Soda Cloud Integration

When `verify_public` runs with `publish=True`, results are sent to Soda Cloud where you can:
- View data quality metrics
- Track data quality trends
- Set up alerts for data quality issues

## Troubleshooting

### DAG Not Appearing

- Check that `AIRFLOW_HOME` is set correctly
- Verify the DAG file is in the `dags/` folder
- Check Airflow logs: `$AIRFLOW_HOME/logs/`

### Import Errors

- Ensure all dependencies are installed: `pip install -r requirements.txt`
- Check that all required modules are available in your Python environment

### Connection Issues

- Verify `.env` file has all required variables
- Test database connection: `./soda.sh data-source-test`
- Test Soda Cloud connection: `./soda.sh soda-cloud-test`
- Test both: `./soda.sh connection-test`

## Best Practices Demonstrated

1. **In-Memory Validation**: Data is validated in-memory (DuckDB) before database write
2. **Fail Fast**: Pipeline stops immediately if data quality checks fail
3. **Separation of Concerns**: Each task has a single responsibility
4. **Observability**: Results are published to Soda Cloud for monitoring
5. **Idempotency**: Tasks can be rerun safely
