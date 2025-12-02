# Running Orders Pipeline with Airflow

This guide explains how to run the orders pipeline with Apache Airflow, demonstrating how Soda data quality checks integrate into a production data pipeline.

## Overview

The Airflow DAG (`orders_pipeline_dag.py`) orchestrates the following workflow:

1. **load_to_staging**: Load CSV data and insert into `staging.orders`
2. **verify_staging**: Verify data quality contract on staging (data quality gate)
3. **publish_to_public**: If verification passes, copy data to `public.orders`
4. **verify_public**: Verify contract on public and publish results to Soda Cloud

The pipeline **fails** if any data quality check fails, preventing bad data from reaching production.

## Setup

### 1. Install Airflow

```bash
# Install Airflow (standalone mode for development)
pip install apache-airflow

# This automatically initializes the database and creates a default admin user
# Default credentials: admin / admin
airflow standalone

```

### 2. Configure Airflow

add to your `.env` file:
```
AIRFLOW_HOME=/Users/benjaminpirotte/Documents/soda/soda-contract-webinar
```

### 3. DAG Files Location

The DAG files are already in the correct location:
- `dags/orders_pipeline_dag.py` - The Airflow DAG
- `dags/orders_pipeline.py` - The pipeline functions (imported by the DAG)

Since `AIRFLOW_HOME` is set to your project root, Airflow will automatically discover DAGs in the `dags/` folder. No additional configuration needed!

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
4. Optionally, pass a `batch_id` via the "Trigger DAG w/ config" option:
   ```json
   {
     "batch_id": "1"
   }
   ```

## Understanding the Pipeline

### Task Flow

```
load_to_staging → verify_staging → publish_to_public → verify_public
```

- **load_to_staging**: Loads CSV and inserts into staging
- **verify_staging**: **Data Quality Gate** - If this fails, the pipeline stops
- **publish_to_public**: Only runs if staging verification passes
- **verify_public**: Final verification with publishing to Soda Cloud

### Data Quality Gates

The `verify_staging` task acts as a **data quality gate**:
- ✅ If verification passes → data proceeds to production
- ❌ If verification fails → pipeline stops, bad data never reaches production

This demonstrates how Soda prevents bad data from contaminating your production tables.

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

## Testing with Different Batches

To test with different CSV batches, trigger the DAG with a config:

```json
{
  "batch_id": "2"
}
```

- **Odd-numbered batches** (1, 3, 5, etc.): Good quality data - should pass
- **Even-numbered batches** (2, 4, 6, etc.): Bad quality data - should fail at verify_staging

## Troubleshooting

### DAG Not Appearing

- Check that `AIRFLOW_HOME` is set correctly
- Verify the DAG file is in the `dags_folder`
- Check Airflow logs: `$AIRFLOW_HOME/logs/`

### Import Errors

- Ensure all dependencies are installed: `pip install -r requirements.txt`
- Check that `orders_pipeline.py` is in the same directory as the DAG

### Connection Issues

- Verify `.env` file has all required variables
- Test database connection: `./soda.sh data-source-test`
- Test Soda Cloud connection: `./soda.sh soda-cloud-test`

## Best Practices Demonstrated

1. **Staging Environment**: Data is validated in staging before production
2. **Fail Fast**: Pipeline stops immediately if data quality checks fail
3. **Separation of Concerns**: Each task has a single responsibility
4. **Observability**: Results are published to Soda Cloud for monitoring
5. **Idempotency**: Tasks can be rerun safely

