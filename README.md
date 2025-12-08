# Soda Contract Webinar Demo

This project demonstrates how to integrate Soda data quality contracts into an Apache Airflow data pipeline. It shows how to validate data quality before writing to production databases, preventing bad data from contaminating your data warehouse.

## Overview

The demo includes:
- **Airflow DAG** that orchestrates a data pipeline with Soda contract verification
- **Soda Contracts** that define data quality rules
- **Test Data Generation** for demonstrating data quality checks
- **PostgreSQL Integration** for storing validated data
- **Soda Cloud Integration** for monitoring and observability

## Architecture

```
┌─────────────┐
│   Load      │  Generate 5 rows of test data
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Verify      │  Verify contract with DuckDB (in-memory)
│ (DuckDB)    │  ⚠️ Data Quality Gate
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Insert      │  Insert verified data to PostgreSQL
│ (PostgreSQL)│
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Verify      │  Verify contract on PostgreSQL
│ (PostgreSQL)│  Publish results to Soda Cloud
└─────────────┘
```

## Quick Start

### 1. Prerequisites

- Python 3.10+
- PostgreSQL database
- Soda Cloud account (for publishing results)

### 2. Installation

```bash

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install Soda packages from private PyPI
pip install -i https://pypi.dev.sodadata.io/simple -U soda-postgres soda-duckdb

# Install other dependencies
pip install -r requirements.txt
```

### 3. Configuration

Create a `.env` file in the project root:

```env
# PostgreSQL Connection
POSTGRES_HOST=your-postgres-host
POSTGRES_PORT=5432
POSTGRES_DATABASE=your-database
POSTGRES_USER=your-username
POSTGRES_PASSWORD=your-password

# Soda Cloud
SODA_CLOUD_ENV=your-environment
SODA_CLOUD_API_KEY=your-api-key
SODA_CLOUD_API_SECRET=your-api-secret

# Airflow (optional)
AIRFLOW_HOME=/path/to/soda-contract-webinar
```

### 4. Database Setup

Create the orders table:

```bash
psql -h <host> -U <user> -d <database> -f ddl/create_orders_table.sql
```

### 5. Test Connections

```bash

# Test both connections
./soda.sh connection-test
```

### 6. Run the Pipeline

#### Option A: Using Airflow (Recommended)

See [AIRFLOW_README.md](AIRFLOW_README.md) for detailed instructions.

```bash
# Start Airflow
airflow standalone

# Access UI at http://localhost:8080
# Trigger the DAG: orders_pipeline_with_soda
```

#### Option B: Using Soda CLI

```bash
# Verify contract
./soda.sh verify

# Generate contract
./soda.sh generate

# Publish contract
./soda.sh publish
```

## Pipeline Steps

### Step 1: Load (Generate Test Data)

Generates 5 rows of test order data with:
- Valid UUIDs for order_id and customer_id
- Random dates, amounts, addresses, and statuses
- Option to generate errors for testing (`hasErrors: true`)

### Step 2: Verify Locally (DuckDB)

- Validates data quality using DuckDB in-memory
- Acts as a **data quality gate** before database write
- Fails the pipeline if checks don't pass

### Step 3: Insert to PostgreSQL

- Inserts verified data into `public.orders` table
- Only runs if Step 2 passes

### Step 4: Verify on PostgreSQL

- Verifies contract on the actual PostgreSQL table
- Publishes results to Soda Cloud for monitoring

## Data Quality Checks

The contract (`contracts/webinardb/postgres/public/orders.yaml`) includes:

- **Schema validation**: No extra columns, fixed column order
- **UUID validation**: order_id and customer_id must be valid UUID v4
- **Date validation**: No missing dates, shipping_date must be after order_date
- **Status validation**: Must be one of: PENDING, SHIPPED, CANCELLED, RETURNED, REFUNDED
- **Amount validation**: Must be non-negative
- **Address validation**: Length between 5 and 200 characters
- **Country code validation**: Must be uppercase 2-letter ISO code
- **Uniqueness**: No duplicate order_ids

## Testing Error Handling

To test the pipeline with data quality errors:

1. In Airflow UI, trigger the DAG with config:
   ```json
   {
     "hasErrors": true
   }
   ```

2. The pipeline will generate data with:
   - Row 0: shipping_date before order_date
   - Row 1: Invalid status value
   - Row 2: Missing status value

3. The pipeline should fail at `verify_locally` step

## Soda CLI Commands

The `soda.sh` script provides convenient commands on top of Soda CLI.

## Troubleshooting

### Import Errors

```bash
# Ensure all dependencies are installed
pip install -r requirements.txt

# Verify Soda packages
pip list | grep soda
```

### Connection Issues

```bash
# Test connections
./soda.sh connection-test
```

### Airflow Issues

See [AIRFLOW_README.md](AIRFLOW_README.md) for Airflow-specific troubleshooting.

## Documentation

- [Soda v4 Reference Documentation](https://docs.soda.io/soda-v4/reference) - Complete reference for Soda's interfaces, configuration options, and APIs

