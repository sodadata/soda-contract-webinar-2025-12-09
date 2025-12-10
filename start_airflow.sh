#!/bin/bash
# Start Airflow standalone with correct AIRFLOW_HOME

# Get the absolute path of the directory containing this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export AIRFLOW_HOME="$SCRIPT_DIR"

# Set all paths to use absolute paths (required by Airflow)
export AIRFLOW__CORE__DAGS_FOLDER="${SCRIPT_DIR}/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="${SCRIPT_DIR}/plugins"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///${SCRIPT_DIR}/airflow.db"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="${SCRIPT_DIR}/logs"
export AIRFLOW__LOGGING__DAG_PROCESSOR_CHILD_PROCESS_LOG_DIRECTORY="${SCRIPT_DIR}/logs/dag_processor"

# Fix for macOS fork() issue with Objective-C runtime
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__PARALLELISM=32
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16
export AIRFLOW__SCHEDULER__PARSING_PROCESSES=4
export AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT=120


# Increase memory limits for Airflow tasks
# Set Python memory-related environment variables
export PYTHONHASHSEED=0
# Increase virtual memory limit (8GB = 8388608 KB)
ulimit -v 8388608 2>/dev/null || true
# Increase data segment size (8GB = 8388608 KB)
ulimit -d 8388608 2>/dev/null || true
# Increase stack size (64MB = 65536 KB)
ulimit -s 65536 2>/dev/null || true

source venv/bin/activate

# Ensure Airflow uses the venv's Python
export PYTHONPATH="${VIRTUAL_ENV}/lib/python$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')/site-packages:${PYTHONPATH}"
export PATH="${VIRTUAL_ENV}/bin:${PATH}"

# Verify Python path
echo "Using Python: $(which python)"
echo "Python path: ${PYTHONPATH}"

airflow standalone

