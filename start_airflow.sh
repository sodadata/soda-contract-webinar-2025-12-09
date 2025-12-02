#!/bin/bash
# Start Airflow standalone with correct AIRFLOW_HOME

export AIRFLOW_HOME=/Users/benjaminpirotte/Documents/soda/soda-contract-webinar
# Fix for macOS fork() issue with Objective-C runtime
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

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

