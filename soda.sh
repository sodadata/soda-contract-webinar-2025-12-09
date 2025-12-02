#!/bin/bash

# Script to verify a Soda contract without publishing or test connections
# Usage: 
#   ./soda.sh data-source-test                  # Test data source connection
#   ./soda.sh soda-cloud-test                   # Test Soda Cloud connection
#   ./soda.sh verify                            # Verify a contract
#   ./soda.sh generate                          # Generate a contract
#   ./soda.sh fetch-proposal <request_id>.<proposal_id>  # Fetch a proposal

set -e

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Activate virtual environment
source "$SCRIPT_DIR/venv/bin/activate"

DATA_SOURCE="soda-db-config.yaml"
CLOUD_CONFIG="soda-cloud-config.yaml"
CONTRACT_PATH="contracts/webinardb/postgres/public/orders.yaml"

# If first argument is "data-source-test", test the data source connection
if [ "$1" == "data-source-test" ]; then
    # Check if data source file exists
    if [ ! -f "$DATA_SOURCE" ]; then
        echo "Error: Data source file not found: $DATA_SOURCE"
        exit 1
    fi
    echo "Testing data source connection..."
    echo "Using data source: $DATA_SOURCE"
    echo ""
    echo "Running: soda data-source test -ds $DATA_SOURCE"
    echo ""
    soda data-source test -ds "$DATA_SOURCE"
    exit 0
fi

# If first argument is "soda-cloud-test", test the Soda Cloud connection
if [ "$1" == "soda-cloud-test" ]; then
    # Check if cloud config file exists
    if [ ! -f "$CLOUD_CONFIG" ]; then
        echo "Error: Soda Cloud config file not found: $CLOUD_CONFIG"
        exit 1
    fi
    echo "Testing Soda Cloud connection..."
    echo "Using cloud config: $CLOUD_CONFIG"
    echo ""
    echo "Running: soda cloud test -sc $CLOUD_CONFIG"
    echo ""
    soda cloud test -sc "$CLOUD_CONFIG"
    exit 0
fi

# If first argument is "verify", verify a contract
if [ "$1" == "verify" ]; then
    # Check if data source file exists
    if [ ! -f "$DATA_SOURCE" ]; then
        echo "Error: Data source file not found: $DATA_SOURCE"
        exit 1
    fi
    
    # Check if contract file exists
    if [ ! -f "$CONTRACT_PATH" ]; then
        echo "Error: Contract file not found: $CONTRACT_PATH"
        exit 1
    fi
    
    echo "Verifying contract: $CONTRACT_PATH"
    echo "Using data source: $DATA_SOURCE"
    echo ""
    echo "Running: soda contract verify --data-source $DATA_SOURCE --contract $CONTRACT_PATH"
    echo ""
    
    soda contract verify --data-source "$DATA_SOURCE" --contract "$CONTRACT_PATH"
    exit 0
fi

# If first argument is "generate", generate a contract
if [ "$1" == "generate" ]; then
    # Hardcoded dataset path
    DATASET="webinardb/postgres/public/orders"
    
    # Check if data source file exists
    if [ ! -f "$DATA_SOURCE" ]; then
        echo "Error: Data source file not found: $DATA_SOURCE"
        exit 1
    fi
    
    # Check if cloud config file exists
    if [ ! -f "$CLOUD_CONFIG" ]; then
        echo "Error: Soda Cloud config file not found: $CLOUD_CONFIG"
        exit 1
    fi
    
    echo "Generating contract..."
    echo "Dataset: $DATASET"
    echo "Output file: $CONTRACT_PATH"
    echo "Using data source: $DATA_SOURCE"
    echo "Using cloud config: $CLOUD_CONFIG"
    echo ""
    echo "Running: soda contract create --dataset $DATASET --file $CONTRACT_PATH --data-source $DATA_SOURCE --soda-cloud $CLOUD_CONFIG"
    echo ""
    
    soda contract create --dataset "$DATASET" --file "$CONTRACT_PATH" --data-source "$DATA_SOURCE" --soda-cloud "$CLOUD_CONFIG"
    exit 0
fi

# If first argument is "fetch-proposal", fetch a proposal
if [ "$1" == "fetch-proposal" ]; then
    # Check if cloud config file exists
    if [ ! -f "$CLOUD_CONFIG" ]; then
        echo "Error: Soda Cloud config file not found: $CLOUD_CONFIG"
        exit 1
    fi
    
    # Check if argument is provided
    if [ -z "$2" ]; then
        echo "Error: Request ID and Proposal ID are required"
        echo "Usage: $0 fetch-proposal <request_id>.<proposal_id>"
        echo "Example: $0 fetch-proposal 45.1"
        exit 1
    fi
    
    # Parse request_id.proposal_id format
    ARG="$2"
    if [[ "$ARG" == *"."* ]]; then
        REQUEST_ID="${ARG%.*}"
        PROPOSAL_ID="${ARG##*.}"
    else
        echo "Error: Invalid format. Expected <request_id>.<proposal_id>"
        echo "Usage: $0 fetch-proposal <request_id>.<proposal_id>"
        echo "Example: $0 fetch-proposal 45.1"
        exit 1
    fi
    
    # Validate that both IDs are present
    if [ -z "$REQUEST_ID" ] || [ -z "$PROPOSAL_ID" ]; then
        echo "Error: Both Request ID and Proposal ID are required"
        echo "Usage: $0 fetch-proposal <request_id>.<proposal_id>"
        echo "Example: $0 fetch-proposal 45.1"
        exit 1
    fi
    
    echo "Fetching proposal..."
    echo "Request ID: $REQUEST_ID"
    echo "Proposal ID: $PROPOSAL_ID"
    echo "Output path: $CONTRACT_PATH"
    echo "Using cloud config: $CLOUD_CONFIG"
    echo ""
    echo "Running: soda request fetch -r $REQUEST_ID -p $PROPOSAL_ID -sc $CLOUD_CONFIG --f $CONTRACT_PATH"
    echo ""
    
    soda request fetch -r "$REQUEST_ID" -p "$PROPOSAL_ID" -sc "$CLOUD_CONFIG" --f "$CONTRACT_PATH"
    exit 0
fi

# If no recognized command, show usage
echo "Error: Unknown command: $1"
echo "Usage: $0 data-source-test               # Test data source connection"
echo "       $0 soda-cloud-test                # Test Soda Cloud connection"
echo "       $0 verify                          # Verify a contract"
echo "       $0 generate                        # Generate a contract"
echo "       $0 fetch-proposal <request_id>.<proposal_id>  # Fetch a proposal"
echo ""
echo "Examples:"
echo "  $0 verify"
echo "  $0 generate"
echo "  $0 fetch-proposal 45.1"
exit 1

