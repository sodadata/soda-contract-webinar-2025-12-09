#!/bin/bash

# Script to verify a Soda contract without publishing or test connections
# Usage: 
#   ./soda.sh data-source-test                  # Test data source connection
#   ./soda.sh soda-cloud-test                   # Test Soda Cloud connection
#   ./soda.sh connection-test                   # Test both data source and Soda Cloud connections
#   ./soda.sh verify                            # Verify a contract (with publishing)
#   ./soda.sh verify -p <contract_path>         # Verify a contract with custom path (with publishing)
#   ./soda.sh publish                           # Publish a contract to Soda Cloud
#   ./soda.sh publish -p <contract_path>       # Publish a contract with custom path
#   ./soda.sh generate                          # Generate a contract
#   ./soda.sh generate -p <contract_path>       # Generate a contract with custom path
#   ./soda.sh fetch-proposal <request_id>.<proposal_id>  # Fetch a proposal
#   ./soda.sh fetch-proposal <request_id>.<proposal_id> -p <contract_path>  # Fetch with custom path

set -e

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Activate virtual environment
source "$SCRIPT_DIR/venv/bin/activate"

DATA_SOURCE="soda-db-config.yaml"
CLOUD_CONFIG="soda-cloud-config.yaml"
CONTRACT_PATH="contracts/webinardb/postgres/public/orders.yaml"

# Function to parse -p flag from arguments
parse_contract_path() {
    local args=("$@")
    for i in "${!args[@]}"; do
        if [ "${args[i]}" == "-p" ] && [ -n "${args[i+1]}" ]; then
            echo "${args[i+1]}"
            return 0
        fi
    done
    echo ""
}

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

# If first argument is "connection-test", test both data source and Soda Cloud connections
if [ "$1" == "connection-test" ]; then
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
    
    echo "=========================================="
    echo "Testing Data Source Connection"
    echo "=========================================="
    echo "Using data source: $DATA_SOURCE"
    echo ""
    echo "Running: soda data-source test -ds $DATA_SOURCE"
    echo ""
    
    if ! soda data-source test -ds "$DATA_SOURCE"; then
        echo ""
        echo "❌ Data source connection test failed"
        exit 1
    fi
    
    echo ""
    echo "=========================================="
    echo "Testing Soda Cloud Connection"
    echo "=========================================="
    echo "Using cloud config: $CLOUD_CONFIG"
    echo ""
    echo "Running: soda cloud test -sc $CLOUD_CONFIG"
    echo ""
    
    if ! soda cloud test -sc "$CLOUD_CONFIG"; then
        echo ""
        echo "❌ Soda Cloud connection test failed"
        exit 1
    fi
    
    echo ""
    echo "✅ All connection tests passed!"
    exit 0
fi

# If first argument is "verify", verify a contract
if [ "$1" == "verify" ]; then
    # Parse -p flag if provided
    CUSTOM_PATH=$(parse_contract_path "$@")
    if [ -n "$CUSTOM_PATH" ]; then
        CONTRACT_PATH="$CUSTOM_PATH"
    fi
    
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
    
    # Check if contract file exists
    if [ ! -f "$CONTRACT_PATH" ]; then
        echo "Error: Contract file not found: $CONTRACT_PATH"
        exit 1
    fi
    
    echo "Verifying contract: $CONTRACT_PATH"
    echo "Using data source: $DATA_SOURCE"
    echo "Using cloud config: $CLOUD_CONFIG"
    echo "Publishing to Soda Cloud: Yes"
    echo ""
    echo "Running: soda contract verify --data-source $DATA_SOURCE --contract $CONTRACT_PATH --soda-cloud $CLOUD_CONFIG --publish"
    echo ""
    
    soda contract verify --data-source "$DATA_SOURCE" --contract "$CONTRACT_PATH" --soda-cloud "$CLOUD_CONFIG" --publish
    exit 0
fi

# If first argument is "publish", publish a contract to Soda Cloud
if [ "$1" == "publish" ]; then
    # Parse -p flag if provided
    CUSTOM_PATH=$(parse_contract_path "$@")
    if [ -n "$CUSTOM_PATH" ]; then
        CONTRACT_PATH="$CUSTOM_PATH"
    fi
    
    # Check if cloud config file exists
    if [ ! -f "$CLOUD_CONFIG" ]; then
        echo "Error: Soda Cloud config file not found: $CLOUD_CONFIG"
        exit 1
    fi
    
    # Check if contract file exists
    if [ ! -f "$CONTRACT_PATH" ]; then
        echo "Error: Contract file not found: $CONTRACT_PATH"
        exit 1
    fi
    
    echo "Publishing contract: $CONTRACT_PATH"
    echo "Using cloud config: $CLOUD_CONFIG"
    echo ""
    echo "Running: soda contract publish --contract $CONTRACT_PATH --soda-cloud $CLOUD_CONFIG"
    echo ""
    
    soda contract publish --contract "$CONTRACT_PATH" --soda-cloud "$CLOUD_CONFIG"
    exit 0
fi

# If first argument is "generate", generate a contract
if [ "$1" == "generate" ]; then
    # Parse -p flag if provided
    CUSTOM_PATH=$(parse_contract_path "$@")
    if [ -n "$CUSTOM_PATH" ]; then
        CONTRACT_PATH="$CUSTOM_PATH"
    fi
    
    # Extract dataset path from contract path
    # e.g., contracts/webinardb/postgres/public/orders.yaml -> webinardb/postgres/public/orders
    DATASET="${CONTRACT_PATH#contracts/}"  # Remove "contracts/" prefix
    DATASET="${DATASET%.yaml}"             # Remove ".yaml" suffix
    
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
    echo "Running: soda contract create --dataset $DATASET --file $CONTRACT_PATH --data-source $DATA_SOURCE"
    echo ""
    
    soda contract create --dataset "$DATASET" --file "$CONTRACT_PATH" --data-source "$DATA_SOURCE"
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
        echo "Usage: $0 fetch-proposal <request_id>.<proposal_id> [-p <contract_path>]"
        echo "Example: $0 fetch-proposal 45.1"
        echo "Example: $0 fetch-proposal 45.1 -p contracts/custom/path.yaml"
        exit 1
    fi
    
    # Parse request_id.proposal_id format
    ARG="$2"
    if [[ "$ARG" == *"."* ]]; then
        REQUEST_ID="${ARG%.*}"
        PROPOSAL_ID="${ARG##*.}"
    else
        echo "Error: Invalid format. Expected <request_id>.<proposal_id>"
        echo "Usage: $0 fetch-proposal <request_id>.<proposal_id> [-p <contract_path>]"
        echo "Example: $0 fetch-proposal 45.1"
        exit 1
    fi
    
    # Validate that both IDs are present
    if [ -z "$REQUEST_ID" ] || [ -z "$PROPOSAL_ID" ]; then
        echo "Error: Both Request ID and Proposal ID are required"
        echo "Usage: $0 fetch-proposal <request_id>.<proposal_id> [-p <contract_path>]"
        echo "Example: $0 fetch-proposal 45.1"
        exit 1
    fi
    
    # Parse -p flag if provided
    CUSTOM_PATH=$(parse_contract_path "$@")
    if [ -n "$CUSTOM_PATH" ]; then
        CONTRACT_PATH="$CUSTOM_PATH"
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
echo "       $0 connection-test                # Test both data source and Soda Cloud connections"
echo "       $0 verify [-p <contract_path>]    # Verify a contract (with publishing)"
echo "       $0 publish [-p <contract_path>]   # Publish a contract to Soda Cloud"
echo "       $0 generate [-p <contract_path>]  # Generate a contract"
echo "       $0 fetch-proposal <request_id>.<proposal_id> [-p <contract_path>]  # Fetch a proposal"
echo ""
echo "Examples:"
echo "  $0 connection-test"
echo "  $0 verify"
echo "  $0 verify -p contracts/custom/path.yaml"
echo "  $0 publish"
echo "  $0 publish -p contracts/custom/path.yaml"
echo "  $0 generate"
echo "  $0 generate -p contracts/custom/path.yaml"
echo "  $0 fetch-proposal 45.1"
echo "  $0 fetch-proposal 45.1 -p contracts/custom/path.yaml"
exit 1

