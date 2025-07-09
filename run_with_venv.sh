#!/bin/bash

# Wrapper script to run Python script with virtual environment and Supabase integration
# Usage: ./run_with_venv.sh --date 2025-01-01 --idempotent --exchanges LSE CME NYQ --verbose
# Special commands:
#   ./run_with_venv.sh --test-supabase     # Test Supabase connection
#   ./run_with_venv.sh --install-deps      # Install/update all dependencies
#   ./run_with_venv.sh --check-deps        # Check if all dependencies are installed

# Set the project directory to the directory where this script is located
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/venv"
SCRIPT_NAME="load_january_simple.py"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if virtual environment exists
check_venv() {
    if [ ! -d "$VENV_DIR" ]; then
        print_error "Virtual environment not found at $VENV_DIR"
        print_status "Creating virtual environment..."
        python3 -m venv "$VENV_DIR"
        if [ $? -eq 0 ]; then
            print_success "Virtual environment created successfully"
        else
            print_error "Failed to create virtual environment"
            exit 1
        fi
    fi
}

# Function to install dependencies
install_dependencies() {
    print_status "Installing/updating dependencies..."
    
    # Upgrade pip first
    pip install --upgrade pip
    
    # Install requirements
    if [ -f "$PROJECT_DIR/requirements.txt" ]; then
        pip install -r "$PROJECT_DIR/requirements.txt"
        if [ $? -eq 0 ]; then
            print_success "Dependencies installed successfully"
        else
            print_error "Failed to install dependencies"
            exit 1
        fi
    else
        print_warning "requirements.txt not found, installing core dependencies..."
        pip install pandas duckdb boto3 s3fs psycopg2-binary python-dateutil requests
    fi
}

# Function to check if dependencies are installed
check_dependencies() {
    print_status "Checking dependencies..."
    
    local deps_ok=true
    local required_packages=("pandas" "duckdb" "boto3" "s3fs" "psycopg2" "dateutil" "requests")
    
    for package in "${required_packages[@]}"; do
        python -c "import $package" 2>/dev/null
        if [ $? -eq 0 ]; then
            print_success "$package is installed"
        else
            print_error "$package is NOT installed"
            deps_ok=false
        fi
    done
    
    if [ "$deps_ok" = false ]; then
        print_warning "Some dependencies are missing. Run with --install-deps to install them."
        return 1
    else
        print_success "All required dependencies are installed"
        return 0
    fi
}

# Function to test Supabase connection
test_supabase() {
    print_status "Testing Supabase connection..."
    python test_supabase_connection.py
    return $?
}

# Change to project directory
cd "$PROJECT_DIR" || {
    print_error "Cannot change to project directory: $PROJECT_DIR"
    exit 1
}

# Check if virtual environment exists
check_venv

# Activate virtual environment
print_status "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

if [ $? -ne 0 ]; then
    print_error "Failed to activate virtual environment"
    exit 1
fi

# Set AWS credentials (should be set in environment or ~/.aws/credentials)
print_status "Checking AWS credentials..."
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    print_warning "AWS credentials not found in environment variables"
    print_status "Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"
    print_status "Or configure AWS credentials using: aws configure"
fi

# Set Supabase credentials (can be overridden by environment variables)
print_status "Checking Supabase credentials..."
if [ -z "$SUPABASE_HOST" ] || [ -z "$SUPABASE_USER" ] || [ -z "$SUPABASE_PASSWORD" ]; then
    print_warning "Supabase credentials not found in environment variables"
    print_status "Please set the following environment variables:"
    print_status "  SUPABASE_HOST (e.g., your-project.supabase.co)"
    print_status "  SUPABASE_USER (e.g., postgres)"
    print_status "  SUPABASE_PASSWORD (your database password)"
    print_status "  SUPABASE_DATABASE (optional, defaults to postgres)"
    print_status "  SUPABASE_PORT (optional, defaults to 6543)"
fi

# Set defaults for optional Supabase parameters
export SUPABASE_PORT="${SUPABASE_PORT:-6543}"
export SUPABASE_DATABASE="${SUPABASE_DATABASE:-postgres}"

# Handle special commands
case "$1" in
    --install-deps)
        install_dependencies
        exit $?
        ;;
    --check-deps)
        check_dependencies
        exit $?
        ;;
    --test-supabase)
        check_dependencies
        if [ $? -eq 0 ]; then
            test_supabase
        fi
        exit $?
        ;;
    --help)
        echo "Usage: $0 [OPTIONS] [SCRIPT_ARGUMENTS]"
        echo ""
        echo "Special commands:"
        echo "  --install-deps    Install/update all required dependencies"
        echo "  --check-deps      Check if all dependencies are installed"
        echo "  --test-supabase   Test Supabase connection"
        echo "  --help           Show this help message"
        echo ""
        echo "Script arguments (passed to $SCRIPT_NAME):"
        echo "  --date YYYY-MM-DD        Date to process"
        echo "  --exchanges LSE CME NYQ  Exchanges to process"
        echo "  --idempotent            Skip existing data"
        echo "  --verbose               Enable verbose logging"
        echo ""
        echo "Examples:"
        echo "  $0 --date 2025-01-15 --idempotent"
        echo "  $0 --date 2025-01-15 --exchanges LSE --verbose"
        echo "  $0 --test-supabase"
        echo "  $0 --install-deps"
        exit 0
        ;;
esac

# Check dependencies before running main script
check_dependencies
if [ $? -ne 0 ]; then
    print_warning "Dependencies check failed. Consider running: $0 --install-deps"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_status "Exiting..."
        exit 1
    fi
fi

# Run the Python script with all passed arguments
print_status "Running $SCRIPT_NAME with Supabase integration..."
print_status "Arguments: $*"
echo ""

python "$SCRIPT_NAME" "$@"
exit_code=$?

# Print final status
echo ""
if [ $exit_code -eq 0 ]; then
    print_success "Script completed successfully"
    print_status "Data has been written to both DuckDB (local) and Supabase (cloud)"
else
    print_error "Script failed with exit code: $exit_code"
fi

# Deactivate virtual environment (optional - happens automatically when script ends)
deactivate

exit $exit_code 