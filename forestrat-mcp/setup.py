#!/usr/bin/env python3
"""
Setup script for Forestrat MCP Server
"""

import os
import sys
import subprocess
from pathlib import Path

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        return False
    print(f"✓ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    return True

def check_database_exists():
    """Check if the database file exists"""
    db_path = Path("../multi_exchange_data_lake.duckdb")
    if db_path.exists():
        print(f"✓ Database found at {db_path.resolve()}")
        return True
    else:
        print(f"❌ Database not found at {db_path.resolve()}")
        print("Please ensure the database file exists before running the MCP server")
        return False

def install_dependencies():
    """Install required dependencies"""
    print("Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✓ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False

def create_env_file():
    """Create .env file with default configuration"""
    env_file = Path(".env")
    if not env_file.exists():
        env_content = """# Forestrat MCP Server Configuration
FORESTRAT_DB_PATH=../multi_exchange_data_lake.duckdb
LOG_LEVEL=INFO
"""
        with open(env_file, 'w') as f:
            f.write(env_content)
        print("✓ Created .env file with default configuration")
    else:
        print("✓ .env file already exists")

def run_tests():
    """Run the test suite"""
    print("Running tests...")
    try:
        result = subprocess.run([sys.executable, "test_connection.py"], capture_output=True, text=True)
        if result.returncode == 0:
            print("✓ All tests passed")
            return True
        else:
            print("❌ Tests failed:")
            print(result.stdout)
            print(result.stderr)
            return False
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False

def main():
    """Main setup function"""
    print("Forestrat MCP Server Setup")
    print("=" * 30)
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Check if database exists
    if not check_database_exists():
        print("\nPlease create or locate your DuckDB database file before continuing.")
        print("You can set the path using the FORESTRAT_DB_PATH environment variable.")
        sys.exit(1)
    
    # Install dependencies
    if not install_dependencies():
        sys.exit(1)
    
    # Create environment file
    create_env_file()
    
    # Run tests
    if not run_tests():
        print("\n⚠️  Setup completed but tests failed.")
        print("The server may still work, but please check the errors above.")
        sys.exit(1)
    
    print("\n🎉 Setup completed successfully!")
    print("\nTo run the MCP server:")
    print("  python main.py")
    print("\nTo test the connection:")
    print("  python test_connection.py")
    print("\nTo run with custom database path:")
    print("  FORESTRAT_DB_PATH=/path/to/your/database.duckdb python main.py")

if __name__ == "__main__":
    main() 