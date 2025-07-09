"""
Supabase Configuration for NiFi Deployment

This module provides secure credential management for Supabase connections
in NiFi environments using environment variables.
"""

import os
from typing import Dict, Optional

def get_supabase_config() -> Dict[str, str]:
    """
    Get Supabase configuration from environment variables.
    
    For NiFi deployment, set these environment variables:
    - SUPABASE_HOST
    - SUPABASE_PORT  
    - SUPABASE_DATABASE
    - SUPABASE_USER
    - SUPABASE_PASSWORD
    
    Returns:
        Dict with Supabase connection parameters
    """
    
    # Get configuration from environment variables
    config = {
        'host': os.getenv('SUPABASE_HOST'),
        'port': int(os.getenv('SUPABASE_PORT', '6543')),
        'database': os.getenv('SUPABASE_DATABASE', 'postgres'),
        'user': os.getenv('SUPABASE_USER'),
        'password': os.getenv('SUPABASE_PASSWORD')
    }
    
    # Validate required environment variables
    required_env_vars = ['SUPABASE_HOST', 'SUPABASE_USER', 'SUPABASE_PASSWORD']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}. "
            f"Please set these before running the script."
        )
    
    return config

def test_supabase_config() -> bool:
    """Test if Supabase configuration is valid"""
    try:
        config = get_supabase_config()
        
        # Basic validation
        required_fields = ['host', 'port', 'database', 'user', 'password']
        for field in required_fields:
            if not config.get(field):
                print(f"❌ Missing required field: {field}")
                return False
        
        print("✅ Supabase configuration is valid")
        print(f"   Host: {config['host']}")
        print(f"   Port: {config['port']}")
        print(f"   Database: {config['database']}")
        print(f"   User: {config['user'][:10]}...")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

if __name__ == "__main__":
    # Test configuration
    test_supabase_config() 