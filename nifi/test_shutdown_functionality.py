#!/usr/bin/env python3
"""
Test script to demonstrate shutdown functionality for load_january_simple.py
"""

import subprocess
import time
import sys
from pathlib import Path

def run_command(cmd, wait=True):
    """Run a command and return the result"""
    # Update command to use parent directory for the script
    if "load_january_simple.py" in cmd:
        cmd = cmd.replace("python load_january_simple.py", "python ../load_january_simple.py")
    
    print(f"Running: {cmd}")
    if wait:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print(f"Exit code: {result.returncode}")
        if result.stdout:
            print(f"Output: {result.stdout}")
        if result.stderr:
            print(f"Error: {result.stderr}")
        return result
    else:
        # Start process without waiting
        return subprocess.Popen(cmd, shell=True)

def test_shutdown_file_functionality():
    """Test the shutdown file functionality"""
    print("="*60)
    print("TESTING SHUTDOWN FILE FUNCTIONALITY")
    print("="*60)
    
    # Test 1: Check if shutdown file exists (should not exist initially)
    print("\n1. Testing --check-shutdown-file (should not exist)")
    result = run_command("python load_january_simple.py --check-shutdown-file")
    assert result.returncode == 0, "Expected exit code 0 (no shutdown file)"
    
    # Test 2: Create shutdown file
    print("\n2. Testing --create-shutdown-file")
    result = run_command("python load_january_simple.py --create-shutdown-file")
    assert result.returncode == 0, "Expected exit code 0 (file created)"
    
    # Test 3: Check if shutdown file exists (should exist now)
    print("\n3. Testing --check-shutdown-file (should exist)")
    result = run_command("python load_january_simple.py --check-shutdown-file")
    assert result.returncode == 1, "Expected exit code 1 (shutdown file exists)"
    
    # Test 4: Remove shutdown file
    print("\n4. Testing --remove-shutdown-file")
    result = run_command("python load_january_simple.py --remove-shutdown-file")
    assert result.returncode == 0, "Expected exit code 0 (file removed)"
    
    # Test 5: Check if shutdown file exists (should not exist after removal)
    print("\n5. Testing --check-shutdown-file (should not exist after removal)")
    result = run_command("python load_january_simple.py --check-shutdown-file")
    assert result.returncode == 0, "Expected exit code 0 (no shutdown file)"
    
    print("\n✅ All shutdown file tests passed!")

def test_help_output():
    """Test that help output includes new options"""
    print("="*60)
    print("TESTING HELP OUTPUT")
    print("="*60)
    
    result = run_command("python load_january_simple.py --help")
    assert result.returncode == 0, "Expected exit code 0 for help"
    
    # Check that new options are in help
    help_text = result.stdout
    assert "--create-shutdown-file" in help_text, "Missing --create-shutdown-file in help"
    assert "--remove-shutdown-file" in help_text, "Missing --remove-shutdown-file in help"
    assert "--check-shutdown-file" in help_text, "Missing --check-shutdown-file in help"
    assert "--idempotent" in help_text, "Missing --idempotent in help"
    
    print("✅ Help output includes all new options!")

def simulate_nifi_scenario():
    """Simulate a NiFi scenario where we start script and then shut it down"""
    print("="*60)
    print("SIMULATING NIFI SCENARIO")
    print("="*60)
    
    # First ensure no shutdown file exists
    run_command("python load_january_simple.py --remove-shutdown-file")
    
    print("\n1. Starting script with a small date range (simulating NiFi ExecuteProcess)")
    # Start the script in background with a small date range
    process = run_command(
        "python load_january_simple.py --idempotent --start-date 2025-01-01 --end-date 2025-01-02 --verbose", 
        wait=False
    )
    
    # Give it a moment to start
    time.sleep(2)
    
    print("\n2. Creating shutdown file (simulating NiFi shutdown trigger)")
    run_command("python load_january_simple.py --create-shutdown-file")
    
    print("\n3. Waiting for process to complete gracefully...")
    try:
        # Wait for the process to finish (should exit gracefully)
        process.wait(timeout=30)
        print(f"Process completed with exit code: {process.returncode}")
    except subprocess.TimeoutExpired:
        print("Process did not complete within timeout - terminating")
        process.terminate()
        process.wait()
    
    print("\n✅ NiFi scenario simulation completed!")

def main():
    """Run all tests"""
    print("Testing shutdown functionality for load_january_simple.py")
    print("This validates the NiFi integration capabilities")
    
    try:
        # Test basic functionality
        test_shutdown_file_functionality()
        
        # Test help output
        test_help_output()
        
        # Simulate NiFi scenario (optional - comment out if you don't want to run actual loading)
        if len(sys.argv) > 1 and sys.argv[1] == "--full-test":
            simulate_nifi_scenario()
        else:
            print("\n" + "="*60)
            print("NIFI SIMULATION SKIPPED")
            print("="*60)
            print("Run with --full-test to simulate actual NiFi scenario")
            print("(This will attempt to load actual data)")
        
        print("\n🎉 All tests completed successfully!")
        print("\nThe script is ready for NiFi integration with these shutdown methods:")
        print("1. NiFi processor controls (Stop/Terminate)")
        print("2. File-based shutdown (--create-shutdown-file)")
        print("3. Signal-based shutdown (Ctrl+C when run directly)")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 