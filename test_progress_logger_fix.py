#!/usr/bin/env python3
"""
Test script to verify the progress logger thread isolation fix
"""

import sys
import time
import threading
from pathlib import Path
from datetime import date

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from load_january_simple import SimpleMultiExchangeLoader

def test_progress_logger_isolation():
    """Test that progress loggers for different operations are isolated"""
    print("Testing progress logger thread isolation...")
    
    # Create a loader instance
    loader = SimpleMultiExchangeLoader()
    
    # Start progress loggers for different operations
    print("Starting progress logger for LSE 2025-01-01...")
    loader._start_progress_logger("Test Operation", "LSE", date(2025, 1, 1))
    
    time.sleep(2)  # Let it run for a bit
    
    print("Starting progress logger for CME 2025-01-02...")
    loader._start_progress_logger("Test Operation", "CME", date(2025, 1, 2))
    
    time.sleep(2)  # Let both run
    
    print("Starting progress logger for NYQ 2025-01-03...")
    loader._start_progress_logger("Test Operation", "NYQ", date(2025, 1, 3))
    
    # Check that we have multiple active threads
    print(f"Active progress threads: {len(loader._active_progress_threads)}")
    for key in loader._active_progress_threads.keys():
        print(f"  - {key}")
    
    time.sleep(35)  # Wait for progress messages (they log every 30 seconds)
    
    # Stop specific operations
    print("Stopping LSE operation...")
    loader._stop_progress_logger("LSE_2025-01-01_Test Operation")
    
    time.sleep(2)
    
    print("Stopping CME operation...")
    loader._stop_progress_logger("CME_2025-01-02_Test Operation")
    
    time.sleep(2)
    
    print(f"Active progress threads after stopping 2: {len(loader._active_progress_threads)}")
    
    # Clean up all
    print("Cleaning up all progress loggers...")
    loader.cleanup()
    
    print(f"Active progress threads after cleanup: {len(loader._active_progress_threads)}")
    print("✅ Progress logger isolation test completed!")

if __name__ == "__main__":
    test_progress_logger_isolation() 