#!/usr/bin/env python3
"""
Test script for streaming functionality in Forestrat MCP Server

This script demonstrates how the MCP server now sends progress notifications
during long-running operations.
"""

import asyncio
import json
import sys
import subprocess
from typing import Dict, Any, Optional

class StreamingMCPClient:
    """Simple MCP client to test streaming functionality"""
    
    def __init__(self):
        self.process = None
        self.initialized = False
        
    async def start_server(self):
        """Start the MCP server process"""
        self.process = await asyncio.create_subprocess_exec(
            sys.executable, "main_fixed.py",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
    async def send_request(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Send a JSON-RPC request to the server"""
        if not self.process:
            raise RuntimeError("Server not started")
            
        request_json = json.dumps(request) + "\n"
        self.process.stdin.write(request_json.encode())
        await self.process.stdin.drain()
        
        # Read response
        line = await self.process.stdout.readline()
        if line:
            try:
                return json.loads(line.decode().strip())
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}", file=sys.stderr)
                print(f"Raw line: {line.decode().strip()}", file=sys.stderr)
                return None
        return None
    
    async def initialize(self):
        """Initialize the MCP connection"""
        request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "experimental": {
                        "progressNotifications": {}
                    }
                },
                "clientInfo": {
                    "name": "streaming-test-client",
                    "version": "1.0.0"
                }
            }
        }
        
        response = await self.send_request(request)
        if response and response.get("result"):
            self.initialized = True
            print("✓ MCP Server initialized successfully")
            print(f"Server capabilities: {response['result']['capabilities']}")
            
            # Send initialized notification
            notification = {
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {}
            }
            await self.send_request(notification)
            return True
        return False
    
    async def test_streaming_tool(self, tool_name: str, arguments: Dict[str, Any]):
        """Test a tool with streaming progress notifications"""
        if not self.initialized:
            print("❌ Client not initialized")
            return
            
        print(f"\n🔧 Testing streaming tool: {tool_name}")
        print(f"Arguments: {json.dumps(arguments, indent=2)}")
        
        request = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments
            }
        }
        
        # Send the request
        request_json = json.dumps(request) + "\n"
        self.process.stdin.write(request_json.encode())
        await self.process.stdin.drain()
        
        print("📡 Listening for progress notifications and response...")
        
        # Listen for multiple responses (progress notifications + final result)
        progress_count = 0
        final_response = None
        
        while True:
            line = await self.process.stdout.readline()
            if not line:
                break
                
            try:
                response = json.loads(line.decode().strip())
                
                # Check if it's a progress notification
                if response.get("method") == "notifications/progress":
                    progress_count += 1
                    progress_data = response["params"]["value"]
                    print(f"📊 Progress {progress_count}: {progress_data['message']}")
                    
                    if progress_data.get("percentage"):
                        print(f"   └─ {progress_data['percentage']}% complete (Step {progress_data.get('step', 'N/A')})")
                    
                    # Check if this is the completion notification
                    if progress_data.get("kind") == "end":
                        print("✅ Operation completed - waiting for final result...")
                        
                # Check if it's the final response
                elif response.get("id") == 2:
                    final_response = response
                    break
                    
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}", file=sys.stderr)
                print(f"Raw line: {line.decode().strip()}", file=sys.stderr)
                continue
        
        # Display final result
        if final_response:
            if "result" in final_response:
                result_content = final_response["result"]["content"][0]["text"]
                result_data = json.loads(result_content)
                print(f"\n📋 Final Result Summary:")
                print(f"   └─ Data points returned: {len(result_data) if isinstance(result_data, list) else 'N/A'}")
                print(f"   └─ Response size: {len(result_content)} characters")
                print(f"   └─ Progress notifications received: {progress_count}")
            else:
                print(f"❌ Error: {final_response.get('error', 'Unknown error')}")
        else:
            print("❌ No final response received")
    
    async def cleanup(self):
        """Clean up the server process"""
        if self.process:
            self.process.terminate()
            await self.process.wait()

async def main():
    """Main test function"""
    print("🚀 Starting MCP Streaming Test")
    print("=" * 50)
    
    client = StreamingMCPClient()
    
    try:
        # Start server
        print("📡 Starting MCP server...")
        await client.start_server()
        
        # Initialize connection
        if not await client.initialize():
            print("❌ Failed to initialize MCP connection")
            return
        
        # Test streaming with different tools
        test_cases = [
            {
                "tool": "list_datasets",
                "args": {"include_stats": True}
            },
            {
                "tool": "get_data_for_time_range", 
                "args": {
                    "dataset": "cme",
                    "start_date": "2025-01-01",
                    "end_date": "2025-01-02",
                    "limit": 100
                }
            },
            {
                "tool": "query_data",
                "args": {
                    "query": "SELECT COUNT(*) as total_records, COUNT(DISTINCT symbol) as unique_symbols FROM bronze.cme_market_data_raw WHERE trade_date >= '2025-01-01' LIMIT 10",
                    "limit": 1000
                }
            }
        ]
        
        for i, test_case in enumerate(test_cases, 1):
            print(f"\n{'='*20} Test {i}/{len(test_cases)} {'='*20}")
            await client.test_streaming_tool(test_case["tool"], test_case["args"])
            
            if i < len(test_cases):
                print("\n⏳ Waiting 2 seconds before next test...")
                await asyncio.sleep(2)
        
        print(f"\n{'='*50}")
        print("✅ All streaming tests completed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await client.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 