#!/usr/bin/env python3
"""
Example Streaming MCP Client

A simple, standalone example showing how to implement streaming support
in an MCP client for the Forestrat MCP Server.

Usage:
    python example_streaming_client.py
"""

import asyncio
import json
import sys
from typing import Dict, Any, Optional

class ExampleStreamingClient:
    """Example MCP client with streaming support"""
    
    def __init__(self):
        self.process = None
        self.initialized = False
        self.streaming_supported = False
        
    async def start_server(self):
        """Start the MCP server"""
        print("🚀 Starting MCP server...")
        self.process = await asyncio.create_subprocess_exec(
            sys.executable, "main_fixed.py",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
    async def send_request(self, request: Dict[str, Any]):
        """Send a JSON-RPC request to the server"""
        if not self.process:
            raise RuntimeError("Server not started")
            
        request_json = json.dumps(request) + "\n"
        self.process.stdin.write(request_json.encode())
        await self.process.stdin.drain()
        
    async def read_response(self) -> Optional[Dict[str, Any]]:
        """Read a single JSON-RPC response from the server"""
        if not self.process:
            return None
            
        line = await self.process.stdout.readline()
        if line:
            try:
                return json.loads(line.decode().strip())
            except json.JSONDecodeError:
                return None
        return None
    
    async def initialize(self) -> bool:
        """Initialize the MCP connection with streaming support"""
        print("📡 Initializing MCP connection with streaming support...")
        
        # Step 1: Send initialize request with streaming capability
        request = {
            "jsonrpc": "2.0",
            "id": "init-1",
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "experimental": {
                        "progressNotifications": {}  # ← KEY: Object, not boolean!
                    }
                },
                "clientInfo": {
                    "name": "example-streaming-client",
                    "version": "1.0.0"
                }
            }
        }
        
        await self.send_request(request)
        
        # Step 2: Read initialize response
        response = await self.read_response()
        
        if response and response.get("id") == "init-1":
            result = response.get("result", {})
            server_capabilities = result.get("capabilities", {})
            experimental = server_capabilities.get("experimental", {})
            
            # Check if server supports progress notifications
            self.streaming_supported = "progressNotifications" in experimental
            self.initialized = True
            
            print(f"✅ Server initialized!")
            print(f"📊 Streaming supported: {self.streaming_supported}")
            print(f"🔧 Server capabilities: {server_capabilities}")
            
            # Step 3: Send initialized notification
            await self.send_request({
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {}
            })
            
            return True
        
        print("❌ Failed to initialize server")
        return False
    
    async def call_tool_with_streaming_demo(self, tool_name: str, arguments: Dict[str, Any]):
        """Demonstrate calling a tool with streaming progress notifications"""
        
        if not self.initialized:
            raise RuntimeError("Client not initialized")
        
        print(f"\n🔧 Calling tool: {tool_name}")
        print(f"📋 Arguments: {json.dumps(arguments, indent=2)}")
        print("📡 Listening for progress notifications and final response...")
        
        request_id = f"tool-{tool_name}-demo"
        
        # Step 1: Send tool call request
        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments
            }
        }
        
        await self.send_request(request)
        
        # Step 2: Listen for progress notifications and final response
        progress_notifications = []
        final_response = None
        
        while True:
            response = await self.read_response()
            if not response:
                break
            
            # Handle progress notifications
            if response.get("method") == "notifications/progress":
                progress_data = response["params"]["value"]
                progress_notifications.append(progress_data)
                
                # Display progress update
                message = progress_data.get('message', 'Processing...')
                percentage = progress_data.get('percentage')
                step = progress_data.get('step', 'N/A')
                kind = progress_data.get('kind', 'report')
                
                if percentage is not None:
                    print(f"📊 Progress: {message} ({percentage}% - Step {step})")
                else:
                    print(f"📊 Progress: {message} (Step {step})")
                
                # Check for completion
                if kind == "end":
                    print("✅ Operation completed - waiting for final result...")
            
            # Handle final response
            elif response.get("id") == request_id:
                final_response = response
                print("📋 Final response received!")
                break
        
        # Display results
        if final_response:
            if "result" in final_response:
                result_content = final_response["result"]["content"][0]["text"]
                result_data = json.loads(result_content)
                
                print(f"\n📈 Results Summary:")
                print(f"   ├─ Progress notifications: {len(progress_notifications)}")
                print(f"   ├─ Response size: {len(result_content)} characters")
                
                if isinstance(result_data, list):
                    print(f"   └─ Data points returned: {len(result_data)}")
                elif isinstance(result_data, dict):
                    print(f"   └─ Result keys: {list(result_data.keys())}")
                
                # Show first few characters of result
                preview = result_content[:200] + "..." if len(result_content) > 200 else result_content
                print(f"\n📄 Result preview:\n{preview}")
                
            else:
                error = final_response.get("error", "Unknown error")
                print(f"❌ Tool call failed: {error}")
        else:
            print("❌ No final response received")
        
        return {
            "response": final_response,
            "progress": progress_notifications
        }
    
    async def cleanup(self):
        """Clean up the server process"""
        if self.process:
            print("🛑 Shutting down server...")
            self.process.terminate()
            await self.process.wait()

async def demo_streaming_tools():
    """Demonstrate streaming with various tools"""
    
    client = ExampleStreamingClient()
    
    try:
        # Initialize
        await client.start_server()
        
        if not await client.initialize():
            print("❌ Failed to initialize client")
            return
        
        # Demo tools with different characteristics
        demo_tools = [
            {
                "name": "list_datasets",
                "args": {"include_stats": True},
                "description": "List all datasets with statistics"
            },
            {
                "name": "get_data_for_time_range", 
                "args": {
                    "dataset": "cme",
                    "start_date": "2025-01-01",
                    "end_date": "2025-01-02",
                    "limit": 50
                },
                "description": "Query data for a specific time range"
            },
            {
                "name": "query_data",
                "args": {
                    "query": "SELECT exchange, COUNT(*) as record_count FROM bronze.cme_market_data_raw WHERE trade_date >= '2025-01-01' GROUP BY exchange LIMIT 5",
                    "limit": 100
                },
                "description": "Execute a custom SQL query"
            }
        ]
        
        print("\n" + "="*60)
        print("🎯 STREAMING DEMO - Multiple Tools")
        print("="*60)
        
        for i, tool in enumerate(demo_tools, 1):
            print(f"\n{'─'*20} Demo {i}/{len(demo_tools)} {'─'*20}")
            print(f"📝 {tool['description']}")
            
            result = await client.call_tool_with_streaming_demo(
                tool["name"], 
                tool["args"]
            )
            
            if i < len(demo_tools):
                print("\n⏳ Waiting 3 seconds before next demo...")
                await asyncio.sleep(3)
        
        print(f"\n{'='*60}")
        print("🎉 All streaming demos completed successfully!")
        print("="*60)
        
    except KeyboardInterrupt:
        print("\n⚠️ Demo interrupted by user")
        
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await client.cleanup()

async def simple_streaming_example():
    """Simple example showing just the core concepts"""
    
    print("🚀 Simple Streaming Example")
    print("="*40)
    
    # Start server process
    process = await asyncio.create_subprocess_exec(
        sys.executable, "main_fixed.py",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE
    )
    
    async def send(request):
        """Helper to send JSON-RPC request"""
        request_json = json.dumps(request) + "\n"
        process.stdin.write(request_json.encode())
        await process.stdin.drain()
    
    async def receive():
        """Helper to receive JSON-RPC response"""
        line = await process.stdout.readline()
        return json.loads(line.decode().strip()) if line else None
    
    try:
        # 1. Initialize with streaming capability
        print("📡 Step 1: Initialize with streaming capability")
        await send({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "experimental": {
                        "progressNotifications": {}  # Object, not boolean!
                    }
                },
                "clientInfo": {"name": "simple-example", "version": "1.0.0"}
            }
        })
        
        init_response = await receive()
        streaming_supported = "progressNotifications" in init_response["result"]["capabilities"].get("experimental", {})
        print(f"✅ Server initialized - Streaming: {streaming_supported}")
        
        # Send initialized notification
        await send({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        })
        
        # 2. Call tool and listen for streaming
        print("\n📡 Step 2: Call tool with streaming")
        await send({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "list_datasets",
                "arguments": {"include_stats": True}
            }
        })
        
        # 3. Handle responses
        print("📊 Step 3: Handle streaming responses")
        progress_count = 0
        
        while True:
            response = await receive()
            if not response:
                break
            
            # Progress notification
            if response.get("method") == "notifications/progress":
                progress_count += 1
                progress = response["params"]["value"]
                
                message = progress["message"]
                percentage = progress.get("percentage", "?")
                print(f"   📊 Progress {progress_count}: {message} ({percentage}%)")
                
                if progress.get("kind") == "end":
                    print("   ✅ Tool execution completed!")
            
            # Final result
            elif response.get("id") == 2:
                print("   📋 Final result received!")
                result_size = len(response["result"]["content"][0]["text"])
                print(f"   📏 Result size: {result_size} characters")
                break
        
        print(f"\n🎉 Simple example completed! Received {progress_count} progress updates.")
        
    finally:
        process.terminate()

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Example MCP Streaming Client")
    parser.add_argument(
        "--simple", 
        action="store_true", 
        help="Run simple example instead of full demo"
    )
    
    args = parser.parse_args()
    
    print("🎯 Example MCP Streaming Client")
    print("="*50)
    print()
    print("This example demonstrates how to implement streaming support")
    print("in an MCP client to receive real-time progress notifications.")
    print()
    
    if args.simple:
        print("Running simple example...")
        asyncio.run(simple_streaming_example())
    else:
        print("Running full demo...")
        asyncio.run(demo_streaming_tools())

if __name__ == "__main__":
    main() 