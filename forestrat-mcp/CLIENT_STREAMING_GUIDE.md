# MCP Client Streaming Implementation Guide

## Overview

This guide shows MCP clients how to implement streaming support to receive real-time progress notifications from the Forestrat MCP Server during long-running operations.

## 🚀 Quick Start

### 1. Declare Streaming Capability During Initialization

When initializing the MCP connection, declare support for progress notifications:

```json
{
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
      "name": "your-client-name",
      "version": "1.0.0"
    }
  }
}
```

### 2. Check Server Support

Verify the server supports streaming in its response:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "capabilities": {
      "experimental": {
        "progressNotifications": {}
      }
    }
  }
}
```

## 📋 Complete Implementation Examples

### Python Client Implementation

```python
import asyncio
import json
import sys
from typing import Dict, Any, Optional, Callable

class StreamingMCPClient:
    def __init__(self, progress_callback: Optional[Callable] = None):
        self.process = None
        self.initialized = False
        self.streaming_supported = False
        self.progress_callback = progress_callback or self._default_progress_handler
        
    async def _default_progress_handler(self, progress_data: Dict[str, Any]):
        """Default progress handler - override for custom behavior"""
        message = progress_data.get('message', 'Processing...')
        percentage = progress_data.get('percentage')
        step = progress_data.get('step', 'N/A')
        
        if percentage is not None:
            print(f"📊 Progress: {message} ({percentage}% - Step {step})")
        else:
            print(f"📊 Progress: {message} (Step {step})")
    
    async def start_server(self, server_command: list):
        """Start the MCP server process"""
        self.process = await asyncio.create_subprocess_exec(
            *server_command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
    
    async def send_request(self, request: Dict[str, Any]) -> None:
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
        request = {
            "jsonrpc": "2.0",
            "id": "init-1",
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "experimental": {
                        "progressNotifications": {}
                    }
                },
                "clientInfo": {
                    "name": "streaming-client",
                    "version": "1.0.0"
                }
            }
        }
        
        await self.send_request(request)
        response = await self.read_response()
        
        if response and response.get("id") == "init-1":
            result = response.get("result", {})
            server_capabilities = result.get("capabilities", {})
            experimental = server_capabilities.get("experimental", {})
            
            # Check if server supports progress notifications
            self.streaming_supported = "progressNotifications" in experimental
            self.initialized = True
            
            # Send initialized notification
            await self.send_request({
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {}
            })
            
            print(f"✓ Initialized - Streaming supported: {self.streaming_supported}")
            return True
        
        return False
    
    async def call_tool_with_streaming(
        self, 
        tool_name: str, 
        arguments: Dict[str, Any],
        request_id: str = None
    ) -> Dict[str, Any]:
        """Call a tool and handle streaming progress notifications"""
        
        if not self.initialized:
            raise RuntimeError("Client not initialized")
        
        request_id = request_id or f"tool-{tool_name}-{id(arguments)}"
        
        # Send tool call request
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
        
        # Listen for progress notifications and final response
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
                
                # Call progress callback
                await self.progress_callback(progress_data)
                
                # Check for completion
                if progress_data.get("kind") == "end":
                    print("✅ Operation completed - waiting for final result...")
            
            # Handle final response
            elif response.get("id") == request_id:
                final_response = response
                break
        
        return {
            "response": final_response,
            "progress": progress_notifications
        }

# Usage Example
async def main():
    client = StreamingMCPClient()
    
    # Start server
    await client.start_server(["python", "main_fixed.py"])
    
    # Initialize with streaming support
    if await client.initialize():
        # Call a tool with streaming
        result = await client.call_tool_with_streaming(
            "list_datasets",
            {"include_stats": True}
        )
        
        print(f"Final result: {result['response']}")
        print(f"Progress updates received: {len(result['progress'])}")

if __name__ == "__main__":
    asyncio.run(main())
```

### JavaScript/TypeScript Client Implementation

```typescript
interface ProgressData {
  kind: "report" | "end";
  message: string;
  percentage?: number;
  step: number;
  tool: string;
  data?: any;
}

interface MCPResponse {
  jsonrpc: string;
  id?: string;
  method?: string;
  params?: any;
  result?: any;
  error?: any;
}

class StreamingMCPClient {
  private process: any = null;
  private initialized: boolean = false;
  private streamingSupported: boolean = false;
  
  constructor(private progressCallback?: (progress: ProgressData) => void) {
    this.progressCallback = progressCallback || this.defaultProgressHandler;
  }
  
  private defaultProgressHandler(progress: ProgressData): void {
    const { message, percentage, step } = progress;
    if (percentage !== undefined) {
      console.log(`📊 Progress: ${message} (${percentage}% - Step ${step})`);
    } else {
      console.log(`📊 Progress: ${message} (Step ${step})`);
    }
  }
  
  async startServer(serverCommand: string[]): Promise<void> {
    const { spawn } = require('child_process');
    this.process = spawn(serverCommand[0], serverCommand.slice(1), {
      stdio: ['pipe', 'pipe', 'pipe']
    });
  }
  
  async sendRequest(request: any): Promise<void> {
    if (!this.process) {
      throw new Error("Server not started");
    }
    
    const requestJson = JSON.stringify(request) + '\n';
    this.process.stdin.write(requestJson);
  }
  
  async readResponse(): Promise<MCPResponse | null> {
    return new Promise((resolve) => {
      if (!this.process) {
        resolve(null);
        return;
      }
      
      this.process.stdout.once('data', (data: Buffer) => {
        try {
          const response = JSON.parse(data.toString().trim());
          resolve(response);
        } catch (error) {
          resolve(null);
        }
      });
    });
  }
  
  async initialize(): Promise<boolean> {
    const request = {
      jsonrpc: "2.0",
      id: "init-1",
      method: "initialize",
      params: {
        protocolVersion: "2024-11-05",
        capabilities: {
          experimental: {
            progressNotifications: {}
          }
        },
        clientInfo: {
          name: "streaming-client-js",
          version: "1.0.0"
        }
      }
    };
    
    await this.sendRequest(request);
    const response = await this.readResponse();
    
    if (response && response.id === "init-1") {
      const capabilities = response.result?.capabilities?.experimental || {};
      this.streamingSupported = "progressNotifications" in capabilities;
      this.initialized = true;
      
      // Send initialized notification
      await this.sendRequest({
        jsonrpc: "2.0",
        method: "notifications/initialized",
        params: {}
      });
      
      console.log(`✓ Initialized - Streaming supported: ${this.streamingSupported}`);
      return true;
    }
    
    return false;
  }
  
  async callToolWithStreaming(
    toolName: string,
    arguments: any,
    requestId?: string
  ): Promise<{response: MCPResponse, progress: ProgressData[]}> {
    
    if (!this.initialized) {
      throw new Error("Client not initialized");
    }
    
    requestId = requestId || `tool-${toolName}-${Date.now()}`;
    
    // Send tool call request
    const request = {
      jsonrpc: "2.0",
      id: requestId,
      method: "tools/call",
      params: {
        name: toolName,
        arguments: arguments
      }
    };
    
    await this.sendRequest(request);
    
    // Listen for progress and final response
    const progressNotifications: ProgressData[] = [];
    let finalResponse: MCPResponse | null = null;
    
    while (true) {
      const response = await this.readResponse();
      if (!response) break;
      
      // Handle progress notifications
      if (response.method === "notifications/progress") {
        const progressData = response.params.value;
        progressNotifications.push(progressData);
        
        // Call progress callback
        this.progressCallback?.(progressData);
        
        // Check for completion
        if (progressData.kind === "end") {
          console.log("✅ Operation completed - waiting for final result...");
        }
      }
      // Handle final response
      else if (response.id === requestId) {
        finalResponse = response;
        break;
      }
    }
    
    return {
      response: finalResponse!,
      progress: progressNotifications
    };
  }
}
```

### Simplified Client for Testing

```python
import asyncio
import json
import subprocess

async def simple_streaming_test():
    """Simple example showing the core streaming concepts"""
    
    # 1. Start server
    process = await asyncio.create_subprocess_exec(
        "python", "main_fixed.py",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE
    )
    
    async def send_request(request):
        request_json = json.dumps(request) + "\n"
        process.stdin.write(request_json.encode())
        await process.stdin.drain()
    
    async def read_response():
        line = await process.stdout.readline()
        return json.loads(line.decode().strip()) if line else None
    
    # 2. Initialize with streaming capability
    await send_request({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "experimental": {
                    "progressNotifications": {}  # ← Key: object not boolean
                }
            },
            "clientInfo": {"name": "test", "version": "1.0.0"}
        }
    })
    
    init_response = await read_response()
    print(f"Server capabilities: {init_response['result']['capabilities']}")
    
    # Send initialized notification
    await send_request({
        "jsonrpc": "2.0",
        "method": "notifications/initialized",
        "params": {}
    })
    
    # 3. Call tool and listen for progress
    await send_request({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "list_datasets",
            "arguments": {"include_stats": True}
        }
    })
    
    # 4. Handle streaming responses
    progress_count = 0
    while True:
        response = await read_response()
        if not response:
            break
            
        # Progress notification
        if response.get("method") == "notifications/progress":
            progress_count += 1
            progress = response["params"]["value"]
            print(f"📊 Progress {progress_count}: {progress['message']}")
            
            if progress.get("percentage"):
                print(f"   └─ {progress['percentage']}% complete")
            
            # Operation completed
            if progress.get("kind") == "end":
                print("✅ Operation completed!")
        
        # Final result
        elif response.get("id") == 2:
            print("📋 Final result received!")
            break
    
    process.terminate()

# Run the test
asyncio.run(simple_streaming_test())
```

## 🔄 Message Flow Diagram

```
Client                           Server
  │                               │
  │ 1. Initialize (with streaming) │
  │ ─────────────────────────────→ │
  │                               │
  │ 2. Initialize Response         │
  │ ←───────────────────────────── │
  │                               │
  │ 3. Initialized Notification   │
  │ ─────────────────────────────→ │
  │                               │
  │ 4. Tool Call Request          │
  │ ─────────────────────────────→ │
  │                               │
  │ 5. Progress Notification      │
  │ ←───────────────────────────── │
  │                               │
  │ 6. Progress Notification      │
  │ ←───────────────────────────── │
  │                               │
  │ 7. Completion Notification    │
  │ ←───────────────────────────── │
  │                               │
  │ 8. Final Tool Response        │
  │ ←───────────────────────────── │
```

## 📝 Progress Notification Format

### Report Notification
```json
{
  "jsonrpc": "2.0",
  "method": "notifications/progress",
  "params": {
    "progressToken": "request-id",
    "value": {
      "kind": "report",
      "message": "Processing data...",
      "percentage": 45,
      "step": 3,
      "tool": "generate_minute_bars_data",
      "data": {}
    }
  }
}
```

### Completion Notification
```json
{
  "jsonrpc": "2.0",
  "method": "notifications/progress",
  "params": {
    "progressToken": "request-id",
    "value": {
      "kind": "end",
      "message": "Tool completed successfully",
      "percentage": 100,
      "step": 4,
      "tool": "generate_minute_bars_data"
    }
  }
}
```

## ⚠️ Important Implementation Notes

### 1. Message Order
- Progress notifications come **before** the final response
- Always wait for the completion notification (`"kind": "end"`)
- The final JSON-RPC response comes **after** all progress notifications

### 2. Error Handling
```python
async def handle_tool_call_with_errors(client, tool_name, arguments):
    try:
        result = await client.call_tool_with_streaming(tool_name, arguments)
        
        # Check for errors in final response
        if "error" in result["response"]:
            print(f"Tool error: {result['response']['error']}")
            return None
            
        return result["response"]["result"]
        
    except Exception as e:
        print(f"Client error: {e}")
        return None
```

### 3. Progress Callback Patterns

```python
# Pattern 1: Simple logging
def simple_progress(progress):
    print(f"{progress['message']} - {progress.get('percentage', 0)}%")

# Pattern 2: UI update
def ui_progress(progress):
    if progress.get("percentage"):
        update_progress_bar(progress["percentage"])
    update_status_text(progress["message"])

# Pattern 3: Detailed tracking
def detailed_progress(progress):
    timestamp = datetime.now().isoformat()
    log_entry = {
        "timestamp": timestamp,
        "tool": progress["tool"],
        "step": progress["step"],
        "message": progress["message"],
        "percentage": progress.get("percentage")
    }
    save_progress_log(log_entry)
```

## 🎯 Best Practices

### 1. Always Declare Capability
```python
# ✅ CORRECT
"capabilities": {
    "experimental": {
        "progressNotifications": {}
    }
}

# ❌ WRONG
"capabilities": {
    "experimental": {
        "progressNotifications": true  # Boolean not supported
    }
}
```

### 2. Handle Missing Streaming
```python
if not client.streaming_supported:
    print("Server doesn't support streaming - using standard mode")
    # Fall back to regular tool calls without progress
```

### 3. Non-blocking Progress Handling
```python
async def non_blocking_progress(progress):
    # Don't block on progress updates
    asyncio.create_task(update_ui(progress))
    asyncio.create_task(log_progress(progress))
```

### 4. Timeout Handling
```python
async def call_tool_with_timeout(client, tool_name, arguments, timeout=60):
    try:
        return await asyncio.wait_for(
            client.call_tool_with_streaming(tool_name, arguments),
            timeout=timeout
        )
    except asyncio.TimeoutError:
        print(f"Tool call timed out after {timeout} seconds")
        return None
```

## 🧪 Testing Your Implementation

Use the provided test server to verify your client:

```bash
# Test basic streaming
python test_streaming.py

# Test with your client
python your_streaming_client.py
```

## 🔧 Troubleshooting

### Common Issues

1. **No progress notifications received**
   - Check that you declared `progressNotifications: {}` (object, not boolean)
   - Verify server response includes streaming capability
   - Ensure you're listening for `notifications/progress` method

2. **Only getting final response**
   - Make sure you're reading multiple responses, not just one
   - Progress notifications come before the final response

3. **Missing completion notification**
   - Look for `"kind": "end"` in progress notifications
   - This indicates the operation finished

4. **JSON parsing errors**
   - Each response is a separate line
   - Parse each line individually as JSON

This implementation guide provides everything you need to add streaming support to your MCP client! 🚀 