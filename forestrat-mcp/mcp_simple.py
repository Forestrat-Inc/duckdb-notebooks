#!/usr/bin/env python3
"""
Raw JSON-RPC MCP Server

This server manually implements the MCP protocol to avoid the pydantic validation
issues in the MCP library decorators.
"""

import asyncio
import json
import sys
import logging
from typing import Any, Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleMCPServer:
    """Simple JSON-RPC MCP Server implementation"""
    
    def __init__(self):
        self.initialized = False
        logger.info("Simple MCP Server initialized")
    
    def create_response(self, request_id: Optional[Any], result: Any) -> Dict[str, Any]:
        """Create a JSON-RPC response"""
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": result
        }
    
    def create_error(self, request_id: Optional[Any], code: int, message: str) -> Dict[str, Any]:
        """Create a JSON-RPC error response"""
        return {
            "jsonrpc": "2.0", 
            "id": request_id,
            "error": {
                "code": code,
                "message": message
            }
        }
    
    def handle_initialize(self, request_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle initialize request"""
        logger.info("Handling initialize request")
        self.initialized = True
        
        return self.create_response(request_id, {
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {}
            },
                         "serverInfo": {
                 "name": "mcp-simple",
                 "version": "1.0.0"
             }
        })
    
    def handle_list_tools(self, request_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/list request"""
        logger.info("Handling tools/list request")
        
        if not self.initialized:
            return self.create_error(request_id, -32002, "Server not initialized")
        
        tools = [
            {
                "name": "echo",
                "description": "Echo back the input message",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "message": {
                            "type": "string",
                            "description": "Message to echo back"
                        }
                    },
                    "required": ["message"]
                }
            },
            {
                "name": "add",
                "description": "Add two numbers together", 
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "a": {
                            "type": "number",
                            "description": "First number"
                        },
                        "b": {
                            "type": "number",
                            "description": "Second number"
                        }
                    },
                    "required": ["a", "b"]
                }
            },
            {
                "name": "get_time",
                "description": "Get current timestamp",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False
                }
            }
        ]
        
        return self.create_response(request_id, {
            "tools": tools
        })
    
    def handle_call_tool(self, request_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/call request"""
        name = params.get("name")
        arguments = params.get("arguments", {})
        
        logger.info(f"Handling tool call: {name} with arguments: {arguments}")
        
        if not self.initialized:
            return self.create_error(request_id, -32002, "Server not initialized")
        
        try:
            if name == "echo":
                message = arguments.get("message", "")
                return self.create_response(request_id, {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Echo: {message}"
                        }
                    ]
                })
            
            elif name == "add":
                a = float(arguments.get("a", 0))
                b = float(arguments.get("b", 0))
                result = a + b
                return self.create_response(request_id, {
                    "content": [
                        {
                            "type": "text", 
                            "text": f"Result: {a} + {b} = {result}"
                        }
                    ]
                })
            
            elif name == "get_time":
                import datetime
                now = datetime.datetime.now().isoformat()
                return self.create_response(request_id, {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Current time: {now}"
                        }
                    ]
                })
            
            else:
                return self.create_error(request_id, -32601, f"Unknown tool: {name}")
                
        except Exception as e:
            logger.error(f"Error handling tool call: {e}")
            return self.create_error(request_id, -32603, f"Internal error: {str(e)}")
    
    async def handle_request(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle a JSON-RPC request"""
        method = request.get("method")
        request_id = request.get("id")
        params = request.get("params", {})
        
        logger.info(f"Handling request: {method}")
        
        if method == "initialize":
            return self.handle_initialize(request_id, params)
        elif method == "notifications/initialized":
            # This is a notification, no response needed
            logger.info("Received initialized notification")
            return None
        elif method == "tools/list":
            return self.handle_list_tools(request_id, params)
        elif method == "tools/call":
            return self.handle_call_tool(request_id, params)
        else:
            return self.create_error(request_id, -32601, f"Method not found: {method}")
    
    async def run(self):
        """Run the server with stdio"""
        logger.info("Starting simple MCP server")
        
        try:
            while True:
                # Read line from stdin
                line = await asyncio.get_event_loop().run_in_executor(
                    None, sys.stdin.readline
                )
                
                if not line:
                    break
                
                try:
                    # Parse JSON request
                    request = json.loads(line.strip())
                    logger.debug(f"Received request: {request}")
                    
                    # Handle request
                    response = await self.handle_request(request)
                    
                    # Send response if needed
                    if response:
                        response_json = json.dumps(response)
                        print(response_json, flush=True)
                        logger.debug(f"Sent response: {response_json}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON: {e}")
                    error_response = self.create_error(None, -32700, "Parse error")
                    print(json.dumps(error_response), flush=True)
                except Exception as e:
                    logger.error(f"Error handling request: {e}")
                    error_response = self.create_error(None, -32603, "Internal error")
                    print(json.dumps(error_response), flush=True)
                    
        except KeyboardInterrupt:
            logger.info("Server shutting down...")
        except Exception as e:
            logger.error(f"Server error: {e}")
            raise

async def main():
    """Main entry point"""
    server = SimpleMCPServer()
    await server.run()

if __name__ == "__main__":
    asyncio.run(main()) 