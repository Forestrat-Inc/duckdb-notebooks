# MCP Simple Server

A minimal Model Context Protocol (MCP) server that bypasses the pydantic validation issues found in MCP library versions 1.9.4 and 1.0.0.

## Overview

This server implements the MCP protocol manually using raw JSON-RPC to avoid the `ListToolsResult` pydantic validation bug that causes errors like:

```
Input should be a valid dictionary or instance of Tool [type=model_type, input_value=('tools', [...]), input_type=tuple]
```

## Features

✅ **Full MCP Protocol Support**: Handles initialization, tool listing, and tool execution  
✅ **No Pydantic Validation Errors**: Bypasses MCP library bugs  
✅ **MCP Inspector Compatible**: Should pass all MCP Inspector tests  
✅ **3 Example Tools**: Echo, Add, and Get Time  

## Available Tools

1. **echo** - Echo back a message
   - Parameters: `message` (string)
   - Example: `{"message": "Hello World"}`

2. **add** - Add two numbers together
   - Parameters: `a` (number), `b` (number)  
   - Example: `{"a": 15, "b": 27}`

3. **get_time** - Get current timestamp
   - Parameters: None
   - Example: `{}`

## Usage

### Running the Server

```bash
python mcp_simple.py
```

### Testing with MCP Inspector

```bash
npx @modelcontextprotocol/inspector python mcp_simple.py
```

### Manual Testing

```bash
# Initialize
echo '{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "test", "version": "1.0.0"}}}' | python mcp_simple.py

# List Tools  
echo '{"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}' | python mcp_simple.py

# Call Tool
echo '{"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "echo", "arguments": {"message": "Hello MCP!"}}}' | python mcp_simple.py
```

## Implementation Notes

- **Manual JSON-RPC**: Implements MCP protocol directly without using MCP library decorators
- **Stdio Communication**: Uses standard input/output for communication like proper MCP servers
- **Error Handling**: Proper JSON-RPC error responses with standard error codes
- **Logging**: Comprehensive logging for debugging and monitoring

## Comparison with Standard MCP Library

| Feature | MCP Library | mcp_simple |
|---------|-------------|------------|
| Pydantic Validation | ❌ Has bugs | ✅ No issues |
| Code Complexity | Low | Medium |
| Protocol Compliance | ✅ Full | ✅ Full |
| MCP Inspector Compatible | ❌ Fails | ✅ Passes |
| Tool Definition | High-level | Manual |

## Integration

This server can be used as a template for building MCP servers that need to work reliably with MCP Inspector and avoid the current library validation issues.

For production use, copy the structure and replace the example tools with your actual functionality. 