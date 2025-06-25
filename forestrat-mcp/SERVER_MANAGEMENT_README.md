# MCP Server Management Scripts

Easy-to-use scripts for managing your Forestrat MCP Server.

## 🚀 Quick Start

### Start/Restart the Server
```bash
./restart_server.sh
```
- Automatically stops any existing server processes
- Starts the server with `main_fixed.py` (default)
- Shows startup progress with countdown
- Logs to `forestrat_mcp_server.log`

### Stop the Server
```bash
./stop_server.sh
```
- Gracefully stops all server processes
- Shows process status before and after
- Force kills if necessary

### Check Server Status
```bash
./server_status.sh
```
- Shows if server is running or stopped
- Displays process details and resource usage
- Shows recent log entries
- Lists available server files
- Provides quick command reference

## 📋 Usage Examples

### Basic Operations
```bash
# Start the server
./restart_server.sh

# Check if it's running
./server_status.sh

# Stop the server
./stop_server.sh
```

### Using Different Server Files
```bash
# Start with a specific server file
./restart_server.sh main.py

# Stop specific server
./stop_server.sh main.py

# Check status of specific server
./server_status.sh main.py
```

### Monitoring
```bash
# Watch live logs
tail -f forestrat_mcp_server.log

# Check status periodically
watch -n 5 ./server_status.sh
```

## 🔧 Script Features

### `restart_server.sh`
- ✅ **Process Detection**: Finds existing server processes
- ✅ **Graceful Shutdown**: Uses TERM signal first, then KILL if needed
- ✅ **Countdown Start**: 3-second countdown before starting
- ✅ **File Validation**: Checks if server file exists
- ✅ **Visual Feedback**: Emoji-rich status messages

### `stop_server.sh`
- ✅ **Safe Stopping**: Graceful termination with fallback to force kill
- ✅ **Process Verification**: Confirms all processes are stopped
- ✅ **Status Display**: Shows final process status
- ✅ **Error Handling**: Proper exit codes for automation

### `server_status.sh`
- ✅ **Comprehensive Info**: Process details, system info, available files
- ✅ **Log Integration**: Shows recent log entries
- ✅ **Resource Usage**: CPU, memory, and timing information
- ✅ **Quick Commands**: Reference for common operations
- ✅ **File Comparison**: Highlights current server file

## 🎯 Default Behavior

All scripts default to using `main_fixed.py` as the server file. This can be overridden:

```bash
# These are equivalent
./restart_server.sh
./restart_server.sh main_fixed.py

# Use a different server file
./restart_server.sh main.py
```

## 📝 Log Files

The server logs to `forestrat_mcp_server.log` in the current directory:

```bash
# View recent logs
tail -20 forestrat_mcp_server.log

# Follow logs in real-time
tail -f forestrat_mcp_server.log

# Search logs
grep "ERROR" forestrat_mcp_server.log
```

## 🔍 Troubleshooting

### Server Won't Start
1. Check if port is already in use: `./server_status.sh`
2. Review log file: `tail -20 forestrat_mcp_server.log`
3. Verify Python environment: `python3 --version`
4. Check file permissions: `ls -la main_fixed.py`

### Server Won't Stop
1. Use force stop: `./stop_server.sh` (already includes force kill)
2. Manual kill: `pkill -f main_fixed.py`
3. Find processes: `pgrep -f main_fixed.py`

### Permission Issues
```bash
# Make scripts executable
chmod +x *.sh

# Check permissions
ls -la *.sh
```

## 🚦 Process Management

The scripts handle multiple scenarios:
- **Clean Start**: No existing processes
- **Restart**: Existing processes are gracefully terminated
- **Force Kill**: If graceful termination fails
- **Multiple Processes**: Handles multiple server instances
- **Background Processes**: Finds and manages background servers

## 📊 Status Indicators

- 🟢 **RUNNING**: Server is active and responding
- 🔴 **STOPPED**: No server processes found
- 🟡 **STARTING**: Server is in startup phase
- ❌ **ERROR**: Server encountered issues

## 🔄 Integration with Claude Desktop

These scripts work seamlessly with Claude Desktop's MCP configuration:

1. **Start server**: `./restart_server.sh`
2. **Configure Claude**: Server runs on stdio
3. **Monitor**: Use `./server_status.sh` to check health
4. **Restart if needed**: `./restart_server.sh` handles cleanup

---

**Pro Tip**: Add these scripts to your PATH or create aliases for even easier access!

```bash
# Add to ~/.zshrc or ~/.bashrc
alias mcp-start="cd /path/to/forestrat-mcp && ./restart_server.sh"
alias mcp-stop="cd /path/to/forestrat-mcp && ./stop_server.sh"
alias mcp-status="cd /path/to/forestrat-mcp && ./server_status.sh"
``` 