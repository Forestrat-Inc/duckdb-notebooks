# Forestrat MCP Server - Docker Setup

This directory contains everything needed to run the Forestrat MCP Server in Docker containers.

## Quick Start

### Prerequisites
- Docker installed and running
- DuckDB database file at `../multi_exchange_data_lake.duckdb`

### Build and Run
```bash
# Build the Docker image
./docker-run.sh build

# Start the MCP server
./docker-run.sh start

# Check status
./docker-run.sh status

# View logs
./docker-run.sh logs
```

## Docker Files Overview

### Core Files
- **`Dockerfile`** - Multi-stage Docker image definition
- **`docker-compose.yml`** - Docker Compose configuration for easy management
- **`.dockerignore`** - Excludes unnecessary files from Docker context
- **`docker-run.sh`** - Management script for easy container operations

### Configuration
- **Database Path**: `/app/data/multi_exchange_data_lake.duckdb` (inside container)
- **Log Directory**: `/app/logs` (mounted from host)
- **Environment Variables**: Configurable via Docker environment

## Management Commands

The `docker-run.sh` script provides convenient commands:

```bash
./docker-run.sh build     # Build the Docker image
./docker-run.sh start     # Start the MCP server container
./docker-run.sh stop      # Stop and remove the container
./docker-run.sh restart   # Restart the container
./docker-run.sh logs      # Follow container logs
./docker-run.sh status    # Show container and health status
./docker-run.sh shell     # Open shell in running container
./docker-run.sh dev       # Start development container with code mounting
./docker-run.sh clean     # Remove containers and images
```

## Docker Compose Usage

Alternative to the management script:

```bash
# Start with docker-compose
docker-compose up -d

# Start development environment
docker-compose --profile dev up

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

## Volume Mounts

### Production Container
- **Database**: `../multi_exchange_data_lake.duckdb` → `/app/data/multi_exchange_data_lake.duckdb` (read-only)
- **Logs**: `./logs` → `/app/logs` (read-write)
- **Config**: `./config.py` → `/app/config.py` (read-only)

### Development Container
- **Source Code**: `./main_fixed.py`, `./config.py`, `./database.py` → `/app/` (read-write)
- **Database**: `../multi_exchange_data_lake.duckdb` → `/app/data/multi_exchange_data_lake.duckdb` (read-write)
- **Logs**: `./logs` → `/app/logs` (read-write)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_PATH` | `/app/data/multi_exchange_data_lake.duckdb` | Path to DuckDB database |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `PYTHONUNBUFFERED` | `1` | Ensure real-time log output |

## Health Monitoring

The container includes health checks that:
- Test database connectivity every 30 seconds
- Allow 10 seconds for each check
- Mark unhealthy after 3 consecutive failures
- Wait 10 seconds before starting checks

Check health status:
```bash
./docker-run.sh status
# or
docker inspect forestrat-mcp-server --format='{{.State.Health.Status}}'
```

## Resource Limits

Default resource constraints:
- **Memory Limit**: 2GB
- **CPU Limit**: 1.0 cores
- **Memory Reservation**: 512MB
- **CPU Reservation**: 0.25 cores

Modify in `docker-compose.yml` if needed.

## Development Workflow

1. **Make code changes** in `main_fixed.py`, `config.py`, or `database.py`
2. **Test with development container**:
   ```bash
   ./docker-run.sh dev
   # Code changes are reflected immediately due to volume mounting
   ```
3. **Rebuild and deploy**:
   ```bash
   ./docker-run.sh build
   ./docker-run.sh restart
   ```

## Troubleshooting

### Container Won't Start
```bash
# Check if database file exists
ls -la ../multi_exchange_data_lake.duckdb

# Check Docker logs
./docker-run.sh logs

# Verify image built correctly
docker images | grep forestrat-mcp
```

### Health Check Failures
```bash
# Check health status
./docker-run.sh status

# Test database connection manually
./docker-run.sh shell
python -c "from database import DuckDBConnection; import os; db = DuckDBConnection(os.getenv('DATABASE_PATH')); print(db.test_connection())"
```

### Permission Issues
```bash
# Fix log directory permissions
sudo chown -R $(id -u):$(id -g) ./logs

# Rebuild with correct permissions
./docker-run.sh clean
./docker-run.sh build
```

### Memory Issues
```bash
# Monitor resource usage
docker stats forestrat-mcp-server

# Increase memory limits in docker-compose.yml
# Then restart:
./docker-run.sh restart
```

## Integration with MCP Clients

### Cursor IDE Configuration
Add to your MCP client configuration:
```json
{
  "forestrat-mcp": {
    "command": "docker",
    "args": ["exec", "-i", "forestrat-mcp-server", "python", "main_fixed.py"]
  }
}
```

### Direct Docker Communication
```bash
# Interactive session
docker exec -it forestrat-mcp-server python main_fixed.py

# Send JSON-RPC commands
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' | \
  docker exec -i forestrat-mcp-server python main_fixed.py
```

## Security Considerations

- Container runs as non-root user `app`
- Database mounted read-only in production
- No network ports exposed by default
- Minimal attack surface with slim base image

## Performance Optimization

- **Multi-stage build** minimizes image size
- **Layer caching** optimizes rebuild times
- **Resource limits** prevent resource exhaustion
- **Health checks** ensure service reliability

## Backup and Recovery

```bash
# Backup container configuration
docker inspect forestrat-mcp-server > container-config.json

# Export image
docker save forestrat-mcp:latest | gzip > forestrat-mcp-backup.tar.gz

# Restore image
gunzip -c forestrat-mcp-backup.tar.gz | docker load
``` 