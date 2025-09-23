# Databricks MCP Server  - A Learning Implementation

This MCP (Model Context Protocol) server provides tools for interacting with Databricks clusters, jobs, and workspace operations. Built as a learning exercise to understand the MCP protocol with minimal dependencies, this implementation manually handles the protocol specification rather than relying on the official MCP SDK.

The server demonstrates how to implement MCP protocol compliance from scratch, making it an educational example for understanding the underlying protocol mechanics while delivering practical Databricks functionality. 

It was tested on my macbook with claude desktop (0.13.19) and my personal databricks account. See a screenshot of this mcp server working: I was able to list my workspace sub folders correctly via claude desktop -> this databricks mcp server -> my databricks worksapce. `Screenshot 2025-09-22 at 8.04.05 PM.png` 

## Features
**Full compatibility with MCP clients using protocol version 2024-11-05

**Lightweight architecture with direct JSON-RPC 2.0 implementation

**Python 3.8+ compatibility for broader environment support

**Comprehensive Databricks integration without external MCP libraries

This MCP server provides the following tools:

- **databricks_test_connection**: Test connection to Databricks workspace
- **databricks_list_clusters**: List all clusters in the workspace
- **databricks_get_cluster**: Get detailed information about a specific cluster
- **databricks_list_jobs**: List all jobs in the workspace
- **databricks_run_job**: Run a Databricks job with optional parameters
- **databricks_get_job_run**: Get details about a specific job run
- **databricks_execute_sql**: Execute SQL queries (placeholder implementation)
- **databricks_list_workspace**: List contents of the Databricks workspace

## Prerequisites

1. Python 3.8 or higher
2. A Databricks workspace with appropriate permissions
3. A Databricks Personal Access Token

## Installation

1. Clone or download this repository
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```


## Configuration

1. Copy the example configuration file:
```bash
cp config.example.env .env
```

2. Edit the `.env` file with your Databricks credentials:
```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your_personal_access_token_here
DATABRICKS_WORKSPACE_ID=your_workspace_id_here
```

### Getting Your Databricks Credentials

1. **Host URL**: Your Databricks workspace URL (e.g., `https://adb-1234567890123456.7.azuredatabricks.net`)
2. **Personal Access Token**: 
   - Go to User Settings > Developer > Access Tokens
   - Click "Generate new token"
   - Give it a name and expiration date
   - Copy the generated token
3. **Workspace ID** (optional): Usually not needed for personal access tokens

## Usage

### Running the MCP Server

Run the simple server directly:
```bash
python mcp_server_simple.py
```


### Using with MCP Clients

The server can be used with any MCP-compatible host. Here's an example configuration for Claude Desktop:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "python",
      "args": ["/path/to/mcp_server_simple.py"],
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_TOKEN": "your_personal_access_token_here",
        "DATABRICKS_WORKSPACE_ID": "your_workspace_id"
      }
    }
  }
}
```

### Available Tools

#### Test Connection
```json
{
  "name": "databricks_test_connection",
  "arguments": {}
}
```

#### List Clusters
```json
{
  "name": "databricks_list_clusters",
  "arguments": {}
}
```

#### Get Cluster Details
```json
{
  "name": "databricks_get_cluster",
  "arguments": {
    "cluster_id": "1234-567890-abcdef"
  }
}
```

#### List Jobs
```json
{
  "name": "databricks_list_jobs",
  "arguments": {}
}
```

#### Run Job
```json
{
  "name": "databricks_run_job",
  "arguments": {
    "job_id": "1234567890",
    "parameters": {
      "param1": "value1",
      "param2": "value2"
    }
  }
}
```

#### Get Job Run Details
```json
{
  "name": "databricks_get_job_run",
  "arguments": {
    "run_id": "1234567890"
  }
}
```

#### List Workspace Contents
```json
{
  "name": "databricks_list_workspace",
  "arguments": {
    "path": "/Users/your-email@domain.com"
  }
}
```

## Security Considerations

- Never commit your `.env` file to version control
- Use environment variables for production deployments
- Regularly rotate your Databricks Personal Access Tokens
- Ensure your token has minimal required permissions

## Troubleshooting

### Common Issues

1. **Connection Failed**: 
   - Verify your `DATABRICKS_HOST` URL is correct
   - Check that your Personal Access Token is valid and not expired
   - Ensure your workspace allows API access

2. **Permission Denied**:
   - Verify your token has appropriate permissions
   - Check that you have access to the clusters/jobs you're trying to interact with

3. **Import Errors**:
   - Make sure all dependencies are installed: `pip install -r requirements.txt`
   - Check Python version compatibility

### Debug Mode

To enable debug logging, set the environment variable:
```bash
export PYTHONPATH=.
export LOG_LEVEL=DEBUG
python mcp_server_simple.py
```

### Common Issues

#### Python Path Issues (spawn python ENOENT)

If you get `spawn python ENOENT` errors in Claude Desktop, it means Claude Desktop can't find the `python` command. This commonly happens with conda/virtual environments.

**Solution**: Use the full path to your Python executable in the Claude Desktop config:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "/full/path/to/your/python",
      "args": ["/path/to/mcp_server_simple.py"],
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_TOKEN": "your_personal_access_token_here"
      }
    }
  }
}
```

Find your Python path with:
```bash
which python
# or
which python3
```

## Contributing

Feel free to submit issues and enhancement requests! contact nyang63@gmail.com

## License

You are free to use it any way you would like to, but I do not assume any and all responsibility for any adverse effect.
