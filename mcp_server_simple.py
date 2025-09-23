#!/usr/bin/env python3
"""
Simple MCP Server for Databricks Integration
A simplified version that works with Python 3.8+ without requiring the MCP library
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Databricks SDK imports
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.compute import DataSecurityMode
    from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
except ImportError:
    print("Warning: databricks-sdk not installed. Install with: pip install databricks-sdk")
    WorkspaceClient = None

@dataclass
class DatabricksConfig:
    """Configuration for Databricks connection"""
    host: str
    token: str
    workspace_id: Optional[str] = None

class SimpleMCPServer:
    """Simple MCP Server for Databricks operations"""
    
    def __init__(self):
        self.client: Optional[WorkspaceClient] = None
        self.config: Optional[DatabricksConfig] = None
    
    def _load_config(self) -> DatabricksConfig:
        """Load Databricks configuration from environment variables"""
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        workspace_id = os.getenv("DATABRICKS_WORKSPACE_ID")
        
        if not host or not token:
            raise ValueError(
                "DATABRICKS_HOST and DATABRICKS_TOKEN environment variables are required"
            )
        
        return DatabricksConfig(host=host, token=token, workspace_id=workspace_id)
    
    def _ensure_client(self):
        """Ensure Databricks client is initialized"""
        if self.client is None:
            if WorkspaceClient is None:
                raise RuntimeError("Databricks SDK not installed")
            
            self.config = self._load_config()
            self.client = WorkspaceClient(
                host=self.config.host,
                token=self.config.token
            )
    
    def _send_response(self, response: Dict[str, Any]):
        """Send response to MCP client"""
        print(json.dumps(response), flush=True)
    
    def _handle_initialize(self, params: Dict[str, Any], request_id: Any) -> Dict[str, Any]:
        """Handle initialization request"""
        if request_id is None:
            request_id = 0  # Default ID for initialization
            
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "databricks-mcp",
                    "version": "1.0.0"
                }
            }
        }
    
    def _handle_list_tools(self, params: Dict[str, Any], request_id: Any) -> Dict[str, Any]:
        """Handle list tools request"""
        tools = [
            {
                "name": "databricks_test_connection",
                "description": "Test connection to Databricks workspace",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "databricks_list_clusters",
                "description": "List all Databricks clusters",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "databricks_get_cluster",
                "description": "Get details of a specific cluster",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "cluster_id": {
                            "type": "string",
                            "description": "The cluster ID to get details for"
                        }
                    },
                    "required": ["cluster_id"]
                }
            },
            {
                "name": "databricks_list_jobs",
                "description": "List all Databricks jobs",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "databricks_run_job",
                "description": "Run a Databricks job",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "job_id": {
                            "type": "string",
                            "description": "The job ID to run"
                        },
                        "parameters": {
                            "type": "object",
                            "description": "Job parameters (optional)"
                        }
                    },
                    "required": ["job_id"]
                }
            },
            {
                "name": "databricks_get_job_run",
                "description": "Get details of a job run",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "run_id": {
                            "type": "string",
                            "description": "The run ID to get details for"
                        }
                    },
                    "required": ["run_id"]
                }
            },
            {
                "name": "databricks_list_workspace",
                "description": "List workspace contents",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Workspace path to list (default: /)",
                            "default": "/"
                        }
                    },
                    "required": []
                }
            }
        ]
        
        if request_id is None:
            request_id = 1  # Default ID for list tools
            
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"tools": tools}
        }
    
    def _handle_call_tool(self, params: Dict[str, Any], request_id: Any) -> Dict[str, Any]:
        """Handle tool call request"""
        try:
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            
            self._ensure_client()
            
            if tool_name == "databricks_test_connection":
                result = self._test_connection()
            elif tool_name == "databricks_list_clusters":
                result = self._list_clusters()
            elif tool_name == "databricks_get_cluster":
                result = self._get_cluster(arguments.get("cluster_id"))
            elif tool_name == "databricks_list_jobs":
                result = self._list_jobs()
            elif tool_name == "databricks_run_job":
                result = self._run_job(arguments.get("job_id"), arguments.get("parameters"))
            elif tool_name == "databricks_get_job_run":
                result = self._get_job_run(arguments.get("run_id"))
            elif tool_name == "databricks_list_workspace":
                result = self._list_workspace(arguments.get("path", "/"))
            else:
                raise ValueError(f"Unknown tool: {tool_name}")
            
            if request_id is None:
                request_id = 2  # Default ID for tool calls
                
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": result
                        }
                    ]
                }
            }
                
        except Exception as e:
            logger.error(f"Error executing tool {params.get('name')}: {e}")
            if request_id is None:
                request_id = 3  # Default ID for errors
                
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": str(e)
                }
            }
    
    def _test_connection(self) -> str:
        """Test connection to Databricks"""
        try:
            user = self.client.current_user.me()
            return f"Successfully connected to Databricks workspace!\nHost: {self.config.host}\nUser: {user.user_name}"
        except Exception as e:
            raise RuntimeError(f"Connection failed: {str(e)}")
    
    def _list_clusters(self) -> str:
        """List all clusters"""
        try:
            clusters = list(self.client.clusters.list())
            cluster_info = []
            
            for cluster in clusters:
                cluster_info.append({
                    "cluster_id": cluster.cluster_id,
                    "cluster_name": cluster.cluster_name,
                    "state": cluster.state.value if cluster.state else "Unknown",
                    "driver_node_type": cluster.driver_node_type_id,
                    "worker_node_type": cluster.node_type_id,
                    "num_workers": cluster.num_workers,
                    "autotermination_minutes": cluster.autotermination_minutes
                })
            
            return json.dumps(cluster_info, indent=2)
        except Exception as e:
            raise RuntimeError(f"Error listing clusters: {str(e)}")
    
    def _get_cluster(self, cluster_id: str) -> str:
        """Get cluster details"""
        try:
            cluster = self.client.clusters.get(cluster_id)
            cluster_info = {
                "cluster_id": cluster.cluster_id,
                "cluster_name": cluster.cluster_name,
                "state": cluster.state.value if cluster.state else "Unknown",
                "driver_node_type": cluster.driver_node_type_id,
                "worker_node_type": cluster.node_type_id,
                "num_workers": cluster.num_workers,
                "autotermination_minutes": cluster.autotermination_minutes,
                "spark_version": cluster.spark_version,
                "created_time": cluster.start_time,
                "creator_user_name": cluster.creator_user_name
            }
            
            return json.dumps(cluster_info, indent=2)
        except Exception as e:
            raise RuntimeError(f"Error getting cluster: {str(e)}")
    
    def _list_jobs(self) -> str:
        """List all jobs"""
        try:
            jobs = list(self.client.jobs.list())
            job_info = []
            
            for job in jobs:
                job_info.append({
                    "job_id": job.job_id,
                    "name": job.settings.name,
                    "created_time": job.created_time,
                    "creator_user_name": job.creator_user_name,
                    "run_as_user_name": job.run_as_user_name,
                    "timeout_seconds": job.settings.timeout_seconds
                })
            
            return json.dumps(job_info, indent=2)
        except Exception as e:
            raise RuntimeError(f"Error listing jobs: {str(e)}")
    
    def _run_job(self, job_id: str, parameters: Optional[Dict] = None) -> str:
        """Run a job"""
        try:
            run = self.client.jobs.run_now(
                job_id=job_id,
                notebook_params=parameters or {}
            )
            
            return f"Job run started successfully!\nRun ID: {run.run_id}\nJob ID: {job_id}"
        except Exception as e:
            raise RuntimeError(f"Error running job: {str(e)}")
    
    def _get_job_run(self, run_id: str) -> str:
        """Get job run details"""
        try:
            run = self.client.jobs.get_run(run_id)
            
            run_info = {
                "run_id": run.run_id,
                "job_id": run.job_id,
                "run_name": run.run_name,
                "life_cycle_state": run.state.life_cycle_state.value,
                "result_state": run.state.result_state.value if run.state.result_state else None,
                "start_time": run.start_time,
                "setup_duration": run.setup_duration,
                "execution_duration": run.execution_duration,
                "cleanup_duration": run.cleanup_duration,
                "run_duration": run.run_duration,
                "creator_user_name": run.creator_user_name
            }
            
            return json.dumps(run_info, indent=2)
        except Exception as e:
            raise RuntimeError(f"Error getting job run: {str(e)}")
    
    def _list_workspace(self, path: str) -> str:
        """List workspace contents"""
        try:
            objects = list(self.client.workspace.list(path))
            object_info = []
            
            for obj in objects:
                object_info.append({
                    "path": obj.path,
                    "object_type": obj.object_type.value,
                    "language": obj.language.value if obj.language else None,
                    "created_at": obj.created_at
                })
            
            return json.dumps(object_info, indent=2)
        except Exception as e:
            raise RuntimeError(f"Error listing workspace: {str(e)}")
    
    def run(self):
        """Run the simple MCP server"""
        logger.info("Starting Simple Databricks MCP Server...")
        
        try:
            while True:
                line = input()
                if not line.strip():
                    continue
                
                try:
                    request = json.loads(line)
                    method = request.get("method")
                    params = request.get("params", {})
                    request_id = request.get("id")
                    
                    if method == "initialize":
                        response = self._handle_initialize(params, request_id)
                    elif method == "tools/list":
                        response = self._handle_list_tools(params, request_id)
                    elif method == "tools/call":
                        response = self._handle_call_tool(params, request_id)
                    else:
                        response = {
                            "jsonrpc": "2.0",
                            "id": request_id or 0,
                            "error": {
                                "code": -32601,
                                "message": f"Method not found: {method}"
                            }
                        }
                    
                    self._send_response(response)
                    
                except json.JSONDecodeError:
                    logger.error("Invalid JSON received")
                    continue
                except Exception as e:
                    logger.error(f"Error processing request: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Server stopped by user")
        except EOFError:
            logger.info("Server stopped - EOF received")

def main():
    """Main entry point"""
    server = SimpleMCPServer()
    server.run()

if __name__ == "__main__":
    main()
