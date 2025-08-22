import asyncio
import sys
import json
import time
import logging
from pathlib import Path
from typing import List, Dict, Any

from google.adk.agents import LlmAgent
from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset, StdioServerParameters

try:
    from data_engineering_optimizer.dataops_agent_prompt import DATAOPS_AGENT_PROMPT
except ImportError:
    from dataops_agent_prompt import DATAOPS_AGENT_PROMPT

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fix for Windows subprocess support
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

class OptimizedMCPManager:
    """Manages MCP server connections with connection pooling and sequential initialization."""
    
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.toolsets: List[MCPToolset] = []
        self.failed_servers: List[str] = []
        
    def load_config(self) -> Dict[str, Any]:
        """Load MCP server configuration."""
        with open(self.config_path, "r") as f:
            return json.load(f)["mcpServers"]
    
    async def initialize_server_sequential(self, name: str, config: Dict[str, Any]) -> MCPToolset:
        """Initialize a single MCP server with timeout handling."""
        if config.get("disabled", False):
            logger.info(f"Skipping disabled MCP Server: {name}")
            return None
        
        logger.info(f"Initializing DataOps MCP Server: {name}")
        start_time = time.time()
        
        try:
            # Create toolset with timeout
            toolset = MCPToolset(
                connection_params=StdioServerParameters(
                    command=config["command"],
                    args=config["args"],
                    env=config.get("env", {})
                )
            )
            
            # Test the connection by trying to get tools (with timeout)
            try:
                # Use configured timeout or default to 20 seconds
                timeout = config.get("timeout", 20.0)
                await asyncio.wait_for(
                    self._test_toolset_connection(toolset), 
                    timeout=timeout
                )
                
                elapsed = time.time() - start_time
                logger.info(f"✅ {name} initialized successfully in {elapsed:.2f}s")
                return toolset
                
            except asyncio.TimeoutError:
                timeout = config.get("timeout", 20.0)
                logger.warning(f"⏰ {name} initialization timed out after {timeout}s")
                self.failed_servers.append(f"{name} (timeout)")
                return None
                
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"❌ {name} failed to initialize after {elapsed:.2f}s: {e}")
            self.failed_servers.append(f"{name} (error: {str(e)[:50]})")
            return None
    
    async def _test_toolset_connection(self, toolset: MCPToolset):
        """Test if a toolset can connect and get tools."""
        try:
            # Try to get tools to verify the connection works
            tools = await toolset.get_tools()
            logger.debug(f"Toolset connection test passed, found {len(tools) if tools else 0} tools")
        except Exception as e:
            logger.debug(f"Toolset connection test failed: {e}")
            raise
    
    async def initialize_all_servers(self) -> List[MCPToolset]:
        """Initialize all enabled MCP servers sequentially."""
        logger.info("🚀 Starting DataOps MCP server initialization")
        
        mcp_config = self.load_config()
        successful_toolsets = []
        
        # Initialize servers one by one to avoid resource contention
        for name, config in mcp_config.items():
            if config.get("disabled", False):
                continue
                
            # Add a small delay between server initializations
            if successful_toolsets:
                await asyncio.sleep(2.0)  # 2 second delay between servers
            
            # Implement retry logic
            max_retries = config.get("retries", 1)
            for attempt in range(max_retries + 1):
                if attempt > 0:
                    logger.info(f"Retry attempt {attempt}/{max_retries} for {name}")
                    await asyncio.sleep(5.0)  # Wait before retry
                
                toolset = await self.initialize_server_sequential(name, config)
                if toolset:
                    successful_toolsets.append(toolset)
                    break
                elif attempt == max_retries:
                    logger.error(f"Failed to initialize {name} after {max_retries + 1} attempts")
            
            # Log progress
            logger.info(f"Progress: {len(successful_toolsets)} successful, {len(self.failed_servers)} failed")
        
        # Summary
        logger.info(f"🎯 DataOps MCP Server Initialization Complete:")
        logger.info(f"   ✅ Successful: {len(successful_toolsets)}")
        logger.info(f"   ❌ Failed: {len(self.failed_servers)}")
        
        if self.failed_servers:
            logger.warning(f"   Failed servers: {', '.join(self.failed_servers)}")
        
        self.toolsets = successful_toolsets
        return successful_toolsets

async def create_dataops_agent() -> LlmAgent:
    """Create an optimized DataOps agent with MCP pipeline management capabilities."""
    
    # Initialize database connections first
    logger.info("🔧 Initializing pipeline database connections...")
    try:
        from data_engineering_optimizer.pipeline_db_utils import PipelineDatabaseManager
        db_manager = PipelineDatabaseManager()
        stats = db_manager.get_connection_stats()
        logger.info(f"✅ Pipeline database ready: {stats}")
    except Exception as e:
        logger.warning(f"⚠️ Pipeline database initialization failed: {e}")
    
    # Initialize MCP servers
    config_path = Path(__file__).resolve().parent / "mcp_config.json"
    mcp_manager = OptimizedMCPManager(config_path)
    
    toolsets = await mcp_manager.initialize_all_servers()
    
    # Create agent with available toolsets
    logger.info("🤖 Creating DataOps Agent...")
    
    agent = LlmAgent(
        model="gemini-2.5-flash",
        name="dataops_pipeline_optimizer_agent",
        instruction=DATAOPS_AGENT_PROMPT,
        tools=toolsets if toolsets else []  # Use empty list if no tools available
    )
    
    logger.info(f"✅ DataOps Agent created with {len(toolsets)} MCP toolsets")
    
    # Log available capabilities
    if toolsets:
        logger.info("🔧 Available DataOps MCP capabilities:")
        for i, toolset in enumerate(toolsets, 1):
            logger.info(f"   {i}. {type(toolset).__name__}")
    else:
        logger.warning("⚠️ Agent created without MCP tools - will have limited functionality")
    
    return agent

class DataOpsAgent:
    """Enhanced DataOps Agent with advanced pipeline management capabilities."""
    
    def __init__(self, agent: LlmAgent):
        self.agent = agent
        self.monitoring_active = False
        self.optimization_history = []
        
    async def start_continuous_monitoring(self, monitoring_interval_minutes: int = 10):
        """Start continuous pipeline performance monitoring."""
        self.monitoring_active = True
        logger.info(f"🔍 Starting continuous monitoring (interval: {monitoring_interval_minutes} minutes)")
        
        while self.monitoring_active:
            try:
                # Trigger performance analysis
                monitoring_result = await self.agent.run_async(
                    "Perform routine pipeline performance monitoring. Check for degradation and optimization opportunities."
                )
                
                logger.info(f"Monitoring cycle completed: {monitoring_result}")
                
                # Wait for next monitoring cycle
                await asyncio.sleep(monitoring_interval_minutes * 60)
                
            except Exception as e:
                logger.error(f"Error in monitoring cycle: {e}")
                await asyncio.sleep(60)  # Short wait on error
    
    def stop_monitoring(self):
        """Stop continuous monitoring."""
        self.monitoring_active = False
        logger.info("🛑 Stopping continuous monitoring")
    
    async def handle_emergency_optimization(self, pipeline_id: str, severity: str = "HIGH"):
        """Handle emergency pipeline optimization scenarios."""
        logger.warning(f"🚨 Emergency optimization triggered for pipeline {pipeline_id} (severity: {severity})")
        
        emergency_prompt = f"""
        EMERGENCY OPTIMIZATION REQUIRED:
        Pipeline: {pipeline_id}
        Severity: {severity}
        
        Execute immediate emergency procedures:
        1. Assess current pipeline status and performance degradation
        2. Identify root cause and affected downstream systems
        3. Execute emergency rerouting if available
        4. Implement immediate optimization actions
        5. Notify stakeholders and create incident tracking
        6. Provide detailed incident report with recovery plan
        
        Respond in structured JSON format with emergency actions taken.
        """
        
        try:
            result = await self.agent.run_async(emergency_prompt)
            self.optimization_history.append({
                "timestamp": time.time(),
                "type": "emergency",
                "pipeline_id": pipeline_id,
                "severity": severity,
                "result": result
            })
            
            logger.info(f"Emergency optimization completed for {pipeline_id}")
            return result
            
        except Exception as e:
            logger.error(f"Emergency optimization failed for {pipeline_id}: {e}")
            raise
    
    async def generate_optimization_report(self, days_back: int = 7) -> Dict[str, Any]:
        """Generate comprehensive optimization report."""
        report_prompt = f"""
        Generate a comprehensive DataOps optimization report for the last {days_back} days:
        
        1. Pipeline Performance Summary
        2. Optimization Actions Taken
        3. Resource Utilization Trends
        4. Cost Impact Analysis
        5. SLA Compliance Metrics
        6. Recommendations for Next Period
        
        Include specific metrics, trends, and actionable insights.
        Format as structured JSON with executive summary.
        """
        
        try:
            report = await self.agent.run_async(report_prompt)
            
            # Add optimization history from this agent
            report_data = {
                "report_period_days": days_back,
                "generated_at": time.time(),
                "agent_optimization_history": self.optimization_history[-50:],  # Last 50 actions
                "llm_generated_report": report
            }
            
            return report_data
            
        except Exception as e:
            logger.error(f"Failed to generate optimization report: {e}")
            return {"error": str(e), "generated_at": time.time()}

# Create the optimized DataOps agent
async def main():
    """Main function to create and test the DataOps agent."""
    try:
        base_agent = await create_dataops_agent()
        dataops_agent = DataOpsAgent(base_agent)
        
        logger.info("🎉 DataOps Agent ready for pipeline optimization!")
        return dataops_agent
    except Exception as e:
        logger.error(f"❌ Failed to create DataOps agent: {e}")
        raise

# For compatibility with the existing agent interface
if __name__ == "__main__":
    # If run directly, create and test the agent
    asyncio.run(main())
else:
    # If imported, create the agent synchronously for ADK
    try:
        # Create event loop if none exists
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Create the DataOps agent
        root_agent = loop.run_until_complete(create_dataops_agent())
        
    except Exception as e:
        logger.error(f"❌ Failed to create DataOps agent during import: {e}")
        # Fallback to basic agent without MCP tools
        root_agent = LlmAgent(
            model="gemini-2.5-flash",
            name="fallback_dataops_agent",
            instruction=DATAOPS_AGENT_PROMPT,
            tools=[]
        )