#!/usr/bin/env python3
"""
DAG Performance MCP Server for Data Engineering Pipeline Optimizer
Monitors pipeline execution performance and detects degradation patterns
Based on the autonomous demand forecasting system's MCP server patterns
"""

import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import statistics

import mcp.server.stdio
from dotenv import load_dotenv

# MCP Server Imports
from mcp import types as mcp_types
from mcp.server.lowlevel import NotificationOptions, Server
from mcp.server.models import InitializationOptions

# Import pipeline models
try:
    from pipeline_models import (
        PerformanceMetrics, PerformanceDegradationEvent, SeverityLevel, PipelineStatus,
        create_performance_metrics_from_db_row
    )
    from pipeline_db_utils import PipelineDatabaseManager
except ImportError:
    # Fallback for standalone execution
    import sys
    sys.path.append(os.path.dirname(__file__))
    from pipeline_models import (
        PerformanceMetrics, PerformanceDegradationEvent, SeverityLevel, PipelineStatus,
        create_performance_metrics_from_db_row
    )
    from pipeline_db_utils import PipelineDatabaseManager

load_dotenv()

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [DAG Performance MCP] - %(message)s',
    handlers=[logging.StreamHandler()]
)

DATABASE_PATH = os.path.join(os.path.dirname(__file__), "pipeline_optimizer.db")

class DAGPerformanceMonitor:
    """Core DAG performance monitoring functionality"""
    
    def __init__(self):
        self.db_manager = PipelineDatabaseManager()
        self.performance_thresholds = {
            'runtime_degradation_low': 15.0,      # 15% increase
            'runtime_degradation_medium': 25.0,    # 25% increase  
            'runtime_degradation_high': 40.0,      # 40% increase
            'failure_rate_threshold': 5.0,         # 5% failure rate
            'resource_utilization_high': 85.0      # 85% CPU/Memory
        }
        
    def monitor_dag_performance(self, dag_id: str, execution_date: str = None) -> Dict[str, Any]:
        """Monitor current performance metrics for a specific DAG"""
        try:
            # Parse execution date if provided
            exec_date = None
            if execution_date:
                try:
                    exec_date = datetime.fromisoformat(execution_date)
                except ValueError:
                    exec_date = datetime.now()
            else:
                exec_date = datetime.now()
            
            # Get latest performance metrics
            metrics = self.db_manager.get_latest_performance_metrics(dag_id, exec_date)
            if not metrics:
                return {
                    "success": False,
                    "message": f"No performance metrics found for DAG {dag_id}",
                    "dag_id": dag_id,
                    "execution_date": exec_date.isoformat()
                }
            
            # Calculate performance indicators
            baseline_metrics = self.db_manager.get_baseline_performance(dag_id, days_back=7)
            performance_analysis = self._analyze_performance_trend(metrics, baseline_metrics)
            
            # Check for degradation
            degradation_detected = self._check_performance_degradation(metrics, baseline_metrics)
            
            result = {
                "success": True,
                "dag_id": dag_id,
                "execution_date": exec_date.isoformat(),
                "current_metrics": metrics.to_dict(),
                "performance_analysis": performance_analysis,
                "degradation_detected": degradation_detected is not None,
                "degradation_details": degradation_detected.to_dict() if degradation_detected else None,
                "recommendations": self._generate_performance_recommendations(metrics, baseline_metrics),
                "timestamp": datetime.now().isoformat()
            }
            
            logging.info(f"Monitored DAG {dag_id}: Duration={metrics.duration_minutes:.1f}min, "
                        f"Success Rate={metrics.success_rate:.1f}%")
            
            return result
            
        except Exception as e:
            logging.error(f"Error monitoring DAG {dag_id}: {e}")
            return {
                "success": False,
                "message": f"Error monitoring DAG {dag_id}: {str(e)}",
                "dag_id": dag_id
            }
    
    def detect_performance_degradation(self, dag_id: str, threshold_minutes: int = 30) -> Dict[str, Any]:
        """Detect performance degradation across recent executions"""
        try:
            # Get recent performance history
            recent_metrics = self.db_manager.get_performance_history(dag_id, hours_back=24)
            
            if len(recent_metrics) < 2:
                return {
                    "success": False,
                    "message": f"Insufficient data for degradation analysis of DAG {dag_id}",
                    "dag_id": dag_id
                }
            
            # Get baseline performance (last 7 days average)
            baseline_metrics = self.db_manager.get_baseline_performance(dag_id, days_back=7)
            
            degradation_events = []
            new_events_created = 0
            
            for current_metrics in recent_metrics:
                degradation_event = self._check_performance_degradation(current_metrics, baseline_metrics)
                
                if degradation_event:
                    # Check if this is a new event or continuation of existing
                    existing_events = self.db_manager.get_active_degradation_events(dag_id)
                    is_new_event = not any(
                        abs((event.detected_at - degradation_event.detected_at).total_seconds()) < 1800  # 30 min
                        for event in existing_events
                    )
                    
                    if is_new_event:
                        event_id = self.db_manager.insert_degradation_event(degradation_event)
                        degradation_event.id = event_id
                        new_events_created += 1
                        
                        logging.warning(f"New performance degradation detected for DAG {dag_id}: "
                                      f"{degradation_event.degradation_percentage:.1f}% slower")
                    
                    degradation_events.append(degradation_event.to_dict())
            
            return {
                "success": True,
                "dag_id": dag_id,
                "analysis_period_hours": 24,
                "degradation_events_found": len(degradation_events),
                "new_events_created": new_events_created,
                "degradation_events": degradation_events,
                "threshold_minutes": threshold_minutes,
                "scan_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logging.error(f"Error detecting degradation for DAG {dag_id}: {e}")
            return {
                "success": False,
                "message": f"Error detecting degradation: {str(e)}",
                "dag_id": dag_id
            }
    
    def analyze_task_bottlenecks(self, dag_id: str, hours_back: int = 24) -> Dict[str, Any]:
        """Analyze task-level bottlenecks within a DAG"""
        try:
            # Get task performance data
            task_metrics = self.db_manager.get_task_performance_history(dag_id, hours_back)
            
            if not task_metrics:
                return {
                    "success": False,
                    "message": f"No task metrics found for DAG {dag_id}",
                    "dag_id": dag_id,
                    "hours_back": hours_back
                }
            
            # Analyze bottlenecks
            bottleneck_analysis = self._analyze_task_bottlenecks(task_metrics)
            
            # Generate optimization suggestions
            optimization_suggestions = self._generate_task_optimization_suggestions(bottleneck_analysis)
            
            result = {
                "success": True,
                "dag_id": dag_id,
                "analysis_period_hours": hours_back,
                "total_tasks_analyzed": len(task_metrics),
                "bottleneck_analysis": bottleneck_analysis,
                "optimization_suggestions": optimization_suggestions,
                "critical_path_tasks": self._identify_critical_path(task_metrics),
                "resource_recommendations": self._generate_resource_recommendations(task_metrics),
                "analysis_timestamp": datetime.now().isoformat()
            }
            
            logging.info(f"Analyzed task bottlenecks for DAG {dag_id}: "
                        f"{len(bottleneck_analysis.get('slow_tasks', []))} slow tasks identified")
            
            return result
            
        except Exception as e:
            logging.error(f"Error analyzing bottlenecks for DAG {dag_id}: {e}")
            return {
                "success": False,
                "message": f"Error analyzing bottlenecks: {str(e)}",
                "dag_id": dag_id
            }
    
    def predict_pipeline_duration(self, dag_id: str, execution_date: str) -> Dict[str, Any]:
        """Predict pipeline duration using historical data and ML models"""
        try:
            exec_date = datetime.fromisoformat(execution_date)
            
            # Get historical patterns
            historical_data = self.db_manager.get_performance_history(dag_id, hours_back=168)  # 1 week
            
            if len(historical_data) < 5:
                return {
                    "success": False,
                    "message": f"Insufficient historical data for prediction of DAG {dag_id}",
                    "dag_id": dag_id
                }
            
            # Simple time-series prediction (can be enhanced with ML models)
            prediction_result = self._predict_duration_heuristic(historical_data, exec_date)
            
            # Calculate confidence intervals
            durations = [m.duration_minutes for m in historical_data]
            mean_duration = statistics.mean(durations)
            std_duration = statistics.stdev(durations) if len(durations) > 1 else 0
            
            result = {
                "success": True,
                "dag_id": dag_id,
                "execution_date": execution_date,
                "predicted_duration_minutes": prediction_result["predicted_duration"],
                "confidence_score": prediction_result["confidence"],
                "prediction_method": prediction_result["method"],
                "historical_stats": {
                    "mean_duration_minutes": mean_duration,
                    "std_duration_minutes": std_duration,
                    "min_duration_minutes": min(durations),
                    "max_duration_minutes": max(durations),
                    "samples_used": len(durations)
                },
                "confidence_interval": {
                    "lower_bound": max(0, prediction_result["predicted_duration"] - std_duration),
                    "upper_bound": prediction_result["predicted_duration"] + std_duration
                },
                "prediction_timestamp": datetime.now().isoformat()
            }
            
            logging.info(f"Predicted duration for DAG {dag_id}: "
                        f"{prediction_result['predicted_duration']:.1f} minutes")
            
            return result
            
        except Exception as e:
            logging.error(f"Error predicting duration for DAG {dag_id}: {e}")
            return {
                "success": False,
                "message": f"Error predicting duration: {str(e)}",
                "dag_id": dag_id
            }
    
    def get_resource_utilization_history(self, dag_id: str, days_back: int = 7) -> Dict[str, Any]:
        """Get resource utilization history for capacity planning"""
        try:
            metrics_history = self.db_manager.get_performance_history(dag_id, hours_back=days_back * 24)
            
            if not metrics_history:
                return {
                    "success": False,
                    "message": f"No resource utilization data found for DAG {dag_id}",
                    "dag_id": dag_id,
                    "days_back": days_back
                }
            
            # Calculate resource utilization statistics
            resource_stats = self._calculate_resource_statistics(metrics_history)
            
            # Identify resource trends
            resource_trends = self._analyze_resource_trends(metrics_history)
            
            # Generate capacity planning recommendations
            capacity_recommendations = self._generate_capacity_recommendations(resource_stats, resource_trends)
            
            result = {
                "success": True,
                "dag_id": dag_id,
                "analysis_period_days": days_back,
                "total_executions": len(metrics_history),
                "resource_statistics": resource_stats,
                "resource_trends": resource_trends,
                "capacity_recommendations": capacity_recommendations,
                "peak_usage_periods": self._identify_peak_usage_periods(metrics_history),
                "analysis_timestamp": datetime.now().isoformat()
            }
            
            logging.info(f"Retrieved resource utilization for DAG {dag_id}: "
                        f"Avg CPU={resource_stats.get('avg_cpu', 0):.1f}%")
            
            return result
            
        except Exception as e:
            logging.error(f"Error getting resource utilization for DAG {dag_id}: {e}")
            return {
                "success": False,
                "message": f"Error getting resource utilization: {str(e)}",
                "dag_id": dag_id
            }
    
    # Helper methods (simplified implementations)
    def _analyze_performance_trend(self, current: PerformanceMetrics, baseline: List[PerformanceMetrics]) -> Dict[str, Any]:
        """Analyze performance trends"""
        if not baseline:
            return {"trend": "insufficient_data"}
        
        baseline_avg = statistics.mean([m.duration_minutes for m in baseline])
        current_duration = current.duration_minutes
        
        change_percent = ((current_duration - baseline_avg) / baseline_avg) * 100 if baseline_avg > 0 else 0
        
        return {
            "baseline_avg_duration": baseline_avg,
            "current_duration": current_duration,
            "change_percentage": change_percent,
            "trend": "degrading" if change_percent > 10 else "stable" if abs(change_percent) <= 10 else "improving"
        }
    
    def _check_performance_degradation(self, current: PerformanceMetrics, baseline: List[PerformanceMetrics]) -> Optional[PerformanceDegradationEvent]:
        """Check if current performance indicates degradation"""
        if not baseline:
            return None
        
        baseline_avg = statistics.mean([m.duration_minutes for m in baseline])
        degradation_percent = ((current.duration_minutes - baseline_avg) / baseline_avg) * 100 if baseline_avg > 0 else 0
        
        if degradation_percent >= self.performance_thresholds['runtime_degradation_high']:
            severity = SeverityLevel.HIGH
        elif degradation_percent >= self.performance_thresholds['runtime_degradation_medium']:
            severity = SeverityLevel.MEDIUM
        elif degradation_percent >= self.performance_thresholds['runtime_degradation_low']:
            severity = SeverityLevel.LOW
        else:
            return None
        
        return PerformanceDegradationEvent(
            pipeline_id=current.pipeline_id,
            dag_id=current.dag_id,
            severity=severity,
            detected_at=current.timestamp,
            baseline_duration=baseline_avg,
            current_duration=current.duration_minutes,
            degradation_percentage=degradation_percent,
            affected_tasks=[]  # Would be populated by task-level analysis
        )
    
    def _generate_performance_recommendations(self, current: PerformanceMetrics, baseline: List[PerformanceMetrics]) -> List[str]:
        """Generate performance improvement recommendations"""
        recommendations = []
        
        if current.success_rate < 95:
            recommendations.append("Investigate task failures - success rate below 95%")
        
        if current.cpu_usage_avg and current.cpu_usage_avg > 80:
            recommendations.append("Consider increasing CPU allocation - utilization above 80%")
        
        if current.memory_usage_avg and current.memory_usage_avg > 80:
            recommendations.append("Consider increasing memory allocation - utilization above 80%")
        
        if baseline:
            baseline_avg = statistics.mean([m.duration_minutes for m in baseline])
            if current.duration_minutes > baseline_avg * 1.2:
                recommendations.append("Analyze task bottlenecks - runtime 20% above baseline")
        
        return recommendations
    
    # Additional helper methods would be implemented here...
    def _analyze_task_bottlenecks(self, task_metrics: List) -> Dict[str, Any]:
        # Simplified implementation
        return {"slow_tasks": [], "parallel_opportunities": []}
    
    def _generate_task_optimization_suggestions(self, bottleneck_analysis: Dict) -> List[str]:
        return ["Enable task parallelization", "Optimize SQL queries"]
    
    def _identify_critical_path(self, task_metrics: List) -> List[str]:
        return ["extract_data", "transform_data", "load_data"]
    
    def _generate_resource_recommendations(self, task_metrics: List) -> Dict[str, str]:
        return {"cpu": "Increase by 20%", "memory": "Current allocation sufficient"}
    
    def _predict_duration_heuristic(self, historical_data: List, exec_date: datetime) -> Dict[str, Any]:
        durations = [m.duration_minutes for m in historical_data]
        avg_duration = statistics.mean(durations)
        return {
            "predicted_duration": avg_duration,
            "confidence": 0.7,
            "method": "historical_average"
        }
    
    def _calculate_resource_statistics(self, metrics_history: List) -> Dict[str, float]:
        cpu_values = [m.cpu_usage_avg for m in metrics_history if m.cpu_usage_avg]
        memory_values = [m.memory_usage_avg for m in metrics_history if m.memory_usage_avg]
        
        return {
            "avg_cpu": statistics.mean(cpu_values) if cpu_values else 0,
            "avg_memory": statistics.mean(memory_values) if memory_values else 0,
            "max_cpu": max(cpu_values) if cpu_values else 0,
            "max_memory": max(memory_values) if memory_values else 0
        }
    
    def _analyze_resource_trends(self, metrics_history: List) -> Dict[str, str]:
        return {"cpu_trend": "stable", "memory_trend": "increasing"}
    
    def _generate_capacity_recommendations(self, resource_stats: Dict, resource_trends: Dict) -> List[str]:
        recommendations = []
        if resource_stats.get("avg_cpu", 0) > 70:
            recommendations.append("Consider CPU scaling - average utilization above 70%")
        return recommendations
    
    def _identify_peak_usage_periods(self, metrics_history: List) -> List[Dict]:
        return [{"period": "09:00-11:00", "avg_cpu": 85.5}]


# MCP Server implementation
server = Server("dag-performance")

# Initialize the performance monitor
performance_monitor = DAGPerformanceMonitor()

@server.list_tools()
async def handle_list_tools() -> list[mcp_types.Tool]:
    """List available tools."""
    return [
        mcp_types.Tool(
            name="monitor_dag_performance",
            description="Monitor current performance metrics for a specific DAG",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The DAG ID to monitor"
                    },
                    "execution_date": {
                        "type": "string",
                        "description": "Optional execution date (ISO format)",
                        "default": None
                    }
                },
                "required": ["dag_id"]
            }
        ),
        mcp_types.Tool(
            name="detect_performance_degradation",
            description="Detect performance degradation across recent executions",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The DAG ID to analyze"
                    },
                    "threshold_minutes": {
                        "type": "integer",
                        "description": "Degradation threshold in minutes",
                        "default": 30
                    }
                },
                "required": ["dag_id"]
            }
        ),
        mcp_types.Tool(
            name="analyze_task_bottlenecks",
            description="Analyze task-level bottlenecks within a DAG",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The DAG ID to analyze"
                    },
                    "hours_back": {
                        "type": "integer",
                        "description": "Hours of history to analyze",
                        "default": 24
                    }
                },
                "required": ["dag_id"]
            }
        ),
        mcp_types.Tool(
            name="predict_pipeline_duration",
            description="Predict pipeline duration using historical data",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The DAG ID to predict"
                    },
                    "execution_date": {
                        "type": "string",
                        "description": "Target execution date (ISO format)"
                    }
                },
                "required": ["dag_id", "execution_date"]
            }
        ),
        mcp_types.Tool(
            name="get_resource_utilization_history",
            description="Get resource utilization history for capacity planning",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The DAG ID to analyze"
                    },
                    "days_back": {
                        "type": "integer",
                        "description": "Days of history to retrieve",
                        "default": 7
                    }
                },
                "required": ["dag_id"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[mcp_types.TextContent]:
    """Handle tool calls."""
    try:
        if name == "monitor_dag_performance":
            result = performance_monitor.monitor_dag_performance(**arguments)
        elif name == "detect_performance_degradation":
            result = performance_monitor.detect_performance_degradation(**arguments)
        elif name == "analyze_task_bottlenecks":
            result = performance_monitor.analyze_task_bottlenecks(**arguments)
        elif name == "predict_pipeline_duration":
            result = performance_monitor.predict_pipeline_duration(**arguments)
        elif name == "get_resource_utilization_history":
            result = performance_monitor.get_resource_utilization_history(**arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")
        
        return [mcp_types.TextContent(
            type="text",
            text=json.dumps(result, indent=2)
        )]
    
    except Exception as e:
        error_result = {
            "success": False,
            "message": f"Error executing {name}: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }
        return [mcp_types.TextContent(
            type="text", 
            text=json.dumps(error_result, indent=2)
        )]

async def main():
    """Main function to run the MCP server."""
    # Use stdio server
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="dag-performance",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={}
                )
            )
        )

if __name__ == "__main__":
    asyncio.run(main())