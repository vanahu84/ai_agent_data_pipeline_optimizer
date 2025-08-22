"""
Core data models for Data Engineering Pipeline Optimizer system.
Based on the autonomous demand forecasting system's model patterns.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
import json


class PipelineStatus(Enum):
    """Pipeline execution status."""
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    RETRYING = "RETRYING"
    QUEUED = "QUEUED"


class SeverityLevel(Enum):
    """Performance degradation severity levels."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class OptimizationType(Enum):
    """Types of pipeline optimizations."""
    DAG_RESTRUCTURE = "DAG_RESTRUCTURE"
    RESOURCE_SCALING = "RESOURCE_SCALING"
    TASK_PARALLELIZATION = "TASK_PARALLELIZATION"
    CACHING_OPTIMIZATION = "CACHING_OPTIMIZATION"
    DEPENDENCY_OPTIMIZATION = "DEPENDENCY_OPTIMIZATION"


class DataQualityStatus(Enum):
    """Data quality check status."""
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"
    PENDING = "PENDING"


@dataclass
class Pipeline:
    """Pipeline definition model."""
    id: str
    name: str
    dag_id: str
    description: Optional[str] = None
    owner: Optional[str] = None
    schedule_interval: Optional[str] = None
    start_date: Optional[datetime] = None
    tags: List[str] = field(default_factory=list)
    max_active_runs: int = 1
    catchup: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class PipelineRun:
    """Pipeline execution run model."""
    id: str
    pipeline_id: str
    dag_id: str
    execution_date: datetime
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    status: PipelineStatus = PipelineStatus.QUEUED
    duration_seconds: Optional[int] = None
    log_url: Optional[str] = None
    error_message: Optional[str] = None
    
    @property
    def duration_minutes(self) -> Optional[float]:
        """Calculate duration in minutes."""
        if self.duration_seconds:
            return self.duration_seconds / 60.0
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'id': self.id,
            'pipeline_id': self.pipeline_id,
            'dag_id': self.dag_id,
            'execution_date': self.execution_date.isoformat(),
            'start_date': self.start_date.isoformat() if self.start_date else None,
            'end_date': self.end_date.isoformat() if self.end_date else None,
            'status': self.status.value,
            'duration_seconds': self.duration_seconds,
            'duration_minutes': self.duration_minutes,
            'log_url': self.log_url,
            'error_message': self.error_message
        }


@dataclass
class PerformanceMetrics:
    """Pipeline performance metrics model."""
    pipeline_id: str
    dag_id: str
    timestamp: datetime
    execution_date: datetime
    duration_seconds: int
    task_count: int
    failed_task_count: int = 0
    retried_task_count: int = 0
    cpu_usage_avg: Optional[float] = None
    memory_usage_avg: Optional[float] = None
    disk_io_mb: Optional[float] = None
    network_io_mb: Optional[float] = None
    
    @property
    def success_rate(self) -> float:
        """Calculate task success rate."""
        if self.task_count == 0:
            return 100.0
        return ((self.task_count - self.failed_task_count) / self.task_count) * 100
    
    @property
    def duration_minutes(self) -> float:
        """Duration in minutes."""
        return self.duration_seconds / 60.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'pipeline_id': self.pipeline_id,
            'dag_id': self.dag_id,
            'timestamp': self.timestamp.isoformat(),
            'execution_date': self.execution_date.isoformat(),
            'duration_seconds': self.duration_seconds,
            'duration_minutes': self.duration_minutes,
            'task_count': self.task_count,
            'failed_task_count': self.failed_task_count,
            'retried_task_count': self.retried_task_count,
            'success_rate': self.success_rate,
            'cpu_usage_avg': self.cpu_usage_avg,
            'memory_usage_avg': self.memory_usage_avg,
            'disk_io_mb': self.disk_io_mb,
            'network_io_mb': self.network_io_mb
        }


@dataclass
class PerformanceDegradationEvent:
    """Performance degradation event model."""
    pipeline_id: str
    dag_id: str
    severity: SeverityLevel
    detected_at: datetime
    baseline_duration: float  # minutes
    current_duration: float   # minutes
    degradation_percentage: float
    affected_tasks: List[str] = field(default_factory=list)
    root_cause_analysis: Optional[Dict[str, Any]] = None
    resolved_at: Optional[datetime] = None
    id: Optional[int] = None
    
    @property
    def is_resolved(self) -> bool:
        return self.resolved_at is not None
    
    @property
    def duration_impact_minutes(self) -> float:
        return self.current_duration - self.baseline_duration
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'id': self.id,
            'pipeline_id': self.pipeline_id,
            'dag_id': self.dag_id,
            'severity': self.severity.value,
            'detected_at': self.detected_at.isoformat(),
            'baseline_duration': self.baseline_duration,
            'current_duration': self.current_duration,
            'degradation_percentage': self.degradation_percentage,
            'duration_impact_minutes': self.duration_impact_minutes,
            'affected_tasks': self.affected_tasks,
            'root_cause_analysis': self.root_cause_analysis,
            'resolved_at': self.resolved_at.isoformat() if self.resolved_at else None,
            'is_resolved': self.is_resolved
        }


@dataclass
class DataQualityMetrics:
    """Data quality metrics model."""
    dataset_id: str
    pipeline_id: str
    timestamp: datetime
    completeness_score: float  # 0-100
    accuracy_score: Optional[float] = None
    consistency_score: Optional[float] = None
    timeliness_score: Optional[float] = None
    validity_score: Optional[float] = None
    uniqueness_score: Optional[float] = None
    row_count: Optional[int] = None
    null_percentage: Optional[float] = None
    duplicate_percentage: Optional[float] = None
    schema_changes: List[Dict[str, Any]] = field(default_factory=list)
    
    @property
    def overall_quality_score(self) -> float:
        """Calculate overall data quality score."""
        scores = [score for score in [
            self.completeness_score,
            self.accuracy_score,
            self.consistency_score,
            self.timeliness_score,
            self.validity_score,
            self.uniqueness_score
        ] if score is not None]
        
        return sum(scores) / len(scores) if scores else 0.0
    
    @property
    def quality_status(self) -> DataQualityStatus:
        """Determine quality status based on score."""
        score = self.overall_quality_score
        if score >= 95:
            return DataQualityStatus.PASSED
        elif score >= 85:
            return DataQualityStatus.WARNING
        else:
            return DataQualityStatus.FAILED
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'dataset_id': self.dataset_id,
            'pipeline_id': self.pipeline_id,
            'timestamp': self.timestamp.isoformat(),
            'completeness_score': self.completeness_score,
            'accuracy_score': self.accuracy_score,
            'consistency_score': self.consistency_score,
            'timeliness_score': self.timeliness_score,
            'validity_score': self.validity_score,
            'uniqueness_score': self.uniqueness_score,
            'overall_quality_score': self.overall_quality_score,
            'quality_status': self.quality_status.value,
            'row_count': self.row_count,
            'null_percentage': self.null_percentage,
            'duplicate_percentage': self.duplicate_percentage,
            'schema_changes': self.schema_changes
        }


@dataclass
class OptimizationAction:
    """Pipeline optimization action model."""
    id: str
    pipeline_id: str
    optimization_type: OptimizationType
    description: str
    target_improvement: str
    estimated_duration_minutes: int
    created_at: datetime
    executed_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: PipelineStatus = PipelineStatus.QUEUED
    results: Optional[Dict[str, Any]] = None
    rollback_plan: Optional[Dict[str, Any]] = None
    
    @property
    def execution_duration_minutes(self) -> Optional[float]:
        """Calculate execution duration in minutes."""
        if self.executed_at and self.completed_at:
            return (self.completed_at - self.executed_at).total_seconds() / 60.0
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'id': self.id,
            'pipeline_id': self.pipeline_id,
            'optimization_type': self.optimization_type.value,
            'description': self.description,
            'target_improvement': self.target_improvement,
            'estimated_duration_minutes': self.estimated_duration_minutes,
            'created_at': self.created_at.isoformat(),
            'executed_at': self.executed_at.isoformat() if self.executed_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'status': self.status.value,
            'execution_duration_minutes': self.execution_duration_minutes,
            'results': self.results,
            'rollback_plan': self.rollback_plan
        }


# Helper functions for creating models from database rows
def create_pipeline_from_db_row(row) -> Pipeline:
    """Create Pipeline instance from database row."""
    return Pipeline(
        id=row['id'],
        name=row['name'],
        dag_id=row['dag_id'],
        description=row['description'],
        owner=row['owner'],
        schedule_interval=row['schedule_interval'],
        start_date=datetime.fromisoformat(row['start_date']) if row['start_date'] else None,
        tags=json.loads(row['tags']) if row['tags'] else [],
        max_active_runs=row['max_active_runs'] or 1,
        catchup=bool(row['catchup']),
        created_at=datetime.fromisoformat(row['created_at']) if row['created_at'] else None,
        updated_at=datetime.fromisoformat(row['updated_at']) if row['updated_at'] else None
    )


def create_performance_metrics_from_db_row(row) -> PerformanceMetrics:
    """Create PerformanceMetrics instance from database row."""
    return PerformanceMetrics(
        pipeline_id=row['pipeline_id'],
        dag_id=row['dag_id'],
        timestamp=datetime.fromisoformat(row['timestamp']),
        execution_date=datetime.fromisoformat(row['execution_date']),
        duration_seconds=row['duration_seconds'],
        task_count=row['task_count'],
        failed_task_count=row['failed_task_count'] or 0,
        retried_task_count=row['retried_task_count'] or 0,
        cpu_usage_avg=row['cpu_usage_avg'],
        memory_usage_avg=row['memory_usage_avg'],
        disk_io_mb=row['disk_io_mb'],
        network_io_mb=row['network_io_mb']
    )