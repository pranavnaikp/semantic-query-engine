"""
Chart templates and configuration for deterministic visualization.
"""

from typing import Dict, List, Any, Optional
from enum import Enum
import plotly.graph_objects as go
import plotly.express as px


class ChartType(str, Enum):
    """Supported chart types."""
    BAR = "bar"
    LINE = "line"
    PIE = "pie"
    SCATTER = "scatter"
    HEATMAP = "heatmap"
    METRIC_CARD = "metric_card"
    TABLE = "table"


class ChartConfig:
    """Configuration for chart generation."""
    
    def __init__(
        self,
        chart_type: ChartType,
        title: str,
        x_axis: Optional[str] = None,
        y_axis: Optional[str] = None,
        color_scheme: str = "plotly",
        height: int = 400,
        width: int = 600,
        margin: Dict[str, int] = None,
        template: str = "plotly_white"
    ):
        self.chart_type = chart_type
        self.title = title
        self.x_axis = x_axis
        self.y_axis = y_axis
        self.color_scheme = color_scheme
        self.height = height
        self.width = width
        self.margin = margin or {"l": 50, "r": 50, "t": 50, "b": 50}
        self.template = template
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "type": self.chart_type.value,
            "title": self.title,
            "x_axis": self.x_axis,
            "y_axis": self.y_axis,
            "height": self.height,
            "width": self.width,
            "margin": self.margin,
            "template": self.template
        }


class ChartTemplateRegistry:
    """
    Registry of chart templates for different data patterns.
    Deterministic - same data pattern always produces same chart type.
    """
    
    def __init__(self):
        self.templates = self._initialize_templates()
    
    def _initialize_templates(self) -> Dict[str, Dict[str, Any]]:
        """Initialize chart templates."""
        return {
            "single_metric_no_dimensions": {
                "chart_type": ChartType.METRIC_CARD,
                "description": "Single metric value display",
                "conditions": {
                    "metric_count": 1,
                    "dimension_count": 0,
                    "row_count_max": 1
                }
            },
            "time_series": {
                "chart_type": ChartType.LINE,
                "description": "Metric over time",
                "conditions": {
                    "metric_count": 1,
                    "dimension_count": 1,
                    "dimension_is_time": True,
                    "row_count_min": 3
                }
            },
            "category_comparison": {
                "chart_type": ChartType.BAR,
                "description": "Metric comparison across categories",
                "conditions": {
                    "metric_count": 1,
                    "dimension_count": 1,
                    "row_count_max": 20
                }
            },
            "distribution": {
                "chart_type": ChartType.PIE,
                "description": "Distribution of categories",
                "conditions": {
                    "metric_count": 0,
                    "dimension_count": 1,
                    "row_count_max": 10
                }
            },
            "correlation": {
                "chart_type": ChartType.SCATTER,
                "description": "Relationship between two metrics",
                "conditions": {
                    "metric_count": 2,
                    "dimension_count": 0,
                    "row_count_min": 10
                }
            },
            "multi_metric_comparison": {
                "chart_type": ChartType.BAR,
                "description": "Multiple metrics across categories",
                "conditions": {
                    "metric_count_min": 2,
                    "dimension_count": 1,
                    "row_count_max": 10
                }
            },
            "heatmap_grid": {
                "chart_type": ChartType.HEATMAP,
                "description": "Two-dimensional data grid",
                "conditions": {
                    "metric_count": 1,
                    "dimension_count": 2,
                    "row_count_min": 4
                }
            },
            "large_dataset": {
                "chart_type": ChartType.TABLE,
                "description": "Large dataset table view",
                "conditions": {
                    "row_count_min": 50
                }
            }
        }
    
    def get_template_for_data(
        self,
        metric_count: int,
        dimension_count: int,
        row_count: int,
        dimensions: List[str] = None
    ) -> ChartType:
        """
        Determine chart type based on data characteristics.
        Deterministic - same inputs always produce same output.
        """
        dimensions = dimensions or []
        
        # Check if any dimension is time-related
        dimension_is_time = any(
            any(time_word in dim.lower() 
                for time_word in ['date', 'time', 'month', 'year', 'week', 'day', 'hour'])
            for dim in dimensions
        ) if dimensions else False
        
        # Evaluate templates in priority order
        template_checks = [
            # Single metric card
            (metric_count == 1 and dimension_count == 0 and row_count <= 1,
             ChartType.METRIC_CARD),
            
            # Time series
            (metric_count == 1 and dimension_count == 1 and dimension_is_time and row_count >= 3,
             ChartType.LINE),
            
            # Category distribution (pie for small sets)
            (metric_count == 0 and dimension_count == 1 and row_count <= 10,
             ChartType.PIE),
            
            # Category comparison (bar chart)
            (metric_count == 1 and dimension_count == 1 and row_count <= 20,
             ChartType.BAR),
            
            # Multiple metrics
            (metric_count >= 2 and dimension_count == 1 and row_count <= 10,
             ChartType.BAR),
            
            # Two dimensions (heatmap)
            (metric_count == 1 and dimension_count == 2 and row_count >= 4,
             ChartType.HEATMAP),
            
            # Large dataset
            (row_count > 50,
             ChartType.TABLE),
            
            # Correlation/scatter
            (metric_count == 2 and dimension_count == 0 and row_count >= 10,
             ChartType.SCATTER),
            
            # Default fallback
            (True, ChartType.TABLE)
        ]
        
        for condition, chart_type in template_checks:
            if condition:
                return chart_type
        
        return ChartType.TABLE
    
    def get_chart_config(
        self,
        chart_type: ChartType,
        title: str,
        data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Get configuration for a specific chart type."""
        base_config = {
            "responsive": True,
            "displayModeBar": True,
            "displaylogo": False
        }
        
        if chart_type == ChartType.BAR:
            return {
                **base_config,
                "type": "bar",
                "layout": {
                    "title": title,
                    "xaxis": {"title": "Category", "type": "category"},
                    "yaxis": {"title": "Value"},
                    "barmode": "group",
                    "height": 400,
                    "template": "plotly_white"
                },
                "config": {"displayModeBar": True}
            }
        
        elif chart_type == ChartType.LINE:
            return {
                **base_config,
                "type": "line",
                "layout": {
                    "title": title,
                    "xaxis": {"title": "Time", "type": "date"},
                    "yaxis": {"title": "Value"},
                    "height": 400,
                    "template": "plotly_white"
                }
            }
        
        elif chart_type == ChartType.PIE:
            return {
                **base_config,
                "type": "pie",
                "layout": {
                    "title": title,
                    "height": 400,
                    "template": "plotly_white"
                }
            }
        
        elif chart_type == ChartType.METRIC_CARD:
            value = data[0].get(list(data[0].keys())[0], 0) if data else 0
            return {
                "type": "metric_card",
                "value": value,
                "title": title,
                "format": "number"
            }
        
        else:  # TABLE
            return {
                "type": "table",
                "title": title,
                "columns": list(data[0].keys()) if data else [],
                "data": data
            }


# Global registry instance
chart_registry = ChartTemplateRegistry()