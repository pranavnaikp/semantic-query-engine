# visualization/generator.py

"""
Deterministic visualization generation based on query results.
No LLM involvement - pure rule-based chart selection.
"""

from typing import Dict, List, Any, Optional
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import json
import base64
from io import BytesIO
from enum import Enum


class ChartType(str, Enum):
    """Supported chart types."""
    BAR = "bar"
    LINE = "line"
    PIE = "pie"
    SCATTER = "scatter"
    METRIC_CARD = "metric_card"
    TABLE = "table"


class VisualizationGenerator:
    """
    Generates visualizations deterministically based on query results.
    Rules:
    - 1 dimension + 1 metric = Bar chart
    - Time dimension + 1 metric = Line chart
    - 1 dimension only (distribution) = Pie chart
    - No dimensions (single metric) = Metric card
    - Multiple dimensions = Table
    """
    
    def __init__(self):
        self.chart_configs = {
            ChartType.BAR: {
                "template": "plotly_white",
                "margin": dict(l=50, r=50, t=50, b=50),
                "height": 400
            },
            ChartType.LINE: {
                "template": "plotly_white",
                "margin": dict(l=50, r=50, t=50, b=50),
                "height": 400
            },
            ChartType.PIE: {
                "template": "plotly_white",
                "margin": dict(l=50, r=50, t=50, b=50),
                "height": 400
            }
        }
    
    def determine_chart_type(
        self, 
        dimensions: List[str],
        metric_name: str,
        data: List[Dict]
    ) -> ChartType:
        """
        Deterministically determine the best chart type.
        Same inputs ALWAYS produce same chart type.
        """
        num_dimensions = len(dimensions)
        num_rows = len(data)
        
        if num_dimensions == 0 and metric_name:
            # Single metric value
            return ChartType.METRIC_CARD
        
        elif num_dimensions == 1 and metric_name:
            # Check if it's a time dimension
            dim_name = dimensions[0]
            if any(time_word in dim_name.lower() for time_word in ['date', 'time', 'month', 'year', 'week', 'day']):
                # Time series data - line chart
                return ChartType.LINE
            elif num_rows <= 10:
                # Small number of categories - bar chart
                return ChartType.BAR
            else:
                # Many categories - still bar chart but consider table
                return ChartType.BAR
        
        elif num_dimensions == 1 and not metric_name:
            # Distribution (counts) - pie chart for small sets
            if num_rows <= 8:
                return ChartType.PIE
            else:
                return ChartType.BAR
        
        elif num_dimensions > 1:
            # Multiple dimensions - table view
            return ChartType.TABLE
        
        else:
            # Default fallback
            return ChartType.TABLE
    
    def generate_bar_chart(
        self,
        data: List[Dict],
        dimensions: List[str],
        metric_name: str,
        title: str
    ) -> Dict[str, Any]:
        """Generate a bar chart deterministically."""
        df = pd.DataFrame(data)
        
        if not dimensions:
            raise ValueError("Bar chart requires at least one dimension")
        
        dim = dimensions[0]
        
        # Sort by metric value descending (deterministic)
        if metric_name:
            df = df.sort_values(metric_name, ascending=False)
        
        # Create figure
        if metric_name:
            fig = px.bar(
                df,
                x=dim,
                y=metric_name,
                title=title,
                labels={dim: dim.replace('_', ' ').title(), 
                       metric_name: metric_name.replace('_', ' ').title()}
            )
        else:
            # Count distribution
            value_counts = df[dim].value_counts().reset_index()
            value_counts.columns = [dim, 'count']
            fig = px.bar(
                value_counts,
                x=dim,
                y='count',
                title=title,
                labels={dim: dim.replace('_', ' ').title(), 
                       'count': 'Count'}
            )
        
        # Apply deterministic styling
        fig.update_layout(**self.chart_configs[ChartType.BAR])
        fig.update_traces(marker_color='steelblue')
        
        return self._fig_to_dict(fig)
    
    def generate_line_chart(
        self,
        data: List[Dict],
        dimensions: List[str],
        metric_name: str,
        title: str
    ) -> Dict[str, Any]:
        """Generate a line chart deterministically."""
        df = pd.DataFrame(data)
        
        if not dimensions:
            raise ValueError("Line chart requires at least one dimension")
        
        dim = dimensions[0]
        
        # Sort by dimension (for time series)
        df = df.sort_values(dim)
        
        fig = px.line(
            df,
            x=dim,
            y=metric_name,
            title=title,
            markers=True,
            labels={dim: dim.replace('_', ' ').title(), 
                   metric_name: metric_name.replace('_', ' ').title()}
        )
        
        # Apply deterministic styling
        fig.update_layout(**self.chart_configs[ChartType.LINE])
        fig.update_traces(line=dict(color='steelblue', width=3))
        
        return self._fig_to_dict(fig)
    
    def generate_pie_chart(
        self,
        data: List[Dict],
        dimensions: List[str],
        metric_name: Optional[str],
        title: str
    ) -> Dict[str, Any]:
        """Generate a pie chart deterministically."""
        df = pd.DataFrame(data)
        
        if not dimensions:
            raise ValueError("Pie chart requires at least one dimension")
        
        dim = dimensions[0]
        
        if metric_name:
            # Use provided metric values
            fig = px.pie(
                df,
                names=dim,
                values=metric_name,
                title=title,
                labels={dim: dim.replace('_', ' ').title()}
            )
        else:
            # Count distribution
            value_counts = df[dim].value_counts().reset_index()
            value_counts.columns = [dim, 'count']
            fig = px.pie(
                value_counts,
                names=dim,
                values='count',
                title=title,
                labels={dim: dim.replace('_', ' ').title()}
            )
        
        # Apply deterministic styling
        fig.update_layout(**self.chart_configs[ChartType.PIE])
        fig.update_traces(textposition='inside', textinfo='percent+label')
        
        return self._fig_to_dict(fig)
    
    def generate_metric_card(
        self,
        data: List[Dict],
        metric_name: str,
        title: str
    ) -> Dict[str, Any]:
        """Generate a metric card (single value display)."""
        if not data:
            value = 0
        else:
            # Get the metric value from the first row
            row = data[0]
            value = row.get(metric_name, 0)
        
        # Format the value
        formatted_value = self._format_value(value, metric_name)
        
        return {
            "type": "metric_card",
            "title": title,
            "value": value,
            "formatted_value": formatted_value,
            "metadata": {
                "metric_name": metric_name,
                "data_points": len(data)
            }
        }
    
    def generate_table_view(
        self,
        data: List[Dict],
        dimensions: List[str],
        metric_name: Optional[str],
        title: str
    ) -> Dict[str, Any]:
        """Generate a table view for complex data."""
        df = pd.DataFrame(data)
        
        # Format values
        for col in df.columns:
            if metric_name and col == metric_name:
                df[col] = df[col].apply(lambda x: self._format_value(x, metric_name))
        
        return {
            "type": "table",
            "title": title,
            "columns": list(df.columns),
            "data": df.to_dict('records'),
            "row_count": len(df),
            "metadata": {
                "dimensions": dimensions,
                "metric": metric_name
            }
        }
    
    def generate_visualization(
        self,
        data: List[Dict],
        dimensions: List[str],
        metric_name: str,
        query_title: str
    ) -> Dict[str, Any]:
        """
        Main method: generate visualization deterministically.
        Same data ALWAYS produces same visualization.
        """
        # Determine chart type
        chart_type = self.determine_chart_type(dimensions, metric_name, data)
        
        # Generate appropriate visualization
        if chart_type == ChartType.BAR:
            result = self.generate_bar_chart(data, dimensions, metric_name, query_title)
        elif chart_type == ChartType.LINE:
            result = self.generate_line_chart(data, dimensions, metric_name, query_title)
        elif chart_type == ChartType.PIE:
            result = self.generate_pie_chart(data, dimensions, metric_name, query_title)
        elif chart_type == ChartType.METRIC_CARD:
            result = self.generate_metric_card(data, metric_name, query_title)
        else:  # TABLE
            result = self.generate_table_view(data, dimensions, metric_name, query_title)
        
        # Add metadata
        result["chart_type"] = chart_type.value
        result["deterministic_hash"] = self._generate_deterministic_hash(
            data, dimensions, metric_name
        )
        
        return result
    
    def _fig_to_dict(self, fig) -> Dict[str, Any]:
        """Convert Plotly figure to dictionary representation."""
        # Convert to JSON-serializable dict
        fig_dict = fig.to_dict()
        
        # Get image as base64 for API responses
        img_bytes = fig.to_image(format="png", width=800, height=400)
        img_base64 = base64.b64encode(img_bytes).decode('utf-8')
        
        return {
            "type": "plotly",
            "figure": fig_dict,
            "image_base64": img_base64,
            "layout": fig_dict.get('layout', {}),
            "data": fig_dict.get('data', [])
        }
    
    def _format_value(self, value, metric_name: str) -> str:
        """Format value based on metric type."""
        if pd.isna(value):
            return "N/A"
        
        # Check metric name for hints about formatting
        metric_lower = metric_name.lower()
        
        if any(currency_word in metric_lower for currency_word in ['revenue', 'profit', 'amount', 'price', 'cost']):
            # Currency formatting
            return f"${value:,.2f}"
        elif 'percent' in metric_lower or 'rate' in metric_lower:
            # Percentage
            return f"{value:.1%}"
        elif isinstance(value, (int, float)):
            # Numeric with commas
            if value.is_integer():
                return f"{int(value):,}"
            else:
                return f"{value:,.2f}"
        else:
            return str(value)
    
    def _generate_deterministic_hash(self, data, dimensions, metric_name) -> str:
        """
        Generate a deterministic hash for the visualization.
        Ensures same inputs produce same visualization.
        """
        import hashlib
        
        # Create a string representation of inputs
        input_str = f"{sorted(dimensions)}:{metric_name}:{len(data)}"
        
        # Add data values (first few rows for hash)
        for i, row in enumerate(data[:10]):  # Limit to first 10 rows for performance
            input_str += f":{i}:{sorted(row.items())}"
        
        return hashlib.md5(input_str.encode()).hexdigest()[:8]