"""
Visualization package for deterministic chart generation.
Rule-based chart selection - no LLM involvement.
"""

from visualization.chart_templates import (
    ChartType,
    ChartConfig,
    ChartTemplateRegistry
)

from visualization.generator import (
    VisualizationGenerator,
    determine_chart_type
)

__all__ = [
    'ChartType',
    'ChartConfig',
    'ChartTemplateRegistry',
    'VisualizationGenerator',
    'determine_chart_type'
]

__version__ = "1.0.0"