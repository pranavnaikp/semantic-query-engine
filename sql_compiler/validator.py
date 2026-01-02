"""
Semantic validation layer.
Validates intent against semantic catalog before SQL generation.
"""

from typing import List
from semantic_catalog.catalog import CATALOG
from intent_extractor.intent_models import QueryIntent, TimeRange


class SemanticValidator:
    """
    Validates query intent against semantic catalog.
    Ensures:
    1. Metric exists
    2. Dimensions exist
    3. Join paths exist between metric and dimensions
    4. Time dimension exists for time-based queries
    5. Filter dimensions exist
    """
    
    def __init__(self, catalog=CATALOG):
        self.catalog = catalog
    
    def validate_intent(self, intent: QueryIntent) -> List[str]:
        """
        Validate intent against semantic catalog.
        Returns list of validation errors, empty list if valid.
        """
        errors = []
        
        # 1. Validate metric exists
        try:
            metric = self.catalog.get_metric(intent.metric)
        except ValueError:
            errors.append(f"Metric '{intent.metric}' not found in catalog")
            return errors  # Can't continue without valid metric
        
        # 2. Validate dimensions exist
        for dim_name in intent.dimensions:
            try:
                self.catalog.get_dimension(dim_name)
            except ValueError:
                errors.append(f"Dimension '{dim_name}' not found in catalog")
        
        # 3. Validate metric can be grouped by dimensions (join paths exist)
        if intent.dimensions:
            try:
                self.catalog.validate_metric_dimension_combo(
                    intent.metric, 
                    intent.dimensions
                )
            except ValueError as e:
                errors.append(str(e))
        
        # 4. Validate time dimension if time range is specified
        if intent.time_range and metric.time_dimension:
            # Check if time dimension exists
            try:
                time_dim = self.catalog.get_dimension(metric.time_dimension)
                # Check if time dimension is in dimensions list
                if metric.time_dimension not in intent.dimensions:
                    # Time filtering will be applied, but not grouping
                    pass
            except ValueError:
                errors.append(
                    f"Time dimension '{metric.time_dimension}' for metric "
                    f"'{intent.metric}' not found in catalog"
                )
        elif intent.time_range and not metric.time_dimension:
            errors.append(
                f"Metric '{intent.metric}' does not have a time dimension, "
                f"but time range was specified"
            )
        
        # 5. Validate filter dimensions exist
        for filter_cond in intent.filters:
            try:
                self.catalog.get_dimension(filter_cond.dimension)
            except ValueError:
                errors.append(f"Filter dimension '{filter_cond.dimension}' not found in catalog")
        
        # 6. Validate time range format
        if intent.time_range:
            if intent.time_range.type == "custom":
                if not intent.time_range.start_date or not intent.time_range.end_date:
                    errors.append("Custom time range requires both start_date and end_date")
                elif intent.time_range.start_date > intent.time_range.end_date:
                    errors.append("Start date must be before end date")
        
        return errors
    
    def get_metric_time_dimension(self, metric_name: str) -> str:
        """Get the time dimension for a metric."""
        metric = self.catalog.get_metric(metric_name)
        return metric.time_dimension