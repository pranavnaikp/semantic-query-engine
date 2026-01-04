"""
SQL compiler for your PostgreSQL database.
Generates actual SQL queries based on semantic intent.
"""

from typing import Dict, List, Any, Optional
from intent_extractor.intent_models import QueryIntent
from semantic_catalog.catalog import CATALOG


class SQLCompiler:
    """Compiles semantic intent into executable SQL."""
    
    def __init__(self, catalog):
        self.catalog = catalog
    
    def compile_sql(self, intent: QueryIntent) -> Dict[str, Any]:
        """
        Compile intent into PostgreSQL SQL query.
        Returns SQL and metadata.
        """
        metric_name = intent.metric
        dimensions = intent.dimensions or []
        filters = intent.filters or []
        time_range = intent.time_range
        limit = intent.limit or 1000
        
        # Get metric and its entity
        metric = self.catalog.get_metric(metric_name)
        if not metric:
            raise ValueError(f"Metric '{metric_name}' not found in catalog")
        
        entity = self.catalog.get_entity_for_metric(metric_name)
        
        # Build SELECT clause
        select_parts = []
        
        # Add dimensions with proper column names
        for dim_name in dimensions:
            dim = self.catalog.get_dimension(dim_name)
            if dim:
                select_parts.append(f'"{dim.column_name}" as "{dim_name}"')
            else:
                # If dimension not found, use the name as-is (for debugging)
                select_parts.append(f'"{dim_name}"')
        
        # Add metric with aggregation
        aggregation = metric.aggregation.value
        if aggregation == "COUNT":
            select_parts.append(f'{aggregation}({metric.column_name}) as "{metric_name}"')
        else:
            select_parts.append(f'{aggregation}({metric.column_name}) as "{metric_name}"')
        
        select_clause = "SELECT\n  " + ",\n  ".join(select_parts)
        
        # Build FROM clause with joins
        from_clause = self._build_from_clause(entity, metric, dimensions, filters)
        
        # Build WHERE clause
        where_clauses = []
        
        # Add time range filter
        if time_range:
            time_filter = self._build_time_filter(time_range, entity)
            if time_filter:
                where_clauses.append(time_filter)
        
        # Add other filters
        for filter_obj in filters:
            filter_sql = self._build_filter_sql(filter_obj)
            if filter_sql:
                where_clauses.append(filter_sql)
        
        where_clause = ""
        if where_clauses:
            where_clause = "WHERE\n  " + "\n  AND ".join(where_clauses)
        
        # Build GROUP BY clause
        group_by_clause = ""
        if dimensions:
            # Group by dimension positions (1, 2, 3...)
            group_by_indices = [str(i+1) for i in range(len(dimensions))]
            group_by_clause = "GROUP BY\n  " + ",\n  ".join(group_by_indices)
        
        # Build ORDER BY clause
        order_by_clause = ""
        if dimensions:
            # Order by first dimension
            order_by_clause = f"ORDER BY\n  {dimensions[0]}"
        elif metric_name:
            # Order by metric value descending
            order_by_clause = f"ORDER BY\n  {metric_name} DESC"
        
        # Build LIMIT clause
        limit_clause = f"LIMIT {limit}"
        
        # Combine all clauses
        sql_parts = [
            select_clause,
            from_clause,
            where_clause,
            group_by_clause,
            order_by_clause,
            limit_clause
        ]
        
        sql = "\n".join([part for part in sql_parts if part])
        
        return {
            "sql": sql,
            "metric": metric_name,
            "dimensions": dimensions,
            "filters": filters,
            "time_range": time_range,
            "entity": entity.name if entity else None,
            "is_valid": True
        }
    
    def _build_from_clause(self, entity, metric, dimensions, filters) -> str:
        """Build FROM clause with necessary joins."""
        base_table = f'{entity.primary_schema}.{entity.primary_table}'
        from_parts = [f"FROM {base_table}"]
        
        # Always join customers for country dimension
        needs_customer_join = (
            "country" in dimensions or 
            "country_code" in dimensions or
            any(f.get("dimension") in ["country", "country_code"] for f in filters)
        )
        
        if needs_customer_join and entity.name == "orders":
            from_parts.append("LEFT JOIN ref.customers ON sales.orders.customer_id = ref.customers.customer_id")
        
        # Join segments if needed
        needs_segment_join = (
            "segment" in dimensions or 
            "segment_name" in dimensions or
            any(f.get("dimension") in ["segment", "segment_name"] for f in filters)
        )
        
        if needs_segment_join and entity.name == "orders":
            from_parts.append("LEFT JOIN analytics.customer_segments ON sales.orders.customer_id = analytics.customer_segments.customer_id")
        
        return "\n".join(from_parts)
    

    def _build_time_filter(self, time_range, entity) -> Optional[str]:
        """Build time filter SQL."""
        if not time_range or not entity:
            return None
        
        # Find time dimension in the entity
        time_dim = None
        for dim in entity.dimensions.values():
            if dim.data_type.value in ["DATE", "TIMESTAMP"]:
                time_dim = dim
                break
        
        if not time_dim:
            return None
        
        # Handle both TimeRange Pydantic object and dict
        if hasattr(time_range, 'type'):
            # It's a TimeRange Pydantic object
            time_range_type = time_range.type.value  # Get string value from enum
            start_date = time_range.start_date
            end_date = time_range.end_date
        else:
            # It's a dict
            time_range_type = time_range.get("type")
            start_date = time_range.get("start_date")
            end_date = time_range.get("end_date")
        
        if time_range_type == "last_quarter":
            return f"{time_dim.column_name} >= CURRENT_DATE - INTERVAL '3 months'"
        elif time_range_type == "last_month":
            return f"{time_dim.column_name} >= CURRENT_DATE - INTERVAL '1 month'"
        elif time_range_type == "last_year":
            return f"{time_dim.column_name} >= CURRENT_DATE - INTERVAL '1 year'"
        elif time_range_type == "custom" and start_date and end_date:
            return f"{time_dim.column_name} BETWEEN '{start_date}' AND '{end_date}'"
        
        return None

    def _build_filter_sql(self, filter_obj: Dict) -> Optional[str]:
        """Build filter SQL from filter object."""
        dimension = filter_obj.get("dimension")
        operator = filter_obj.get("operator")
        values = filter_obj.get("values", [])
        
        if not dimension or not operator or not values:
            return None
        
        dim = self.catalog.get_dimension(dimension)
        if not dim:
            return None
        
        column = dim.column_name
        
        if operator == "equals":
            return f"{column} = '{values[0]}'"
        elif operator == "not_equals":
            return f"{column} != '{values[0]}'"
        elif operator == "in":
            value_list = ", ".join([f"'{v}'" for v in values])
            return f"{column} IN ({value_list})"
        elif operator == "not_in":
            value_list = ", ".join([f"'{v}'" for v in values])
            return f"{column} NOT IN ({value_list})"
        elif operator == "greater_than":
            return f"{column} > {values[0]}"
        elif operator == "less_than":
            return f"{column} < {values[0]}"
        
        return None