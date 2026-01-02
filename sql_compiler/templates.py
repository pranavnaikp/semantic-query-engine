"""
SQL templates for deterministic SQL generation.
Same intent ALWAYS produces same SQL.
No LLM involvement here - pure deterministic logic.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, date, timedelta
from intent_extractor.intent_models import TimeRange, TimeRangeType, FilterCondition
from semantic_catalog.models import Dimension, Entity


class TimeRangeResolver:
    """
    Resolves time ranges to SQL date ranges deterministically.
    Same time range type ALWAYS produces same date range.
    """
    
    @staticmethod
    def get_date_range(time_range: Optional[TimeRange], reference_date: date = None) -> tuple[date, date]:
        """
        Get start and end dates for a time range type.
        Deterministic - same input always produces same output.
        """
        if not time_range:
            return None, None
        
        ref_date = reference_date or date.today()
        
        if time_range.type == TimeRangeType.CUSTOM:
            if not time_range.start_date or not time_range.end_date:
                raise ValueError("Custom time range requires start_date and end_date")
            return time_range.start_date, time_range.end_date
        
        elif time_range.type == TimeRangeType.LAST_QUARTER:
            return TimeRangeResolver._get_last_quarter(ref_date)
        
        elif time_range.type == TimeRangeType.LAST_MONTH:
            return TimeRangeResolver._get_last_month(ref_date)
        
        elif time_range.type == TimeRangeType.LAST_WEEK:
            return TimeRangeResolver._get_last_week(ref_date)
        
        elif time_range.type == TimeRangeType.LAST_YEAR:
            return TimeRangeResolver._get_last_year(ref_date)
        
        elif time_range.type == TimeRangeType.CURRENT_QUARTER:
            return TimeRangeResolver._get_current_quarter(ref_date)
        
        elif time_range.type == TimeRangeType.CURRENT_MONTH:
            return TimeRangeResolver._get_current_month(ref_date)
        
        elif time_range.type == TimeRangeType.CURRENT_WEEK:
            return TimeRangeResolver._get_current_week(ref_date)
        
        elif time_range.type == TimeRangeType.CURRENT_YEAR:
            return TimeRangeResolver._get_current_year(ref_date)
        
        else:
            raise ValueError(f"Unsupported time range type: {time_range.type}")
    
    @staticmethod
    def _get_last_quarter(ref_date: date) -> tuple[date, date]:
        """Get last quarter (deterministic calculation)."""
        # Quarters: Q1 (Jan-Mar), Q2 (Apr-Jun), Q3 (Jul-Sep), Q4 (Oct-Dec)
        current_month = ref_date.month
        current_year = ref_date.year
        
        if current_month in [1, 2, 3]:  # Q1
            # Last quarter is Q4 of previous year
            start = date(current_year - 1, 10, 1)
            end = date(current_year - 1, 12, 31)
        elif current_month in [4, 5, 6]:  # Q2
            # Last quarter is Q1 of current year
            start = date(current_year, 1, 1)
            end = date(current_year, 3, 31)
        elif current_month in [7, 8, 9]:  # Q3
            # Last quarter is Q2 of current year
            start = date(current_year, 4, 1)
            end = date(current_year, 6, 30)
        else:  # [10, 11, 12] - Q4
            # Last quarter is Q3 of current year
            start = date(current_year, 7, 1)
            end = date(current_year, 9, 30)
        
        return start, end
    
    @staticmethod
    def _get_last_month(ref_date: date) -> tuple[date, date]:
        """Get last month (deterministic calculation)."""
        # First day of current month
        if ref_date.month == 1:
            last_month_start = date(ref_date.year - 1, 12, 1)
            last_month_end = date(ref_date.year - 1, 12, 31)
        else:
            last_month_start = date(ref_date.year, ref_date.month - 1, 1)
            # Last day of last month
            last_month_end = date(ref_date.year, ref_date.month, 1) - timedelta(days=1)
        
        return last_month_start, last_month_end
    
    @staticmethod
    def _get_last_week(ref_date: date) -> tuple[date, date]:
        """Get last week (Monday to Sunday)."""
        # Find Monday of this week
        days_since_monday = ref_date.weekday()  # Monday=0, Sunday=6
        monday_this_week = ref_date - timedelta(days=days_since_monday)
        
        # Last week = previous Monday to Sunday
        monday_last_week = monday_this_week - timedelta(days=7)
        sunday_last_week = monday_last_week + timedelta(days=6)
        
        return monday_last_week, sunday_last_week
    
    @staticmethod
    def _get_last_year(ref_date: date) -> tuple[date, date]:
        """Get last calendar year."""
        last_year = ref_date.year - 1
        return date(last_year, 1, 1), date(last_year, 12, 31)
    
    @staticmethod
    def _get_current_quarter(ref_date: date) -> tuple[date, date]:
        """Get current quarter."""
        current_month = ref_date.month
        current_year = ref_date.year
        
        if current_month in [1, 2, 3]:  # Q1
            start = date(current_year, 1, 1)
            end = date(current_year, 3, 31)
        elif current_month in [4, 5, 6]:  # Q2
            start = date(current_year, 4, 1)
            end = date(current_year, 6, 30)
        elif current_month in [7, 8, 9]:  # Q3
            start = date(current_year, 7, 1)
            end = date(current_year, 9, 30)
        else:  # [10, 11, 12] - Q4
            start = date(current_year, 10, 1)
            end = date(current_year, 12, 31)
        
        return start, end
    
    @staticmethod
    def _get_current_month(ref_date: date) -> tuple[date, date]:
        """Get current month."""
        start = date(ref_date.year, ref_date.month, 1)
        # Last day of current month
        if ref_date.month == 12:
            end = date(ref_date.year + 1, 1, 1) - timedelta(days=1)
        else:
            end = date(ref_date.year, ref_date.month + 1, 1) - timedelta(days=1)
        
        return start, end
    
    @staticmethod
    def _get_current_week(ref_date: date) -> tuple[date, date]:
        """Get current week (Monday to Sunday)."""
        days_since_monday = ref_date.weekday()  # Monday=0, Sunday=6
        monday = ref_date - timedelta(days=days_since_monday)
        sunday = monday + timedelta(days=6)
        
        return monday, sunday
    
    @staticmethod
    def _get_current_year(ref_date: date) -> tuple[date, date]:
        """Get current calendar year."""
        current_year = ref_date.year
        return date(current_year, 1, 1), date(current_year, 12, 31)
    
    @staticmethod
    def get_time_filter_sql(
        time_range: Optional[TimeRange],
        time_dimension: Dimension,
        alias: str,
        entity: Entity
    ) -> str:
        """Generate SQL WHERE clause for time range filter."""
        if not time_range or not time_dimension:
            return ""
        
        start_date, end_date = TimeRangeResolver.get_date_range(time_range)
        if not start_date or not end_date:
            return ""
        
        # Get column reference with proper quoting
        column_ref = f"{alias}.{entity.quote_column(time_dimension.column_name)}"
        
        # If dimension has SQL expression (like DATE(created_at)), use it
        if time_dimension.sql_expression:
            # Replace column name in expression with aliased version
            expr = time_dimension.sql_expression.replace(
                time_dimension.column_name,
                f"{alias}.{entity.quote_column(time_dimension.column_name)}"
            )
            return f"{expr} BETWEEN '{start_date}' AND '{end_date}'"
        else:
            return f"{column_ref} BETWEEN '{start_date}' AND '{end_date}'"


class FilterSQLBuilder:
    """
    Builds SQL WHERE conditions for filters deterministically.
    """
    
    OPERATOR_MAP = {
        "equals": "=",
        "not_equals": "!=",
        "in": "IN",
        "not_in": "NOT IN",
        "greater_than": ">",
        "less_than": "<",
        "greater_than_or_equal": ">=",
        "less_than_or_equal": "<="
    }
    
    @staticmethod
    def build_filter_sql(
        filter_cond: FilterCondition,
        dimension: Dimension,
        alias: str,
        entity: Entity
    ) -> str:
        """
        Build SQL condition for a filter.
        Deterministic - same filter always produces same SQL.
        """
        column_ref = f"{alias}.{entity.quote_column(dimension.column_name)}"
        
        # Get SQL operator
        sql_operator = FilterSQLBuilder.OPERATOR_MAP.get(
            filter_cond.operator,
            "="  # Default
        )
        
        # Handle different operator types
        if sql_operator in ["IN", "NOT IN"]:
            # Format: column IN ('val1', 'val2', ...)
            values_formatted = FilterSQLBuilder._format_values(
                filter_cond.values, 
                dimension.data_type
            )
            return f"{column_ref} {sql_operator} ({', '.join(values_formatted)})"
        
        elif len(filter_cond.values) == 1:
            # Single value comparison
            value_formatted = FilterSQLBuilder._format_value(
                filter_cond.values[0],
                dimension.data_type
            )
            return f"{column_ref} {sql_operator} {value_formatted}"
        
        else:
            # Multiple values with equals becomes IN
            values_formatted = FilterSQLBuilder._format_values(
                filter_cond.values,
                dimension.data_type
            )
            return f"{column_ref} IN ({', '.join(values_formatted)})"
    
    @staticmethod
    def _format_value(value: str, data_type: str) -> str:
        """Format value for SQL based on data type."""
        if data_type in ["number", "integer", "float"]:
            # Try to convert to number
            try:
                float(value)
                return str(value)
            except ValueError:
                # If not a number, treat as string
                return f"'{value}'"
        elif data_type == "boolean":
            # Convert to PostgreSQL boolean
            if value.lower() in ["true", "t", "yes", "y", "1"]:
                return "TRUE"
            elif value.lower() in ["false", "f", "no", "n", "0"]:
                return "FALSE"
            else:
                return f"'{value}'"
        else:
            # String, date, datetime - quote it
            return f"'{value}'"
    
    @staticmethod
    def _format_values(values: List[str], data_type: str) -> List[str]:
        """Format multiple values for SQL."""
        return [FilterSQLBuilder._format_value(v, data_type) for v in values]


class SQLTemplates:
    """
    Collection of SQL templates for different query patterns.
    All templates are deterministic - same inputs produce same SQL.
    """
    
    @staticmethod
    def build_select_clause(
        dimensions: List[Dimension],
        metric_expression: str,
        entity_aliases: Dict[str, str]
    ) -> str:
        """Build SELECT clause deterministically."""
        select_parts = []
        
        # Add dimensions in the order they appear in the intent
        for dim in dimensions:
            entity = dim.entity_name
            alias = entity_aliases[entity]
            
            if dim.sql_expression:
                # Use the SQL expression, replacing column references
                expr = dim.sql_expression
                # Simple column replacement (in production, use a proper parser)
                expr = expr.replace(
                    dim.column_name,
                    f"{alias}.{dim.column_name}"
                )
                select_parts.append(f"{expr} AS \"{dim.name}\"")
            else:
                select_parts.append(f"{alias}.{dim.column_name} AS \"{dim.name}\"")
        
        # Add metric aggregation
        select_parts.append(metric_expression)
        
        return ",\n  ".join(select_parts)
    
    @staticmethod
    def build_from_clause(
        primary_entity: Entity,
        primary_alias: str
    ) -> str:
        """Build FROM clause with schema support."""
        return f"FROM {primary_entity.fully_qualified_table} {primary_alias}"
    
    @staticmethod
    def build_join_clauses(
        join_sqls: List[str]
    ) -> str:
        """Build JOIN clauses."""
        return "\n".join(join_sqls) if join_sqls else ""
    
    @staticmethod
    def build_where_clause(
        where_conditions: List[str]
    ) -> str:
        """Build WHERE clause."""
        if not where_conditions:
            return ""
        return f"WHERE {' AND '.join(where_conditions)}"
    
    @staticmethod
    def build_group_by_clause(
        dimension_count: int
    ) -> str:
        """Build GROUP BY clause using column positions."""
        if dimension_count == 0:
            return ""
        
        positions = [str(i + 1) for i in range(dimension_count)]
        return f"GROUP BY {', '.join(positions)}"
    
    @staticmethod
    def build_order_by_clause(
        dimensions: List[Dimension],
        sort_direction: str = "ASC"
    ) -> str:
        """Build ORDER BY clause."""
        if not dimensions:
            return ""
        
        # Order by all dimensions in the order they appear
        positions = [str(i + 1) for i in range(len(dimensions))]
        order_by_parts = [f"{pos} {sort_direction}" for pos in positions]
        return f"ORDER BY {', '.join(order_by_parts)}"
    
    @staticmethod
    def build_limit_clause(limit: Optional[int]) -> str:
        """Build LIMIT clause."""
        if not limit:
            return ""
        return f"LIMIT {limit}"
    
    @staticmethod
    def assemble_full_sql(
        select_clause: str,
        from_clause: str,
        join_clause: str,
        where_clause: str,
        group_by_clause: str,
        order_by_clause: str,
        limit_clause: str
    ) -> str:
        """Assemble complete SQL query deterministically."""
        parts = [
            f"SELECT\n  {select_clause}",
            from_clause,
            join_clause,
            where_clause,
            group_by_clause,
            order_by_clause,
            limit_clause
        ]
        
        # Filter out empty parts and join with newlines
        non_empty_parts = [p for p in parts if p]
        return "\n".join(non_empty_parts) + ";"