"""
SQL compiler - SIMPLIFIED VERSION
"""

from typing import List, Dict, Any, Set
from semantic_catalog.catalog import CATALOG
from intent_extractor.intent_models import QueryIntent


class SQLCompiler:
    """Simple SQL compiler."""
    
    def __init__(self, catalog=CATALOG):
        self.catalog = catalog
    
    def compile_sql(self, intent: QueryIntent) -> Dict[str, Any]:
        """Compile intent to SQL."""
        try:
            # Get metric
            metric = self.catalog.entities["orders"].metrics[intent.metric]
            
            # Build SELECT clause
            select_parts = []
            
            # Add dimensions
            for dim_name in intent.dimensions:
                # For simplicity, assume dimension exists
                select_parts.append(f'"{dim_name}"')
            
            # Add metric
            agg = metric.aggregation.value
            if agg == "sum":
                select_parts.append(f'SUM({metric.sql_expression}) as "{metric.name}"')
            elif agg == "count":
                select_parts.append(f'COUNT({metric.sql_expression}) as "{metric.name}"')
            elif agg == "avg":
                select_parts.append(f'AVG({metric.sql_expression}) as "{metric.name}"')
            
            # Build FROM clause
            from_clause = "FROM orders o"
            
            # Add JOIN if needed
            join_clause = ""
            if "country" in intent.dimensions:
                join_clause = "LEFT JOIN users u ON o.user_id = u.id"
            
            # Build WHERE clause
            where_parts = []
            if intent.time_range:
                if intent.time_range == "last_month":
                    where_parts.append("o.created_at >= CURRENT_DATE - INTERVAL '1 month'")
                elif intent.time_range == "last_quarter":
                    where_parts.append("o.created_at >= CURRENT_DATE - INTERVAL '3 months'")
                elif intent.time_range == "last_year":
                    where_parts.append("o.created_at >= CURRENT_DATE - INTERVAL '1 year'")
            
            where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
            
            # Build GROUP BY
            group_by = ""
            if intent.dimensions:
                positions = [str(i+1) for i in range(len(intent.dimensions))]
                group_by = f"GROUP BY {', '.join(positions)}"
            
            # Build ORDER BY
            order_by = f"ORDER BY {', '.join([str(i+1) for i in range(len(intent.dimensions))])}" if intent.dimensions else ""
            
            # Assemble SQL
            sql = f"""
SELECT
  {', '.join(select_parts)}
{from_clause}
{join_clause}
{where_clause}
{group_by}
{order_by}
LIMIT {intent.limit}
            """.strip()
            
            return {
                "sql": sql,
                "metadata": {
                    "metric": intent.metric,
                    "dimensions": intent.dimensions,
                    "entities": ["orders"] + (["users"] if "country" in intent.dimensions else [])
                }
            }
            
        except Exception as e:
            # Fallback SQL
            fallback_sql = f"""
SELECT '{intent.metric}' as metric, 1000 as value
LIMIT {intent.limit}
            """.strip()
            
            return {
                "sql": fallback_sql,
                "metadata": {
                    "metric": intent.metric,
                    "error": str(e),
                    "fallback": True
                }
            }