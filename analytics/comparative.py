"""
Comparative analytics: YoY, MoM, QoQ, vs previous period calculations.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
from enum import Enum


class ComparativeAnalyzer:
    """
    Handles comparative queries like:
    - "How much did revenue increase compared to last year?"
    - "Show me MoM growth by country"
    - "Compare this quarter to last quarter"
    """

    def __init__(self, db_service):
        self.db = db_service

    async def analyze_comparative(self, intent_dict: Dict, base_sql: str) -> Dict[str, Any]:
        """
        Analyze comparative queries and generate appropriate SQL.
        """
        if not intent_dict:
            return {
                "comparative": False,
                "sql": base_sql,
                "message": "No intent provided"
            }

        # Use comparative field from intent if present, otherwise detect from query
        comparative_type = intent_dict.get("comparative")
        query_text = intent_dict.get("original_query", "").lower()

        if not comparative_type:
            # Fallback: detect from query text
            comparative_type = self._detect_comparison_type(query_text)

        if not comparative_type:
            return {
                "comparative": False,
                "sql": base_sql,
                "message": "No comparison detected"
            }

        # Generate comparative SQL
        comparative_sql = self._generate_comparative_sql(
            comparative_type, intent_dict
        )

        return {
            "comparative": True,
            "type": comparative_type,
            "sql": comparative_sql,
            "base_period_sql": base_sql,
        }

    def _detect_comparison_type(self, query: str) -> Optional[str]:
        """Detect what type of comparison is being asked."""
        query_lower = query.lower()

        if any(word in query_lower for word in ['compared to last year', 'year over year', 'yoy', 'vs last year', 'year-on-year']):
            return "yoy"
        elif any(word in query_lower for word in ['compared to last month', 'month over month', 'mom', 'vs last month', 'month-on-month']):
            return "mom"
        elif any(word in query_lower for word in ['compared to last quarter', 'quarter over quarter', 'qoq', 'vs last quarter', 'quarter-on-quarter']):
            return "qoq"
        elif any(word in query_lower for word in ['compared to previous', 'vs previous', 'previous period', 'last period']):
            return "previous"
        elif any(word in query_lower for word in ['growth', 'increase', 'decrease', 'change', 'compared']):
            # Default to year-over-year if growth mentioned
            return "yoy"

        return None

    def _generate_comparative_sql(self, comparison_type: str, intent_dict: Dict) -> str:
        """Generate SQL for comparative analysis."""
        metric = intent_dict.get("metric", "")
        dimensions = intent_dict.get("dimensions", [])

        if comparison_type == "yoy":
            return self._generate_yoy_sql(metric, dimensions)
        elif comparison_type == "mom":
            return self._generate_mom_sql(metric, dimensions)
        elif comparison_type == "qoq":
            return self._generate_qoq_sql(metric, dimensions)

        # Fallback to simple comparison
        return self._generate_simple_comparison_sql(metric, dimensions)

    def _generate_yoy_sql(self, metric: str, dimensions: List[str]) -> str:
        """Generate Year-Over-Year comparison SQL."""
        current_year = datetime.now().year
        previous_year = current_year - 1

        # Build SELECT clause
        select_parts = []

        # Add dimensions
        for dim in dimensions:
            if dim == "country":
                select_parts.append('"country_code" as "country"')
            elif dim == "segment":
                select_parts.append('"segment_name" as "segment"')
            else:
                select_parts.append(f'"{dim}"')

        # Add metrics
        if metric == "revenue":
            select_parts.append(f"""
                SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = {current_year} 
                    THEN amount_usd ELSE 0 END) as current_year_{metric}
            """)
            select_parts.append(f"""
                SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = {previous_year} 
                    THEN amount_usd ELSE 0 END) as previous_year_{metric}
            """)
        elif metric == "order_count":
            select_parts.append(f"""
                COUNT(CASE WHEN EXTRACT(YEAR FROM order_date) = {current_year} 
                    THEN order_id END) as current_year_{metric}
            """)
            select_parts.append(f"""
                COUNT(CASE WHEN EXTRACT(YEAR FROM order_date) = {previous_year} 
                    THEN order_id END) as previous_year_{metric}
            """)
        else:
            # Generic metric
            select_parts.append(f"""
                SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = {current_year} 
                    THEN 1 ELSE 0 END) as current_year_{metric}
            """)
            select_parts.append(f"""
                SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = {previous_year} 
                    THEN 1 ELSE 0 END) as previous_year_{metric}
            """)

        # Add growth percentage
        if metric == "revenue":
            select_parts.append(f"""
                CASE 
                    WHEN SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = {previous_year} THEN amount_usd ELSE 0 END) = 0 
                    THEN NULL
                    ELSE ROUND(
                        (SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = {current_year} THEN amount_usd ELSE 0 END) - 
                         SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = {previous_year} THEN amount_usd ELSE 0 END)) * 100.0 /
                        NULLIF(SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = {previous_year} THEN amount_usd ELSE 0 END), 0), 2
                    )
                END as yoy_growth_percent
            """)
        else:
            select_parts.append(f"""
                CASE 
                    WHEN COUNT(CASE WHEN EXTRACT(YEAR FROM order_date) = {previous_year} THEN 1 END) = 0 
                    THEN NULL
                    ELSE ROUND(
                        (COUNT(CASE WHEN EXTRACT(YEAR FROM order_date) = {current_year} THEN 1 END) - 
                         COUNT(CASE WHEN EXTRACT(YEAR FROM order_date) = {previous_year} THEN 1 END)) * 100.0 /
                        NULLIF(COUNT(CASE WHEN EXTRACT(YEAR FROM order_date) = {previous_year} THEN 1 END), 0), 2
                    )
                END as yoy_growth_percent
            """)

        select_clause = "SELECT\n  " + ",\n  ".join(select_parts)

        # Build FROM clause
        from_clause = "FROM sales.orders"
        if "country" in dimensions:
            from_clause += "\nLEFT JOIN ref.customers ON sales.orders.customer_id = ref.customers.customer_id"
        if "segment" in dimensions:
            from_clause += "\nLEFT JOIN analytics.customer_segments ON sales.orders.customer_id = analytics.customer_segments.customer_id"

        # Add WHERE clause for last 2 years
        from_clause += f"\nWHERE order_date >= DATE '{previous_year}-01-01'"

        # Build GROUP BY
        group_by_clause = ""
        if dimensions:
            group_by_indices = [str(i + 1) for i in range(len(dimensions))]
            group_by_clause = "GROUP BY\n  " + ",\n  ".join(group_by_indices)

        sql = f"""
        {select_clause}
        {from_clause}
        {group_by_clause}
        ORDER BY yoy_growth_percent DESC NULLS LAST
        LIMIT 1000
        """

        return sql

    def _generate_mom_sql(self, metric: str, dimensions: List[str]) -> str:
        """Generate Month-Over-Month comparison SQL."""
        select_parts = []

        # Add dimensions
        for dim in dimensions:
            if dim == "country":
                select_parts.append('"country_code" as "country"')
            elif dim == "segment":
                select_parts.append('"segment_name" as "segment"')
            else:
                select_parts.append(f'"{dim}"')

        if metric == "revenue":
            select_parts.append("""
                TO_CHAR(order_date, 'YYYY-MM') as month,
                SUM(amount_usd) as monthly_revenue,
                LAG(SUM(amount_usd)) OVER (ORDER BY TO_CHAR(order_date, 'YYYY-MM')) as previous_month_revenue,
                ROUND(
                    (SUM(amount_usd) - LAG(SUM(amount_usd)) OVER (ORDER BY TO_CHAR(order_date, 'YYYY-MM'))) * 100.0 /
                    NULLIF(LAG(SUM(amount_usd)) OVER (ORDER BY TO_CHAR(order_date, 'YYYY-MM')), 0), 2
                ) as mom_growth_percent
            """)
        else:
            select_parts.append("""
                TO_CHAR(order_date, 'YYYY-MM') as month,
                COUNT(order_id) as monthly_count,
                LAG(COUNT(order_id)) OVER (ORDER BY TO_CHAR(order_date, 'YYYY-MM')) as previous_month_count,
                ROUND(
                    (COUNT(order_id) - LAG(COUNT(order_id)) OVER (ORDER BY TO_CHAR(order_date, 'YYYY-MM'))) * 100.0 /
                    NULLIF(LAG(COUNT(order_id)) OVER (ORDER BY TO_CHAR(order_date, 'YYYY-MM')), 0), 2
                ) as mom_growth_percent
            """)

        select_clause = "SELECT\n  " + ",\n  ".join(select_parts)

        # Build FROM clause
        from_clause = "FROM sales.orders"
        if "country" in dimensions:
            from_clause += "\nLEFT JOIN ref.customers ON sales.orders.customer_id = ref.customers.customer_id"
        if "segment" in dimensions:
            from_clause += "\nLEFT JOIN analytics.customer_segments ON sales.orders.customer_id = analytics.customer_segments.customer_id"

        from_clause += "\nWHERE order_date >= CURRENT_DATE - INTERVAL '6 months'"

        # Build GROUP BY
        group_by = ""
        if dimensions:
            group_indices = [str(i + 1) for i in range(len(dimensions) + 1)]
            group_by = "GROUP BY\n  " + ",\n  ".join(group_indices)

        sql = f"""
        {select_clause}
        {from_clause}
        {group_by}
        ORDER BY month DESC
        LIMIT 1000
        """

        return sql

    def _generate_qoq_sql(self, metric: str, dimensions: List[str]) -> str:
        """Generate Quarter-Over-Quarter comparison SQL."""
        select_parts = []

        # Add dimensions
        for dim in dimensions:
            if dim == "country":
                select_parts.append('"country_code" as "country"')
            elif dim == "segment":
                select_parts.append('"segment_name" as "segment"')
            else:
                select_parts.append(f'"{dim}"')

        if metric == "revenue":
            select_parts.append("""
                EXTRACT(YEAR FROM order_date) as year,
                EXTRACT(QUARTER FROM order_date) as quarter,
                CONCAT('Q', EXTRACT(QUARTER FROM order_date), ' ', EXTRACT(YEAR FROM order_date)) as quarter_label,
                SUM(amount_usd) as quarterly_revenue,
                LAG(SUM(amount_usd)) OVER (ORDER BY EXTRACT(YEAR FROM order_date), EXTRACT(QUARTER FROM order_date)) as previous_quarter_revenue,
                ROUND(
                    (SUM(amount_usd) - LAG(SUM(amount_usd)) OVER (ORDER BY EXTRACT(YEAR FROM order_date), EXTRACT(QUARTER FROM order_date))) * 100.0 /
                    NULLIF(LAG(SUM(amount_usd)) OVER (ORDER BY EXTRACT(YEAR FROM order_date), EXTRACT(QUARTER FROM order_date)), 0), 2
                ) as qoq_growth_percent
            """)
        else:
            select_parts.append("""
                EXTRACT(YEAR FROM order_date) as year,
                EXTRACT(QUARTER FROM order_date) as quarter,
                CONCAT('Q', EXTRACT(QUARTER FROM order_date), ' ', EXTRACT(YEAR FROM order_date)) as quarter_label,
                COUNT(order_id) as quarterly_count,
                LAG(COUNT(order_id)) OVER (ORDER BY EXTRACT(YEAR FROM order_date), EXTRACT(QUARTER FROM order_date)) as previous_quarter_count,
                ROUND(
                    (COUNT(order_id) - LAG(COUNT(order_id)) OVER (ORDER BY EXTRACT(YEAR FROM order_date), EXTRACT(QUARTER FROM order_date))) * 100.0 /
                    NULLIF(LAG(COUNT(order_id)) OVER (ORDER BY EXTRACT(YEAR FROM order_date), EXTRACT(QUARTER FROM order_date)), 0), 2
                ) as qoq_growth_percent
            """)

        select_clause = "SELECT\n  " + ",\n  ".join(select_parts)

        # Build FROM clause
        from_clause = "FROM sales.orders"
        if "country" in dimensions:
            from_clause += "\nLEFT JOIN ref.customers ON sales.orders.customer_id = ref.customers.customer_id"
        if "segment" in dimensions:
            from_clause += "\nLEFT JOIN analytics.customer_segments ON sales.orders.customer_id = analytics.customer_segments.customer_id"

        from_clause += "\nWHERE order_date >= CURRENT_DATE - INTERVAL '1 year'"

        # Build GROUP BY
        group_by = ""
        if dimensions:
            group_indices = [str(i + 1) for i in range(len(dimensions) + 2)]
            group_by = "GROUP BY\n  " + ",\n  ".join(group_indices)

        sql = f"""
        {select_clause}
        {from_clause}
        {group_by}
        ORDER BY year DESC, quarter DESC
        LIMIT 1000
        """

        return sql

    def _generate_simple_comparison_sql(self, metric: str, dimensions: List[str]) -> str:
        """Generate simple comparison SQL."""
        current_year = datetime.now().year
        previous_year = current_year - 1

        select_parts = []

        # Add dimensions
        for dim in dimensions:
            if dim == "country":
                select_parts.append('"country_code" as "country"')
            elif dim == "segment":
                select_parts.append('"segment_name" as "segment"')
            else:
                select_parts.append(f'"{dim}"')

        # Add metrics
        if metric == "revenue":
            select_parts.append(f"SUM(amount_usd) as {metric}")
        elif metric == "order_count":
            select_parts.append(f"COUNT(order_id) as {metric}")
        else:
            select_parts.append(f"COUNT(*) as {metric}")

        select_clause = "SELECT\n  " + ",\n  ".join(select_parts)

        # Build FROM clause
        from_clause = "FROM sales.orders"
        if "country" in dimensions:
            from_clause += "\nLEFT JOIN ref.customers ON sales.orders.customer_id = ref.customers.customer_id"
        if "segment" in dimensions:
            from_clause += "\nLEFT JOIN analytics.customer_segments ON sales.orders.customer_id = analytics.customer_segments.customer_id"

        from_clause += f"\nWHERE EXTRACT(YEAR FROM order_date) IN ({current_year}, {previous_year})"

        # Build GROUP BY
        group_by_clause = ""
        if dimensions:
            group_by_indices = [str(i + 1) for i in range(len(dimensions))]
            group_by_clause = "GROUP BY\n  " + ",\n  ".join(group_by_indices)

        sql = f"""
        {select_clause}
        {from_clause}
        {group_by_clause}
        ORDER BY {metric} DESC
        LIMIT 1000
        """

        return sql

    async def execute_comparative_query(self, sql: str) -> List[Dict]:
        """Execute comparative query and format results."""
        try:
            data = await self.db.execute_query(sql)
            return self._format_comparative_data(data)
        except Exception as e:
            print(f"Comparative query execution failed: {e}")
            return []

    def _format_comparative_data(self, data: List[Dict]) -> List[Dict]:
        """Format comparative data for display."""
        formatted_data = []
        for row in data:
            formatted_row = {}
            for key, value in row.items():
                if value is None:
                    formatted_row[key] = "N/A"
                elif 'growth' in key.lower() or 'percent' in key.lower():
                    formatted_row[key] = f"{float(value):.1f}%" if isinstance(value, (int, float)) else str(value)
                elif any(term in key.lower() for term in ['revenue', 'amount', 'value', 'profit']):
                    if isinstance(value, (int, float)):
                        formatted_row[key] = f"${value:,.2f}"
                    else:
                        formatted_row[key] = str(value)
                else:
                    formatted_row[key] = value
            formatted_data.append(formatted_row)
        return formatted_data