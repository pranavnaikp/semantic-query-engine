# semantic_catalog/catalog.py - COMPLETE UPDATE

from semantic_catalog.models import (
    Entity, Dimension, Metric, Relationship,
    AggregationType, DataType, RelationshipType, DatabaseDialect
)
from semantic_catalog.models import SemanticCatalog


def create_multi_schema_catalog() -> SemanticCatalog:
    """
    Create catalog with multiple schemas:
    - sales schema: Transactional data
    - analytics schema: Aggregated data
    - reference schema: Lookup tables
    """
    catalog = SemanticCatalog()
    
    # ========== SALES SCHEMA (sales) ==========
    orders_entity = Entity(
        name="orders",
        description="Customer orders",
        schema_name="sales",
        table_name="orders",
        alias_prefix="o",
        dialect=DatabaseDialect.POSTGRESQL,
        primary_key="order_id"
    )
    
    orders_entity.add_dimension(Dimension(
        name="order_date",
        description="Order date",
        data_type=DataType.DATE,
        column_name="order_date",
        entity_name="orders",
        sql_expression="DATE_TRUNC('day', order_date)"  # PostgreSQL-specific
    ))
    
    orders_entity.add_metric(Metric(
        name="total_sales",
        description="Total sales amount",
        aggregation=AggregationType.SUM,
        sql_expression="amount_usd",
        entity_name="orders"
    ))
    
    orders_entity.add_metric(Metric(
        name="net_profit",
        description="Net profit after costs",
        aggregation=AggregationType.SUM,
        sql_expression="{sales.orders.amount_usd} - {sales.orders.cost_usd}",
        entity_name="orders"
    ))
    
    catalog.add_entity(orders_entity)
    
    # ========== REFERENCE SCHEMA (ref) ==========
    customers_entity = Entity(
        name="customers",
        description="Customer reference data",
        schema_name="ref",
        table_name="customers",
        alias_prefix="c",
        dialect=DatabaseDialect.POSTGRESQL,
        primary_key="customer_id"
    )
    
    customers_entity.add_dimension(Dimension(
        name="customer_name",
        description="Customer name",
        data_type=DataType.STRING,
        column_name="full_name",
        entity_name="customers"
    ))
    
    customers_entity.add_dimension(Dimension(
        name="country",
        description="Customer country",
        data_type=DataType.STRING,
        column_name="country_code",
        entity_name="customers",
        sql_expression="UPPER({ref.customers.country_code})"  # Schema reference
    ))
    
    catalog.add_entity(customers_entity)
    
    # ========== ANALYTICS SCHEMA (analytics) ==========
    customer_segments_entity = Entity(
        name="customer_segments",
        description="Customer segmentation",
        schema_name="analytics",
        table_name="customer_segments",
        alias_prefix="cs",
        dialect=DatabaseDialect.POSTGRESQL,
        primary_key="customer_id"
    )
    
    customer_segments_entity.add_dimension(Dimension(
        name="segment",
        description="Customer segment",
        data_type=DataType.STRING,
        column_name="segment_name",
        entity_name="customer_segments"
    ))
    
    catalog.add_entity(customer_segments_entity)
    
    # ========== CROSS-SCHEMA RELATIONSHIPS ==========
    # sales.orders → ref.customers
    orders_to_customers = Relationship(
        name="orders_to_customers",
        from_entity="orders",
        to_entity="customers",
        relationship_type=RelationshipType.MANY_TO_ONE,
        join_conditions=[
            {"left": "sales.orders.customer_id", "right": "ref.customers.customer_id"}
        ]
    )
    
    # ref.customers → analytics.customer_segments
    customers_to_segments = Relationship(
        name="customers_to_segments",
        from_entity="customers",
        to_entity="customer_segments",
        relationship_type=RelationshipType.ONE_TO_ONE,
        join_conditions=[
            {"left": "ref.customers.customer_id", "right": "analytics.customer_segments.customer_id"}
        ]
    )
    
    orders_entity.add_relationship(orders_to_customers)
    customers_entity.add_relationship(customers_to_segments)
    
    return catalog


# Example: Snowflake-specific catalog
def create_snowflake_catalog() -> SemanticCatalog:
    """Catalog for Snowflake with different schema structure."""
    catalog = SemanticCatalog()
    
    # Snowflake often uses database.schema.table
    orders_entity = Entity(
        name="orders",
        description="Orders in Snowflake",
        database="PROD_DB",
        schema_name="SALES",
        table_name="ORDERS",
        alias_prefix="ORD",
        dialect=DatabaseDialect.SNOWFLAKE,
        quote_identifiers=True  # Snowflake is case-sensitive
    )
    
    # Snowflake-specific SQL
    orders_entity.add_dimension(Dimension(
        name="order_month",
        description="Order month",
        data_type=DataType.DATE,
        column_name="ORDER_DATE",
        entity_name="orders",
        sql_expression="DATE_TRUNC('MONTH', ORDER_DATE)"  # Snowflake syntax
    ))
    
    catalog.add_entity(orders_entity)
    
    return catalog


# Global catalog instance
CATALOG = create_multi_schema_catalog()