"""
Catalog initialization and pre-defined semantic models.
In production, this would load from YAML/DB, but for MVP we define in code.
"""

from semantic_catalog.models import (
    Entity, Dimension, Metric, Relationship, 
    AggregationType, DataType, RelationshipType
)
from semantic_catalog.models import SemanticCatalog


def create_sample_catalog() -> SemanticCatalog:
    """
    Create a sample semantic catalog with orders and users entities.
    This is our business logic definition - single source of truth.
    """
    catalog = SemanticCatalog()
    
    # ========== USERS ENTITY ==========
    users_entity = Entity(
        name="users",
        description="Users/Customers of the platform",
        table_name="users",
        primary_key="id"
    )
    
    # User dimensions
    users_entity.add_dimension(Dimension(
        name="user_id",
        description="Unique user identifier",
        data_type=DataType.NUMBER,
        column_name="id",
        entity_name="users"
    ))
    
    users_entity.add_dimension(Dimension(
        name="country",
        description="User's country",
        data_type=DataType.STRING,
        column_name="country_code",
        entity_name="users"
    ))
    
    users_entity.add_dimension(Dimension(
        name="registration_date",
        description="Date user registered",
        data_type=DataType.DATE,
        column_name="created_at",
        entity_name="users",
        sql_expression="DATE(created_at)"  # Extract date part
    ))
    
    users_entity.add_dimension(Dimension(
        name="user_segment",
        description="User segmentation (free, premium, enterprise)",
        data_type=DataType.STRING,
        column_name="segment",
        entity_name="users"
    ))
    
    # User metrics
    users_entity.add_metric(Metric(
        name="user_count",
        description="Total number of users",
        aggregation=AggregationType.COUNT,
        sql_expression="id",
        entity_name="users",
        time_dimension="registration_date"
    ))
    
    catalog.add_entity(users_entity)
    
    # ========== ORDERS ENTITY ==========
    orders_entity = Entity(
        name="orders",
        description="Customer orders",
        table_name="orders",
        primary_key="id"
    )
    
    # Order dimensions
    orders_entity.add_dimension(Dimension(
        name="order_id",
        description="Unique order identifier",
        data_type=DataType.NUMBER,
        column_name="id",
        entity_name="orders"
    ))
    
    orders_entity.add_dimension(Dimension(
        name="order_date",
        description="Date order was placed",
        data_type=DataType.DATE,
        column_name="created_at",
        entity_name="orders",
        sql_expression="DATE(created_at)"
    ))
    
    orders_entity.add_dimension(Dimension(
        name="order_status",
        description="Status of the order",
        data_type=DataType.STRING,
        column_name="status",
        entity_name="orders"
    ))
    
    orders_entity.add_dimension(Dimension(
        name="product_category",
        description="Category of product ordered",
        data_type=DataType.STRING,
        column_name="category",
        entity_name="orders"
    ))
    
    # Order metrics
    orders_entity.add_metric(Metric(
        name="revenue",
        description="Total revenue (gross)",
        aggregation=AggregationType.SUM,
        sql_expression="amount",
        entity_name="orders",
        time_dimension="order_date",
        format="currency"
    ))
    
    orders_entity.add_metric(Metric(
        name="net_profit",
        description="Net profit after costs",
        aggregation=AggregationType.SUM,
        sql_expression="amount - cost",  # Business logic defined here!
        entity_name="orders",
        time_dimension="order_date",
        format="currency"
    ))
    
    orders_entity.add_metric(Metric(
        name="order_count",
        description="Total number of orders",
        aggregation=AggregationType.COUNT,
        sql_expression="id",
        entity_name="orders",
        time_dimension="order_date"
    ))
    
    orders_entity.add_metric(Metric(
        name="average_order_value",
        description="Average revenue per order",
        aggregation=AggregationType.AVG,
        sql_expression="amount",
        entity_name="orders",
        format="currency"
    ))
    
    orders_entity.add_metric(Metric(
        name="unique_customers",
        description="Count of distinct customers who placed orders",
        aggregation=AggregationType.COUNT_DISTINCT,
        sql_expression="user_id",
        entity_name="orders",
        time_dimension="order_date"
    ))
    
    catalog.add_entity(orders_entity)
    
    # ========== RELATIONSHIPS ==========
    # Users ←→ Orders (one-to-many)
    users_orders_rel = Relationship(
        name="users_to_orders",
        from_entity="users",
        to_entity="orders",
        relationship_type=RelationshipType.ONE_TO_MANY,
        join_conditions=[
            {"left": "id", "right": "user_id"}
        ]
    )
    
    orders_users_rel = Relationship(
        name="orders_to_users",
        from_entity="orders",
        to_entity="users",
        relationship_type=RelationshipType.MANY_TO_ONE,
        join_conditions=[
            {"left": "user_id", "right": "id"}
        ]
    )
    
    users_entity.add_relationship(users_orders_rel)
    orders_entity.add_relationship(orders_users_rel)
    
    return catalog


# Global catalog instance
CATALOG = create_sample_catalog()