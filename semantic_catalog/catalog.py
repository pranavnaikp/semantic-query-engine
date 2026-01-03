"""
Catalog initialization - SIMPLIFIED VERSION
"""

from semantic_catalog.models import (
    Entity, Dimension, Metric, Relationship,
    AggregationType, DataType, RelationshipType
)
from semantic_catalog.models import SemanticCatalog


def create_sample_catalog() -> SemanticCatalog:
    """Create a simple sample catalog."""
    catalog = SemanticCatalog()
    
    # Users entity
    users_entity = Entity(
        name="users",
        description="Users/Customers",
        schema_name="public",
        table_name="users",
        alias_prefix="u",
        primary_key="id"
    )
    
    users_entity.dimensions = {
        "user_id": Dimension(
            name="user_id",
            description="User ID",
            data_type=DataType.NUMBER,
            column_name="id",
            entity_name="users"
        ),
        "country": Dimension(
            name="country",
            description="Country",
            data_type=DataType.STRING,
            column_name="country_code",
            entity_name="users"
        ),
        "registration_date": Dimension(
            name="registration_date",
            description="Registration date",
            data_type=DataType.DATE,
            column_name="created_at",
            entity_name="users",
            sql_expression="DATE(created_at)"
        )
    }
    
    users_entity.metrics = {
        "user_count": Metric(
            name="user_count",
            description="User count",
            aggregation=AggregationType.COUNT,
            sql_expression="id",
            entity_name="users"
        )
    }
    
    # Orders entity
    orders_entity = Entity(
        name="orders",
        description="Customer orders",
        schema_name="public",
        table_name="orders",
        alias_prefix="o",
        primary_key="id"
    )
    
    orders_entity.dimensions = {
        "order_id": Dimension(
            name="order_id",
            description="Order ID",
            data_type=DataType.NUMBER,
            column_name="id",
            entity_name="orders"
        ),
        "order_date": Dimension(
            name="order_date",
            description="Order date",
            data_type=DataType.DATE,
            column_name="created_at",
            entity_name="orders",
            sql_expression="DATE(created_at)"
        ),
        "product_category": Dimension(
            name="product_category",
            description="Product category",
            data_type=DataType.STRING,
            column_name="category",
            entity_name="orders"
        )
    }
    
    orders_entity.metrics = {
        "revenue": Metric(
            name="revenue",
            description="Revenue",
            aggregation=AggregationType.SUM,
            sql_expression="amount",
            entity_name="orders",
            time_dimension="order_date",
            format="currency"
        ),
        "net_profit": Metric(
            name="net_profit",
            description="Net profit",
            aggregation=AggregationType.SUM,
            sql_expression="amount - cost",
            entity_name="orders",
            time_dimension="order_date",
            format="currency"
        ),
        "order_count": Metric(
            name="order_count",
            description="Order count",
            aggregation=AggregationType.COUNT,
            sql_expression="id",
            entity_name="orders",
            time_dimension="order_date"
        )
    }
    
    # Add relationships
    orders_to_users = Relationship(
        name="orders_to_users",
        from_entity="orders",
        to_entity="users",
        relationship_type=RelationshipType.MANY_TO_ONE,
        join_conditions=[{"left": "user_id", "right": "id"}]
    )
    
    orders_entity.relationships["orders_to_users"] = orders_to_users
    
    # Add entities to catalog
    catalog.entities["users"] = users_entity
    catalog.entities["orders"] = orders_entity
    
    return catalog


# Global catalog instance
CATALOG = create_sample_catalog()