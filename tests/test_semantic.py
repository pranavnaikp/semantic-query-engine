"""
Tests for semantic catalog and models.
"""

import pytest
from semantic_catalog.models import (
    Dimension, Metric, Entity, Relationship, SemanticCatalog,
    AggregationType, DataType, RelationshipType
)


class TestDimension:
    """Test Dimension model."""
    
    def test_dimension_creation(self):
        """Test basic dimension creation."""
        dim = Dimension(
            name="test_dim",
            description="Test dimension",
            data_type=DataType.STRING,
            column_name="test_col",
            entity_name="test_entity"
        )
        
        assert dim.name == "test_dim"
        assert dim.data_type == DataType.STRING
        assert dim.column_name == "test_col"
    
    def test_dimension_with_expression(self):
        """Test dimension with SQL expression."""
        dim = Dimension(
            name="year",
            data_type=DataType.NUMBER,
            column_name="created_at",
            entity_name="orders",
            sql_expression="EXTRACT(YEAR FROM created_at)"
        )
        
        assert dim.sql_expression == "EXTRACT(YEAR FROM created_at)"


class TestMetric:
    """Test Metric model."""
    
    def test_metric_creation(self):
        """Test basic metric creation."""
        metric = Metric(
            name="revenue",
            description="Total revenue",
            aggregation=AggregationType.SUM,
            sql_expression="amount",
            entity_name="orders"
        )
        
        assert metric.name == "revenue"
        assert metric.aggregation == AggregationType.SUM
        assert metric.sql_expression == "amount"
    
    def test_metric_aggregation_expression(self):
        """Test metric aggregation expression generation."""
        metric = Metric(
            name="total_sales",
            aggregation=AggregationType.SUM,
            sql_expression="sales_amount",
            entity_name="orders"
        )
        
        # Mock entity for testing
        class MockEntity:
            quote_column = lambda self, x: f'"{x}"'
        
        entity = MockEntity()
        expression = metric.get_aggregation_expression(entity, "o")
        
        assert "SUM" in expression
        assert "total_sales" in expression


class TestEntity:
    """Test Entity model."""
    
    def test_entity_creation(self):
        """Test basic entity creation."""
        entity = Entity(
            name="orders",
            description="Customer orders",
            schema_name="sales",
            table_name="orders",
            alias_prefix="o"
        )
        
        assert entity.name == "orders"
        assert entity.schema_name == "sales"
        assert entity.table_name == "orders"
        assert entity.fully_qualified_name == '"sales"."orders"'
    
    def test_entity_add_dimension(self):
        """Test adding dimension to entity."""
        entity = Entity(
            name="users",
            table_name="users",
            schema_name="public"
        )
        
        dim = Dimension(
            name="user_id",
            data_type=DataType.NUMBER,
            column_name="id",
            entity_name="users"
        )
        
        entity.add_dimension(dim)
        assert "user_id" in entity.dimensions
        assert entity.dimensions["user_id"] == dim


class TestRelationship:
    """Test Relationship model."""
    
    def test_relationship_creation(self):
        """Test basic relationship creation."""
        rel = Relationship(
            name="orders_to_users",
            from_entity="orders",
            to_entity="users",
            relationship_type=RelationshipType.MANY_TO_ONE,
            join_conditions=[
                {"left": "user_id", "right": "id"}
            ]
        )
        
        assert rel.name == "orders_to_users"
        assert rel.from_entity == "orders"
        assert rel.relationship_type == RelationshipType.MANY_TO_ONE


class TestSemanticCatalog:
    """Test SemanticCatalog functionality."""
    
    def test_catalog_creation(self):
        """Test catalog creation and entity management."""
        catalog = SemanticCatalog()
        
        entity = Entity(
            name="test_entity",
            table_name="test_table",
            schema_name="test_schema"
        )
        
        catalog.add_entity(entity)
        assert "test_entity" in catalog.entities
        assert catalog.get_entity("test_entity") == entity
    
    def test_catalog_validation(self):
        """Test catalog validation logic."""
        catalog = SemanticCatalog()
        
        # Create two entities with a relationship
        users = Entity(
            name="users",
            table_name="users",
            schema_name="public"
        )
        
        orders = Entity(
            name="orders",
            table_name="orders",
            schema_name="sales"
        )
        
        # Add relationship
        rel = Relationship(
            name="orders_to_users",
            from_entity="orders",
            to_entity="users",
            relationship_type=RelationshipType.MANY_TO_ONE,
            join_conditions=[{"left": "user_id", "right": "id"}]
        )
        
        orders.add_relationship(rel)
        
        catalog.add_entity(users)
        catalog.add_entity(orders)
        
        # Test join path exists
        assert catalog._join_path_exists("orders", "users")
        assert not catalog._join_path_exists("users", "nonexistent")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])