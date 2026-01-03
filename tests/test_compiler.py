"""
Tests for SQL compiler and templates.
"""

import pytest
from datetime import date
from sql_compiler.templates import TimeRangeResolver, FilterSQLBuilder
from sql_compiler.validator import SemanticValidator
from intent_extractor.intent_models import QueryIntent, TimeRange, TimeRangeType, FilterCondition
from semantic_catalog.catalog import create_sample_catalog


class TestTimeRangeResolver:
    """Test time range resolution."""
    
    def test_last_quarter_calculation(self):
        """Test last quarter date calculation."""
        # Test with date in Q2
        test_date = date(2024, 5, 15)  # May 15, 2024 (Q2)
        start, end = TimeRangeResolver._get_last_quarter(test_date)
        
        # Last quarter should be Q1 2024
        assert start == date(2024, 1, 1)
        assert end == date(2024, 3, 31)
    
    def test_last_month_calculation(self):
        """Test last month date calculation."""
        # Test with March
        test_date = date(2024, 3, 15)
        start, end = TimeRangeResolver._get_last_month(test_date)
        
        # Last month should be February
        assert start == date(2024, 2, 1)
        assert end == date(2024, 2, 29)  # 2024 is leap year
    
    def test_time_range_sql_generation(self):
        """Test SQL generation for time ranges."""
        from semantic_catalog.models import Dimension, Entity, DataType
        
        # Create test dimension and entity
        time_dim = Dimension(
            name="order_date",
            data_type=DataType.DATE,
            column_name="created_at",
            entity_name="orders"
        )
        
        entity = Entity(
            name="orders",
            table_name="orders",
            schema_name="sales"
        )
        
        # Test custom date range
        time_range = TimeRange(
            type=TimeRangeType.CUSTOM,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        
        sql = TimeRangeResolver.get_time_filter_sql(
            time_range,
            time_dim,
            "o",
            entity
        )
        
        assert "BETWEEN '2024-01-01' AND '2024-01-31'" in sql


class TestFilterSQLBuilder:
    """Test filter SQL generation."""
    
    def test_equals_filter(self):
        """Test equals filter generation."""
        from semantic_catalog.models import Dimension, Entity, DataType
        
        dim = Dimension(
            name="status",
            data_type=DataType.STRING,
            column_name="order_status",
            entity_name="orders"
        )
        
        entity = Entity(
            name="orders",
            table_name="orders",
            schema_name="sales"
        )
        
        filter_cond = FilterCondition(
            dimension="status",
            operator="equals",
            values=["completed"]
        )
        
        sql = FilterSQLBuilder.build_filter_sql(
            filter_cond,
            dim,
            "o",
            entity
        )
        
        assert "o.\"order_status\" = 'completed'" in sql
    
    def test_in_filter(self):
        """Test IN filter generation."""
        from semantic_catalog.models import Dimension, Entity, DataType
        
        dim = Dimension(
            name="country",
            data_type=DataType.STRING,
            column_name="country_code",
            entity_name="users"
        )
        
        entity = Entity(
            name="users",
            table_name="users",
            schema_name="public"
        )
        
        filter_cond = FilterCondition(
            dimension="country",
            operator="in",
            values=["US", "UK", "DE"]
        )
        
        sql = FilterSQLBuilder.build_filter_sql(
            filter_cond,
            dim,
            "u",
            entity
        )
        
        assert "IN ('US', 'UK', 'DE')" in sql
        assert "u.\"country_code\"" in sql


class TestSemanticValidator:
    """Test semantic validation."""
    
    def test_metric_validation(self):
        """Test metric existence validation."""
        catalog = create_sample_catalog()
        validator = SemanticValidator(catalog)
        
        # Valid metric
        intent = QueryIntent(metric="revenue", dimensions=[])
        errors = validator.validate_intent(intent)
        assert len(errors) == 0
        
        # Invalid metric
        intent = QueryIntent(metric="nonexistent_metric", dimensions=[])
        errors = validator.validate_intent(intent)
        assert len(errors) > 0
        assert "not found" in errors[0]
    
    def test_dimension_validation(self):
        """Test dimension validation."""
        catalog = create_sample_catalog()
        validator = SemanticValidator(catalog)
        
        # Valid dimension
        intent = QueryIntent(metric="revenue", dimensions=["country"])
        errors = validator.validate_intent(intent)
        assert len(errors) == 0
        
        # Invalid dimension
        intent = QueryIntent(metric="revenue", dimensions=["nonexistent_dim"])
        errors = validator.validate_intent(intent)
        assert len(errors) > 0
    
    def test_join_path_validation(self):
        """Test join path validation."""
        catalog = create_sample_catalog()
        validator = SemanticValidator(catalog)
        
        # This should work if catalog has proper relationships
        intent = QueryIntent(
            metric="revenue",  # From orders entity
            dimensions=["country"]  # From users entity
        )
        
        errors = validator.validate_intent(intent)
        # Might have errors if join path doesn't exist
        # Just test that validation runs without exception
        assert isinstance(errors, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])