"""
Integration tests for the complete pipeline.
"""

import pytest
import json
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient

from main import app
from semantic_catalog.catalog import create_sample_catalog
from intent_extractor.intent_models import QueryIntent, TimeRange, TimeRangeType
from sql_compiler.compiler import SQLCompiler


class TestIntegration:
    """Integration tests for the complete system."""
    
    def setup_method(self):
        """Setup before each test."""
        self.client = TestClient(app)
        self.catalog = create_sample_catalog()
        self.compiler = SQLCompiler(self.catalog)
    
    def test_natural_language_endpoint(self):
        """Test the natural language query endpoint."""
        # Mock the LLM extractor to return a predictable intent
        with patch('main.intent_extractor.extract_intent') as mock_extract:
            mock_extract.return_value = Mock(
                success=True,
                intent=QueryIntent(
                    metric="revenue",
                    dimensions=["country"],
                    time_range=TimeRange(type=TimeRangeType.LAST_QUARTER)
                ),
                error=None,
                raw_query="Show me revenue by country for last quarter"
            )
            
            response = self.client.post(
                "/nl-query",
                headers={"X-API-Key": "test_key"},
                json={"query": "Show me revenue by country for last quarter"}
            )
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] == True
            assert "data" in data
            assert "visualization" in data
    
    def test_structured_query_endpoint(self):
        """Test the structured query endpoint."""
        query_intent = {
            "metric": "revenue",
            "dimensions": ["country"],
            "time_range": {
                "type": "last_quarter"
            },
            "limit": 100
        }
        
        response = self.client.post(
            "/query",
            headers={"X-API-Key": "test_key"},
            json=query_intent
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        assert "sql" in data
        assert "data" in data
    
    def test_catalog_endpoints(self):
        """Test catalog information endpoints."""
        # Test metrics endpoint
        response = self.client.get(
            "/catalog/metrics",
            headers={"X-API-Key": "test_key"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        assert "metrics" in data
        
        # Test dimensions endpoint
        response = self.client.get(
            "/catalog/dimensions",
            headers={"X-API-Key": "test_key"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        assert "dimensions" in data
    
    def test_health_endpoint(self):
        """Test health check endpoint."""
        response = self.client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "components" in data
    
    def test_explain_query_endpoint(self):
        """Test query explanation endpoint."""
        response = self.client.get(
            "/explain-query",
            params={
                "metric": "revenue",
                "dimensions": "country",
                "time_range": "last_quarter"
            },
            headers={"X-API-Key": "test_key"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        assert "explanation" in data
        assert "sql_preview" in data["explanation"]
    
    def test_schema_endpoints(self):
        """Test schema-related endpoints."""
        # Test schemas endpoint
        response = self.client.get(
            "/schemas",
            headers={"X-API-Key": "test_key"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        assert "schemas" in data
    
    def test_error_handling(self):
        """Test error handling in the API."""
        # Test with invalid metric
        query_intent = {
            "metric": "nonexistent_metric",
            "dimensions": []
        }
        
        response = self.client.post(
            "/query",
            headers={"X-API-Key": "test_key"},
            json=query_intent
        )
        
        # Should return 400 with validation errors
        assert response.status_code == 400
        data = response.json()
        assert data["success"] == False
        assert "errors" in data.get("detail", {})
    
    def test_sql_compilation_integration(self):
        """Test SQL compilation with sample catalog."""
        intent = QueryIntent(
            metric="revenue",
            dimensions=["country", "order_date"],
            time_range=TimeRange(type=TimeRangeType.LAST_MONTH)
        )
        
        # This should not raise exceptions
        try:
            result = self.compiler.compile_sql(intent)
            assert "sql" in result
            assert "metadata" in result
            assert isinstance(result["sql"], str)
            assert len(result["sql"]) > 0
        except Exception as e:
            pytest.fail(f"SQL compilation failed: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])