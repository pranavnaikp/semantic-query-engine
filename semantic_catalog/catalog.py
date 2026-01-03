"""
Semantic catalog mapping natural language to actual database schema.
Updated for your PostgreSQL tables.
"""

from typing import Dict, List, Optional
from enum import Enum
from dataclasses import dataclass
from pydantic import BaseModel


class AggregationType(str, Enum):
    SUM = "SUM"
    AVG = "AVG"
    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    MIN = "MIN"
    MAX = "MAX"


class DataType(str, Enum):
    STRING = "STRING"
    NUMBER = "NUMBER"
    DATE = "DATE"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"


@dataclass
class Dimension:
    name: str
    description: str
    data_type: DataType
    column_name: str  # Actual database column
    table_name: str   # Actual database table
    schema_name: str  # Actual database schema


@dataclass
class Metric:
    name: str
    description: str
    aggregation: AggregationType
    column_name: str  # Actual database column
    table_name: str   # Actual database table
    schema_name: str  # Actual database schema
    data_type: DataType = DataType.NUMBER


@dataclass
class Entity:
    name: str
    description: str
    primary_table: str
    primary_schema: str
    metrics: Dict[str, Metric]
    dimensions: Dict[str, Dimension]
    joins: List[Dict]  # Join paths to other entities


class SemanticCatalog:
    """Catalog of available metrics and dimensions."""
    
    def __init__(self):
        # Initialize entities
        self.entities = self._create_entities()
        # Create dimension name mapping
        self.dimension_name_map = self._create_dimension_name_map()
    
    def _create_dimension_name_map(self) -> Dict[str, str]:
        """Create mapping from natural language names to actual dimension names."""
        mapping = {}
        for entity in self.entities.values():
            for dim_name, dimension in entity.dimensions.items():
                # Add the dimension itself
                mapping[dim_name] = dim_name
                
                # Add common aliases
                if dim_name == "status":
                    mapping["order_status"] = dim_name
                    mapping["status"] = dim_name
                elif dim_name == "country_code":
                    mapping["country"] = dim_name
                    mapping["location"] = dim_name
                elif dim_name == "segment_name":
                    mapping["segment"] = dim_name
                    mapping["customer_segment"] = dim_name
                elif dim_name == "order_date":
                    mapping["date"] = dim_name
                    mapping["time"] = dim_name
                    mapping["month"] = dim_name
                elif dim_name == "full_name":
                    mapping["customer_name"] = dim_name
                    mapping["name"] = dim_name
        
        return mapping
    
    def _create_entities(self) -> Dict[str, Entity]:
        """Create entity definitions for your PostgreSQL tables."""
        
        # Sales Orders Entity
        orders_entity = Entity(
            name="orders",
            description="Sales orders data",
            primary_table="orders",
            primary_schema="sales",
            metrics={
                "revenue": Metric(
                    name="revenue",
                    description="Total order amount in USD",
                    aggregation=AggregationType.SUM,
                    column_name="amount_usd",
                    table_name="orders",
                    schema_name="sales",
                    data_type=DataType.NUMBER
                ),
                "order_count": Metric(
                    name="order_count",
                    description="Number of orders",
                    aggregation=AggregationType.COUNT,
                    column_name="order_id",
                    table_name="orders",
                    schema_name="sales",
                    data_type=DataType.NUMBER
                ),
                "average_order_value": Metric(
                    name="average_order_value",
                    description="Average order amount",
                    aggregation=AggregationType.AVG,
                    column_name="amount_usd",
                    table_name="orders",
                    schema_name="sales",
                    data_type=DataType.NUMBER
                ),
                "total_amount": Metric(
                    name="total_amount",
                    description="Total order amount",
                    aggregation=AggregationType.SUM,
                    column_name="amount_usd",
                    table_name="orders",
                    schema_name="sales",
                    data_type=DataType.NUMBER
                )
            },
            dimensions={
                "order_date": Dimension(
                    name="order_date",
                    description="Date when order was placed",
                    data_type=DataType.DATE,
                    column_name="order_date",
                    table_name="orders",
                    schema_name="sales"
                ),
                "status": Dimension(
                    name="status",
                    description="Order status",
                    data_type=DataType.STRING,
                    column_name="status",
                    table_name="orders",
                    schema_name="sales"
                ),
                "customer_id": Dimension(
                    name="customer_id",
                    description="Customer identifier",
                    data_type=DataType.NUMBER,
                    column_name="customer_id",
                    table_name="orders",
                    schema_name="sales"
                )
            },
            joins=[
                {
                    "target_entity": "customers",
                    "join_type": "LEFT JOIN",
                    "condition": "orders.customer_id = customers.customer_id"
                },
                {
                    "target_entity": "customer_segments",
                    "join_type": "LEFT JOIN",
                    "condition": "orders.customer_id = customer_segments.customer_id"
                }
            ]
        )
        
        # Customers Entity
        customers_entity = Entity(
            name="customers",
            description="Customer information",
            primary_table="customers",
            primary_schema="ref",
            metrics={
                "customer_count": Metric(
                    name="customer_count",
                    description="Number of customers",
                    aggregation=AggregationType.COUNT,
                    column_name="customer_id",
                    table_name="customers",
                    schema_name="ref",
                    data_type=DataType.NUMBER
                )
            },
            dimensions={
                "country_code": Dimension(
                    name="country_code",
                    description="Customer country code",
                    data_type=DataType.STRING,
                    column_name="country_code",
                    table_name="customers",
                    schema_name="ref"
                ),
                "country": Dimension(
                    name="country",
                    description="Customer country",
                    data_type=DataType.STRING,
                    column_name="country_code",
                    table_name="customers",
                    schema_name="ref"
                ),
                "full_name": Dimension(
                    name="full_name",
                    description="Customer name",
                    data_type=DataType.STRING,
                    column_name="full_name",
                    table_name="customers",
                    schema_name="ref"
                ),
                "email": Dimension(
                    name="email",
                    description="Customer email",
                    data_type=DataType.STRING,
                    column_name="email",
                    table_name="customers",
                    schema_name="ref"
                )
            },
            joins=[]
        )
        
        # Customer Segments Entity
        segments_entity = Entity(
            name="customer_segments",
            description="Customer segmentation data",
            primary_table="customer_segments",
            primary_schema="analytics",
            metrics={
                "total_lifetime_value": Metric(
                    name="total_lifetime_value",
                    description="Total lifetime value of customers",
                    aggregation=AggregationType.SUM,
                    column_name="lifetime_value",
                    table_name="customer_segments",
                    schema_name="analytics",
                    data_type=DataType.NUMBER
                ),
                "average_lifetime_value": Metric(
                    name="average_lifetime_value",
                    description="Average lifetime value per customer",
                    aggregation=AggregationType.AVG,
                    column_name="lifetime_value",
                    table_name="customer_segments",
                    schema_name="analytics",
                    data_type=DataType.NUMBER
                )
            },
            dimensions={
                "segment_name": Dimension(
                    name="segment_name",
                    description="Customer segment name",
                    data_type=DataType.STRING,
                    column_name="segment_name",
                    table_name="customer_segments",
                    schema_name="analytics"
                ),
                "segment": Dimension(
                    name="segment",
                    description="Customer segment",
                    data_type=DataType.STRING,
                    column_name="segment_name",
                    table_name="customer_segments",
                    schema_name="analytics"
                )
            },
            joins=[]
        )
        
        return {
            "orders": orders_entity,
            "customers": customers_entity,
            "customer_segments": segments_entity
        }
    
    def get_dimension(self, dimension_name: str) -> Optional[Dimension]:
        """Get dimension by name from any entity, with name mapping."""
        # First try to map the name
        mapped_name = self.dimension_name_map.get(dimension_name, dimension_name)
        
        for entity in self.entities.values():
            if mapped_name in entity.dimensions:
                return entity.dimensions[mapped_name]
        
        # If not found with mapping, try original name
        for entity in self.entities.values():
            if dimension_name in entity.dimensions:
                return entity.dimensions[dimension_name]
        
        return None
    
    def get_metric(self, metric_name: str) -> Optional[Metric]:
        """Get metric by name from any entity."""
        for entity in self.entities.values():
            if metric_name in entity.metrics:
                return entity.metrics[metric_name]
        return None
    
    def get_entity_for_metric(self, metric_name: str) -> Optional[Entity]:
        """Get entity that contains the given metric."""
        for entity_name, entity in self.entities.items():
            if metric_name in entity.metrics:
                return entity
        return None
    
    def get_all_metrics(self) -> List[Dict]:
        """Get all available metrics."""
        all_metrics = []
        for entity_name, entity in self.entities.items():
            for metric_name, metric in entity.metrics.items():
                all_metrics.append({
                    "name": metric_name,
                    "description": metric.description,
                    "entity": entity_name,
                    "aggregation": metric.aggregation.value
                })
        return all_metrics
    
    def get_all_dimensions(self) -> List[Dict]:
        """Get all available dimensions."""
        all_dimensions = []
        for entity_name, entity in self.entities.items():
            for dim_name, dimension in entity.dimensions.items():
                all_dimensions.append({
                    "name": dim_name,
                    "description": dimension.description,
                    "entity": entity_name,
                    "type": dimension.data_type.value
                })
        return all_dimensions


# Global catalog instance
CATALOG = SemanticCatalog()