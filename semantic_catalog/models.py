"""
Semantic Catalog Models
Defines the structure of our semantic layer - the single source of truth.
Inspired by Cube.js but simplified for MVP.
"""

from typing import Dict, List, Optional, Union, Any
from pydantic import BaseModel, Field, validator
from datetime import datetime
from enum import Enum


class AggregationType(str, Enum):
    """Supported aggregation types. Extend as needed."""
    SUM = "sum"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    AVG = "avg"
    MIN = "min"
    MAX = "max"


class DataType(str, Enum):
    """Supported data types for dimensions."""
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"


class RelationshipType(str, Enum):
    """Types of relationships between entities."""
    ONE_TO_MANY = "one_to_many"  # orders.user_id → users.id
    MANY_TO_ONE = "many_to_one"  # users.id → orders.user_id
    ONE_TO_ONE = "one_to_one"


class Dimension(BaseModel):
    """
    A dimension represents a column or derived attribute that can be used for grouping.
    Dimensions are the 'by' in 'revenue by country'.
    """
    name: str = Field(..., description="Unique name of the dimension")
    description: str = Field("", description="Human-readable description")
    data_type: DataType = Field(..., description="Data type of the dimension")
    column_name: str = Field(..., description="Physical column name in database")
    entity_name: str = Field(..., description="Name of the entity this belongs to")
    
    # Optional transformation (e.g., extract year from date)
    sql_expression: Optional[str] = Field(
        None, 
        description="SQL expression to compute dimension value. If None, uses column_name."
    )
    
    # Formatting hints for visualization
    format: Optional[str] = Field(None, description="Format string for display")
    
    def get_select_expression(self) -> str:
        """Get the SQL expression for SELECT clause."""
        if self.sql_expression:
            return f"{self.sql_expression} AS {self.name}"
        return f"{self.column_name} AS {self.name}"
    
    def get_group_by_expression(self) -> str:
        """Get the SQL expression for GROUP BY clause."""
        if self.sql_expression:
            return self.sql_expression
        return self.column_name


class Metric(BaseModel):
    """
    A metric represents a business measure that can be aggregated.
    Metrics are the 'what' in 'show me revenue'.
    """
    name: str = Field(..., description="Unique name of the metric")
    description: str = Field("", description="Human-readable description")
    aggregation: AggregationType = Field(..., description="Aggregation type")
    
    # SQL expression for the base measure (before aggregation)
    # Example: "revenue - cost" for net_profit
    sql_expression: str = Field(
        ...,
        description="SQL expression that defines the measure to aggregate"
    )
    
    # Entity this metric belongs to (primary entity)
    entity_name: str = Field(..., description="Name of the primary entity")
    
    # Time dimension for time-based queries (optional)
    time_dimension: Optional[str] = Field(
        None,
        description="Name of the time dimension for this metric (e.g., 'order_date')"
    )
    
    # Required dimensions (for validation)
    required_dimensions: List[str] = Field(
        default_factory=list,
        description="Dimensions that must be included when querying this metric"
    )
    
    # Formatting
    format: str = Field("number", description="Format for display (number, currency, percent)")
    
    def get_aggregation_expression(self) -> str:
        """Get the SQL expression with aggregation."""
        agg_map = {
            AggregationType.SUM: "SUM",
            AggregationType.COUNT: "COUNT",
            AggregationType.COUNT_DISTINCT: "COUNT(DISTINCT",
            AggregationType.AVG: "AVG",
            AggregationType.MIN: "MIN",
            AggregationType.MAX: "MAX",
        }
        
        agg_func = agg_map[self.aggregation]
        
        if self.aggregation == AggregationType.COUNT_DISTINCT:
            # COUNT(DISTINCT expr) syntax
            return f"{agg_func} {self.sql_expression}) AS {self.name}"
        
        return f"{agg_func}({self.sql_expression}) AS {self.name}"


class Relationship(BaseModel):
    """
    Defines how entities relate to each other for JOIN operations.
    """
    name: str = Field(..., description="Unique name for the relationship")
    from_entity: str = Field(..., description="Source entity name")
    to_entity: str = Field(..., description="Target entity name")
    relationship_type: RelationshipType = Field(..., description="Type of relationship")
    
    # JOIN conditions
    join_conditions: List[Dict[str, str]] = Field(
        ...,
        description="List of join conditions as {'left': 'column', 'right': 'column'}"
    )
    
    def get_join_sql(self, from_alias: str = "a", to_alias: str = "b") -> str:
        """Generate SQL JOIN clause."""
        conditions = []
        for cond in self.join_conditions:
            left = cond.get('left')
            right = cond.get('right')
            if left and right:
                conditions.append(f"{from_alias}.{left} = {to_alias}.{right}")
        
        if not conditions:
            raise ValueError(f"No valid join conditions for relationship {self.name}")
        
        join_type = "LEFT JOIN" if self.relationship_type in [RelationshipType.ONE_TO_MANY, RelationshipType.MANY_TO_ONE] else "INNER JOIN"
        
        return f"{join_type} {self.to_entity} {to_alias} ON {' AND '.join(conditions)}"



# semantic_catalog/models.py (UPDATED)

class Entity(BaseModel):
    """
    An entity represents a business concept with full database qualification.
    """
    name: str = Field(..., description="Unique name of the entity")
    description: str = Field("", description="Human-readable description")
    schema_name: str = Field("public", description="Database schema name")
    table_name: str = Field(..., description="Physical table name in database")
    alias_prefix: str = Field("e", description="Prefix for table aliases")
    
    # Primary key column
    primary_key: Optional[str] = Field(None, description="Primary key column name")
    
    # Database-specific quoting
    quote_identifiers: bool = Field(True, description="Whether to quote identifiers")
    
    @property
    def fully_qualified_table(self) -> str:
        """Get fully qualified table name: schema.table_name"""
        if self.quote_identifiers:
            return f'"{self.schema_name}"."{self.table_name}"'
        return f"{self.schema_name}.{self.table_name}"
    
    def get_alias(self, index: int = 0) -> str:
        """Get unique table alias for this entity."""
        return f"{self.alias_prefix}_{index}"
    
    def quote_column(self, column_name: str) -> str:
        """Get quoted column name if needed."""
        if self.quote_identifiers:
            return f'"{column_name}"'
        return column_name
    
    def get_qualified_column(self, column_name: str, alias: Optional[str] = None) -> str:
        """Get fully qualified column: alias.column or table.column"""
        col = self.quote_column(column_name)
        if alias:
            return f"{alias}.{col}"
        return f"{self.fully_qualified_table}.{col}"
    
    
class SemanticCatalog(BaseModel):
    """
    The main catalog storing all semantic definitions.
    This is our single source of truth for business logic.
    """
    entities: Dict[str, Entity] = Field(default_factory=dict)
    
    def add_entity(self, entity: Entity) -> None:
        """Add an entity to the catalog."""
        self.entities[entity.name] = entity
    
    def get_entity(self, entity_name: str) -> Entity:
        """Get an entity by name."""
        if entity_name not in self.entities:
            raise ValueError(f"Entity '{entity_name}' not found in catalog")
        return self.entities[entity_name]
    
    def get_dimension(self, dimension_name: str) -> Dimension:
        """Get a dimension by name (searches across all entities)."""
        for entity in self.entities.values():
            if dimension_name in entity.dimensions:
                return entity.dimensions[dimension_name]
        raise ValueError(f"Dimension '{dimension_name}' not found in catalog")
    
    def get_metric(self, metric_name: str) -> Metric:
        """Get a metric by name (searches across all entities)."""
        for entity in self.entities.values():
            if metric_name in entity.metrics:
                return entity.metrics[metric_name]
        raise ValueError(f"Metric '{metric_name}' not found in catalog")
    
    def validate_metric_dimension_combo(self, metric_name: str, dimension_names: List[str]) -> None:
        """
        Validate that a metric can be queried with given dimensions.
        Checks join paths and required dimensions.
        """
        metric = self.get_metric(metric_name)
        primary_entity = self.get_entity(metric.entity_name)
        
        # Check required dimensions
        for required_dim in metric.required_dimensions:
            if required_dim not in dimension_names:
                raise ValueError(
                    f"Metric '{metric_name}' requires dimension '{required_dim}'"
                )
        
        # For each dimension, check if join path exists from metric's entity
        for dim_name in dimension_names:
            dimension = self.get_dimension(dim_name)
            if dimension.entity_name != primary_entity.name:
                # Need to check if there's a join path
                if not self._join_path_exists(primary_entity.name, dimension.entity_name):
                    raise ValueError(
                        f"No join path from entity '{primary_entity.name}' "
                        f"(metric '{metric_name}') to entity '{dimension.entity_name}' "
                        f"(dimension '{dim_name}')"
                    )
    
    def _join_path_exists(self, from_entity: str, to_entity: str, visited: set = None) -> bool:
        """Check if there's a join path between two entities (BFS)."""
        if from_entity == to_entity:
            return True
        
        if visited is None:
            visited = set()
        
        visited.add(from_entity)
        
        entity = self.get_entity(from_entity)
        
        # Check direct relationships
        for rel in entity.relationships.values():
            next_entity = rel.to_entity if rel.from_entity == from_entity else rel.from_entity
            
            if next_entity not in visited:
                if self._join_path_exists(next_entity, to_entity, visited):
                    return True
        
        return False