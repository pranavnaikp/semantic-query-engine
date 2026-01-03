"""
Semantic catalog models - SIMPLIFIED VERSION
"""

from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from enum import Enum


class AggregationType(str, Enum):
    SUM = "sum"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    AVG = "avg"
    MIN = "min"
    MAX = "max"


class DataType(str, Enum):
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"


class RelationshipType(str, Enum):
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    ONE_TO_ONE = "one_to_one"


# REMOVED DatabaseDialect - using simple string
# class DatabaseDialect(str, Enum):
#     POSTGRESQL = "postgresql"


class Dimension(BaseModel):
    name: str = Field(..., description="Dimension name")
    description: str = Field("", description="Description")
    data_type: DataType = Field(..., description="Data type")
    column_name: str = Field(..., description="Column name")
    entity_name: str = Field(..., description="Entity name")
    sql_expression: Optional[str] = Field(None, description="SQL expression")
    format: Optional[str] = Field(None, description="Display format")


class Metric(BaseModel):
    name: str = Field(..., description="Metric name")
    description: str = Field("", description="Description")
    aggregation: AggregationType = Field(..., description="Aggregation type")
    sql_expression: str = Field(..., description="SQL expression")
    entity_name: str = Field(..., description="Entity name")
    time_dimension: Optional[str] = Field(None, description="Time dimension")
    required_dimensions: List[str] = Field(default_factory=list)
    format: str = Field("number", description="Display format")


class Relationship(BaseModel):
    name: str = Field(..., description="Relationship name")
    from_entity: str = Field(..., description="From entity")
    to_entity: str = Field(..., description="To entity")
    relationship_type: RelationshipType = Field(..., description="Type")
    join_conditions: List[Dict[str, str]] = Field(
        ...,
        description="Join conditions"
    )


class Entity(BaseModel):
    name: str = Field(..., description="Entity name")
    description: str = Field("", description="Description")
    schema_name: str = Field("public", description="Schema name")
    table_name: str = Field(..., description="Table name")
    alias_prefix: str = Field("t", description="Alias prefix")
    primary_key: Optional[str] = Field(None, description="Primary key")
    
    dimensions: Dict[str, Dimension] = Field(default_factory=dict)
    metrics: Dict[str, Metric] = Field(default_factory=dict)
    relationships: Dict[str, Relationship] = Field(default_factory=dict)


class SemanticCatalog(BaseModel):
    entities: Dict[str, Entity] = Field(default_factory=dict)