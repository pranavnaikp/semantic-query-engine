"""
Semantic catalog package - the single source of truth for business logic.
Defines metrics, dimensions, entities, and relationships.
"""

from semantic_catalog.models import (
    AggregationType,
    DataType,
    RelationshipType,
    DatabaseDialect,
    Dimension,
    Metric,
    Relationship,
    Entity,
    SemanticCatalog
)

from semantic_catalog.catalog import (
    create_sample_catalog,
    create_multi_schema_catalog,
    create_snowflake_catalog,
    CATALOG
)

__all__ = [
    'AggregationType',
    'DataType',
    'RelationshipType',
    'DatabaseDialect',
    'Dimension',
    'Metric',
    'Relationship',
    'Entity',
    'SemanticCatalog',
    'create_sample_catalog',
    'create_multi_schema_catalog',
    'create_snowflake_catalog',
    'CATALOG'
]

__version__ = "2.0.0"