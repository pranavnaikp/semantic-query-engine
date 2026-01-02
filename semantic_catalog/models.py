# semantic_catalog/models.py - COMPLETE UPDATE

from typing import Dict, List, Optional, Union, Any
from pydantic import BaseModel, Field, validator, root_validator
from datetime import datetime
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


class DatabaseDialect(str, Enum):
    POSTGRESQL = "postgresql"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"


class Entity(BaseModel):
    """
    Entity with full schema.table support and dialect-aware SQL generation.
    """
    name: str = Field(..., description="Unique name of the entity")
    description: str = Field("", description="Human-readable description")
    database: str = Field("default", description="Database connection name")
    schema_name: str = Field("public", description="Database schema name")
    table_name: str = Field(..., description="Physical table name in database")
    alias_prefix: str = Field("t", description="Prefix for table aliases")
    dialect: DatabaseDialect = Field(DatabaseDialect.POSTGRESQL, description="Database dialect")
    
    # Primary key and unique constraints
    primary_key: Optional[str] = Field(None, description="Primary key column name")
    unique_keys: List[List[str]] = Field(default_factory=list, description="Unique key columns")
    
    # Quoting rules per dialect
    quote_identifiers: bool = Field(True, description="Whether to quote identifiers")
    
    # Dimensions and metrics
    dimensions: Dict[str, 'Dimension'] = Field(default_factory=dict)
    metrics: Dict[str, 'Metric'] = Field(default_factory=dict)
    relationships: Dict[str, 'Relationship'] = Field(default_factory=dict)
    
    @property
    def fully_qualified_name(self) -> str:
        """Get fully qualified name: schema.table_name with proper quoting."""
        return self._quote_identifier(f"{self.schema_name}.{self.table_name}")
    
    def get_alias(self, index: int = 0) -> str:
        """Get unique table alias for this entity."""
        return f"{self.alias_prefix}{index if index > 0 else ''}"
    
    def quote_column(self, column_name: str) -> str:
        """Get quoted column name if needed."""
        return self._quote_identifier(column_name)
    
    def get_qualified_column(self, column_name: str, alias: Optional[str] = None) -> str:
        """Get fully qualified column: alias.column or schema.table.column"""
        quoted_column = self.quote_column(column_name)
        
        if alias:
            return f"{alias}.{quoted_column}"
        
        # Return fully qualified if no alias
        return f"{self.fully_qualified_name}.{quoted_column}"
    
    def _quote_identifier(self, identifier: str) -> str:
        """Quote identifier based on dialect and settings."""
        if not self.quote_identifiers:
            return identifier
        
        dialect_quote_map = {
            DatabaseDialect.POSTGRESQL: '"',
            DatabaseDialect.SNOWFLAKE: '"',
            DatabaseDialect.BIGQUERY: '`',
            DatabaseDialect.REDSHIFT: '"'
        }
        
        quote_char = dialect_quote_map.get(self.dialect, '"')
        
        # Handle schema.table.column
        if '.' in identifier:
            parts = identifier.split('.')
            quoted_parts = [f"{quote_char}{part}{quote_char}" for part in parts]
            return '.'.join(quoted_parts)
        
        return f"{quote_char}{identifier}{quote_char}"
    
    def get_dialect_specific_sql(self, sql_template: str) -> str:
        """Apply dialect-specific SQL transformations."""
        if self.dialect == DatabaseDialect.SNOWFLAKE:
            # Convert ILIKE to UPPER for case-insensitive search
            sql_template = sql_template.replace(" ILIKE ", " UPPER(column) LIKE UPPER('value') ")
        
        elif self.dialect == DatabaseDialect.BIGQUERY:
            # Handle BigQuery-specific functions
            sql_template = sql_template.replace("DATE_TRUNC('day'", "DATE_TRUNC(date_column, DAY")
            sql_template = sql_template.replace("::date", "DATE(")
        
        return sql_template
    
    def add_dimension(self, dimension: 'Dimension') -> None:
        """Add a dimension to this entity."""
        if dimension.entity_name != self.name:
            raise ValueError(f"Dimension belongs to entity {dimension.entity_name}, not {self.name}")
        self.dimensions[dimension.name] = dimension
    
    def add_metric(self, metric: 'Metric') -> None:
        """Add a metric to this entity."""
        if metric.entity_name != self.name:
            raise ValueError(f"Metric belongs to entity {metric.entity_name}, not {self.name}")
        self.metrics[metric.name] = metric
    
    def add_relationship(self, relationship: 'Relationship') -> None:
        """Add a relationship to this entity."""
        self.relationships[relationship.name] = relationship


class Dimension(BaseModel):
    """Dimension with schema-aware SQL expressions."""
    name: str = Field(..., description="Unique name of the dimension")
    description: str = Field("", description="Human-readable description")
    data_type: DataType = Field(..., description="Data type of the dimension")
    column_name: str = Field(..., description="Physical column name in database")
    entity_name: str = Field(..., description="Name of the entity this belongs to")
    
    # SQL expression that can reference other entities with schema
    sql_expression: Optional[str] = Field(
        None,
        description="SQL expression to compute dimension value. Can use {schema.table.column} syntax."
    )
    
    # Formatting
    format: Optional[str] = Field(None, description="Format string for display")
    
    def get_select_expression(self, entity: Entity, alias: str) -> str:
        """Get the SQL expression for SELECT clause with proper schema references."""
        if self.sql_expression:
            # Replace {schema.table.column} placeholders
            expr = self._resolve_schema_references(self.sql_expression)
            # Replace column name with aliased version
            expr = expr.replace(self.column_name, f"{alias}.{entity.quote_column(self.column_name)}")
            return f"{expr} AS {entity.quote_column(self.name)}"
        
        return f"{alias}.{entity.quote_column(self.column_name)} AS {entity.quote_column(self.name)}"
    
    def get_group_by_expression(self, entity: Entity, alias: str) -> str:
        """Get the SQL expression for GROUP BY clause."""
        if self.sql_expression:
            expr = self._resolve_schema_references(self.sql_expression)
            return expr.replace(self.column_name, f"{alias}.{entity.quote_column(self.column_name)}")
        
        return f"{alias}.{entity.quote_column(self.column_name)}"
    
    def _resolve_schema_references(self, expression: str) -> str:
        """Resolve {schema.table.column} references in SQL expressions."""
        import re
        
        # Pattern to match {schema.table.column} or {table.column}
        pattern = r'\{([^}]+)\}'
        
        def replace_match(match):
            reference = match.group(1)
            parts = reference.split('.')
            
            if len(parts) == 3:  # schema.table.column
                schema, table, column = parts
                return f'"{schema}"."{table}"."{column}"'
            elif len(parts) == 2:  # table.column
                table, column = parts
                return f'"{table}"."{column}"'
            else:  # Just column
                return f'"{reference}"'
        
        return re.sub(pattern, replace_match, expression)


class Metric(BaseModel):
    """Metric with schema-aware SQL expressions."""
    name: str = Field(..., description="Unique name of the metric")
    description: str = Field("", description="Human-readable description")
    aggregation: AggregationType = Field(..., description="Aggregation type")
    
    # SQL expression that can reference other entities with schema
    sql_expression: str = Field(
        ...,
        description="SQL expression that defines the measure. Can use {schema.table.column} syntax."
    )
    
    entity_name: str = Field(..., description="Name of the primary entity")
    time_dimension: Optional[str] = Field(None, description="Time dimension for this metric")
    required_dimensions: List[str] = Field(default_factory=list)
    format: str = Field("number", description="Format for display")
    
    def get_aggregation_expression(self, entity: Entity, alias: str) -> str:
        """Get SQL expression with aggregation and schema references."""
        # Resolve schema references in the expression
        resolved_expr = self._resolve_schema_references(self.sql_expression)
        
        # Replace column references with aliased versions
        # This is simplified - in production you'd parse the SQL properly
        resolved_expr = resolved_expr.replace(
            self._extract_base_column(self.sql_expression),
            f"{alias}.{entity.quote_column(self._extract_base_column(self.sql_expression))}"
        )
        
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
            return f"{agg_func} {resolved_expr}) AS {entity.quote_column(self.name)}"
        
        return f"{agg_func}({resolved_expr}) AS {entity.quote_column(self.name)}"
    
    def _resolve_schema_references(self, expression: str) -> str:
        """Resolve {schema.table.column} references."""
        import re
        
        pattern = r'\{([^}]+)\}'
        
        def replace_match(match):
            reference = match.group(1)
            parts = reference.split('.')
            
            if len(parts) == 3:
                schema, table, column = parts
                return f'"{schema}"."{table}"."{column}"'
            elif len(parts) == 2:
                table, column = parts
                return f'"{table}"."{column}"'
            else:
                return f'"{reference}"'
        
        return re.sub(pattern, replace_match, expression)
    
    def _extract_base_column(self, expression: str) -> str:
        """Extract the base column name from SQL expression (simplified)."""
        # Remove schema.table. references
        import re
        expr = re.sub(r'[\w_]+\.', '', expression)  # Remove schema. and table.
        expr = re.sub(r'\{.*?\}', '', expr)  # Remove {references}
        
        # Find column-like patterns
        columns = re.findall(r'[\w_]+', expr)
        return columns[0] if columns else "value"


class Relationship(BaseModel):
    """Relationship with schema-aware join conditions."""
    name: str = Field(..., description="Unique name for the relationship")
    from_entity: str = Field(..., description="Source entity name")
    to_entity: str = Field(..., description="Target entity name")
    relationship_type: RelationshipType = Field(..., description="Type of relationship")
    
    # Join conditions with schema support
    join_conditions: List[Dict[str, str]] = Field(
        ...,
        description="Join conditions as {'left': 'schema.table.column', 'right': 'schema.table.column'}"
    )
    
    def get_join_sql(
        self,
        catalog: 'SemanticCatalog',
        from_alias: str,
        to_alias: str
    ) -> str:
        """Generate SQL JOIN clause with schema support."""
        from_entity = catalog.get_entity(self.from_entity)
        to_entity = catalog.get_entity(self.to_entity)
        
        conditions = []
        for cond in self.join_conditions:
            left = cond.get('left')
            right = cond.get('right')
            
            if left and right:
                # Parse left side
                left_parts = left.split('.')
                if len(left_parts) == 1:
                    left_col = from_entity.quote_column(left_parts[0])
                    left_qualified = f"{from_alias}.{left_col}"
                else:
                    left_qualified = self._qualify_column(left, from_entity)
                
                # Parse right side
                right_parts = right.split('.')
                if len(right_parts) == 1:
                    right_col = to_entity.quote_column(right_parts[0])
                    right_qualified = f"{to_alias}.{right_col}"
                else:
                    right_qualified = self._qualify_column(right, to_entity)
                
                conditions.append(f"{left_qualified} = {right_qualified}")
        
        if not conditions:
            raise ValueError(f"No valid join conditions for relationship {self.name}")
        
        join_type = "LEFT JOIN" if self.relationship_type in [
            RelationshipType.ONE_TO_MANY,
            RelationshipType.MANY_TO_ONE
        ] else "INNER JOIN"
        
        return f"{join_type} {to_entity.fully_qualified_name} {to_alias} ON {' AND '.join(conditions)}"
    
    def _qualify_column(self, column_ref: str, entity: Entity) -> str:
        """Fully qualify a column reference."""
        parts = column_ref.split('.')
        
        if len(parts) == 3:  # schema.table.column
            schema, table, column = parts
            return f'"{schema}"."{table}"."{column}"'
        elif len(parts) == 2:  # table.column
            table, column = parts
            return f'"{table}"."{column}"'
        else:  # Just column
            return entity.quote_column(column_ref)


class SemanticCatalog(BaseModel):
    """Catalog with schema-aware entity management."""
    entities: Dict[str, Entity] = Field(default_factory=dict)
    
    def add_entity(self, entity: Entity) -> None:
        self.entities[entity.name] = entity
    
    def get_entity(self, entity_name: str) -> Entity:
        if entity_name not in self.entities:
            raise ValueError(f"Entity '{entity_name}' not found in catalog")
        return self.entities[entity_name]
    
    def get_dimension(self, dimension_name: str) -> Dimension:
        for entity in self.entities.values():
            if dimension_name in entity.dimensions:
                return entity.dimensions[dimension_name]
        raise ValueError(f"Dimension '{dimension_name}' not found in catalog")
    
    def get_metric(self, metric_name: str) -> Metric:
        for entity in self.entities.values():
            if metric_name in entity.metrics:
                return entity.metrics[metric_name]
        raise ValueError(f"Metric '{metric_name}' not found in catalog")
    
    def validate_join_path(self, from_entity: str, to_entity: str) -> List[Relationship]:
        """Find join path between entities, considering schemas."""
        if from_entity == to_entity:
            return []
        
        visited = set()
        path = []
        
        def dfs(current: str, target: str) -> bool:
            if current == target:
                return True
            
            visited.add(current)
            entity = self.get_entity(current)
            
            for rel in entity.relationships.values():
                next_entity = rel.to_entity if rel.from_entity == current else rel.from_entity
                
                if next_entity not in visited:
                    path.append(rel)
                    if dfs(next_entity, target):
                        return True
                    path.pop()
            
            return False
        
        if dfs(from_entity, to_entity):
            return path
        
        raise ValueError(f"No join path from '{from_entity}' to '{to_entity}'")