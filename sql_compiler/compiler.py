# sql_compiler/compiler.py - COMPLETE UPDATE

from typing import List, Dict, Tuple, Set, Any
from collections import defaultdict
from semantic_catalog.catalog import CATALOG
from semantic_catalog.models import Entity
from intent_extractor.intent_models import QueryIntent
from sql_compiler.templates import SQLTemplates, TimeRangeResolver, FilterSQLBuilder


class SchemaAwareAliasManager:
    """
    Manages table aliases with schema awareness.
    Generates aliases like: sales_orders as o1, ref_customers as c1
    """
    
    def __init__(self):
        self.entity_aliases: Dict[str, str] = {}  # entity_name -> alias
        self.schema_entity_map: Dict[str, List[str]] = defaultdict(list)
        self.alias_counter: Dict[str, int] = defaultdict(int)
        self.used_aliases: Set[str] = set()
    
    def register_entity(self, entity_name: str, entity: Entity):
        """Register entity and its schema."""
        schema_key = f"{entity.schema_name}_{entity.table_name}"
        self.schema_entity_map[schema_key].append(entity_name)
    
    def get_alias(self, entity_name: str, entity: Entity) -> str:
        """Get or create alias for an entity."""
        if entity_name in self.entity_aliases:
            return self.entity_aliases[entity_name]
        
        # Generate alias based on schema and table
        base_alias = self._generate_base_alias(entity)
        
        # Ensure uniqueness
        counter = 0
        while True:
            alias = f"{base_alias}{counter if counter > 0 else ''}"
            if alias not in self.used_aliases:
                self.used_aliases.add(alias)
                self.entity_aliases[entity_name] = alias
                return alias
            counter += 1
    
    def _generate_base_alias(self, entity: Entity) -> str:
        """Generate base alias from schema and table name."""
        # Use first letter of schema and table
        schema_initials = ''.join([word[0] for word in entity.schema_name.split('_')])
        table_initials = ''.join([word[0] for word in entity.table_name.split('_')])
        
        return f"{schema_initials.lower()}{table_initials.lower()}"
    
    def get_all_aliases(self) -> Dict[str, str]:
        return self.entity_aliases.copy()


class SchemaJoinPathResolver:
    """
    Resolves JOIN paths considering schema boundaries.
    Prefers joins within same schema when possible.
    """
    
    def __init__(self, catalog: SemanticCatalog):
        self.catalog = catalog
        self.alias_manager = SchemaAwareAliasManager()
    
    def resolve_join_path(
        self,
        start_entity_name: str,
        target_entity_names: List[str]
    ) -> Tuple[List[str], Dict[str, str], List[str]]:
        """
        Resolve join path with schema awareness.
        Returns: (join_sqls, entity_to_alias_map, join_order)
        """
        # Register all entities
        all_entities = set([start_entity_name] + target_entity_names)
        for entity_name in all_entities:
            entity = self.catalog.get_entity(entity_name)
            self.alias_manager.register_entity(entity_name, entity)
        
        # Get alias for start entity
        start_entity = self.catalog.get_entity(start_entity_name)
        start_alias = self.alias_manager.get_alias(start_entity_name, start_entity)
        entity_aliases = {start_entity_name: start_alias}
        
        # Build joins using BFS with schema priority
        joins = []
        join_order = [start_entity_name]
        visited = {start_entity_name}
        
        queue = [(start_entity_name, start_entity)]
        
        while queue and len(visited) < len(all_entities):
            current_name, current_entity = queue.pop(0)
            
            # Try to join to target entities
            for target_name in target_entity_names:
                if target_name in visited:
                    continue
                
                try:
                    # Find join path
                    join_path = self.catalog.validate_join_path(current_name, target_name)
                    
                    # Execute the join path
                    for rel in join_path:
                        from_entity_name = rel.from_entity
                        to_entity_name = rel.to_entity
                        
                        # Skip if already visited
                        if from_entity_name in visited and to_entity_name in visited:
                            continue
                        
                        # Determine which is new
                        if from_entity_name in visited:
                            new_entity_name = to_entity_name
                            from_entity = self.catalog.get_entity(from_entity_name)
                            to_entity = self.catalog.get_entity(to_entity_name)
                            from_alias = entity_aliases[from_entity_name]
                            to_alias = self.alias_manager.get_alias(new_entity_name, to_entity)
                        else:
                            new_entity_name = from_entity_name
                            from_entity = self.catalog.get_entity(from_entity_name)
                            to_entity = self.catalog.get_entity(to_entity_name)
                            from_alias = self.alias_manager.get_alias(new_entity_name, from_entity)
                            to_alias = entity_aliases[to_entity_name]
                        
                        # Generate join SQL
                        join_sql = rel.get_join_sql(
                            catalog=self.catalog,
                            from_alias=from_alias,
                            to_alias=to_alias
                        )
                        
                        joins.append(join_sql)
                        entity_aliases[new_entity_name] = from_alias if new_entity_name == from_entity_name else to_alias
                        visited.add(new_entity_name)
                        join_order.append(new_entity_name)
                        queue.append((new_entity_name, self.catalog.get_entity(new_entity_name)))
                        
                except ValueError:
                    # No direct path from current to target, continue
                    continue
        
        # Verify all targets reached
        missing = [e for e in target_entity_names if e not in visited]
        if missing:
            raise ValueError(f"Cannot reach entities: {missing} from {start_entity_name}")
        
        return joins, entity_aliases, join_order


class SchemaAwareSQLCompiler:
    """
    SQL compiler with full schema.table support.
    """
    
    def __init__(self, catalog: SemanticCatalog = CATALOG):
        self.catalog = catalog
        self.templates = SQLTemplates()
        self.time_resolver = TimeRangeResolver()
        self.filter_builder = FilterSQLBuilder()
    
    def compile_sql(self, intent: QueryIntent) -> Dict[str, Any]:
        """
        Compile intent to SQL with schema support.
        """
        # Get metric and primary entity
        metric = self.catalog.get_metric(intent.metric)
        primary_entity = self.catalog.get_entity(metric.entity_name)
        
        # Collect all involved entities
        involved_entities = self._collect_involved_entities(intent, metric)
        
        # Resolve join paths with schema awareness
        resolver = SchemaJoinPathResolver(self.catalog)
        join_sqls, entity_aliases, join_order = resolver.resolve_join_path(
            primary_entity.name,
            list(involved_entities - {primary_entity.name})
        )
        
        # Build SELECT clause with schema-aware expressions
        select_parts = self._build_select_clause(intent, entity_aliases)
        
        # Add metric with schema references
        primary_alias = entity_aliases[primary_entity.name]
        metric_expr = metric.get_aggregation_expression(primary_entity, primary_alias)
        select_parts.append(metric_expr)
        
        # Build FROM clause with schema
        from_clause = f"FROM {primary_entity.fully_qualified_name} {primary_alias}"
        
        # Build WHERE clause with schema-aware filters
        where_conditions = self._build_where_clause(intent, entity_aliases)
        
        # Build complete SQL
        select_clause = ",\n  ".join(select_parts)
        join_clause = "\n".join(join_sqls) if join_sqls else ""
        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        
        group_by_clause = self.templates.build_group_by_clause(len(intent.dimensions))
        order_by_clause = self.templates.build_order_by_clause(intent.dimensions)
        limit_clause = f"LIMIT {intent.limit}" if intent.limit else ""
        
        sql = self.templates.assemble_full_sql(
            select_clause=select_clause,
            from_clause=from_clause,
            join_clause=join_clause,
            where_clause=where_clause,
            group_by_clause=group_by_clause,
            order_by_clause=order_by_clause,
            limit_clause=limit_clause
        )
        
        return {
            "sql": sql,
            "metadata": {
                "entities_involved": list(involved_entities),
                "entity_aliases": entity_aliases,
                "join_order": join_order,
                "primary_entity": primary_entity.name,
                "primary_schema": primary_entity.schema_name,
                "primary_table": primary_entity.table_name,
                "joins_count": len(join_sqls),
                "schema_crossings": self._count_schema_crossings(join_sqls)
            }
        }
    
    def _collect_involved_entities(self, intent: QueryIntent, metric: Metric) -> Set[str]:
        """Collect all entities involved in the query."""
        entities = {metric.entity_name}
        
        # Entities for dimensions
        for dim_name in intent.dimensions:
            dim = self.catalog.get_dimension(dim_name)
            entities.add(dim.entity_name)
        
        # Entities for filters
        for filter_cond in intent.filters:
            dim = self.catalog.get_dimension(filter_cond.dimension)
            entities.add(dim.entity_name)
        
        return entities
    
    def _build_select_clause(self, intent: QueryIntent, entity_aliases: Dict[str, str]) -> List[str]:
        """Build SELECT clause with schema-aware dimension expressions."""
        select_parts = []
        
        for dim_name in intent.dimensions:
            dim = self.catalog.get_dimension(dim_name)
            entity = self.catalog.get_entity(dim.entity_name)
            alias = entity_aliases[dim.entity_name]
            
            select_expr = dim.get_select_expression(entity, alias)
            select_parts.append(select_expr)
        
        return select_parts
    
    def _build_where_clause(self, intent: QueryIntent, entity_aliases: Dict[str, str]) -> List[str]:
        """Build WHERE clause with schema-aware filters."""
        conditions = []
        
        # Time range filter
        if intent.time_range:
            metric = self.catalog.get_metric(intent.metric)
            if metric.time_dimension:
                time_dim = self.catalog.get_dimension(metric.time_dimension)
                entity = self.catalog.get_entity(time_dim.entity_name)
                alias = entity_aliases[time_dim.entity_name]
                
                time_filter = self.time_resolver.get_time_filter_sql(
                    intent.time_range,
                    time_dim,
                    alias,
                    entity
                )
                if time_filter:
                    conditions.append(time_filter)
        
        # Additional filters
        for filter_cond in intent.filters:
            dim = self.catalog.get_dimension(filter_cond.dimension)
            entity = self.catalog.get_entity(dim.entity_name)
            alias = entity_aliases[dim.entity_name]
            
            filter_sql = self.filter_builder.build_filter_sql(
                filter_cond, dim, alias, entity
            )
            conditions.append(filter_sql)
        
        return conditions
    
    def _count_schema_crossings(self, join_sqls: List[str]) -> int:
        """Count how many joins cross schema boundaries."""
        crossings = 0
        for join_sql in join_sqls:
            # Check if join references different schemas
            # Simple heuristic: count occurrences of different schema names
            import re
            schema_pattern = r'"(\w+)"\.'
            schemas = re.findall(schema_pattern, join_sql)
            if len(set(schemas)) > 1:
                crossings += 1
        return crossings