"""
SQL compiler package for deterministic SQL generation.
NO LLM involvement - pure deterministic logic based on semantic catalog.
"""

from sql_compiler.templates import (
    TimeRangeResolver,
    FilterSQLBuilder,
    SQLTemplates
)

from sql_compiler.validator import SemanticValidator
from sql_compiler.compiler import (
    AliasManager,
    JoinPathResolver,
    SchemaJoinPathResolver,
    SQLCompiler,
    SchemaAwareSQLCompiler
)

__all__ = [
    'TimeRangeResolver',
    'FilterSQLBuilder',
    'SQLTemplates',
    'SemanticValidator',
    'AliasManager',
    'JoinPathResolver',
    'SchemaJoinPathResolver',
    'SQLCompiler',
    'SchemaAwareSQLCompiler'
]

__version__ = "1.0.0"