"""
Database package for multi-schema connection management and query execution.
"""

from database.connection import (
    DatabaseConfig,
    ConnectionPool,
    MultiSchemaConnectionManager,
    connection_manager
)

from database.executor import (
    QueryResult,
    QueryProfiler,
    QueryCache,
    SchemaAwareQueryExecutor
)

__all__ = [
    'DatabaseConfig',
    'ConnectionPool',
    'MultiSchemaConnectionManager',
    'connection_manager',
    'QueryResult',
    'QueryProfiler',
    'QueryCache',
    'SchemaAwareQueryExecutor'
]

__version__ = "1.0.0"