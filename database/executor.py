# database/executor.py - UPDATED FOR SCHEMAS

import os
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import time
import logging


class MultiSchemaConnection:
    """Manages connections to multiple schemas/databases."""
    
    def __init__(self):
        self.connections: Dict[str, Dict[str, psycopg2.extensions.connection]] = {}
        self.configs = self._load_configs()
        self.logger = logging.getLogger(__name__)
    
    def _load_configs(self) -> Dict[str, Dict]:
        """Load database configurations for different schemas."""
        return {
            "sales": {
                "host": os.getenv("SALES_DB_HOST", "localhost"),
                "port": int(os.getenv("SALES_DB_PORT", 5432)),
                "database": os.getenv("SALES_DB_NAME", "sales_db"),
                "user": os.getenv("SALES_DB_USER", "sales_user"),
                "password": os.getenv("SALES_DB_PASSWORD", ""),
                "schema": "sales"
            },
            "analytics": {
                "host": os.getenv("ANALYTICS_DB_HOST", "localhost"),
                "port": int(os.getenv("ANALYTICS_DB_PORT", 5432)),
                "database": os.getenv("ANALYTICS_DB_NAME", "analytics_db"),
                "user": os.getenv("ANALYTICS_DB_USER", "analytics_user"),
                "password": os.getenv("ANALYTICS_DB_PASSWORD", ""),
                "schema": "analytics"
            },
            "ref": {
                "host": os.getenv("REF_DB_HOST", "localhost"),
                "port": int(os.getenv("REF_DB_PORT", 5432)),
                "database": os.getenv("REF_DB_NAME", "ref_db"),
                "user": os.getenv("REF_DB_USER", "ref_user"),
                "password": os.getenv("REF_DB_PASSWORD", ""),
                "schema": "ref"
            }
        }
    
    @contextmanager
    def get_connection(self, schema: str = "default", tenant_id: str = "default"):
        """Get connection for specific schema."""
        conn_key = f"{tenant_id}_{schema}"
        
        if schema not in self.connections:
            self.connections[schema] = {}
        
        if conn_key not in self.connections[schema]:
            config = self.configs.get(schema, self.configs["sales"])
            
            self.logger.info(f"Connecting to {schema} schema...")
            
            conn = psycopg2.connect(
                host=config["host"],
                port=config["port"],
                database=config["database"],
                user=config["user"],
                password=config["password"],
                connect_timeout=10
            )
            
            # Set search path if schema specified
            if "schema" in config:
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {config['schema']}, public;")
            
            self.connections[schema][conn_key] = conn
        
        conn = self.connections[schema][conn_key]
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            raise e
        else:
            conn.commit()
    
    def get_connection_for_entity(self, entity_name: str, catalog, tenant_id: str = "default"):
        """Get connection for a specific entity's schema."""
        entity = catalog.get_entity(entity_name)
        schema = entity.schema_name
        
        # Map schema names to connection configs
        schema_map = {
            "sales": "sales",
            "analytics": "analytics",
            "ref": "ref",
            "public": "sales"  # Default
        }
        
        connection_key = schema_map.get(schema, "sales")
        return self.get_connection(connection_key, tenant_id)
    
    def close_all(self):
        """Close all connections."""
        for schema_conns in self.connections.values():
            for conn in schema_conns.values():
                try:
                    conn.close()
                except:
                    pass
        self.connections.clear()


class SchemaAwareQueryExecutor:
    """
    Executes queries across multiple schemas.
    Handles cross-schema joins by using fully qualified table names.
    """
    
    def __init__(self, connection_pool: MultiSchemaConnection = None):
        self.connections = connection_pool or MultiSchemaConnection()
        self.logger = logging.getLogger(__name__)
    
    def execute_cross_schema_query(
        self,
        sql: str,
        schemas_involved: List[str],
        tenant_id: str = "default"
    ) -> Dict[str, Any]:
        """
        Execute query that involves multiple schemas.
        Uses the primary schema's connection (first in list).
        """
        start_time = time.time()
        
        if not schemas_involved:
            schemas_involved = ["sales"]  # Default
        
        primary_schema = schemas_involved[0]
        
        try:
            with self.connections.get_connection(primary_schema, tenant_id) as conn:
                # Set statement timeout
                with conn.cursor() as cur:
                    cur.execute("SET statement_timeout = 30000;")  # 30 seconds
                
                # Execute query
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    self.logger.info(f"Executing cross-schema query on {primary_schema}...")
                    
                    cur.execute(sql)
                    rows = cur.fetchall()
                    results = [dict(row) for row in rows]
                    
                    execution_time = time.time() - start_time
                    
                    return {
                        "success": True,
                        "data": results,
                        "metadata": {
                            "row_count": len(results),
                            "execution_time_ms": round(execution_time * 1000, 2),
                            "primary_schema": primary_schema,
                            "schemas_involved": schemas_involved,
                            "sql_hash": self._hash_sql(sql)
                        }
                    }
        
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Cross-schema query failed: {str(e)}")
            
            return {
                "success": False,
                "error": str(e),
                "metadata": {
                    "execution_time_ms": round(execution_time * 1000, 2),
                    "error_type": type(e).__name__
                }
            }
    
    def test_schema_connections(self) -> Dict[str, bool]:
        """Test connections to all configured schemas."""
        results = {}
        
        for schema in self.connections.configs.keys():
            try:
                with self.connections.get_connection(schema) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        result = cur.fetchone()
                        results[schema] = result[0] == 1
            except Exception as e:
                self.logger.error(f"Connection test failed for {schema}: {e}")
                results[schema] = False
        
        return results
    
    def _hash_sql(self, sql: str) -> str:
        """Generate hash for SQL."""
        import hashlib
        return hashlib.md5(sql.encode()).hexdigest()[:16]