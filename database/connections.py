"""
SIMPLIFIED MVP: Single PostgreSQL connection.
No multi-schema complexity, just one database.
"""

import os
from typing import Optional
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool
import logging

from dotenv import load_dotenv

load_dotenv()
class PostgreSQLConnection:
    """
    SIMPLIFIED: Single PostgreSQL connection pool.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.pool = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize PostgreSQL connection pool."""
        try:
            # Get connection parameters from environment
            db_config = {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", "5432")),
                "database": os.getenv("DB_NAME", "semantic_db"),
                "user": os.getenv("DB_USER", "postgres"),
                "password": os.getenv("DB_PASSWORD", "postgres"),
                "minconn": int(os.getenv("DB_MIN_CONN", "1")),
                "maxconn": int(os.getenv("DB_MAX_CONN", "10"))
            }
            
            self.logger.info(f"Connecting to PostgreSQL: {db_config['database']}@{db_config['host']}")
            
            # Create connection pool
            self.pool = pool.SimpleConnectionPool(
                minconn=db_config["minconn"],
                maxconn=db_config["maxconn"],
                host=db_config["host"],
                port=db_config["port"],
                database=db_config["database"],
                user=db_config["user"],
                password=db_config["password"]
            )
            
            # Test connection
            self._test_connection()
            
            self.logger.info("PostgreSQL connection pool initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize PostgreSQL connection: {e}")
            raise
    
    def _test_connection(self):
        """Test the database connection."""
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                if result[0] != 1:
                    raise Exception("Connection test failed")
        finally:
            self.pool.putconn(conn)
    
    @contextmanager
    def get_connection(self):
        """
        Get a PostgreSQL connection from the pool.
        Simple context manager - no tenant or schema complexity.
        """
        conn = None
        try:
            conn = self.pool.getconn()
            
            # Set PostgreSQL settings
            with conn.cursor() as cur:
                # Set statement timeout (30 seconds)
                cur.execute("SET statement_timeout = 30000;")
                # Set search path to public schema
                cur.execute("SET search_path TO public;")
                # Enable JSON output
                cur.execute("SET client_encoding = 'UTF8';")
            
            yield conn
            conn.commit()
            
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database error: {e}")
            raise
            
        finally:
            if conn:
                try:
                    self.pool.putconn(conn)
                except:
                    try:
                        conn.close()
                    except:
                        pass
    
    def execute_query(self, sql: str, params: tuple = None) -> list:
        """
        Execute a SQL query and return results.
        Simple wrapper for common operations.
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                
                # If it's a SELECT, fetch results
                if sql.strip().upper().startswith("SELECT"):
                    columns = [desc[0] for desc in cur.description] if cur.description else []
                    rows = cur.fetchall()
                    
                    # Convert to list of dicts
                    result = []
                    for row in rows:
                        result.append(dict(zip(columns, row)))
                    return result
                
                # For non-SELECT, return rowcount
                return [{"rowcount": cur.rowcount}]
    
    def test_connection(self) -> bool:
        """Test if database is reachable."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
                    return result[0] == 1
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def close(self):
        """Close all connections."""
        if self.pool:
            try:
                self.pool.closeall()
                self.logger.info("PostgreSQL connection pool closed")
            except Exception as e:
                self.logger.error(f"Error closing pool: {e}")


# Global instance
db = PostgreSQLConnection()