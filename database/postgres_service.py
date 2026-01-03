"""
PostgreSQL database service for semantic query engine.
"""

import os
import asyncpg
from typing import Dict, List, Any, Optional
from contextlib import asynccontextmanager
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str = os.getenv('DB_HOST', 'localhost')
    port: int = int(os.getenv('DB_PORT', 5432))
    user: str = os.getenv('DB_USER', 'postgres')
    password: str = os.getenv('DB_PASSWORD', '')
    database: str = os.getenv('DB_NAME', 'postgres')
    min_size: int = 1
    max_size: int = 10


class PostgreSQLService:
    """PostgreSQL database service."""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Connect to PostgreSQL and create connection pool."""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                min_size=self.config.min_size,
                max_size=self.config.max_size,
                command_timeout=60
            )
            logger.info(f"✅ Connected to PostgreSQL: {self.config.database}")
            
            # Test connection
            async with self.pool.acquire() as conn:
                version = await conn.fetchval('SELECT version()')
                logger.info(f"PostgreSQL version: {version.split(',')[0]}")
                
        except Exception as e:
            logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            raise
    
    async def close(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")
    
    @asynccontextmanager
    async def get_connection(self):
        """Get a database connection from the pool."""
        if not self.pool:
            raise Exception("Database not connected")
        
        async with self.pool.acquire() as connection:
            yield connection
    
    async def execute_query(self, sql: str, params: Optional[List] = None) -> List[Dict]:
        """Execute SQL query and return results as dictionaries."""
        try:
            async with self.get_connection() as conn:
                # If it's a SELECT query, fetch results
                if sql.strip().upper().startswith('SELECT'):
                    rows = await conn.fetch(sql, *(params or []))
                    return [dict(row) for row in rows]
                else:
                    # For INSERT/UPDATE/DELETE, execute and return affected rows
                    result = await conn.execute(sql, *(params or []))
                    return [{"affected_rows": result.split()[-1]}]
                    
        except Exception as e:
            logger.error(f"Query execution failed: {e}\nSQL: {sql}")
            raise
    
    async def get_table_info(self) -> List[Dict]:
        """Get information about all tables in the database."""
        sql = """
        SELECT 
            table_schema,
            table_name,
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema IN ('sales', 'ref', 'analytics')
        ORDER BY table_schema, table_name, ordinal_position
        """
        return await self.execute_query(sql)
    
    async def test_sample_queries(self) -> Dict[str, Any]:
        """Test sample queries to ensure tables are accessible."""
        test_results = {}
        
        # Test 1: Count customers
        try:
            result = await self.execute_query("SELECT COUNT(*) as customer_count FROM ref.customers")
            test_results['customers'] = result[0]['customer_count']
        except Exception as e:
            test_results['customers_error'] = str(e)
        
        # Test 2: Count orders
        try:
            result = await self.execute_query("SELECT COUNT(*) as order_count FROM sales.orders")
            test_results['orders'] = result[0]['order_count']
        except Exception as e:
            test_results['orders_error'] = str(e)
        
        # Test 3: Get sample data
        try:
            result = await self.execute_query("""
                SELECT o.order_id, c.full_name, o.amount_usd, o.order_date, cs.segment_name
                FROM sales.orders o
                JOIN ref.customers c ON o.customer_id = c.customer_id
                LEFT JOIN analytics.customer_segments cs ON o.customer_id = cs.customer_id
                ORDER BY o.order_date DESC
                LIMIT 5
            """)
            test_results['sample_data'] = result
        except Exception as e:
            test_results['sample_data_error'] = str(e)
        
        return test_results


# Global database instance
db_service = PostgreSQLService()