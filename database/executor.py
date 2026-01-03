"""
SIMPLIFIED MVP: PostgreSQL query executor.
No cross-schema complexity, no external databases.
"""

import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from decimal import Decimal
import hashlib

from database.connection import db


class QueryExecutor:
    """
    SIMPLIFIED: Executes queries on PostgreSQL.
    """
    
    def __init__(self, use_cache: bool = True):
        self.use_cache = use_cache
        self.cache: Dict[str, Dict] = {}
        self.logger = logging.getLogger(__name__)
        self.stats = {
            "total_queries": 0,
            "successful": 0,
            "failed": 0,
            "cache_hits": 0
        }
    
    def _cache_key(self, sql: str) -> str:
        """Generate simple cache key from SQL."""
        return hashlib.md5(sql.encode()).hexdigest()[:16]
    
    def execute(self, sql: str) -> Dict[str, Any]:
        """
        Execute SQL query on PostgreSQL.
        Returns: {"success": bool, "data": list, "error": str, "metadata": dict}
        """
        start_time = time.time()
        
        # Check cache
        if self.use_cache:
            cache_key = self._cache_key(sql)
            if cache_key in self.cache:
                cached = self.cache[cache_key]
                # Check if cache is still valid (5 minutes)
                if time.time() - cached.get("cached_at", 0) < 300:
                    self.stats["cache_hits"] += 1
                    self.logger.info(f"Cache hit for query")
                    return cached["result"]
        
        try:
            self.logger.debug(f"Executing SQL: {sql[:200]}...")
            
            # Execute query
            data = db.execute_query(sql)
            
            execution_time = time.time() - start_time
            
            result = {
                "success": True,
                "data": data,
                "error": None,
                "metadata": {
                    "execution_time_ms": round(execution_time * 1000, 2),
                    "row_count": len(data),
                    "cache_hit": False,
                    "timestamp": datetime.now().isoformat()
                }
            }
            
            # Cache successful results (only if not too large)
            if self.use_cache and len(data) > 0 and len(data) <= 1000:
                cache_key = self._cache_key(sql)
                self.cache[cache_key] = {
                    "result": result,
                    "cached_at": time.time(),
                    "row_count": len(data)
                }
                # Simple cache eviction
                if len(self.cache) > 100:
                    # Remove oldest
                    oldest_key = min(self.cache.keys(), 
                                   key=lambda k: self.cache[k]["cached_at"])
                    del self.cache[oldest_key]
            
            self.stats["total_queries"] += 1
            self.stats["successful"] += 1
            
            self.logger.info(f"Query executed in {execution_time:.2f}s, returned {len(data)} rows")
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            error_msg = str(e)
            self.logger.error(f"Query failed: {error_msg}")
            
            self.stats["total_queries"] += 1
            self.stats["failed"] += 1
            
            return {
                "success": False,
                "data": [],
                "error": error_msg,
                "metadata": {
                    "execution_time_ms": round(execution_time * 1000, 2),
                    "row_count": 0,
                    "error_type": type(e).__name__
                }
            }
    
    def execute_with_mock_data(self, sql: str) -> Dict[str, Any]:
        """
        Generate mock data for testing.
        Use this when you don't have a real database.
        """
        self.logger.info("Using mock data (no PostgreSQL required)")
        
        # Simple mock data based on query type
        sql_lower = sql.lower()
        
        if "revenue" in sql_lower:
            data = [
                {"country": "US", "revenue": 150000.50},
                {"country": "UK", "revenue": 85000.75},
                {"country": "DE", "revenue": 65000.25}
            ]
        elif "order" in sql_lower:
            data = [
                {"month": "Jan", "orders": 1200},
                {"month": "Feb", "orders": 1500},
                {"month": "Mar", "orders": 1800}
            ]
        elif "user" in sql_lower:
            data = [
                {"segment": "enterprise", "users": 50},
                {"segment": "premium", "users": 200},
                {"segment": "free", "users": 1000}
            ]
        else:
            data = [{"value": 100, "result": "mock_data"}]
        
        return {
            "success": True,
            "data": data,
            "error": None,
            "metadata": {
                "execution_time_ms": 100,
                "row_count": len(data),
                "mock_data": True,
                "note": "Using mock data - no database required"
            }
        }
    
    def test_connection(self) -> bool:
        """Test PostgreSQL connection."""
        return db.test_connection()
    
    def clear_cache(self):
        """Clear query cache."""
        self.cache.clear()
        self.logger.info("Query cache cleared")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get execution statistics."""
        return {
            **self.stats,
            "cache_size": len(self.cache),
            "database_connected": self.test_connection()
        }


# Global instance
query_executor = QueryExecutor()