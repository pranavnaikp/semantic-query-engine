"""
SEMANTIC ANALYTICS ENGINE - WITH REAL POSTGRESQL DATA & COMPARATIVE ANALYTICS
"""

import os
from typing import Dict, List, Any, Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
import uvicorn
import asyncio
import json

# Local imports
from semantic_catalog.catalog import CATALOG
from sql_compiler.compiler import SQLCompiler
from intent_extractor.llm_extractor import IntentExtractor
from intent_extractor.intent_models import QueryIntent, IntentExtractionResponse, TimeRange, FilterCondition
from database.postgres_service import db_service
from analytics.comparative import ComparativeAnalyzer

# ====================== INITIALIZE ======================

intent_extractor = IntentExtractor()
sql_compiler = SQLCompiler(CATALOG)
comparative_analyzer = None

# Create FastAPI app
app = FastAPI(
    title="Semantic Analytics Engine",
    description="Natural language to SQL with real PostgreSQL data and comparative analytics",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ====================== HELPER FUNCTIONS ======================

def _safe_to_dict(obj):
    """Safely convert object to dict, handling Pydantic models."""
    if hasattr(obj, 'dict'):
        return obj.dict()
    elif hasattr(obj, '__dict__'):
        return obj.__dict__
    else:
        return obj
    
def _prepare_intent_dict(intent: QueryIntent, query: str) -> Dict:
    """Convert intent to dict for comparative analyzer."""
    # Start with basic fields
    intent_dict = {
        "metric": intent.metric,
        "dimensions": intent.dimensions,
        "limit": intent.limit,
        "original_query": query
    }
    
    # Handle time_range
    if intent.time_range:
        intent_dict["time_range"] = _safe_to_dict(intent.time_range)  # This returns a dict
    else:
        intent_dict["time_range"] = None
    
    # Handle filters
    if intent.filters:
        intent_dict["filters"] = [_safe_to_dict(f) for f in intent.filters]
    else:
        intent_dict["filters"] = []
    
    # Handle comparative
    if intent.comparative:
        intent_dict["comparative"] = intent.comparative.value
    else:
        intent_dict["comparative"] = None
    
    return intent_dict


def _determine_chart_type(intent: QueryIntent, is_comparative: bool = False) -> str:
    """Determine chart type based on intent."""
    if is_comparative:
        if intent.dimensions:
            return "comparative_bar"
        else:
            return "metric_comparison"
    
    if not intent.dimensions:
        return "metric_card"
    elif len(intent.dimensions) == 1:
        return "bar"
    else:
        return "table"

def _generate_fallback_data(intent: QueryIntent) -> List[Dict]:
    """Generate fallback mock data when database fails."""
    import random
    
    metric = intent.metric
    dimensions = intent.dimensions
    
    data = []
    
    if "revenue" in metric:
        if "country" in dimensions:
            countries = ["US", "UK", "DE", "FR", "JP"]
            for country in countries:
                data.append({
                    "country": country,
                    "revenue": random.randint(50000, 200000)
                })
        else:
            data.append({"revenue": sum(random.randint(50000, 200000) for _ in range(5))})
    
    elif "order_count" in metric:
        if "status" in dimensions:
            statuses = ["completed", "pending", "cancelled"]
            for status in statuses:
                data.append({
                    "status": status,
                    "order_count": random.randint(10, 100)
                })
        else:
            data.append({"order_count": random.randint(100, 1000)})
    
    else:
        # Generic fallback
        if dimensions:
            for i in range(5):
                row = {metric: random.randint(1000, 10000)}
                for dim in dimensions:
                    row[dim] = f"Value {i+1}"
                data.append(row)
        else:
            data.append({metric: random.randint(1000, 10000)})
    
    return data


# ====================== LIFECYCLE EVENTS ======================

@app.on_event("startup")
async def startup_event():
    """Initialize database connection on startup."""
    print("🔄 Connecting to PostgreSQL database...")
    try:
        await db_service.connect()
        
        # Test connection
        test_results = await db_service.test_sample_queries()
        print("✅ Database connected successfully!")
        print(f"   Customers: {test_results.get('customers', 'N/A')}")
        print(f"   Orders: {test_results.get('orders', 'N/A')}")
        
        # Initialize comparative analyzer
        global comparative_analyzer
        comparative_analyzer = ComparativeAnalyzer(db_service)
        print("✅ Comparative analytics module loaded")
        
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        print("⚠️  Falling back to mock data mode")

@app.on_event("shutdown")
async def shutdown_event():
    """Close database connection on shutdown."""
    await db_service.close()
    print("👋 Database connection closed")


# ====================== API ENDPOINTS ======================

@app.get("/")
async def root():
    return HTMLResponse("""
    <html>
    <head><title>Semantic Analytics Engine</title></head>
    <body>
        <h1>🚀 Semantic Analytics Engine</h1>
        <p>Natural language → SQL → Visualization (with REAL PostgreSQL data)</p>
        <h3>Try these queries:</h3>
        <ul>
            <li>Show me revenue by country</li>
            <li>How much did revenue increase compared to last year?</li>
            <li>Show me MoM growth by country</li>
            <li>Compare this quarter to last quarter</li>
            <li>Order count by status</li>
            <li>Average order value by customer segment</li>
        </ul>
        <p><a href="/docs">API Documentation</a> | <a href="/health">Health Check</a></p>
    </body>
    </html>
    """)


@app.get("/health")
async def health():
    """Health check endpoint."""
    try:
        # Test database connection
        test_results = await db_service.test_sample_queries()
        db_status = "connected" if 'customers' in test_results else "disconnected"
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "version": "1.0.0",
            "database": db_status,
            "sample_data": "customers" in test_results and "orders" in test_results,
            "comparative_analytics": "loaded" if comparative_analyzer else "not_loaded"
        }
    except Exception as e:
        return {
            "status": "degraded",
            "timestamp": datetime.now().isoformat(),
            "error": str(e),
            "database": "disconnected"
        }


@app.get("/catalog")
async def get_catalog():
    """Get available metrics and dimensions."""
    metrics = CATALOG.get_all_metrics()
    dimensions = CATALOG.get_all_dimensions()
    
    return {
        "metrics": metrics,
        "dimensions": dimensions,
        "timestamp": datetime.now().isoformat(),
        "database_connected": True,
        "comparative_supported": True
    }


@app.post("/query")
async def process_query(payload: Dict[str, Any]):
    """
    Process natural language query with REAL PostgreSQL data.
    Returns clean data for React frontend.
    """
    query = payload.get("query", "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query is required")
    
    print(f"📝 Processing query: '{query}'")
    
    try:
        # Step 1: Extract intent
        intent_result = intent_extractor.extract_intent(query)
        if not intent_result.success:
            intent_result = intent_extractor.extract_intent_fallback(query)
        
        if not intent_result.success:
            raise HTTPException(
                status_code=400,
                detail=f"Could not understand query: {intent_result.error}"
            )
        
        intent = intent_result.intent
        
        print(f"   Intent: metric={intent.metric}, dimensions={intent.dimensions}, comparative={intent.comparative}")
        
        # Step 2: Generate base SQL
        sql_result = sql_compiler.compile_sql(intent)
        base_sql = sql_result["sql"]
        print(f"   Generated base SQL: {base_sql[:200]}...")
        
        # Step 3: Check for comparative analysis
        is_comparative = False
        sql = base_sql
        
        # Prepare intent dict safely
        intent_dict = _prepare_intent_dict(intent, query)
        
        # Debug: Print intent_dict to see structure
        print(f"   Intent dict prepared: {json.dumps(intent_dict, default=str)}")
        
        # Check if we should do comparative analysis
        should_do_comparative = (
            intent.comparative is not None or 
            any(word in query.lower() for word in ['compared', 'growth', 'increase', 'decrease', 'change', 'yoy', 'mom', 'qoq'])
        )
        
        if should_do_comparative and comparative_analyzer:
            print(f"📈 Processing comparative analysis for query: {query}")
            
            try:
                comparative_result = await comparative_analyzer.analyze_comparative(
                    intent_dict, base_sql
                )
                
                if comparative_result.get("comparative"):
                    sql = comparative_result["sql"]
                    is_comparative = True
                    print(f"   Using comparative SQL: {sql[:200]}...")
                else:
                    print(f"   Comparative analysis not applicable: {comparative_result.get('message')}")
                    
            except Exception as comp_error:
                print(f"   Comparative analysis failed: {comp_error}")
                # Continue with base SQL
        
        # Step 4: Execute against PostgreSQL
        print(f"   Executing SQL against PostgreSQL...")
        try:
            if is_comparative:
                data = await comparative_analyzer.execute_comparative_query(sql)
            else:
                data = await db_service.execute_query(sql)
            
            is_real_data = True
            print(f"   Retrieved {len(data)} rows from database")
                
        except Exception as db_error:
            print(f"   Database query failed: {db_error}")
            # Fallback to mock data
            data = _generate_fallback_data(intent)
            is_real_data = False
        
        # Step 5: Determine chart type
        chart_type = _determine_chart_type(intent, is_comparative)
        
        # Step 6: Return CLEAN response for React
        response = {
            "success": True,
            "query": {
                "original": query,
                "intent": intent.dict(),
                "sql": sql,
                "is_comparative": is_comparative,
                "comparative_type": intent.comparative.value if intent.comparative else None
            },
            "data": data,
            "chart_type": chart_type,
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "row_count": len(data),
                "real_data": is_real_data,
                "columns": list(data[0].keys()) if data else [],
                "comparative_analysis": is_comparative
            }
        }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Query processing failed: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/direct")
async def direct_query(intent: QueryIntent):
    """Direct query with structured intent."""
    try:
        sql_result = sql_compiler.compile_sql(intent)
        sql = sql_result["sql"]
        
        data = await db_service.execute_query(sql)
        
        return {
            "intent": intent.dict(),
            "sql": sql,
            "data": data,
            "real_data": True
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/execute-sql")
async def execute_sql(payload: Dict[str, Any]):
    """Execute raw SQL (for testing)."""
    sql = payload.get("sql", "").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="SQL is required")
    
    try:
        data = await db_service.execute_query(sql)
        return {
            "success": True,
            "sql": sql,
            "data": data,
            "row_count": len(data)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    print("\n" + "="*50)
    print("🚀 SEMANTIC ANALYTICS ENGINE")
    print("="*50)
    print("Starting server on http://localhost:8000")
    print("Press Ctrl+C to stop")
    print("="*50 + "\n")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )