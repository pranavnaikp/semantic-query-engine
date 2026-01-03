"""
SEMANTIC ANALYTICS ENGINE - WORKING MVP
Run: python main.py
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from typing import Dict, List, Any, Optional
from datetime import datetime
import json
import random
import uvicorn

# Local imports - they should work now
from semantic_catalog.catalog import CATALOG
from sql_compiler.compiler import SQLCompiler
from intent_extractor.llm_extractor import IntentExtractor
from intent_extractor.intent_models import QueryIntent, IntentExtractionResponse


# ====================== SIMPLE SERVICES ======================

class MockDataGenerator:
    """Generate mock data."""
    
    def generate_data(self, sql: str, intent: QueryIntent) -> List[Dict]:
        metric = intent.metric
        dimensions = intent.dimensions
        
        data = []
        
        if "revenue" in metric:
            if "country" in dimensions:
                countries = ["US", "UK", "Germany", "France", "Japan"]
                for country in countries:
                    data.append({
                        "country": country,
                        "revenue": random.randint(50000, 200000)
                    })
            elif "month" in dimensions or "order_date" in dimensions:
                months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]
                for month in months:
                    data.append({
                        "month": month,
                        "revenue": 100000 + (months.index(month) * 20000)
                    })
            else:
                data.append({"revenue": 1250000})
        
        elif "order_count" in metric:
            if "product_category" in dimensions:
                categories = ["Electronics", "Clothing", "Books"]
                for category in categories:
                    data.append({
                        "product_category": category,
                        "order_count": random.randint(1000, 5000)
                    })
            else:
                data.append({"order_count": 12500})
        
        elif "net_profit" in metric:
            data.append({"net_profit": 850000})
        
        else:
            # Generic data
            if dimensions:
                for i in range(5):
                    row = {metric: random.randint(1000, 10000)}
                    for dim in dimensions:
                        row[dim] = f"Sample {i+1}"
                    data.append(row)
            else:
                data.append({metric: 5000})
        
        return data


class SimpleVisualizer:
    """Simple visualization generator."""
    
    def generate_viz(self, data: List[Dict], dimensions: List[str], metric: str) -> Dict:
        if not data:
            return {"type": "empty", "message": "No data"}
        
        if not dimensions:
            value = data[0].get(metric, 0)
            return {
                "type": "metric_card",
                "title": metric.replace("_", " ").title(),
                "value": value,
                "formatted": f"${value:,.2f}" if "revenue" in metric or "profit" in metric else f"{value:,.0f}"
            }
        
        elif len(dimensions) == 1:
            return {
                "type": "bar_chart",
                "title": f"{metric.replace('_', ' ').title()} by {dimensions[0]}",
                "data": data,
                "x_axis": dimensions[0],
                "y_axis": metric
            }
        
        else:
            return {
                "type": "table",
                "title": f"{metric.replace('_', ' ').title()} Data",
                "columns": list(data[0].keys()) if data else [],
                "data": data
            }


# ====================== INITIALIZE ======================

intent_extractor = IntentExtractor()
sql_compiler = SQLCompiler(CATALOG)
data_generator = MockDataGenerator()
visualizer = SimpleVisualizer()

# Create FastAPI app
app = FastAPI(
    title="Semantic Analytics Engine",
    description="Natural language to SQL with visualization",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ====================== API ENDPOINTS ======================

@app.get("/")
async def root():
    return HTMLResponse("""
    <html>
    <head><title>Semantic Analytics Engine</title></head>
    <body>
        <h1>🚀 Semantic Analytics Engine</h1>
        <p>Natural language → SQL → Visualization</p>
        <p><strong>Try:</strong></p>
        <pre>curl -X POST http://localhost:8000/query \\
  -H "Content-Type: application/json" \\
  -d '{"query": "Show me revenue by country"}'</pre>
        <p><a href="/docs">API Documentation</a> | <a href="/health">Health Check</a></p>
    </body>
    </html>
    """)


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }


@app.get("/catalog")
async def get_catalog():
    """Get available metrics and dimensions."""
    metrics = []
    dimensions = []
    
    for entity_name, entity in CATALOG.entities.items():
        for metric_name, metric in entity.metrics.items():
            metrics.append({
                "name": metric_name,
                "description": metric.description,
                "entity": entity_name,
                "aggregation": metric.aggregation.value
            })
        
        for dim_name, dimension in entity.dimensions.items():
            dimensions.append({
                "name": dim_name,
                "description": dimension.description,
                "entity": entity_name,
                "type": dimension.data_type.value
            })
    
    return {
        "metrics": metrics,
        "dimensions": dimensions
    }


@app.post("/query")
async def process_query(payload: Dict[str, Any]):
    """
    Process natural language query.
    
    Example:
    {
        "query": "Show me revenue by country for last month"
    }
    """
    query = payload.get("query", "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query is required")
    
    print(f"Processing query: {query}")
    
    # Step 1: Extract intent
    intent_result = intent_extractor.extract_intent(query)
    if not intent_result.success:
        # Try fallback
        intent_result = intent_extractor.extract_intent_fallback(query)
        if not intent_result.success:
            raise HTTPException(
                status_code=400,
                detail=f"Could not understand query: {intent_result.error}"
            )
    
    intent = intent_result.intent
    print(f"Extracted intent: {intent.dict()}")
    
    # Step 2: Generate SQL
    sql_result = sql_compiler.compile_sql(intent)
    sql = sql_result["sql"]
    print(f"Generated SQL: {sql[:200]}...")
    
    # Step 3: Generate mock data
    data = data_generator.generate_data(sql, intent)
    
    # Step 4: Generate visualization
    visualization = visualizer.generate_viz(data, intent.dimensions, intent.metric)
    
    # Step 5: Return response
    return {
        "success": True,
        "query": {
            "original": query,
            "intent": intent.dict(),
            "sql": sql
        },
        "data": data,
        "visualization": visualization,
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "row_count": len(data),
            "mock_data": True
        }
    }


@app.post("/direct")
async def direct_query(intent: QueryIntent):
    """Direct query with structured intent."""
    sql_result = sql_compiler.compile_sql(intent)
    sql = sql_result["sql"]
    
    data = data_generator.generate_data(sql, intent)
    visualization = visualizer.generate_viz(data, intent.dimensions, intent.metric)
    
    return {
        "intent": intent.dict(),
        "sql": sql,
        "data": data,
        "visualization": visualization
    }


if __name__ == "__main__":
    print("\n" + "="*50)
    print("🚀 SEMANTIC ANALYTICS ENGINE")
    print("="*50)
    print("Starting server on http://localhost:8000")
    print("Press Ctrl+C to stop")
    print("="*50 + "\n")
    
    uvicorn.run(
        "main:app",  # Changed from "app" to "main:app" as import string
        host="0.0.0.0",
        port=8000,
        reload=True
    )