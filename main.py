# main.py - COMPLETE IMPLEMENTATION WITH SCHEMA SUPPORT

"""
FastAPI application for the semantic analytics engine with full schema support.
Main entry point with REST API endpoints.
"""

from fastapi import FastAPI, HTTPException, Depends, Header, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from typing import Optional, Dict, Any, List
import uvicorn
import logging
import json
import time
from datetime import datetime
import asyncio

from intent_extractor.llm_extractor import IntentExtractor, IntentExtractionResponse
from intent_extractor.intent_models import QueryIntent, TimeRange, TimeRangeType, FilterCondition
from sql_compiler.compiler import SchemaAwareSQLCompiler
from sql_compiler.validator import SemanticValidator
from database.executor import SchemaAwareQueryExecutor, MultiSchemaConnection
from visualization.generator import VisualizationGenerator
from semantic_catalog.catalog import CATALOG
from semantic_catalog.models import Entity
from config import OPENAI_CONFIG, APP_CONFIG


# Configure logging
logging.basicConfig(
    level=logging.INFO if not APP_CONFIG["debug"] else logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("semantic_engine.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Semantic Analytics Engine",
    description="Cube.js-style semantic layer with deterministic SQL generation and schema support",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if APP_CONFIG["debug"] else [
        "http://localhost:3000",
        "http://localhost:8000",
        "https://yourdomain.com"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID", "X-Execution-Time"]
)

# Initialize services
intent_extractor = IntentExtractor(api_key=OPENAI_CONFIG["api_key"])
sql_compiler = SchemaAwareSQLCompiler(CATALOG)
semantic_validator = SemanticValidator(CATALOG)
db_connections = MultiSchemaConnection()
query_executor = SchemaAwareQueryExecutor(db_connections)
viz_generator = VisualizationGenerator()

# Cache for expensive operations
query_cache = {}
cache_lock = asyncio.Lock()


# Dependency for API key validation
async def verify_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    """Verify API key and return tenant information."""
    if APP_CONFIG["debug"] and not x_api_key:
        # Allow debug mode without API key
        return {"tenant_id": "debug", "plan": "enterprise", "rate_limit": 1000}
    
    if not x_api_key:
        raise HTTPException(
            status_code=401,
            detail="API key required. Use X-API-Key header."
        )
    
    # In production, validate against database
    # For now, simple validation
    if not x_api_key.startswith("sk_"):
        raise HTTPException(
            status_code=401,
            detail="Invalid API key format"
        )
    
    # Extract tenant info from API key
    parts = x_api_key.split("_")
    if len(parts) < 2:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    tenant_id = parts[1] if len(parts) > 1 else "default"
    
    # Determine plan based on key pattern
    if tenant_id.startswith("ent_"):
        plan = "enterprise"
        rate_limit = 1000
    elif tenant_id.startswith("pro_"):
        plan = "professional"
        rate_limit = 500
    else:
        plan = "starter"
        rate_limit = 100
    
    return {
        "tenant_id": tenant_id,
        "plan": plan,
        "rate_limit": rate_limit,
        "api_key": x_api_key
    }


# Middleware to add request ID and timing
@app.middleware("http")
async def add_process_time_header(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Execution-Time"] = str(round(process_time * 1000, 2))
    response.headers["X-Request-ID"] = f"req_{int(start_time * 1000)}"
    return response


# Root endpoint
@app.get("/", response_class=HTMLResponse)
async def root():
    """Welcome page with API documentation."""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Semantic Analytics Engine</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
            .container { max-width: 800px; margin: 0 auto; }
            .header { background: #f4f4f4; padding: 20px; border-radius: 5px; }
            .endpoint { background: #f9f9f9; padding: 15px; margin: 10px 0; border-left: 4px solid #007bff; }
            .method { display: inline-block; padding: 3px 8px; border-radius: 3px; color: white; font-weight: bold; }
            .get { background: #28a745; }
            .post { background: #007bff; }
            code { background: #eee; padding: 2px 5px; border-radius: 3px; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚀 Semantic Analytics Engine v2.0</h1>
                <p>Cube.js-style semantic layer with full schema support</p>
                <p><strong>Features:</strong> Natural language queries, deterministic SQL, multi-schema support, automated visualization</p>
            </div>
            
            <h2>📚 API Endpoints</h2>
            
            <div class="endpoint">
                <span class="method get">GET</span> <code>/health</code>
                <p>System health check and status</p>
            </div>
            
            <div class="endpoint">
                <span class="method get">GET</span> <code>/schemas</code>
                <p>List all available schemas in the catalog</p>
            </div>
            
            <div class="endpoint">
                <span class="method get">GET</span> <code>/catalog/metrics</code>
                <p>List all available metrics</p>
            </div>
            
            <div class="endpoint">
                <span class="method post">POST</span> <code>/nl-query</code>
                <p>Natural language query processing</p>
                <p><strong>Body:</strong> <code>{"query": "Show me monthly revenue by country"}</code></p>
            </div>
            
            <div class="endpoint">
                <span class="method post">POST</span> <code>/query</code>
                <p>Structured query processing (bypass LLM)</p>
            </div>
            
            <div class="endpoint">
                <span class="method get">GET</span> <code>/explain-query</code>
                <p>Explain query execution plan with schema analysis</p>
            </div>
            
            <h2>🔑 Authentication</h2>
            <p>Use <code>X-API-Key</code> header with your API key. Debug mode doesn't require API key.</p>
            
            <h2>📖 Documentation</h2>
            <p>
                <a href="/docs">Interactive Swagger UI</a> |
                <a href="/redoc">ReDoc Documentation</a> |
                <a href="/openapi.json">OpenAPI Spec</a>
            </p>
            
            <footer style="margin-top: 40px; color: #666; border-top: 1px solid #eee; padding-top: 20px;">
                <p>Built with FastAPI • Deterministic SQL Generation • Multi-schema Support</p>
            </footer>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.get("/health")
async def health_check():
    """Comprehensive health check for all components."""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "2.0.0",
        "components": {}
    }
    
    # Check database connections
    try:
        db_status = query_executor.test_schema_connections()
        health_status["components"]["database"] = {
            "status": "healthy" if all(db_status.values()) else "degraded",
            "schemas": db_status,
            "healthy_schemas": sum(db_status.values()),
            "total_schemas": len(db_status)
        }
    except Exception as e:
        health_status["components"]["database"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check catalog
    try:
        catalog_metrics = sum(len(e.metrics) for e in CATALOG.entities.values())
        catalog_dimensions = sum(len(e.dimensions) for e in CATALOG.entities.values())
        health_status["components"]["catalog"] = {
            "status": "healthy",
            "entities": len(CATALOG.entities),
            "metrics": catalog_metrics,
            "dimensions": catalog_dimensions,
            "schemas": len(set(e.schema_name for e in CATALOG.entities.values()))
        }
    except Exception as e:
        health_status["components"]["catalog"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check LLM service
    try:
        # Simple test query
        test_result = intent_extractor.extract_intent_fallback("test health")
        health_status["components"]["llm"] = {
            "status": "healthy" if test_result.success else "degraded",
            "model": OPENAI_CONFIG.get("model", "unknown"),
            "test_query_success": test_result.success
        }
    except Exception as e:
        health_status["components"]["llm"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check cache
    health_status["components"]["cache"] = {
        "status": "healthy",
        "entries": len(query_cache),
        "size_kb": sum(len(str(v).encode()) for v in query_cache.values()) / 1024
    }
    
    # Overall status
    if all(comp["status"] == "healthy" for comp in health_status["components"].values()):
        health_status["status"] = "healthy"
    elif any(comp["status"] == "unhealthy" for comp in health_status["components"].values()):
        health_status["status"] = "unhealthy"
    else:
        health_status["status"] = "degraded"
    
    return health_status


@app.get("/schemas")
async def list_schemas(tenant: Dict = Depends(verify_api_key)):
    """List all available schemas in the catalog."""
    try:
        schemas = {}
        
        for entity_name, entity in CATALOG.entities.items():
            schema_name = entity.schema_name
            if schema_name not in schemas:
                schemas[schema_name] = {
                    "name": schema_name,
                    "database": entity.database,
                    "dialect": entity.dialect.value,
                    "entities": [],
                    "quote_style": '"' if entity.quote_identifiers else "none"
                }
            schemas[schema_name]["entities"].append(entity_name)
        
        # Convert to list and sort
        schema_list = []
        for schema_name, schema_info in schemas.items():
            schema_list.append({
                "schema": schema_name,
                "database": schema_info["database"],
                "dialect": schema_info["dialect"],
                "entity_count": len(schema_info["entities"]),
                "quote_style": schema_info["quote_style"],
                "example_entity": schema_info["entities"][0] if schema_info["entities"] else None
            })
        
        schema_list.sort(key=lambda x: x["schema"])
        
        return {
            "success": True,
            "schemas": schema_list,
            "count": len(schema_list),
            "total_entities": len(CATALOG.entities),
            "tenant": tenant["tenant_id"]
        }
        
    except Exception as e:
        logger.error(f"Failed to list schemas: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list schemas: {str(e)}"
        )


@app.get("/schema/{schema_name}/entities")
async def list_entities_in_schema(
    schema_name: str,
    include_details: bool = Query(False, description="Include full entity details"),
    tenant: Dict = Depends(verify_api_key)
):
    """List all entities in a specific schema."""
    try:
        entities = []
        
        for entity_name, entity in CATALOG.entities.items():
            if entity.schema_name == schema_name:
                entity_info = {
                    "name": entity_name,
                    "table": entity.table_name,
                    "description": entity.description,
                    "dimensions": len(entity.dimensions),
                    "metrics": len(entity.metrics),
                    "relationships": len(entity.relationships),
                    "primary_key": entity.primary_key,
                    "alias_prefix": entity.alias_prefix,
                    "fully_qualified": entity.fully_qualified_name
                }
                
                if include_details:
                    # Add detailed information
                    entity_info["dimension_list"] = list(entity.dimensions.keys())
                    entity_info["metric_list"] = list(entity.metrics.keys())
                    entity_info["relationship_list"] = list(entity.relationships.keys())
                
                entities.append(entity_info)
        
        if not entities:
            raise HTTPException(
                status_code=404,
                detail=f"Schema '{schema_name}' not found or has no entities"
            )
        
        return {
            "success": True,
            "schema": schema_name,
            "entities": sorted(entities, key=lambda x: x["name"]),
            "count": len(entities),
            "tenant": tenant["tenant_id"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list entities for schema {schema_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list entities: {str(e)}"
        )


@app.get("/catalog/metrics")
async def list_metrics(
    schema: Optional[str] = Query(None, description="Filter by schema"),
    include_metadata: bool = Query(False, description="Include metric metadata"),
    tenant: Dict = Depends(verify_api_key)
):
    """List all available metrics in the semantic catalog."""
    try:
        metrics = []
        
        for entity_name, entity in CATALOG.entities.items():
            # Filter by schema if specified
            if schema and entity.schema_name != schema:
                continue
            
            for metric_name, metric in entity.metrics.items():
                metric_info = {
                    "name": metric_name,
                    "description": metric.description,
                    "entity": entity_name,
                    "schema": entity.schema_name,
                    "table": entity.table_name,
                    "aggregation": metric.aggregation.value,
                    "time_dimension": metric.time_dimension,
                    "format": metric.format,
                    "required_dimensions": metric.required_dimensions
                }
                
                if include_metadata:
                    metric_info["sql_expression"] = metric.sql_expression
                    metric_info["available_dimensions"] = list(entity.dimensions.keys())
                
                metrics.append(metric_info)
        
        return {
            "success": True,
            "metrics": sorted(metrics, key=lambda x: x["name"]),
            "count": len(metrics),
            "filtered_by_schema": schema,
            "tenant": tenant["tenant_id"]
        }
        
    except Exception as e:
        logger.error(f"Failed to list metrics: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list metrics: {str(e)}"
        )


@app.get("/catalog/dimensions")
async def list_dimensions(
    schema: Optional[str] = Query(None, description="Filter by schema"),
    tenant: Dict = Depends(verify_api_key)
):
    """List all available dimensions in the semantic catalog."""
    try:
        dimensions = []
        
        for entity_name, entity in CATALOG.entities.items():
            if schema and entity.schema_name != schema:
                continue
            
            for dim_name, dimension in entity.dimensions.items():
                dimensions.append({
                    "name": dim_name,
                    "description": dimension.description,
                    "entity": entity_name,
                    "schema": entity.schema_name,
                    "table": entity.table_name,
                    "data_type": dimension.data_type.value,
                    "column_name": dimension.column_name,
                    "has_sql_expression": dimension.sql_expression is not None,
                    "format": dimension.format
                })
        
        return {
            "success": True,
            "dimensions": sorted(dimensions, key=lambda x: x["name"]),
            "count": len(dimensions),
            "filtered_by_schema": schema,
            "tenant": tenant["tenant_id"]
        }
        
    except Exception as e:
        logger.error(f"Failed to list dimensions: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list dimensions: {str(e)}"
        )


@app.get("/catalog/entity/{entity_name}")
async def get_entity_details(
    entity_name: str,
    tenant: Dict = Depends(verify_api_key)
):
    """Get detailed information about a specific entity."""
    try:
        entity = CATALOG.get_entity(entity_name)
        
        # Build dimension details
        dimensions = []
        for dim_name, dimension in entity.dimensions.items():
            dimensions.append({
                "name": dim_name,
                "description": dimension.description,
                "data_type": dimension.data_type.value,
                "column": dimension.column_name,
                "sql_expression": dimension.sql_expression,
                "format": dimension.format
            })
        
        # Build metric details
        metrics = []
        for metric_name, metric in entity.metrics.items():
            metrics.append({
                "name": metric_name,
                "description": metric.description,
                "aggregation": metric.aggregation.value,
                "sql_expression": metric.sql_expression,
                "time_dimension": metric.time_dimension,
                "required_dimensions": metric.required_dimensions,
                "format": metric.format
            })
        
        # Build relationship details
        relationships = []
        for rel_name, relationship in entity.relationships.items():
            relationships.append({
                "name": rel_name,
                "from_entity": relationship.from_entity,
                "to_entity": relationship.to_entity,
                "type": relationship.relationship_type.value,
                "join_conditions": relationship.join_conditions
            })
        
        return {
            "success": True,
            "entity": {
                "name": entity_name,
                "description": entity.description,
                "schema": entity.schema_name,
                "database": entity.database,
                "table": entity.table_name,
                "fully_qualified": entity.fully_qualified_name,
                "dialect": entity.dialect.value,
                "primary_key": entity.primary_key,
                "alias_prefix": entity.alias_prefix,
                "quote_identifiers": entity.quote_identifiers,
                "dimensions": sorted(dimensions, key=lambda x: x["name"]),
                "metrics": sorted(metrics, key=lambda x: x["name"]),
                "relationships": relationships
            },
            "tenant": tenant["tenant_id"]
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to get entity details for {entity_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get entity details: {str(e)}"
        )


@app.post("/nl-query")
async def natural_language_query(
    payload: Dict[str, Any],
    background_tasks: BackgroundTasks,
    use_cache: bool = Query(True, description="Use query cache"),
    generate_viz: bool = Query(True, description="Generate visualization"),
    tenant: Dict = Depends(verify_api_key)
):
    """
    End-to-end natural language query processing with schema support.
    
    Flow: NL → LLM intent → semantic validation → SQL → execution → visualization
    
    Example payload:
    {
        "query": "Show me total sales by customer country for last month",
        "options": {
            "timeout": 30,
            "max_rows": 1000
        }
    }
    """
    start_time = time.time()
    
    try:
        # Extract query from payload
        query = payload.get("query", "").strip()
        if not query:
            raise HTTPException(status_code=400, detail="Query is required")
        
        options = payload.get("options", {})
        timeout = options.get("timeout", APP_CONFIG["max_query_execution_time"])
        max_rows = options.get("max_rows", APP_CONFIG["default_limit"])
        
        logger.info(f"[{tenant['tenant_id']}] Processing NL query: {query[:100]}...")
        
        # Step 1: Check cache
        cache_key = None
        if use_cache:
            cache_key = f"{tenant['tenant_id']}:{query}:{max_rows}"
            async with cache_lock:
                if cache_key in query_cache:
                    cached_result = query_cache[cache_key]
                    # Check if cache is still valid (e.g., not too old)
                    if time.time() - cached_result.get("cached_at", 0) < 300:  # 5 minutes
                        logger.info(f"[{tenant['tenant_id']}] Using cached result for query")
                        cached_result["cached"] = True
                        return cached_result
        
        # Step 2: Extract intent using LLM
        intent_result = intent_extractor.extract_intent(query)
        
        if not intent_result.success:
            # Try fallback
            logger.warning(f"[{tenant['tenant_id']}] LLM extraction failed, using fallback")
            intent_result = intent_extractor.extract_intent_fallback(query)
            if not intent_result.success:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "message": "Failed to understand query",
                        "error": intent_result.error,
                        "suggestion": "Try rephrasing your question or use the /query endpoint with structured input"
                    }
                )
        
        intent = intent_result.intent
        intent.limit = min(intent.limit, max_rows)  # Apply limit from options
        
        logger.info(f"[{tenant['tenant_id']}] Extracted intent: {intent.dict()}")
        
        # Step 3: Semantic validation
        validation_errors = semantic_validator.validate_intent(intent)
        if validation_errors:
            logger.warning(f"[{tenant['tenant_id']}] Validation failed: {validation_errors}")
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Semantic validation failed",
                    "errors": validation_errors,
                    "available_metrics": await _get_available_metrics(),
                    "available_dimensions": await _get_available_dimensions()
                }
            )
        
        # Step 4: Generate SQL deterministically
        try:
            sql_result = sql_compiler.compile_sql(intent)
            sql = sql_result["sql"]
            sql_metadata = sql_result["metadata"]
            
            logger.debug(f"[{tenant['tenant_id']}] Generated SQL: {sql[:500]}...")
            
        except Exception as e:
            logger.error(f"[{tenant['tenant_id']}] SQL compilation failed: {e}")
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "SQL compilation failed",
                    "error": str(e),
                    "intent": intent.dict()
                }
            )
        
        # Step 5: Extract schemas involved
        schemas_involved = list(set([
            CATALOG.get_entity(e).schema_name 
            for e in sql_metadata["entities_involved"]
        ]))
        
        logger.info(f"[{tenant['tenant_id']}] Query involves schemas: {schemas_involved}")
        
        # Step 6: Execute query
        execution_result = query_executor.execute_cross_schema_query(
            sql=sql,
            schemas_involved=schemas_involved,
            tenant_id=tenant["tenant_id"]
        )
        
        if not execution_result["success"]:
            logger.error(f"[{tenant['tenant_id']}] Query execution failed: {execution_result.get('error')}")
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "Query execution failed",
                    "error": execution_result.get("error"),
                    "sql": sql[:1000] if APP_CONFIG["debug"] else None
                }
            )
        
        data = execution_result["data"]
        execution_metadata = execution_result["metadata"]
        
        # Step 7: Generate visualization if requested
        visualization = None
        if generate_viz and data:
            query_title = f"{intent.metric.replace('_', ' ').title()}"
            if intent.dimensions:
                query_title += f" by {', '.join(intent.dimensions)}"
            
            try:
                visualization = viz_generator.generate_visualization(
                    data=data,
                    dimensions=intent.dimensions,
                    metric_name=intent.metric,
                    query_title=query_title
                )
            except Exception as e:
                logger.warning(f"[{tenant['tenant_id']}] Visualization generation failed: {e}")
                visualization = {
                    "type": "error",
                    "error": str(e),
                    "fallback": "table"
                }
        
        # Step 8: Build response
        total_time = time.time() - start_time
        
        response = {
            "success": True,
            "query": {
                "natural_language": query,
                "intent": intent.dict(),
                "sql": sql if APP_CONFIG["debug"] else sql[:500] + "..." if len(sql) > 500 else sql,
                "row_count": len(data),
                "execution_time_ms": execution_metadata["execution_time_ms"]
            },
            "data": data[:100] if len(data) > 100 else data,  # Limit data in response
            "visualization": visualization,
            "metadata": {
                "sql_generation": sql_metadata,
                "execution": execution_metadata,
                "performance": {
                    "total_time_ms": round(total_time * 1000, 2),
                    "intent_extraction_ms": round((intent_result.metadata or {}).get("extraction_time", 0), 2),
                    "sql_generation_ms": round((sql_metadata or {}).get("generation_time", 0), 2),
                    "query_execution_ms": execution_metadata["execution_time_ms"]
                },
                "deterministic_hash": visualization.get("deterministic_hash") if visualization else None,
                "schemas_involved": schemas_involved,
                "cache_key": cache_key,
                "tenant": tenant["tenant_id"]
            }
        }
        
        # Step 9: Cache the result if successful
        if use_cache and cache_key and len(data) > 0:
            async with cache_lock:
                response["cached_at"] = time.time()
                query_cache[cache_key] = response.copy()
                # Limit cache size
                if len(query_cache) > 1000:
                    # Remove oldest entries
                    oldest_keys = sorted(query_cache.keys(), 
                                       key=lambda k: query_cache[k].get("cached_at", 0))[:100]
                    for key in oldest_keys:
                        del query_cache[key]
        
        # Step 10: Log query for analytics (in background)
        background_tasks.add_task(
            _log_query_analytics,
            tenant=tenant,
            query=query,
            intent=intent,
            sql_metadata=sql_metadata,
            execution_metadata=execution_metadata,
            success=True,
            total_time=total_time
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{tenant['tenant_id']}] Unexpected error in NL query: {e}", exc_info=True)
        
        # Log failed query
        background_tasks.add_task(
            _log_query_analytics,
            tenant=tenant,
            query=payload.get("query", ""),
            intent=None,
            sql_metadata=None,
            execution_metadata={"error": str(e)},
            success=False,
            total_time=time.time() - start_time
        )
        
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Internal server error",
                "error": str(e) if APP_CONFIG["debug"] else "An unexpected error occurred",
                "request_id": f"req_{int(start_time * 1000)}"
            }
        )


@app.post("/query")
async def structured_query(
    intent: QueryIntent,
    background_tasks: BackgroundTasks,
    generate_viz: bool = Query(True, description="Generate visualization"),
    tenant: Dict = Depends(verify_api_key)
):
    """
    Direct structured query endpoint (bypasses LLM).
    
    Example:
    {
        "metric": "net_profit",
        "dimensions": ["country"],
        "time_range": {
            "type": "last_quarter"
        },
        "filters": [
            {
                "dimension": "status",
                "operator": "equals",
                "values": ["completed"]
            }
        ],
        "limit": 1000
    }
    """
    start_time = time.time()
    
    try:
        logger.info(f"[{tenant['tenant_id']}] Processing structured query: {intent.metric} with dimensions {intent.dimensions}")
        
        # Step 1: Semantic validation
        validation_errors = semantic_validator.validate_intent(intent)
        if validation_errors:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Semantic validation failed",
                    "errors": validation_errors
                }
            )
        
        # Step 2: Generate SQL
        sql_result = sql_compiler.compile_sql(intent)
        sql = sql_result["sql"]
        sql_metadata = sql_result["metadata"]
        
        # Step 3: Extract schemas involved
        schemas_involved = list(set([
            CATALOG.get_entity(e).schema_name 
            for e in sql_metadata["entities_involved"]
        ]))
        
        # Step 4: Execute query
        execution_result = query_executor.execute_cross_schema_query(
            sql=sql,
            schemas_involved=schemas_involved,
            tenant_id=tenant["tenant_id"]
        )
        
        if not execution_result["success"]:
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "Query execution failed",
                    "error": execution_result.get("error")
                }
            )
        
        data = execution_result["data"]
        
        # Step 5: Generate visualization if requested
        visualization = None
        if generate_viz and data:
            query_title = f"{intent.metric.replace('_', ' ').title()}"
            if intent.dimensions:
                query_title += f" by {', '.join(intent.dimensions)}"
            
            try:
                visualization = viz_generator.generate_visualization(
                    data=data,
                    dimensions=intent.dimensions,
                    metric_name=intent.metric,
                    query_title=query_title
                )
            except Exception as e:
                logger.warning(f"[{tenant['tenant_id']}] Visualization generation failed: {e}")
        
        total_time = time.time() - start_time
        
        # Log query
        background_tasks.add_task(
            _log_query_analytics,
            tenant=tenant,
            query="[STRUCTURED]",
            intent=intent,
            sql_metadata=sql_metadata,
            execution_metadata=execution_result["metadata"],
            success=True,
            total_time=total_time
        )
        
        return {
            "success": True,
            "intent": intent.dict(),
            "sql": sql if APP_CONFIG["debug"] else sql[:500] + "..." if len(sql) > 500 else sql,
            "data": data[:100] if len(data) > 100 else data,
            "visualization": visualization,
            "metadata": {
                "sql_generation": sql_metadata,
                "execution": execution_result["metadata"],
                "total_time_ms": round(total_time * 1000, 2),
                "schemas_involved": schemas_involved,
                "tenant": tenant["tenant_id"]
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{tenant['tenant_id']}] Error in structured query: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Query failed: {str(e)}"
        )


@app.get("/explain-query")
async def explain_query_schema(
    metric: str,
    dimensions: str = Query("", description="Comma-separated list of dimensions"),
    time_range: str = Query(None, description="Time range type (last_quarter, last_month, etc)"),
    filters: str = Query(None, description="JSON string of filters"),
    tenant: Dict = Depends(verify_api_key)
):
    """Explain how a query would be executed across schemas."""
    try:
        # Parse dimensions
        dim_list = [d.strip() for d in dimensions.split(",")] if dimensions else []
        
        # Parse time range
        time_range_obj = None
        if time_range:
            try:
                time_range_obj = TimeRange(type=TimeRangeType(time_range))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid time range type. Must be one of: {', '.join([t.value for t in TimeRangeType])}"
                )
        
        # Parse filters
        filter_list = []
        if filters:
            try:
                filter_data = json.loads(filters)
                if isinstance(filter_data, list):
                    for f in filter_data:
                        filter_list.append(FilterCondition(**f))
            except (json.JSONDecodeError, ValueError) as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid filters format: {str(e)}"
                )
        
        # Create intent
        intent = QueryIntent(
            metric=metric,
            dimensions=dim_list,
            time_range=time_range_obj,
            filters=filter_list
        )
        
        # Validate
        validation_errors = semantic_validator.validate_intent(intent)
        if validation_errors:
            return {
                "success": False,
                "message": "Query would fail validation",
                "errors": validation_errors,
                "explanation": "The query cannot be executed due to semantic issues"
            }
        
        # Generate SQL and metadata
        sql_result = sql_compiler.compile_sql(intent)
        sql = sql_result["sql"]
        sql_metadata = sql_result["metadata"]
        
        # Analyze schema usage
        schema_analysis = {}
        entity_details = {}
        
        for entity_name in sql_metadata["entities_involved"]:
            entity = CATALOG.get_entity(entity_name)
            schema = entity.schema_name
            
            if schema not in schema_analysis:
                schema_analysis[schema] = {
                    "entities": [],
                    "database": entity.database,
                    "dialect": entity.dialect.value
                }
            
            entity_info = {
                "entity": entity_name,
                "table": entity.table_name,
                "alias": sql_metadata["entity_aliases"][entity_name],
                "fully_qualified": entity.fully_qualified_name,
                "role": "primary" if entity_name == sql_metadata["primary_entity"] else "joined"
            }
            
            schema_analysis[schema]["entities"].append(entity_info)
            entity_details[entity_name] = {
                "schema": schema,
                "table": entity.table_name,
                "alias": sql_metadata["entity_aliases"][entity_name]
            }
        
        # Analyze join paths
        join_analysis = []
        if "join_order" in sql_metadata:
            join_order = sql_metadata["join_order"]
            for i, entity in enumerate(join_order):
                if i > 0:
                    from_entity = join_order[i-1]
                    to_entity = entity
                    
                    # Find relationship
                    from_entity_obj = CATALOG.get_entity(from_entity)
                    relationship = None
                    for rel in from_entity_obj.relationships.values():
                        if rel.to_entity == to_entity or rel.from_entity == to_entity:
                            relationship = rel
                            break
                    
                    if relationship:
                        join_analysis.append({
                            "from": from_entity,
                            "to": to_entity,
                            "relationship": relationship.name,
                            "type": relationship.relationship_type.value,
                            "cross_schema": entity_details[from_entity]["schema"] != entity_details[to_entity]["schema"],
                            "conditions": relationship.join_conditions
                        })
        
        return {
            "success": True,
            "intent": intent.dict(),
            "explanation": {
                "query_type": f"Metric '{metric}' grouped by {dim_list if dim_list else 'none'}",
                "sql_preview": sql[:500] + "..." if len(sql) > 500 else sql,
                "schema_analysis": schema_analysis,
                "entity_details": entity_details,
                "join_analysis": join_analysis,
                "statistics": {
                    "total_entities": len(sql_metadata["entities_involved"]),
                    "total_schemas": len(schema_analysis),
                    "cross_schema_joins": sql_metadata.get("schema_crossings", 0),
                    "total_joins": len(join_analysis)
                },
                "execution_plan": {
                    "primary_entity": sql_metadata["primary_entity"],
                    "primary_schema": CATALOG.get_entity(sql_metadata["primary_entity"]).schema_name,
                    "join_order": sql_metadata.get("join_order", []),
                    "estimated_complexity": "low" if len(sql_metadata["entities_involved"]) <= 2 else "medium" if len(sql_metadata["entities_involved"]) <= 4 else "high"
                }
            },
            "metadata": {
                "sql_hash": sql_metadata.get("sql_hash"),
                "generated_at": datetime.now().isoformat(),
                "tenant": tenant["tenant_id"]
            }
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"[{tenant['tenant_id']}] Error explaining query: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to explain query: {str(e)}"
        )


@app.get("/connection-status")
async def connection_status(tenant: Dict = Depends(verify_api_key)):
    """Check connection status for all schemas."""
    try:
        status = query_executor.test_schema_connections()
        
        return {
            "success": True,
            "connections": status,
            "healthy": all(status.values()),
            "total_schemas": len(status),
            "healthy_schemas": sum(status.values()),
            "unhealthy_schemas": [k for k, v in status.items() if not v],
            "timestamp": datetime.now().isoformat(),
            "tenant": tenant["tenant_id"]
        }
        
    except Exception as e:
        logger.error(f"[{tenant['tenant_id']}] Error checking connections: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to check connections: {str(e)}"
        )


@app.get("/debug/intent")
async def debug_intent_extraction(
    query: str,
    use_llm: bool = Query(True, description="Use LLM for extraction"),
    tenant: Dict = Depends(verify_api_key)
):
    """Debug endpoint for intent extraction."""
    if not APP_CONFIG["debug"]:
        raise HTTPException(
            status_code=403,
            detail="Debug endpoint only available in debug mode"
        )
    
    try:
        if use_llm:
            result = intent_extractor.extract_intent(query)
            if not result.success:
                result = intent_extractor.extract_intent_fallback(query)
        else:
            result = intent_extractor.extract_intent_fallback(query)
        
        # Validate the extracted intent
        validation_errors = []
        if result.intent:
            validation_errors = semantic_validator.validate_intent(result.intent)
        
        return {
            **result.dict(),
            "validation_errors": validation_errors,
            "is_valid": len(validation_errors) == 0,
            "available_metrics": await _get_available_metrics(),
            "available_dimensions": await _get_available_dimensions()
        }
        
    except Exception as e:
        logger.error(f"[{tenant['tenant_id']}] Debug intent extraction failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Debug failed: {str(e)}"
        )


@app.get("/debug/sql")
async def debug_sql_generation(
    metric: str,
    dimensions: str = Query("", description="Comma-separated list of dimensions"),
    time_range: str = Query(None, description="Time range type"),
    tenant: Dict = Depends(verify_api_key)
):
    """Debug endpoint for SQL generation."""
    if not APP_CONFIG["debug"]:
        raise HTTPException(
            status_code=403,
            detail="Debug endpoint only available in debug mode"
        )
    
    try:
        # Parse dimensions
        dim_list = [d.strip() for d in dimensions.split(",")] if dimensions else []
        
        # Parse time range
        time_range_obj = None
        if time_range:
            time_range_obj = TimeRange(type=TimeRangeType(time_range))
        
        intent = QueryIntent(
            metric=metric,
            dimensions=dim_list,
            time_range=time_range_obj
        )
        
        # Validate
        validation_errors = semantic_validator.validate_intent(intent)
        
        # Generate SQL
        sql_result = sql_compiler.compile_sql(intent)
        
        return {
            "intent": intent.dict(),
            "validation_errors": validation_errors,
            "sql": sql_result["sql"],
            "metadata": sql_result["metadata"],
            "explained": {
                "entities": sql_result["metadata"]["entities_involved"],
                "aliases": sql_result["metadata"]["entity_aliases"],
                "schemas": list(set([
                    CATALOG.get_entity(e).schema_name 
                    for e in sql_result["metadata"]["entities_involved"]
                ]))
            }
        }
        
    except Exception as e:
        logger.error(f"[{tenant['tenant_id']}] Debug SQL generation failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Debug failed: {str(e)}"
        )


@app.get("/cache/status")
async def cache_status(tenant: Dict = Depends(verify_api_key)):
    """Get cache status and statistics."""
    if not APP_CONFIG["debug"]:
        raise HTTPException(
            status_code=403,
            detail="Cache endpoint only available in debug mode"
        )
    
    async with cache_lock:
        cache_stats = {
            "size": len(query_cache),
            "keys": list(query_cache.keys())[:10],  # First 10 keys
            "oldest": min([v.get("cached_at", 0) for v in query_cache.values()]) if query_cache else 0,
            "newest": max([v.get("cached_at", 0) for v in query_cache.values()]) if query_cache else 0,
            "memory_usage_kb": sum(len(str(v).encode()) for v in query_cache.values()) / 1024
        }
    
    return {
        "success": True,
        "cache_enabled": True,
        "stats": cache_stats,
        "tenant": tenant["tenant_id"]
    }


@app.delete("/cache/clear")
async def clear_cache(tenant: Dict = Depends(verify_api_key)):
    """Clear the query cache."""
    if not APP_CONFIG["debug"]:
        raise HTTPException(
            status_code=403,
            detail="Cache endpoint only available in debug mode"
        )
    
    async with cache_lock:
        cleared_count = len(query_cache)
        query_cache.clear()
    
    return {
        "success": True,
        "message": f"Cleared {cleared_count} cache entries",
        "tenant": tenant["tenant_id"]
    }


# Helper functions
async def _get_available_metrics():
    """Get list of available metrics for error messages."""
    metrics = []
    for entity in CATALOG.entities.values():
        metrics.extend(entity.metrics.keys())
    return sorted(metrics)


async def _get_available_dimensions():
    """Get list of available dimensions for error messages."""
    dimensions = []
    for entity in CATALOG.entities.values():
        dimensions.extend(entity.dimensions.keys())
    return sorted(dimensions)


async def _log_query_analytics(
    tenant: Dict,
    query: str,
    intent: Optional[QueryIntent],
    sql_metadata: Optional[Dict],
    execution_metadata: Dict,
    success: bool,
    total_time: float
):
    """Log query for analytics (runs in background)."""
    try:
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "tenant_id": tenant["tenant_id"],
            "plan": tenant["plan"],
            "query": query[:200] if query else "",
            "success": success,
            "total_time_ms": round(total_time * 1000, 2),
            "execution_time_ms": execution_metadata.get("execution_time_ms", 0),
            "row_count": execution_metadata.get("row_count", 0),
            "schemas_involved": execution_metadata.get("schemas_involved", []),
            "cache_hit": execution_metadata.get("cache_hit", False)
        }
        
        if intent:
            log_entry.update({
                "metric": intent.metric,
                "dimensions": intent.dimensions,
                "has_time_range": intent.time_range is not None,
                "filter_count": len(intent.filters)
            })
        
        if sql_metadata:
            log_entry.update({
                "entities_involved": len(sql_metadata.get("entities_involved", [])),
                "cross_schema_joins": sql_metadata.get("schema_crossings", 0)
            })
        
        # In production, save to database or analytics service
        logger.info(f"QUERY_ANALYTICS: {json.dumps(log_entry)}")
        
    except Exception as e:
        logger.error(f"Failed to log query analytics: {e}")


# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "error": exc.detail if isinstance(exc.detail, str) else exc.detail,
            "path": request.url.path,
            "method": request.method,
            "request_id": request.headers.get("X-Request-ID", "unknown")
        }
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    
    error_detail = str(exc) if APP_CONFIG["debug"] else "Internal server error"
    
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": error_detail,
            "path": request.url.path,
            "method": request.method,
            "request_id": request.headers.get("X-Request-ID", "unknown")
        }
    )


# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    logger.info("🚀 Starting Semantic Analytics Engine...")
    
    # Test database connections
    logger.info("Testing database connections...")
    try:
        db_status = query_executor.test_schema_connections()
        healthy_schemas = [k for k, v in db_status.items() if v]
        unhealthy_schemas = [k for k, v in db_status.items() if not v]
        
        logger.info(f"Database connections: {len(healthy_schemas)} healthy, {len(unhealthy_schemas)} unhealthy")
        if unhealthy_schemas:
            logger.warning(f"Unhealthy schemas: {unhealthy_schemas}")
    except Exception as e:
        logger.error(f"Failed to test database connections: {e}")
    
    # Log catalog stats
    catalog_stats = {
        "entities": len(CATALOG.entities),
        "schemas": len(set(e.schema_name for e in CATALOG.entities.values())),
        "metrics": sum(len(e.metrics) for e in CATALOG.entities.values()),
        "dimensions": sum(len(e.dimensions) for e in CATALOG.entities.values())
    }
    logger.info(f"Catalog loaded: {catalog_stats}")
    
    logger.info("✅ Semantic Analytics Engine started successfully")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("🛑 Shutting down Semantic Analytics Engine...")
    
    # Close database connections
    try:
        db_connections.close_all()
        logger.info("Database connections closed")
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")
    
    logger.info("👋 Semantic Analytics Engine shut down")


# Main entry point
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=APP_CONFIG["debug"],
        log_level="debug" if APP_CONFIG["debug"] else "info",
        access_log=True,
        workers=1 if APP_CONFIG["debug"] else 4
    )