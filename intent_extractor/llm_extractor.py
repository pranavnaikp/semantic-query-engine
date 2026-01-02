"""
LLM-based intent extractor.
ONLY extracts structured intent JSON - NEVER generates SQL!
This is a critical boundary in our architecture.
"""

import os
from typing import Optional
import json
from openai import OpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.chat_models import ChatOpenAI

from intent_extractor.intent_models import QueryIntent, IntentExtractionResponse


class IntentExtractor:
    """
    Uses LLM ONLY to extract structured intent from natural language.
    Business logic remains in semantic catalog - LLM doesn't know about joins or SQL.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize with OpenAI API key."""
        api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI API key is required")
        
        self.client = OpenAI(api_key=api_key)
        self.llm = ChatOpenAI(
            model="gpt-3.5-turbo",
            temperature=0.1,  # Low temperature for consistency
            openai_api_key=api_key
        )
        
        # Define the prompt template
        self.prompt_template = ChatPromptTemplate.from_messages([
            ("system", self._get_system_prompt()),
            ("human", "{query}")
        ])
    
    def _get_system_prompt(self) -> str:
        """
        System prompt that STRICTLY forbids SQL generation.
        Forces LLM to output only structured JSON matching our QueryIntent schema.
        """
        return """
        You are a semantic query intent extractor. Your ONLY job is to extract structured intent from natural language queries.
        
        RULES:
        1. NEVER generate SQL, JOINs, or aggregation logic
        2. ONLY output valid JSON matching the exact schema below
        3. Extract metric names as they appear in the query (e.g., "revenue", "net profit", "user count")
        4. Extract dimension names for grouping (e.g., "by country" -> ["country"])
        5. Extract time ranges if mentioned (e.g., "last quarter" -> type: "last_quarter")
        6. Extract filters if specified (e.g., "where status is active" -> filter on "status" dimension)
        
        OUTPUT SCHEMA (JSON):
        {
            "metric": "string",  # The metric being requested
            "dimensions": ["string"],  # List of dimensions to group by
            "time_range": {  # Optional time range
                "type": "last_quarter|last_month|last_week|last_year|current_quarter|current_month|current_week|current_year|custom",
                "start_date": "YYYY-MM-DD",  # Only if type=custom
                "end_date": "YYYY-MM-DD"     # Only if type=custom
            },
            "filters": [  # Optional additional filters
                {
                    "dimension": "string",
                    "operator": "equals|not_equals|in|not_in|greater_than|less_than",
                    "values": ["string"]
                }
            ],
            "limit": integer  # Optional, default 1000
        }
        
        EXAMPLES:
        
        Query: "Show me monthly revenue"
        Output: {"metric": "revenue", "dimensions": [], "time_range": null, "filters": [], "limit": 1000}
        
        Query: "Give me total net profit for last quarter by country"
        Output: {"metric": "net_profit", "dimensions": ["country"], "time_range": {"type": "last_quarter"}, "filters": [], "limit": 1000}
        
        Query: "Count of orders by product category where status is completed"
        Output: {"metric": "order_count", "dimensions": ["product_category"], "time_range": null, "filters": [{"dimension": "order_status", "operator": "equals", "values": ["completed"]}], "limit": 1000}
        
        Query: "Revenue for enterprise users in Q3 2023"
        Output: {"metric": "revenue", "dimensions": [], "time_range": {"type": "custom", "start_date": "2023-07-01", "end_date": "2023-09-30"}, "filters": [{"dimension": "user_segment", "operator": "equals", "values": ["enterprise"]}], "limit": 1000}
        
        Query: "Average order value by country and month for last year"
        Output: {"metric": "average_order_value", "dimensions": ["country", "order_date"], "time_range": {"type": "last_year"}, "filters": [], "limit": 1000}
        
        REMEMBER: NO SQL, ONLY JSON. If unsure about a dimension name, use the most common business term.
        """
    
    def extract_intent(self, query: str) -> IntentExtractionResponse:
        """
        Extract structured intent from natural language query.
        Returns either the intent or an error message.
        """
        try:
            # Create prompt
            prompt = self.prompt_template.format_messages(query=query)
            
            # Get LLM response
            response = self.llm.invoke(prompt)
            
            # Parse JSON from response
            try:
                # Extract JSON from the response (might have markdown code blocks)
                content = response.content.strip()
                
                # Remove markdown code blocks if present
                if content.startswith("```json"):
                    content = content[7:-3]  # Remove ```json and ```
                elif content.startswith("```"):
                    content = content[3:-3]  # Remove ``` and ```
                
                # Parse JSON
                intent_dict = json.loads(content)
                
                # Validate against our Pydantic model
                intent = QueryIntent(**intent_dict)
                
                return IntentExtractionResponse(
                    success=True,
                    intent=intent,
                    error=None,
                    raw_query=query
                )
                
            except json.JSONDecodeError as e:
                return IntentExtractionResponse(
                    success=False,
                    intent=None,
                    error=f"Failed to parse LLM response as JSON: {str(e)}. Response: {response.content}",
                    raw_query=query
                )
            except Exception as e:
                return IntentExtractionResponse(
                    success=False,
                    intent=None,
                    error=f"Failed to validate intent schema: {str(e)}",
                    raw_query=query
                )
                
        except Exception as e:
            return IntentExtractionResponse(
                success=False,
                intent=None,
                error=f"Intent extraction failed: {str(e)}",
                raw_query=query
            )
    
    def extract_intent_fallback(self, query: str) -> IntentExtractionResponse:
        """
        Fallback method if LLM fails or is unavailable.
        Uses simple rule-based extraction for common patterns.
        """
        # Simple keyword matching for demo purposes
        # In production, this would be more sophisticated
        
        intent_dict = {
            "metric": "revenue",  # Default
            "dimensions": [],
            "time_range": None,
            "filters": [],
            "limit": 1000
        }
        
        query_lower = query.lower()
        
        # Extract metric
        if "net profit" in query_lower:
            intent_dict["metric"] = "net_profit"
        elif "order count" in query_lower or "number of orders" in query_lower:
            intent_dict["metric"] = "order_count"
        elif "user count" in query_lower or "number of users" in query_lower:
            intent_dict["metric"] = "user_count"
        elif "average order" in query_lower or "aov" in query_lower:
            intent_dict["metric"] = "average_order_value"
        elif "unique customer" in query_lower:
            intent_dict["metric"] = "unique_customers"
        
        # Extract dimensions
        if "by country" in query_lower:
            intent_dict["dimensions"].append("country")
        if "by product" in query_lower:
            intent_dict["dimensions"].append("product_category")
        if "by segment" in query_lower:
            intent_dict["dimensions"].append("user_segment")
        if "by status" in query_lower:
            intent_dict["dimensions"].append("order_status")
        if "by month" in query_lower or "monthly" in query_lower:
            # Assuming metric has a time dimension
            intent_dict["dimensions"].append("order_date")
        
        # Extract time range
        if "last quarter" in query_lower:
            intent_dict["time_range"] = {"type": "last_quarter"}
        elif "last month" in query_lower:
            intent_dict["time_range"] = {"type": "last_month"}
        elif "last year" in query_lower:
            intent_dict["time_range"] = {"type": "last_year"}
        
        try:
            intent = QueryIntent(**intent_dict)
            return IntentExtractionResponse(
                success=True,
                intent=intent,
                error=None,
                raw_query=query
            )
        except Exception as e:
            return IntentExtractionResponse(
                success=False,
                intent=None,
                error=f"Fallback extraction failed: {str(e)}",
                raw_query=query
            )