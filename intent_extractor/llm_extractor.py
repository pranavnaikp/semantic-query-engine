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
from dotenv import load_dotenv

load_dotenv()


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
        
        IMPORTANT: For comparative queries, use the "comparative" field with these values:
        - "yoy" for year-over-year comparisons
        - "mom" for month-over-month comparisons  
        - "qoq" for quarter-over-quarter comparisons
        - "previous" for vs previous period
        
        RULES:
        1. NEVER generate SQL, JOINs, or aggregation logic
        2. ONLY output valid JSON matching the exact schema below
        3. Extract metric names as they appear in the query
        4. Extract dimension names for grouping
        5. For comparative queries, set the "comparative" field
        6. When years are mentioned (like 2023, 2024), it's ALWAYS a comparative query
        7. When "and" connects two years or periods, it's ALWAYS comparative
        
        OUTPUT SCHEMA (JSON):
        {
            "metric": "string",
            "dimensions": ["string"],
            "time_range": {...},
            "filters": [...],
            "limit": integer,
            "comparative": "yoy|mom|qoq|previous"  // ONLY if comparative query
        }
        
        EXAMPLES:
        
        Query: "Show me monthly revenue"
        Output: {"metric": "revenue", "dimensions": [], "time_range": null, "filters": [], "limit": 1000}
        
        Query: "Show me revenue by country"
        Output: {"metric": "revenue", "dimensions": ["country"], "time_range": null, "filters": [], "limit": 1000}
        
        Query: "How much did revenue increase compared to last year?"
        Output: {"metric": "revenue", "dimensions": [], "time_range": null, "filters": [], "limit": 1000, "comparative": "yoy"}
        
        Query: "Show me MoM growth by country"
        Output: {"metric": "revenue", "dimensions": ["country"], "time_range": null, "filters": [], "limit": 1000, "comparative": "mom"}
        
        Query: "Compare this quarter's revenue to last quarter"
        Output: {"metric": "revenue", "dimensions": [], "time_range": {"type": "current_quarter"}, "filters": [], "limit": 1000, "comparative": "qoq"}
        
        Query: "Revenue growth by segment year-over-year"
        Output: {"metric": "revenue", "dimensions": ["segment"], "time_range": null, "filters": [], "limit": 1000, "comparative": "yoy"}
        
        Query: "Show me revenue by country for 2023 and 2024"
        Output: {"metric": "revenue", "dimensions": ["country"], "time_range": null, "filters": [], "limit": 1000, "comparative": "yoy"}
        
        Query: "Compare revenue between 2024 and 2023"
        Output: {"metric": "revenue", "dimensions": [], "time_range": null, "filters": [], "limit": 1000, "comparative": "yoy"}
        
        Query: "Revenue in 2023 vs 2024 by segment"
        Output: {"metric": "revenue", "dimensions": ["segment"], "time_range": null, "filters": [], "limit": 1000, "comparative": "yoy"}
        
        Query: "Show me revenue growth from 2023 to 2024"
        Output: {"metric": "revenue", "dimensions": [], "time_range": null, "filters": [], "limit": 1000, "comparative": "yoy"}
        
        REMEMBER: NO SQL, ONLY JSON. 
        - If query mentions two years (2023 and 2024, 2024 vs 2023, from 2023 to 2024), it's ALWAYS comparative (yoy)
        - If query mentions "compare", "vs", "versus", "and", "between X and Y", it's usually comparative
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
            "limit": 1000,
            "comparative": None  # Add comparative field
        }
        
        query_lower = query.lower()
        
        # Extract metric
        if "net profit" in query_lower or "profit" in query_lower:
            intent_dict["metric"] = "net_profit"
        elif "order count" in query_lower or "number of orders" in query_lower or "orders" in query_lower:
            intent_dict["metric"] = "order_count"
        elif "user count" in query_lower or "number of users" in query_lower:
            intent_dict["metric"] = "user_count"
        elif "customer count" in query_lower or "number of customers" in query_lower:
            intent_dict["metric"] = "customer_count"
        elif "average order" in query_lower or "aov" in query_lower:
            intent_dict["metric"] = "average_order_value"
        elif "unique customer" in query_lower:
            intent_dict["metric"] = "unique_customers"
        elif "lifetime value" in query_lower or "ltv" in query_lower:
            intent_dict["metric"] = "total_lifetime_value"
        elif "revenue" in query_lower:
            intent_dict["metric"] = "revenue"
        elif "amount" in query_lower or "sales" in query_lower:
            intent_dict["metric"] = "revenue"
        
        # Extract dimensions
        if "by country" in query_lower or "per country" in query_lower or "country" in query_lower and "by" in query_lower:
            intent_dict["dimensions"].append("country")
        if "by product" in query_lower or "product category" in query_lower:
            intent_dict["dimensions"].append("product_category")
        if "by segment" in query_lower or "customer segment" in query_lower:
            intent_dict["dimensions"].append("segment")
        if "by status" in query_lower or "order status" in query_lower:
            intent_dict["dimensions"].append("status")
        if "by month" in query_lower or "monthly" in query_lower:
            intent_dict["dimensions"].append("order_date")
        if "by date" in query_lower:
            intent_dict["dimensions"].append("order_date")
        if "by customer" in query_lower or "per customer" in query_lower:
            intent_dict["dimensions"].append("full_name")
        
        # Extract time range and check for specific years
        found_years = []
        for year in [2021, 2022, 2023, 2024, 2025, 2026]:
            if str(year) in query:
                found_years.append(year)
        
        if found_years:
            if len(found_years) >= 2:
                # If multiple years mentioned, create a custom time range covering all
                min_year = min(found_years)
                max_year = max(found_years)
                intent_dict["time_range"] = {
                    "type": "custom",
                    "start_date": f"{min_year}-01-01",
                    "end_date": f"{max_year}-12-31"
                }
            else:
                # Single year mentioned
                year = found_years[0]
                intent_dict["time_range"] = {
                    "type": "custom",
                    "start_date": f"{year}-01-01",
                    "end_date": f"{year}-12-31"
                }
        elif "last quarter" in query_lower:
            intent_dict["time_range"] = {"type": "last_quarter"}
        elif "last month" in query_lower:
            intent_dict["time_range"] = {"type": "last_month"}
        elif "last year" in query_lower:
            intent_dict["time_range"] = {"type": "last_year"}
        elif "this quarter" in query_lower:
            intent_dict["time_range"] = {"type": "current_quarter"}
        elif "this month" in query_lower:
            intent_dict["time_range"] = {"type": "current_month"}
        elif "this year" in query_lower:
            intent_dict["time_range"] = {"type": "current_year"}
        elif "current" in query_lower and "quarter" in query_lower:
            intent_dict["time_range"] = {"type": "current_quarter"}
        elif "current" in query_lower and "month" in query_lower:
            intent_dict["time_range"] = {"type": "current_month"}
        elif "current" in query_lower and "year" in query_lower:
            intent_dict["time_range"] = {"type": "current_year"}
        
        # EXTRACT COMPARATIVE INTENT
        comparative = None
        
        # Check for year mentions (if multiple years or comparison keywords with years)
        if len(found_years) >= 2:
            comparative = "yoy"  # Multiple years = year-over-year comparison
        
        # Check for comparative keywords
        elif any(word in query_lower for word in ['compared to last year', 'year over year', 'yoy', 'vs last year', 'year-on-year', 'year over']):
            comparative = "yoy"
        elif any(word in query_lower for word in ['compared to last month', 'month over month', 'mom', 'vs last month', 'month-on-month', 'month over']):
            comparative = "mom"
        elif any(word in query_lower for word in ['compared to last quarter', 'quarter over quarter', 'qoq', 'vs last quarter', 'quarter-on-quarter', 'quarter over']):
            comparative = "qoq"
        elif any(word in query_lower for word in ['compared to previous', 'vs previous', 'previous period', 'last period']):
            comparative = "previous"
        elif any(word in query_lower for word in ['compare', 'vs', 'versus', 'and', 'between', 'difference', 'growth', 'increase', 'decrease', 'change']):
            comparative = "yoy"  # Default to yoy for comparison queries
        
        if comparative:
            intent_dict["comparative"] = comparative
        
        # Extract filters
        filters = []
        
        # Status filters
        if "where status is" in query_lower or "status is" in query_lower:
            if "completed" in query_lower:
                filters.append({
                    "dimension": "status",
                    "operator": "equals",
                    "values": ["completed"]
                })
            elif "pending" in query_lower:
                filters.append({
                    "dimension": "status",
                    "operator": "equals", 
                    "values": ["pending"]
                })
        
        # Segment filters
        if "enterprise" in query_lower and ("segment" in query_lower or "user" in query_lower):
            filters.append({
                "dimension": "segment",
                "operator": "equals",
                "values": ["enterprise"]
            })
        elif "premium" in query_lower and ("segment" in query_lower or "user" in query_lower):
            filters.append({
                "dimension": "segment", 
                "operator": "equals",
                "values": ["premium"]
            })
        elif "standard" in query_lower and ("segment" in query_lower or "user" in query_lower):
            filters.append({
                "dimension": "segment",
                "operator": "equals",
                "values": ["standard"]
            })
        
        # Country filters
        if "us" in query_lower or "usa" in query_lower or "united states" in query_lower:
            if "country" in query_lower or "where" in query_lower:
                filters.append({
                    "dimension": "country",
                    "operator": "equals",
                    "values": ["US"]
                })
        elif "uk" in query_lower or "united kingdom" in query_lower:
            if "country" in query_lower or "where" in query_lower:
                filters.append({
                    "dimension": "country",
                    "operator": "equals", 
                    "values": ["UK"]
                })
        elif "germany" in query_lower or "de" in query_lower:
            if "country" in query_lower or "where" in query_lower:
                filters.append({
                    "dimension": "country",
                    "operator": "equals",
                    "values": ["DE"]
                })
        
        if filters:
            intent_dict["filters"] = filters
        
        # Clean up: remove duplicate dimensions
        if intent_dict["dimensions"]:
            intent_dict["dimensions"] = list(dict.fromkeys(intent_dict["dimensions"]))
        
        # Special handling for comparative queries without explicit metric
        if comparative and intent_dict["metric"] == "revenue" and "revenue" not in query_lower:
            # If comparative query doesn't mention revenue but we defaulted to it
            # Check if another metric is implied
            if "order" in query_lower:
                intent_dict["metric"] = "order_count"
            elif "customer" in query_lower:
                intent_dict["metric"] = "customer_count"
            elif "profit" in query_lower:
                intent_dict["metric"] = "net_profit"
        
        try:
            intent = QueryIntent(**intent_dict)
            return IntentExtractionResponse(
                success=True,
                intent=intent,
                error=None,
                raw_query=query
            )
        except Exception as e:
            print(f"Fallback extraction failed: {e}")
            # Return minimal valid intent as last resort
            minimal_intent_dict = {
                "metric": "revenue",
                "dimensions": [],
                "time_range": None,
                "filters": [],
                "limit": 1000,
                "comparative": None
            }
            try:
                intent = QueryIntent(**minimal_intent_dict)
                return IntentExtractionResponse(
                    success=True,
                    intent=intent,
                    error=f"Used minimal intent after fallback failure: {str(e)}",
                    raw_query=query
                )
            except Exception as final_error:
                return IntentExtractionResponse(
                    success=False,
                    intent=None,
                    error=f"Fallback extraction completely failed: {str(final_error)}",
                    raw_query=query
                )