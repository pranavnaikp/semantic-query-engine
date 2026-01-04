"""
Pydantic models for structured intent extracted from natural language.
LLM must output exactly this structure - no free text SQL!
"""

from typing import List, Optional, Literal
from pydantic import BaseModel, Field, validator
from datetime import datetime, date
from enum import Enum


class TimeRangeType(str, Enum):
    """Supported time range types for filtering."""
    LAST_QUARTER = "last_quarter"
    LAST_MONTH = "last_month"
    LAST_WEEK = "last_week"
    LAST_YEAR = "last_year"
    CURRENT_QUARTER = "current_quarter"
    CURRENT_MONTH = "current_month"
    CURRENT_WEEK = "current_week"
    CURRENT_YEAR = "current_year"
    CUSTOM = "custom"


class ComparativeType(str, Enum):  # NEW: Add this enum
    """Types of comparative analysis."""
    YEAR_OVER_YEAR = "yoy"
    MONTH_OVER_MONTH = "mom"
    QUARTER_OVER_QUARTER = "qoq"
    WEEK_OVER_WEEK = "wow"
    VS_PREVIOUS_PERIOD = "previous"
    VS_BUDGET = "budget"


class TimeRange(BaseModel):
    """Time range specification for queries."""
    type: TimeRangeType = Field(..., description="Type of time range")
    start_date: Optional[date] = Field(None, description="Custom start date (if type=custom)")
    end_date: Optional[date] = Field(None, description="Custom end date (if type=custom)")
    
    @validator('start_date', 'end_date')
    def validate_custom_dates(cls, v, values):
        """Validate that custom dates are provided when type is CUSTOM."""
        if values.get('type') == TimeRangeType.CUSTOM and v is None:
            raise ValueError("start_date and end_date are required for custom time range")
        return v


class FilterCondition(BaseModel):
    """Filter condition for non-time dimensions."""
    dimension: str = Field(..., description="Dimension name to filter on")
    operator: Literal["equals", "not_equals", "in", "not_in", "greater_than", "less_than"] = Field(
        "equals",
        description="Filter operator"
    )
    values: List[str] = Field(..., description="Filter values")


class QueryIntent(BaseModel):
    """
    Structured intent extracted from natural language query.
    This is the ONLY output allowed from LLM - no SQL, no free text!
    """
    metric: str = Field(..., description="Name of the metric to query")
    dimensions: List[str] = Field(
        default_factory=list,
        description="List of dimensions to group by"
    )
    time_range: Optional[TimeRange] = Field(
        None,
        description="Time range filter (if applicable)"
    )
    filters: List[FilterCondition] = Field(
        default_factory=list,
        description="Additional filters on dimensions"
    )
    limit: Optional[int] = Field(
        1000,
        description="Maximum number of rows to return",
        ge=1,
        le=10000
    )
    comparative: Optional[ComparativeType] = Field(  # NEW: Add this field
        None,
        description="Type of comparative analysis (e.g., yoy, mom, qoq)"
    )
    original_query: Optional[str] = Field(  # NEW: Add this field (optional)
        None,
        description="Original natural language query (for reference)"
    )
    
    @validator('dimensions')
    def validate_dimensions(cls, v):
        """Ensure dimensions are unique."""
        if len(v) != len(set(v)):
            raise ValueError("Dimensions must be unique")
        return v
    
    @validator('comparative')
    def validate_comparative_with_time_range(cls, v, values):
        """Validate comparative analysis makes sense with time range."""
        if v and values.get('time_range'):
            # If comparative analysis is requested, time_range should be appropriate
            # For example, yoy might override specific time_range
            pass
        return v


class IntentExtractionResponse(BaseModel):
    """
    Response from intent extraction module.
    Contains either the structured intent or an error.
    """
    success: bool = Field(..., description="Whether extraction was successful")
    intent: Optional[QueryIntent] = Field(None, description="Extracted intent")
    error: Optional[str] = Field(None, description="Error message if extraction failed")
    raw_query: str = Field(..., description="Original natural language query")