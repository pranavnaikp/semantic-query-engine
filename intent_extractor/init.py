"""
Intent extraction package for natural language to structured intent conversion.
LLM is used ONLY for intent extraction, never for SQL generation.
"""

from intent_extractor.intent_models import (
    TimeRangeType,
    TimeRange,
    FilterCondition,
    QueryIntent,
    IntentExtractionResponse
)

from intent_extractor.llm_extractor import IntentExtractor

__all__ = [
    'TimeRangeType',
    'TimeRange',
    'FilterCondition',
    'QueryIntent',
    'IntentExtractionResponse',
    'IntentExtractor'
]

__version__ = "1.0.0"