"""
Integration package with layered architecture:
- raw: Raw data handling and initial processing
- cleansing: Data cleaning and normalization
- business_logic: Core business rules and transformations
- deduplication: Duplicate detection and handling
"""

from . import raw
from . import cleansing
from . import business_logic
from . import deduplication

__all__ = ['raw', 'cleansing', 'business_logic', 'deduplication']
