"""
Factory module for creating appropriate adapters.
"""

from typing import Any, Dict

from .base import BaseAdapter
from .rest import RESTAdapter
from .file import FileAdapter
from .database import DatabaseAdapter
from .xlsx import XLSXAdapter


def create_adapter(source_type: str, config: Dict[str, Any]) -> BaseAdapter:
    """Factory function to create appropriate adapter.

    Args:
        source_type: Type of data source ('rest', 'file', 'database', 'xlsx')
        config: Configuration for the adapter

    Returns:
        Configured adapter instance

    Raises:
        ValueError: If source_type is not supported
    """
    adapters = {
        'rest': RESTAdapter,
        'file': FileAdapter,
        'database': DatabaseAdapter,
        'xlsx': XLSXAdapter
    }

    if source_type not in adapters:
        raise ValueError(f"Unsupported source type: {source_type}")

    return adapters[source_type](config)
