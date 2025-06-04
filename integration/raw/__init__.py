"""
Raw data handling layer.
This layer is responsible for:
- Initial data ingestion
- Basic data validation
- Raw data storage
- Data format conversion
"""

from .ingestion import DataIngestion
from .validation import DataValidator, ValidationRule
from .storage import RawDataStorage
from .adapters import (
    BaseAdapter,
    RESTAdapter,
    FileAdapter,
    DatabaseAdapter,
    create_adapter,
    DataSource
)
from .core import RawDataProcessor

__all__ = [
    'DataIngestion',
    'DataValidator',
    'ValidationRule',
    'RawDataStorage',
    'BaseAdapter',
    'RESTAdapter',
    'FileAdapter',
    'DatabaseAdapter',
    'create_adapter',
    'DataSource',
    'RawDataProcessor',
]
