"""
Raw data processing module.

- Ingestion, storage, and adapter management
"""

from .ingestion import DataIngestion
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
    'RawDataStorage',
    'BaseAdapter',
    'RESTAdapter',
    'FileAdapter',
    'DatabaseAdapter',
    'create_adapter',
    'DataSource',
    'RawDataProcessor',
]
