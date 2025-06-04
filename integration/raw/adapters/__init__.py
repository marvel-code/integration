"""
Adapters package for handling different data source integrations.
"""

from .base import BaseAdapter, DataSource
from .rest import RESTAdapter
from .file import FileAdapter
from .database import DatabaseAdapter
from .xlsx import XLSXAdapter
from .mdb import MDBAdapter
from .factory import create_adapter

__all__ = [
    'BaseAdapter',
    'DataSource',
    'RESTAdapter',
    'FileAdapter',
    'DatabaseAdapter',
    'XLSXAdapter',
    'MDBAdapter',
    'create_adapter',
]
