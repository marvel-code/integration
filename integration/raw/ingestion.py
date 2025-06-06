"""
Data ingestion module for handling data loading and processing.
"""

import logging
from typing import Any, Dict, List, Optional, Union, TypedDict
from pathlib import Path
import json
import csv

from .adapters import create_adapter, BaseAdapter

logger = logging.getLogger(__name__)


class ProcessedData(TypedDict):
    """Type definition for processed data result."""
    data: Dict[str, Any]
    source_path: str
    source_type: str
    output_path: str


class DataIngestion:
    """Class for handling data ingestion operations."""

    SUPPORTED_FORMATS = {
        '.json': 'json',
        '.csv': 'csv',
        '.xlsx': 'xlsx',
        '.xls': 'xlsx',
        '.mdb': 'mdb',
        '.accdb': 'mdb'
    }

    def __init__(self):
        pass

    def _get_source_type(self, file_path: Path) -> Optional[str]:
        """Determine the source type based on file extension.

        Args:
            file_path: Path to the file

        Returns:
            Source type string or None if not supported
        """
        ext = file_path.suffix.lower()
        return self.SUPPORTED_FORMATS.get(ext)

    def _get_adapter_type(self, file_path: Path) -> str:
        """Determine the appropriate adapter type based on file extension.

        Args:
            file_path: Path to the file

        Returns:
            Adapter type string ('file', 'xlsx', or 'mdb')

        Raises:
            ValueError: If file extension is not supported
        """
        ext = file_path.suffix.lower()
        if ext in ['.xlsx', '.xls']:
            return 'xlsx'
        elif ext in ['.json', '.csv']:
            return 'file'
        elif ext in ['.mdb', '.accdb']:
            return 'mdb'
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

    def process_file(self, file_path: Path) -> Optional[ProcessedData]:
        """Process a single file.

        Args:
            file_path: Path to the file to process

        Returns:
            Processed data or None if processing failed
        """
        source_type = self._get_source_type(file_path)
        if not source_type:
            logger.warning(f"Unsupported file format: {file_path}")
            return None

        try:
            # Get appropriate adapter type
            adapter_type = self._get_adapter_type(file_path)

            # Configure adapter for the file
            adapter_config = {
                'path': str(file_path),
                'format': source_type
            }

            adapter = create_adapter(adapter_type, adapter_config)

            # Fetch and transform data
            adapter_data = adapter.fetch()

            return {
                'data': adapter_data,
                'source_path': str(file_path),
                'source_type': source_type
            }

        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            return None
