"""
Data ingestion module for handling data loading and processing.
"""

import logging
from typing import Any, Dict, List, Optional, Union, TypedDict
from pathlib import Path
import json
import csv

from .adapters import create_adapter, BaseAdapter
from .validation import DataValidator, ValidationRule

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
    }

    def __init__(self, validation_rules: Optional[List[ValidationRule]] = None):
        """Initialize the data ingestion handler.

        Args:
            validation_rules: Optional list of validation rules to apply
        """
        self.validator = DataValidator(validation_rules or [])

    @staticmethod
    def load_json(file_path: Union[str, Path]) -> Dict[str, Any]:
        """Load data from a JSON file.

        Args:
            file_path: Path to the JSON file

        Returns:
            Loaded data

        Raises:
            FileNotFoundError: If the file doesn't exist
            json.JSONDecodeError: If the file is not valid JSON
        """
        with open(file_path, 'r') as f:
            return json.load(f)

    @staticmethod
    def load_csv(file_path: Union[str, Path], delimiter: str = ',') -> List[Dict[str, Any]]:
        """Load data from a CSV file.

        Args:
            file_path: Path to the CSV file
            delimiter: Character used to separate values

        Returns:
            List of dictionaries containing the CSV data

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            return list(reader)

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
            Adapter type string ('file' or 'xlsx')

        Raises:
            ValueError: If file extension is not supported
        """
        ext = file_path.suffix.lower()
        if ext in ['.xlsx', '.xls']:
            return 'xlsx'
        elif ext in ['.json', '.csv']:
            return 'file'
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
            adapter = create_adapter(
                adapter_type,
                {
                    'path': str(file_path),
                    'format': source_type
                }
            )

            # Fetch and transform data
            adapter_data = adapter.fetch()

            # Validate data
            validation_errors = self.validator.validate(adapter_data)
            if validation_errors:
                error_msg = "\n".join(validation_errors)
                logger.error(
                    f"Validation failed for {file_path}:\n{error_msg}")
                return None

            return {
                'data': adapter_data,
                'source_path': str(file_path),
                'source_type': source_type
            }

        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            return None
