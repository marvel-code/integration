"""
Core module for raw data processing.
"""

import logging
from typing import Any, Dict, List, Optional, Union, TypedDict
from pathlib import Path
import shutil

from .ingestion import DataIngestion
from .validation import DataValidator, ValidationRule
from .storage import RawDataStorage
from .adapters import create_adapter, BaseAdapter

logger = logging.getLogger(__name__)


class ProcessedData(TypedDict):
    """Type definition for processed data result."""
    data: Dict[str, Any]
    source_path: str
    source_type: str


class RawDataProcessor:
    """Main class for coordinating raw data operations."""

    SUPPORTED_FORMATS = {
        '.json': 'json',
        '.csv': 'csv',
        '.xlsx': 'xlsx',
        '.xls': 'xlsx',
    }

    def __init__(
        self,
        storage_dir: Union[str, Path],
        output_dir: Union[str, Path],
        validation_rules: Optional[List[ValidationRule]] = None
    ):
        """Initialize the raw data processor.

        Args:
            storage_dir: Directory containing raw data files
            output_dir: Directory for storing processed data
            validation_rules: Optional list of validation rules to apply
        """
        self.storage_dir = Path(storage_dir)
        self.output_dir = Path(output_dir)
        self.validator = DataValidator(validation_rules or [])

        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _get_source_type(self, file_path: Path) -> Optional[str]:
        """Determine the source type based on file extension.

        Args:
            file_path: Path to the file

        Returns:
            Source type string or None if not supported
        """
        ext = file_path.suffix.lower()
        return self.SUPPORTED_FORMATS.get(ext)

    def _get_relative_path(self, file_path: Path) -> Path:
        """Get path relative to storage directory.

        Args:
            file_path: Absolute path to the file

        Returns:
            Path relative to storage directory
        """
        return file_path.relative_to(self.storage_dir)

    def _create_output_path(self, source_path: Path) -> Path:
        """Create output path maintaining directory structure.

        Args:
            source_path: Path to the source file

        Returns:
            Path for the output file
        """
        relative_path = self._get_relative_path(source_path)
        output_path = self.output_dir / relative_path.with_suffix('.xlsx')
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path

    def _process_file(self, file_path: Path) -> Optional[ProcessedData]:
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
            # Configure adapter for the file
            adapter = create_adapter(
                'file' if source_type in ['json', 'csv'] else 'xlsx',
                {
                    'path': str(file_path),
                    'format': source_type
                }
            )

            # Fetch and transform data
            raw_data = adapter.fetch()
            transformed_data = adapter.transform(raw_data)

            # Validate data
            validation_errors = self.validator.validate(transformed_data)
            if validation_errors:
                error_msg = "\n".join(validation_errors)
                logger.error(
                    f"Validation failed for {file_path}:\n{error_msg}")
                return None

            # Create output path and save as xlsx
            output_path = self._create_output_path(file_path)
            xlsx_adapter = create_adapter('xlsx', {'path': str(output_path)})
            xlsx_adapter.save(transformed_data['data'], output_path)

            return {
                'data': transformed_data,
                'source_path': str(file_path),
                'source_type': source_type
            }

        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            return None

    def process(self) -> List[ProcessedData]:
        """Process all files in the storage directory.

        Returns:
            List of processed data results
        """
        if not self.storage_dir.exists():
            raise ValueError(
                f"Storage directory does not exist: {self.storage_dir}")

        results = []
        for file_path in self.storage_dir.rglob('*'):
            if file_path.is_file():
                result = self._process_file(file_path)
                if result:
                    results.append(result)

        logger.info(f"Processed {len(results)} files successfully")
        return results

    def load_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """Load data from a file.

        Args:
            file_path: Path to the file

        Returns:
            Loaded data

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        return DataIngestion.load_json(file_path)

    def load_csv(self, file_path: Union[str, Path], delimiter: str = ',') -> List[Dict[str, Any]]:
        """Load data from a CSV file.

        Args:
            file_path: Path to the CSV file
            delimiter: Character used to separate values

        Returns:
            List of dictionaries containing the CSV data

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        return DataIngestion.load_csv(file_path, delimiter)

    def get_stored_data(self, prefix: Optional[str] = None) -> List[Path]:
        """Get list of stored data files.

        Args:
            prefix: Optional prefix to filter files

        Returns:
            List of file paths
        """
        return self.storage_dir.glob('*' if prefix is None else f'*{prefix}*')

    def load_stored_data(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """Load data from storage.

        Args:
            file_path: Path to the stored data file

        Returns:
            Loaded data

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        return self.storage_dir.load(file_path)
