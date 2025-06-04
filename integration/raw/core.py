"""
Core module for raw data processing.
"""

import logging
from typing import Any, Dict, List, Optional, Union, TypedDict
from pathlib import Path

from .ingestion import DataIngestion, ProcessedData
from .validation import ValidationRule
from .storage import RawDataStorage

logger = logging.getLogger(__name__)


class RawDataProcessor:
    """Main class for coordinating raw data operations."""

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
        self.ingestion = DataIngestion(validation_rules)
        self.storage = RawDataStorage(output_dir, storage_dir)

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
                # Process file
                processed_data = self.ingestion.process_file(file_path)
                if not processed_data:
                    continue

                # Save processed data
                output_path = self.storage.save_processed_data(
                    processed_data, file_path)
                if output_path:
                    processed_data['output_path'] = str(output_path)
                    results.append(processed_data)

        logger.info(f"Processed {len(results)} files successfully")
        return results

    def get_stored_data(self, prefix: Optional[str] = None) -> List[Path]:
        """Get list of stored data files.

        Args:
            prefix: Optional prefix to filter files

        Returns:
            List of file paths
        """
        return self.storage.get_stored_files(prefix)

    def load_stored_data(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """Load data from storage.

        Args:
            file_path: Path to the stored data file

        Returns:
            Loaded data

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        return self.storage.load_stored_data(file_path)
