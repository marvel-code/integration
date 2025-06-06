"""
Core module for raw data processing.
"""

import logging
from typing import Any, Dict, List, Optional, Union, TypedDict
from pathlib import Path
from collections import defaultdict

from .ingestion import DataIngestion, ProcessedData
from .storage import RawDataStorage
from .stats import ProcessingStats

logger = logging.getLogger(__name__)


class RawDataProcessor:
    """Main class for coordinating raw data operations."""

    def __init__(
        self,
        storage_dir: Union[str, Path],
        output_dir: Union[str, Path]
    ):
        """Initialize the raw data processor.

        Args:
            storage_dir: Directory containing raw data files
            output_dir: Directory for storing processed data
        """
        self.storage_dir = Path(storage_dir)
        self.ingestion = DataIngestion()
        self.storage = RawDataStorage(output_dir, storage_dir)

    def process(self) -> List[ProcessedData]:
        """Process all files in the storage directory.

        Returns:
            List of processed data results
        """
        if not self.storage_dir.exists():
            raise ValueError(
                f"Storage directory does not exist: {self.storage_dir}")

        stats = ProcessingStats()

        # Fetch tables
        for file_path in self.storage_dir.rglob('*'):
            if file_path.is_file():
                file_type = file_path.suffix.lower()
                stats.increment_total(file_type)

                # Process file
                processed_data = self.ingestion.process_file(file_path)
                if not processed_data:
                    stats.mark_failed()
                    continue

                # Save processed data
                output_path = self.storage.save_processed_data(
                    processed_data, file_path)
                if output_path:
                    processed_data['output_path'] = str(output_path)
                    stats.mark_processed()
                else:
                    stats.mark_failed()

        # Log summary
        stats.log_summary(logger)
