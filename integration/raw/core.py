"""
Core module for raw data processing.
"""

import logging
from typing import Any, Dict, List, Optional, Union, TypedDict
from pathlib import Path
from collections import defaultdict

from .ingestion import DataIngestion, ProcessedData
from .storage import RawDataStorage

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

        # Initialize counters and stats
        stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'by_type': defaultdict(int),
            'by_status': defaultdict(int)
        }

        results = []
        for file_path in self.storage_dir.rglob('*'):
            if file_path.is_file():
                stats['total_files'] += 1
                file_type = file_path.suffix.lower()
                stats['by_type'][file_type] += 1

                # Process file
                processed_data = self.ingestion.process_file(file_path)
                if not processed_data:
                    stats['failed_files'] += 1
                    stats['by_status']['failed'] += 1
                    continue

                # Save processed data
                output_path = self.storage.save_processed_data(
                    processed_data, file_path)
                if output_path:
                    processed_data['output_path'] = str(output_path)
                    results.append(processed_data)
                    stats['processed_files'] += 1
                    stats['by_status']['success'] += 1
                else:
                    stats['failed_files'] += 1
                    stats['by_status']['failed'] += 1

        # Log summary
        logger.info("Raw Layer Processing Summary:")
        logger.info(f"Total files found: {stats['total_files']}")
        logger.info(f"Successfully processed: {stats['processed_files']}")
        logger.info(f"Failed to process: {stats['failed_files']}")

        logger.info("\nFile types processed:")
        for file_type, count in stats['by_type'].items():
            logger.info(f"  {file_type}: {count} files")

        logger.info("\nProcessing status:")
        for status, count in stats['by_status'].items():
            logger.info(f"  {status}: {count} files")

        return results
