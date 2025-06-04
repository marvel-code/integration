"""
Data storage module for handling processed data output.
"""

import logging
from typing import Any, Dict, List, Optional, Union
from pathlib import Path

from .adapters import create_adapter

logger = logging.getLogger(__name__)


class RawDataStorage:
    """Class for handling data storage operations."""

    def __init__(self, output_dir: Union[str, Path], input_dir: Union[str, Path]):
        """Initialize the data storage handler.

        Args:
            output_dir: Directory for storing processed data
            input_dir: Root directory of input files
        """
        self.output_dir = Path(output_dir) / 'raw'
        self.input_dir = Path(input_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _get_relative_path(self, file_path: Path, base_dir: Path) -> Path:
        """Get path relative to base directory.

        Args:
            file_path: Absolute path to the file
            base_dir: Base directory to get relative path from

        Returns:
            Path relative to base directory
        """
        try:
            return file_path.relative_to(base_dir)
        except ValueError:
            # If file_path is not relative to base_dir, return just the filename
            return Path(file_path.name)

    def _create_output_path(self, source_path: Path, header_data: Optional[List[str]] = None) -> Path:
        """Create output path maintaining directory structure.

        Args:
            source_path: Path to the source file
            header_data: Optional list of header data to use in filename

        Returns:
            Path for the output file
        """
        # Get the full relative path from the input directory
        relative_path = self._get_relative_path(source_path, self.input_dir)
        parent_dir = relative_path.parent
        stem = relative_path.stem

        # If we have header data, use it to create a suffix
        if header_data:
            # Join header data with underscores and clean it
            suffix = '_'.join(header_data)
            # Replace special characters and spaces
            suffix = ''.join(
                c if c.isalnum() or c in '_-' else '_' for c in suffix)
            # Limit suffix length and clean up
            suffix = suffix[:50].strip('_-')
            if suffix:
                stem = f"{stem}_{suffix}"
                logger.info(f"Using header data for filename: {suffix}")

        # Create the full output path maintaining the directory structure
        output_path = self.output_dir / parent_dir / f"{stem}.xlsx"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path

    def save_processed_data(self, data: Dict[str, Any], source_path: Path) -> Optional[Path]:
        """Save processed data to file.

        Args:
            data: Processed data to save
            source_path: Path to the source file

        Returns:
            Path to the saved file or None if saving failed
        """
        try:
            # Get header data if available
            header_data = data.get('header_data')
            if header_data:
                logger.info(f"Found header data: {header_data}")

            # Create output path
            output_path = self._create_output_path(source_path, header_data)

            # Save only the actual data, not the metadata
            actual_data = data['data']
            if isinstance(actual_data, dict) and 'data' in actual_data:
                # If the data is nested in a 'data' field, use that
                actual_data = actual_data['data']

            # Save data using XLSX adapter
            xlsx_adapter = create_adapter('xlsx', {'path': str(output_path)})
            xlsx_adapter.save(actual_data, output_path)
            logger.info(f"Saved processed data to: {output_path}")

            return output_path

        except Exception as e:
            logger.error(f"Error saving processed data: {str(e)}")
            return None

    def get_stored_files(self, prefix: Optional[str] = None) -> List[Path]:
        """Get list of stored data files.

        Args:
            prefix: Optional prefix to filter files

        Returns:
            List of file paths
        """
        return list(self.output_dir.glob('*' if prefix is None else f'*{prefix}*'))

    def load_stored_data(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """Load data from storage.

        Args:
            file_path: Path to the stored data file

        Returns:
            Loaded data

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        xlsx_adapter = create_adapter('xlsx', {'path': str(path)})
        return xlsx_adapter.fetch()
