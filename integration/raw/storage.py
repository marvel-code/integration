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

    def _create_output_path(self, source_path: Path, table_name: Optional[str] = None, header_data: Optional[List[str]] = None) -> Path:
        """Create output path preserving input folder structure. Only create subfolder for multi-table files (table_name provided)."""
        relative_path = self._get_relative_path(source_path, self.input_dir)
        orig_ext = relative_path.suffix.lower().lstrip('.')
        relative_dir = relative_path.with_suffix("")
        if table_name:
            # For multi-table files, create a subfolder for the file
            output_dir = self.output_dir / relative_dir.parent / relative_dir.stem
            output_dir.mkdir(parents=True, exist_ok=True)
            base_filename = f"{relative_dir.stem}_{table_name}"
            if orig_ext:
                base_filename = f"{base_filename}_{orig_ext}"
            output_path = output_dir / f"{base_filename}.xlsx"
        else:
            # For single-table files, mirror the input structure, no extra subfolder
            output_dir = self.output_dir / relative_dir.parent
            output_dir.mkdir(parents=True, exist_ok=True)
            base_filename = relative_dir.stem
            # If header_data is provided, append a suffix
            if header_data:
                suffix = '_'.join(header_data)
                suffix = ''.join(
                    c if c.isalnum() or c in '_-' else '_' for c in suffix)
                suffix = suffix[:50].strip('_-')
                if suffix:
                    base_filename = f"{base_filename}_{suffix}"
                    logger.info(f"Using header data for filename: {suffix}")
            if orig_ext:
                base_filename = f"{base_filename}_{orig_ext}"
            output_path = output_dir / f"{base_filename}.xlsx"
        return output_path

    def save_processed_data(self, data: Dict[str, Any], source_path: Path) -> Optional[List[Path]]:
        """Save processed data to file(s), preserving input folder structure. For MDBs, save each table separately."""
        try:
            actual_data = data['data']
            saved_paths = []
            # Check if this is an MDB with multiple tables
            if isinstance(actual_data, dict) and 'tables' in actual_data and 'metadata' in actual_data:
                # Save each table as a separate file
                for table_name, table_data in actual_data['tables'].items():
                    output_path = self._create_output_path(
                        source_path, table_name=table_name)
                    xlsx_adapter = create_adapter(
                        'xlsx', {'path': str(output_path)})
                    xlsx_adapter.save(table_data, output_path)
                    logger.info(
                        f"Saved table '{table_name}' to: {output_path}")
                    saved_paths.append(output_path)
            else:
                # For non-MDB, save as before
                header_data = data.get('header_data')
                output_path = self._create_output_path(
                    source_path, header_data=header_data)
                xlsx_adapter = create_adapter(
                    'xlsx', {'path': str(output_path)})
                xlsx_adapter.save(actual_data, output_path)
                logger.info(f"Saved processed data to: {output_path}")
                saved_paths.append(output_path)
            return saved_paths
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
        # Search for files directly in the output directory (flat structure)
        pattern = f'{prefix}*' if prefix else '*'
        return [f for f in self.output_dir.glob(pattern) if f.is_file()]

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
