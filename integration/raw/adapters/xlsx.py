"""
XLSX adapter module for handling Excel files.
"""

import logging
from typing import Any, Dict, List, Optional
from pathlib import Path
import pandas as pd

from .base import BaseAdapter

logger = logging.getLogger(__name__)


class XLSXAdapter(BaseAdapter):
    """Adapter for Excel file data sources."""

    def _validate_config(self) -> None:
        """Validate Excel file configuration."""
        if 'path' not in self.config:
            raise ValueError("Missing required field: path")
        if 'sheet_name' not in self.config:
            self.config['sheet_name'] = 0  # Default to first sheet

    def _get_engine(self, file_path: Path) -> str:
        """Determine the appropriate engine for reading the Excel file.

        Args:
            file_path: Path to the Excel file

        Returns:
            Engine name ('openpyxl' for .xlsx, 'xlrd' for .xls)

        Raises:
            ValueError: If file extension is not supported
        """
        ext = file_path.suffix.lower()
        if ext == '.xlsx':
            return 'openpyxl'
        elif ext == '.xls':
            return 'xlrd'
        else:
            raise ValueError(f"Unsupported Excel file extension: {ext}")

    def _get_writer_engine(self, file_path: Path) -> str:
        """Determine the appropriate engine for writing the Excel file.

        Args:
            file_path: Path to the Excel file

        Returns:
            Engine name ('openpyxl' for .xlsx, 'xlwt' for .xls)

        Raises:
            ValueError: If file extension is not supported
        """
        ext = file_path.suffix.lower()
        if ext == '.xlsx':
            return 'openpyxl'
        elif ext == '.xls':
            return 'xlwt'
        else:
            raise ValueError(f"Unsupported Excel file extension: {ext}")

    def fetch(self) -> Dict[str, Any]:
        """Fetch data from Excel file.

        Returns:
            Dictionary containing the Excel data

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the sheet doesn't exist
        """
        path = Path(self.config['path'])
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        try:
            # Determine the appropriate engine
            engine = self._get_engine(path)

            # Read Excel file
            df = pd.read_excel(
                path,
                sheet_name=self.config['sheet_name'],
                engine=engine
            )

            # Convert DataFrame to dictionary
            data = {
                'columns': df.columns.tolist(),
                'records': df.to_dict('records')
            }

            # Add metadata
            data['metadata'] = {
                'sheet_name': self.config['sheet_name'],
                'row_count': len(df),
                'column_count': len(df.columns),
                'file_type': path.suffix.lower(),
                'engine_used': engine
            }

            return data

        except Exception as e:
            logger.error(f"Error reading Excel file {path}: {str(e)}")
            raise

    def save(self, data: Dict[str, Any], output_path: Path) -> None:
        """Save data to Excel file.

        Args:
            data: Data to save
            output_path: Path to save the Excel file

        Raises:
            ValueError: If data format is invalid
        """
        try:
            # Convert data to DataFrame
            if 'records' in data:
                df = pd.DataFrame(data['records'])
            else:
                df = pd.DataFrame(data)

            # Create output directory if it doesn't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Determine the appropriate engine
            engine = self._get_writer_engine(output_path)

            # Save to Excel
            df.to_excel(
                output_path,
                index=False,
                engine=engine
            )

            logger.info(
                f"Data saved to Excel file: {output_path} (using engine: {engine})")

        except Exception as e:
            logger.error(f"Error saving Excel file {output_path}: {str(e)}")
            raise
