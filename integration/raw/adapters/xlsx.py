"""
XLSX adapter module for handling Excel files.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import pandas as pd
import re

from .base import BaseAdapter, Table

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

    def _extract_header_data(self, df: pd.DataFrame) -> Tuple[List[str], int]:
        """Extract data from column B above the table and find table start row.

        Args:
            df: DataFrame containing the Excel data

        Returns:
            Tuple of (header_data, table_start_row)
        """
        header_data = []
        table_start_row = 0

        # Check if pattern matches (a1 empty, b1 not empty)
        if pd.isna(df.iloc[0, 0]) and not pd.isna(df.iloc[0, 1]):
            # Extract data from column B until we find a row where both A and B are not empty
            for idx, row in df.iterrows():
                if pd.isna(row.iloc[0]) and not pd.isna(row.iloc[1]):
                    header_data.append(str(row.iloc[1]).strip())
                else:
                    table_start_row = idx
                    break

            # If we found header data, we need to skip those rows
            if header_data:
                table_start_row = len(header_data)

        return header_data, table_start_row

    def _clean_filename(self, text: str) -> str:
        """Clean text to be used in filename.

        Args:
            text: Text to clean

        Returns:
            Cleaned text safe for use in filenames
        """
        # Remove special characters and replace spaces with underscores
        cleaned = re.sub(r'[^\w\s-]', '', text)
        cleaned = re.sub(r'[-\s]+', '_', cleaned)
        return cleaned.strip('-_')

    def fetch(self) -> List[Table]:
        """Fetch data from Excel file.

        Returns:
            List of Table objects containing the Excel data

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the sheet doesn't exist
        """
        path = Path(self.config['path'])
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        try:
            engine = self._get_engine(path)
            df = pd.read_excel(
                path,
                sheet_name=self.config['sheet_name'],
                engine=engine,
                header=None
            )
            header_data, table_start_row = self._extract_header_data(df)
            table_df = df.iloc[table_start_row:].copy()
            header_row = table_df.dropna(how='all').iloc[0]
            table_df = table_df.iloc[1:]
            table_df.columns = header_row
            table_df = table_df.dropna(how='all').reset_index(drop=True)
            columns = list(table_df.columns)
            records = [tuple(row) for row in table_df.values]
            metadata = {
                'sheet_name': self.config['sheet_name'],
                'row_count': len(table_df),
                'column_count': len(table_df.columns),
                'file_type': path.suffix.lower(),
                'engine_used': engine,
                'table_start_row': table_start_row,
                'header_data': header_data
            }
            table = Table(name=str(self.config.get('sheet_name', 0)),
                          columns=columns, records=records, metadata=metadata)
            return [table]
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
