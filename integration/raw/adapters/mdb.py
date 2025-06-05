"""
MDB adapter module for handling Microsoft Access Database files.
"""

import logging
import shutil
from typing import Any, Dict, List
from pathlib import Path
import pandas as pd
import subprocess
import csv
import io

from .base import BaseAdapter

logger = logging.getLogger(__name__)


class MDBAdapter(BaseAdapter):
    """Adapter for Microsoft Access Database files."""

    REQUIRED_TOOLS = ['mdb-tables', 'mdb-export']

    def __init__(self, config: Dict[str, Any]):
        """Initialize the MDB adapter.

        Args:
            config: Configuration dictionary

        Raises:
            RuntimeError: If required mdbtools are not installed
        """
        super().__init__(config)
        self._check_mdbtools()

    def _check_mdbtools(self) -> None:
        """Check if required mdbtools are installed.

        Raises:
            RuntimeError: If any required tool is not found
        """
        missing_tools = []
        for tool in self.REQUIRED_TOOLS:
            if not shutil.which(tool):
                missing_tools.append(tool)

        if missing_tools:
            error_msg = (
                "Required mdbtools are not installed. Please install mdbtools:\n"
                "  - macOS: brew install mdbtools\n"
                "  - Ubuntu/Debian: sudo apt-get install mdbtools\n"
                "  - Windows: Install via WSL or download from mdbtools website\n"
                f"Missing tools: {', '.join(missing_tools)}"
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def _validate_config(self) -> None:
        """Validate MDB configuration."""
        if 'path' not in self.config:
            raise ValueError("Missing required field: path")
        # Remove the requirement for 'table' field since we now support fetching all tables
        # if 'table' not in self.config:
        #     raise ValueError("Missing required field: table")

    def _get_tables(self, file_path: Path) -> List[str]:
        """Get list of tables in the MDB file.

        Args:
            file_path: Path to the MDB file

        Returns:
            List of table names

        Raises:
            subprocess.CalledProcessError: If mdb-tables command fails
        """
        try:
            result = subprocess.run(
                ['mdb-tables', '-1', str(file_path)],
                capture_output=True,
                text=True,
                check=True
            )
            return [table.strip() for table in result.stdout.splitlines() if table.strip()]
        except subprocess.CalledProcessError as e:
            error_msg = f"Error getting tables from {file_path}: {e.stderr}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _export_table_to_csv(self, file_path: Path, table_name: str) -> str:
        """Export a table from MDB file to CSV format.

        Args:
            file_path: Path to the MDB file
            table_name: Name of the table to export

        Returns:
            CSV data as string

        Raises:
            subprocess.CalledProcessError: If mdb-export command fails
        """
        try:
            # Use mdb-export with proper arguments:
            # -H: Include header row
            # --delimiter=,: Use comma as delimiter
            # -Q: Don't wrap text in quotes (we'll handle this in CSV reader)
            # -B: Use TRUE/FALSE for boolean values
            result = subprocess.run(
                [
                    'mdb-export',
                    '-H',  # Include header row
                    '--delimiter=,',  # Use comma as delimiter
                    '-Q',  # Don't wrap text in quotes
                    '-B',  # Use TRUE/FALSE for boolean values
                    str(file_path),
                    table_name
                ],
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            error_msg = f"Error exporting table {table_name} from {file_path}: {e.stderr}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def fetch(self) -> Dict[str, Any]:
        """Fetch data from MDB file.

        Returns:
            Dictionary containing the MDB data for all tables.

        Raises:
            FileNotFoundError: If the file doesn't exist
            RuntimeError: If mdbtools commands fail
        """
        path = Path(self.config['path'])
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        try:
            tables = self._get_tables(path)
            if not tables:
                raise ValueError(f"No tables found in {path}")

            logger.info(f"Fetching all tables: {', '.join(tables)}")
            tables_data = {}

            for table_name in tables:
                try:
                    # Export table to CSV
                    csv_data = self._export_table_to_csv(path, table_name)

                    # Parse CSV data
                    reader = csv.DictReader(io.StringIO(csv_data))
                    records = list(reader)
                    columns = reader.fieldnames or []

                    # Store table data
                    tables_data[table_name] = {
                        'columns': columns,
                        'records': records,
                        'metadata': {
                            'table_name': table_name,
                            'row_count': len(records),
                            'column_count': len(columns),
                            'file_type': path.suffix.lower()
                        }
                    }
                    logger.info(
                        f"Fetched table '{table_name}': {len(records)} rows, {len(columns)} columns")
                except Exception as e:
                    logger.warning(
                        f"Failed to fetch table '{table_name}': {str(e)}")
                    continue

            # Transform data for all tables
            raw_data = {
                'tables': tables_data,
                'metadata': {
                    'file_path': str(path),
                    'file_type': path.suffix.lower(),
                    'total_tables': len(tables_data),
                    'table_names': list(tables_data.keys())
                }
            }

            return self.transform(raw_data)

        except (subprocess.CalledProcessError, RuntimeError) as e:
            logger.error(f"Error reading MDB file {path}: {str(e)}")
            raise
