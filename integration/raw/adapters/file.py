"""
File adapter module for handling file-based data sources.
"""

import json
import csv
from pathlib import Path
from typing import Any, Dict, List

from .base import BaseAdapter, Table


class FileAdapter(BaseAdapter):
    """Adapter for file-based data sources."""

    def _validate_config(self) -> None:
        """Validate file configuration."""
        if 'path' not in self.config:
            raise ValueError("Missing required field: path")
        if 'format' not in self.config:
            raise ValueError("Missing required field: format")
        if self.config['format'] not in ['json', 'csv']:
            raise ValueError("Unsupported file format")

    def fetch(self) -> List[Table]:
        """Fetch data from file.

        Returns:
            List of Table objects containing the file data

        Raises:
            FileNotFoundError: If the file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
        path = Path(self.config['path'])
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        tables = []
        if self.config['format'] == 'json':
            with open(path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            if isinstance(raw_data, list) and raw_data and isinstance(raw_data[0], dict):
                columns = list(raw_data[0].keys())
                records = [tuple(row.get(col, None)
                                 for col in columns) for row in raw_data]
                metadata = {'file_type': 'json', 'row_count': len(
                    records), 'column_count': len(columns)}
                tables.append(Table(name=path.stem, columns=columns,
                              records=records, metadata=metadata))
            else:
                # fallback: treat as one-column table
                columns = ['value']
                records = [(str(raw_data),)]
                metadata = {'file_type': 'json',
                            'row_count': 1, 'column_count': 1}
                tables.append(Table(name=path.stem, columns=columns,
                              records=records, metadata=metadata))
        elif self.config['format'] == 'csv':
            with open(path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
            if rows:
                columns = rows[0]
                records = [tuple(row) for row in rows[1:]]
                metadata = {'file_type': 'csv', 'row_count': len(
                    records), 'column_count': len(columns)}
                tables.append(Table(name=path.stem, columns=columns,
                              records=records, metadata=metadata))
        return tables
