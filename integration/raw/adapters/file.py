"""
File adapter module for handling file-based data sources.
"""

import json
import csv
from pathlib import Path
from typing import Any, Dict

from .base import BaseAdapter


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

    def fetch(self) -> Dict[str, Any]:
        """Fetch data from file.

        Returns:
            Dictionary containing the file data

        Raises:
            FileNotFoundError: If the file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
        path = Path(self.config['path'])
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        if self.config['format'] == 'json':
            with open(path, 'r', encoding='utf-8') as f:
                return {'data': json.load(f)}
        elif self.config['format'] == 'csv':
            data = []
            with open(path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    data.append(dict(row))
            return {'data': data}
