"""
Data ingestion module for handling raw data input.
"""

from typing import Any, Dict, List, Optional
import json
import csv
from pathlib import Path


class DataIngestion:
    """Handles raw data ingestion from various sources."""

    @staticmethod
    def load_json(file_path: str | Path) -> Dict[str, Any]:
        """Load data from a JSON file.

        Args:
            file_path: Path to the JSON file

        Returns:
            Dict containing the loaded JSON data

        Raises:
            FileNotFoundError: If the file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    @staticmethod
    def load_csv(file_path: str | Path, delimiter: str = ',') -> List[Dict[str, Any]]:
        """Load data from a CSV file.

        Args:
            file_path: Path to the CSV file
            delimiter: Character used to separate values

        Returns:
            List of dictionaries containing the CSV data

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            for row in reader:
                data.append(dict(row))
        return data
