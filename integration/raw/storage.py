"""
Storage module for raw data persistence.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


class RawDataStorage:
    """Handles storage of raw data."""

    def __init__(self, storage_dir: str | Path):
        """Initialize storage with a directory path.

        Args:
            storage_dir: Directory where raw data will be stored
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def save(self, data: Dict[str, Any], prefix: str = "raw") -> Path:
        """Save raw data to a file.

        Args:
            data: Data to save
            prefix: Prefix for the filename

        Returns:
            Path to the saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.json"
        file_path = self.storage_dir / filename

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        return file_path

    def load(self, file_path: str | Path) -> Dict[str, Any]:
        """Load raw data from a file.

        Args:
            file_path: Path to the file to load

        Returns:
            Loaded data

        Raises:
            FileNotFoundError: If the file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def list_files(self, prefix: Optional[str] = None) -> List[Path]:
        """List all raw data files.

        Args:
            prefix: Optional prefix to filter files

        Returns:
            List of file paths
        """
        pattern = f"{prefix}_*.json" if prefix else "*.json"
        return sorted(self.storage_dir.glob(pattern))
