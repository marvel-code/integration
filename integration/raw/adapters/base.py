"""
Base adapter module defining core interfaces and abstract classes.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Protocol, List
from datetime import datetime
from dataclasses import dataclass


@dataclass
class Table:
    name: str
    columns: List[str]
    records: List[Any]
    metadata: dict = None

    def as_dicts(self) -> List[dict]:
        return [dict(zip(self.columns, record)) for record in self.records]


class DataSource(Protocol):
    """Protocol defining the interface for data sources."""

    def fetch(self) -> List[Table]:
        """Fetch data from the source."""
        ...


class BaseAdapter(ABC):
    """Base class for all data source adapters."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize adapter with configuration.

        Args:
            config: Configuration dictionary for the adapter
        """
        self.config = config
        self._validate_config()

    @abstractmethod
    def _validate_config(self) -> None:
        """Validate the adapter configuration."""
        pass

    @abstractmethod
    def fetch(self) -> List[Table]:
        """Fetch data from the source.

        Returns:
            List of Table objects containing the fetched data
        """
        pass

    def transform(self, data: List[Table]) -> List[Dict[str, Any]]:
        """Transform the raw data into a standard format.

        Args:
            data: List of Table objects containing the raw data

        Returns:
            List of dictionaries containing the transformed data
        """
        transformed_data = []
        for table in data:
            transformed_data.append({
                'source': self.__class__.__name__,
                'timestamp': datetime.now().isoformat(),
                'data': table.as_dicts()
            })
        return transformed_data
