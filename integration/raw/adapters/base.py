"""
Base adapter module defining core interfaces and abstract classes.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Protocol
from datetime import datetime


class DataSource(Protocol):
    """Protocol defining the interface for data sources."""

    def fetch(self) -> Dict[str, Any]:
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
    def fetch(self) -> Dict[str, Any]:
        """Fetch data from the source.

        Returns:
            Dictionary containing the fetched data
        """
        pass

    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform the raw data into a standard format.

        Args:
            data: Raw data from the source

        Returns:
            Transformed data in standard format
        """
        return {
            'source': self.__class__.__name__,
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
