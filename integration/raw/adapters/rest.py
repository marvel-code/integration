"""
REST API adapter module.
"""

import logging
import requests
from typing import Any, Dict

from .base import BaseAdapter

logger = logging.getLogger(__name__)


class RESTAdapter(BaseAdapter):
    """Adapter for REST API data sources."""

    def _validate_config(self) -> None:
        """Validate REST API configuration."""
        required_fields = ['url', 'method']
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")

    def fetch(self) -> Dict[str, Any]:
        """Fetch data from REST API.

        Returns:
            Dictionary containing the API response

        Raises:
            requests.RequestException: If the API request fails
        """
        try:
            response = requests.request(
                method=self.config['method'],
                url=self.config['url'],
                headers=self.config.get('headers', {}),
                params=self.config.get('params', {}),
                json=self.config.get('body'),
                timeout=self.config.get('timeout', 30)
            )
            response.raise_for_status()
            raw_data = response.json()
            return self.transform(raw_data)
        except requests.RequestException as e:
            logger.error(
                f"Failed to fetch data from {self.config['url']}: {str(e)}")
            raise
