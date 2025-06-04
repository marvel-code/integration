"""
Data validation module for raw data.
"""

from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass


@dataclass
class ValidationRule:
    """Represents a validation rule for data."""
    field: str
    validator: Callable[[Any], bool]
    error_message: str


class DataValidator:
    """Handles validation of raw data."""

    def __init__(self, rules: List[ValidationRule]):
        """Initialize validator with a list of rules.

        Args:
            rules: List of validation rules to apply
        """
        self.rules = rules

    def validate(self, data: Dict[str, Any]) -> List[str]:
        """Validate data against the defined rules.

        Args:
            data: Dictionary containing the data to validate

        Returns:
            List of error messages for failed validations
        """
        errors = []
        for rule in self.rules:
            if rule.field not in data:
                errors.append(f"Missing required field: {rule.field}")
                continue

            if not rule.validator(data[rule.field]):
                errors.append(rule.error_message)

        return errors

    @staticmethod
    def is_not_empty(value: Any) -> bool:
        """Check if a value is not empty.

        Args:
            value: Value to check

        Returns:
            True if the value is not empty, False otherwise
        """
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, (list, dict, set)):
            return bool(value)
        return True
