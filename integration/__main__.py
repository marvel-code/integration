"""
Main entry point for the integration package.
Demonstrates usage of the raw data processing layer.
"""

import logging
import sys
from pathlib import Path
from typing import Dict, Any

from .raw import (
    RawDataProcessor,
)


def setup_logging() -> None:
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def main() -> None:
    """Main entry point for the integration package."""
    setup_logging()
    logger = logging.getLogger(__name__)

    # Get input and output directories from command line arguments
    if len(sys.argv) < 3:
        logger.error("Usage: python -m integration <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])

    # Initialize processor with validation rules
    processor = RawDataProcessor(
        storage_dir=input_dir,
        output_dir=output_dir,
    )

    try:
        # Process all files
        results = processor.process()
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
