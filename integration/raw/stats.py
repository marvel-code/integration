from collections import defaultdict


class ProcessingStats:
    """Class for tracking and logging processing statistics."""

    def __init__(self):
        self.total_files = 0
        self.processed_files = 0
        self.failed_files = 0
        self.by_type = defaultdict(int)
        self.by_status = defaultdict(int)

    def increment_total(self, file_type: str):
        self.total_files += 1
        self.by_type[file_type] += 1

    def mark_processed(self):
        self.processed_files += 1
        self.by_status['success'] += 1

    def mark_failed(self):
        self.failed_files += 1
        self.by_status['failed'] += 1

    def log_summary(self, logger):
        logger.info("Raw Layer Processing Summary:")
        logger.info(f"Total files found: {self.total_files}")
        logger.info(f"Successfully processed: {self.processed_files}")
        logger.info(f"Failed to process: {self.failed_files}")

        logger.info("\nFile types processed:")
        for file_type, count in self.by_type.items():
            logger.info(f"  {file_type}: {count} files")

        logger.info("\nProcessing status:")
        for status, count in self.by_status.items():
            logger.info(f"  {status}: {count} files")
