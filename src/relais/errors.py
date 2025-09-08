"""Error types and policies used throughout Relais pipelines."""

from enum import Enum
from typing import Optional

from relais.index import Index


class ErrorPolicy(Enum):
    """Error handling policies for pipeline execution."""

    FAIL_FAST = "fail_fast"  # Stop entire pipeline on first error
    IGNORE = "ignore"  # Skip failed items, continue processing
    COLLECT = "collect"  # Collect errors, return at end


class PipelineError(Exception):
    """Exception raised when pipeline execution fails."""

    def __init__(
        self,
        message: str,
        original_error: Exception,
        step_name: Optional[str] = None,
        item_index: Optional[Index] = None,
    ):
        """Create a pipeline error with contextual metadata.

        Args:
            message: Human-readable description of the failure.
            original_error: The original exception that was raised.
            step_name: Optional name of the step where the error occurred.
            item_index: Optional index of the item being processed.

        """
        self.original_error = original_error
        self.step_name = step_name
        self.item_index = item_index
        super().__init__(f"{message}: {original_error}")
