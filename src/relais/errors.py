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
        self.original_error = original_error
        self.step_name = step_name
        self.item_index = item_index
        super().__init__(f"{message}: {original_error}")
