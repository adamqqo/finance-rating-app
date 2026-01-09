from __future__ import annotations

import logging
import os
import sys
from typing import Optional


def setup_logging(level: Optional[str] = None) -> None:
    """Configure application logging safely and consistently.

    - Emits logs to STDOUT (avoids Railway/UI marking STDERR as errors).
    - Idempotent: if logging is already configured, it will not add duplicate handlers.
    - Supports runtime override via FINDEXIO_LOG_LEVEL.
    """

    # If the root logger already has handlers, assume logging was configured by
    # the hosting environment (or tests) and avoid duplicating output.
    root = logging.getLogger()
    if root.handlers:
        return

    lvl = (level or os.getenv("FINDEXIO_LOG_LEVEL") or "INFO").upper()
    numeric_level = getattr(logging, lvl, logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        stream=sys.stdout,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
