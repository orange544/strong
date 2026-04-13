from __future__ import annotations

import logging
import sys
from pathlib import Path

from .io import ensure_dir


def setup_logger(name: str, log_file: Path | None = None, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()
    logger.propagate = False

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file is not None:
        ensure_dir(log_file.parent)
        file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

