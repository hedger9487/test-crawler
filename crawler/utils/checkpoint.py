"""Checkpoint utility — save/restore crawler state."""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def checkpoint_exists(path: str) -> bool:
    return Path(path).exists()


def remove_checkpoint(path: str) -> None:
    p = Path(path)
    if p.exists():
        p.unlink()
        logger.info("Removed checkpoint: %s", path)
