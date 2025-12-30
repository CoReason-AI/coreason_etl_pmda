# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import sys
from pathlib import Path

from loguru import logger

# Remove default handler
logger.remove()

# Sink 1: Stdout/Stderr
# Format: Time | Level | Module:Function:Line - Message
logger.add(
    sys.stderr,
    level="INFO",
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    ),
)

# Sink 2: File
# Rotating file: logs/app.log
# Rotation: 500 MB
# Retention: 10 days
# Serialization: JSON
# Enqueue: Async safe
log_path = Path("logs/app.log")
logger.add(
    log_path,
    rotation="500 MB",
    retention="10 days",
    serialize=True,
    enqueue=True,
    level="DEBUG",  # File logs often capture more detail
)

__all__ = ["logger"]
