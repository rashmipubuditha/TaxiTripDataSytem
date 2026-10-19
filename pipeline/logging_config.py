"""Structured (JSON) logging.

Every log record from the 'pipeline' package is written as one JSON object per line to
logs/pipeline.jsonl, e.g.
  {"ts": "2026-10-19T19:40:01.123+00:00", "level": "INFO", "logger": "pipeline.load",
   "event": "batch_staged", "message": "...", "month": "2023-01", "run_id": 12, "rows_read": 250000}

Context fields (month, run_id, flow_run_id) are attached automatically to every record
inside `log_context(...)`, so individual log calls stay short.
Console output is handled by Prefect when running flows (PREFECT_LOGGING_EXTRA_LOGGERS=pipeline),
or by setup_logging(console=True) for plain command-line runs.
"""
from __future__ import annotations

import contextvars
import json
import logging
import logging.handlers
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import PROJECT_ROOT

LOG_FILE = PROJECT_ROOT / "logs" / "pipeline.jsonl"

_context: contextvars.ContextVar[dict | None] = contextvars.ContextVar("log_context", default=None)

# Attributes every LogRecord has; anything else on a record came from `extra=` and is logged.
_STANDARD_ATTRS = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime", "taskName"}


@contextmanager
def log_context(**fields):
    """Attach fields (e.g. month='2023-01', run_id=5) to every log record inside the block."""
    token = _context.set({**(_context.get() or {}), **{k: v for k, v in fields.items() if v is not None}})
    try:
        yield
    finally:
        _context.reset(token)


def bind_context(**fields) -> None:
    """Add fields to the current context (used when a value, like run_id, becomes known later)."""
    _context.set({**(_context.get() or {}), **{k: v for k, v in fields.items() if v is not None}})


class ContextFilter(logging.Filter):
    """Copies the current context fields onto each record."""

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in (_context.get() or {}).items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "event": getattr(record, "event", None) or "log",
            "message": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _STANDARD_ATTRS and key not in entry:
                entry[key] = value
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str, ensure_ascii=False)


def setup_logging(level: int = logging.INFO, console: bool = False, log_file: Path = LOG_FILE) -> logging.Logger:
    """Configure the 'pipeline' logger once: JSON file (rotating 10 MB x 5) + optional console."""
    logger = logging.getLogger("pipeline")
    logger.setLevel(level)
    if not any(getattr(h, "_pipeline_handler", False) for h in logger.handlers):
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=5,
                                                            encoding="utf-8")
        file_handler.setFormatter(JsonFormatter())
        file_handler.addFilter(ContextFilter())
        file_handler._pipeline_handler = True
        logger.addHandler(file_handler)
        if console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s | %(message)s"))
            console_handler._pipeline_handler = True
            logger.addHandler(console_handler)
    return logger
