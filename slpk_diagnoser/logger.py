"""统一日志配置与阶段化日志辅助。"""

from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import parse_qsl, urlsplit, urlunsplit

PACKAGE_LOGGER_NAME = "slpk_diagnoser"
SENSITIVE_FIELD_HINTS = (
    "secret",
    "token",
    "password",
    "credential",
    "signature",
    "access_key",
    "accesskey",
)


class ColoredFormatter(logging.Formatter):
    """TTY 环境中的彩色日志格式器。"""

    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
        "RESET": "\033[0m",
    }

    def format(self, record: logging.LogRecord) -> str:
        raw_level = record.levelname
        try:
            if sys.stderr.isatty():
                color = self.COLORS.get(raw_level, self.COLORS["RESET"])
                record.levelname = f"{color}{raw_level}{self.COLORS['RESET']}"
            return super().format(record)
        finally:
            record.levelname = raw_level


def configure_logging(
    level: int = logging.INFO,
    *,
    verbose: bool = False,
    log_file: str | None = None,
) -> None:
    """配置包级统一日志输出。"""

    root = logging.getLogger(PACKAGE_LOGGER_NAME)
    effective = logging.DEBUG if verbose else level
    root.setLevel(effective)

    for handler in root.handlers[:]:
        root.removeHandler(handler)

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(effective)
    stream_handler.setFormatter(_build_formatter())
    root.addHandler(stream_handler)

    if log_file:
        file_path = Path(log_file).expanduser()
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(file_path, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        root.addHandler(file_handler)

    root.propagate = False


def ensure_default_logging() -> None:
    """API 直接调用时，至少保证 stderr 上有日志。"""

    root = logging.getLogger(PACKAGE_LOGGER_NAME)
    if not root.handlers:
        configure_logging(logging.INFO, verbose=False)


def get_logger(name: str | None = None) -> logging.Logger:
    n = name if name else PACKAGE_LOGGER_NAME
    if not n.startswith(PACKAGE_LOGGER_NAME):
        n = f"{PACKAGE_LOGGER_NAME}.{n}" if n else PACKAGE_LOGGER_NAME
    log = logging.getLogger(n)
    if log.level == logging.NOTSET:
        log.setLevel(logging.NOTSET)
    log.propagate = True
    return log


def setup_logger(
    name: str = PACKAGE_LOGGER_NAME,
    level: int = logging.INFO,
    verbose: bool = False,
    log_file: str | None = None,
) -> logging.Logger:
    configure_logging(level=level, verbose=verbose, log_file=log_file)
    return get_logger(name)


def sanitize_text(value: Any) -> str:
    text = str(value)
    if "://" not in text:
        return text

    parts = urlsplit(text)
    masked_netloc = parts.netloc
    if "@" in masked_netloc:
        _, host = masked_netloc.rsplit("@", 1)
        masked_netloc = f"***@{host}"

    query_items = []
    for key, item in parse_qsl(parts.query, keep_blank_values=True):
        masked = "***" if _looks_sensitive(key) else item
        query_items.append(f"{key}={masked}")
    return urlunsplit((parts.scheme, masked_netloc, parts.path, "&".join(query_items), parts.fragment))


def sanitize_fields(fields: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in fields.items():
        if _looks_sensitive(key):
            safe[key] = "***"
        elif isinstance(value, (str, Path)):
            safe[key] = sanitize_text(value)
        else:
            safe[key] = value
    return safe


def log_operation_start(logger: logging.Logger, operation: str, **kwargs: Any) -> None:
    fields = sanitize_fields(kwargs)
    if fields:
        logger.info("开始执行: %s (%s)", operation, _format_fields(fields))
    else:
        logger.info("开始执行: %s", operation)


def log_operation_complete(logger: logging.Logger, operation: str, **kwargs: Any) -> None:
    fields = sanitize_fields(kwargs)
    if fields:
        logger.info("完成执行: %s (%s)", operation, _format_fields(fields))
    else:
        logger.info("完成执行: %s", operation)


def log_error_context(logger: logging.Logger, error: Exception, context: str = "", **kwargs: Any) -> None:
    payload = {"error_type": type(error).__name__, "detail": error}
    payload.update(kwargs)
    fields = sanitize_fields(payload)
    if context:
        logger.error("%s (%s)", context, _format_fields(fields))
    else:
        logger.error("错误 (%s)", _format_fields(fields))


@contextmanager
def log_timed_operation(
    logger: logging.Logger,
    operation: str,
    **kwargs: Any,
) -> Iterator[None]:
    """记录阶段开始、结束和耗时。"""

    start = time.perf_counter()
    log_operation_start(logger, operation, **kwargs)
    try:
        yield
    except Exception as exc:
        elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
        log_error_context(logger, exc, f"{operation} 失败", elapsed_ms=elapsed_ms, **kwargs)
        raise
    else:
        elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
        log_operation_complete(logger, operation, elapsed_ms=elapsed_ms, **kwargs)


def _build_formatter() -> logging.Formatter:
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    if sys.stderr.isatty():
        return ColoredFormatter(fmt, datefmt="%H:%M:%S")
    return logging.Formatter(fmt, datefmt="%H:%M:%S")


def _format_fields(fields: dict[str, Any]) -> str:
    return ", ".join(f"{key}={value}" for key, value in sorted(fields.items()))


def _looks_sensitive(name: str) -> bool:
    lowered = name.lower()
    return any(hint in lowered for hint in SENSITIVE_FIELD_HINTS)
