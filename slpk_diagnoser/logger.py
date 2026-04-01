"""包级统一日志：单一路由到命名空间根，子模块仅使用 get_logger，避免重复 Handler。"""

from __future__ import annotations

import logging
import sys
from typing import Any

PACKAGE_LOGGER_NAME = "slpk_diagnoser"


class ColoredFormatter(logging.Formatter):
    """控制台 TTY 下为等级着色；不在 LogRecord 上留下副作用。"""

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
                reset = self.COLORS["RESET"]
                record.levelname = f"{color}{raw_level}{reset}"
            return super().format(record)
        finally:
            record.levelname = raw_level


def configure_logging(
    level: int = logging.INFO,
    *,
    verbose: bool = False,
) -> None:
    """在包命名空间根上配置唯一 StreamHandler；须在 CLI 入口尽早调用一次。

    verbose 为 True 时，根 logger 使用 DEBUG，便于排查。
    """
    root = logging.getLogger(PACKAGE_LOGGER_NAME)
    effective = logging.DEBUG if verbose else level
    root.setLevel(effective)

    for h in root.handlers[:]:
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    if sys.stderr.isatty():
        handler.setFormatter(
            ColoredFormatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%H:%M:%S",
            )
        )
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%H:%M:%S",
            )
        )
    root.addHandler(handler)
    root.propagate = False


def ensure_default_logging() -> None:
    """API 直接调用 diagnose 且未走 CLI 时，保证至少有一条 stderr 输出。"""
    root = logging.getLogger(PACKAGE_LOGGER_NAME)
    if not root.handlers:
        configure_logging(logging.INFO, verbose=False)


def get_logger(name: str | None = None) -> logging.Logger:
    """返回子记录器（如 slpk_diagnoser.engine），事件向上汇总至包根。"""
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
) -> logging.Logger:
    """兼容旧接口：等价于 configure_logging 后返回对应名称的 logger。"""
    configure_logging(level=level, verbose=verbose)
    return get_logger(name)


def log_operation_start(logger: logging.Logger, operation: str, **kwargs: Any) -> None:
    if kwargs:
        params = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        logger.info("开始执行: %s (%s)", operation, params)
    else:
        logger.info("开始执行: %s", operation)


def log_operation_complete(logger: logging.Logger, operation: str, **kwargs: Any) -> None:
    if kwargs:
        params = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        logger.info("完成执行: %s (%s)", operation, params)
    else:
        logger.info("完成执行: %s", operation)


def log_error_context(logger: logging.Logger, error: Exception, context: str = "") -> None:
    if context:
        logger.error("%s: %s: %s", context, type(error).__name__, error)
    else:
        logger.error("%s: %s", type(error).__name__, error)
