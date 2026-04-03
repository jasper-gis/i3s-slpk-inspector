"""命令行入口：SLPK / ESLPK / I3S 质量诊断。"""

from __future__ import annotations

import argparse
import logging
import sys
import traceback
from pathlib import Path

from slpk_diagnoser.engine import run_diagnose
from slpk_diagnoser.logger import configure_logging, get_logger, log_error_context
from slpk_diagnoser.package_reader import is_cloud_storage_uri


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="slpk-diagnose",
        description=(
            "SLPK/ESLPK/I3S 场景层包质量诊断器，支持本地 SLPK、ESLPK 目录，以及 "
            "MinIO / 阿里云 OSS / ArcGIS Enterprise Ozone 对象存储。"
        ),
    )
    parser.add_argument(
        "package",
        help=(
            "输入包路径：本地 .slpk/.eslpk 文件、本地 ESLPK 目录，或云存储 URI。"
        ),
    )
    parser.add_argument(
        "-o",
        "--json-out",
        metavar="FILE",
        help="可选：将完整诊断结果写入 JSON 文件。",
    )
    parser.add_argument(
        "--log-file",
        metavar="FILE",
        help="可选：将详细日志同时写入文件。",
    )
    loud = parser.add_mutually_exclusive_group()
    loud.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="启用详细日志（DEBUG）。",
    )
    loud.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="仅输出错误日志到 stderr，诊断正文仍输出到 stdout。",
    )

    args = parser.parse_args()
    level = logging.DEBUG if args.verbose else logging.ERROR if args.quiet else logging.INFO
    configure_logging(level=level, verbose=args.verbose, log_file=args.log_file)
    logger = get_logger(__name__)

    try:
        if not is_cloud_storage_uri(args.package):
            pkg_path = Path(args.package)
            if not pkg_path.exists():
                logger.critical("输入路径不存在: %s", args.package)
                sys.exit(2)

        report_text = run_diagnose(args.package, json_out=args.json_out)
        print(report_text)
        sys.exit(0)

    except FileNotFoundError as exc:
        log_error_context(logger, exc, "文件未找到", package=args.package)
        sys.exit(2)
    except PermissionError as exc:
        log_error_context(logger, exc, "权限错误", package=args.package)
        sys.exit(3)
    except IsADirectoryError as exc:
        log_error_context(logger, exc, "期望文件但得到了目录", package=args.package)
        sys.exit(2)
    except OSError as exc:
        log_error_context(logger, exc, "操作系统错误", package=args.package)
        sys.exit(2)
    except KeyboardInterrupt:
        logger.warning("用户中断操作")
        sys.exit(130)
    except Exception as exc:
        log_error_context(logger, exc, "未预期的错误", package=args.package)
        if args.verbose:
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
