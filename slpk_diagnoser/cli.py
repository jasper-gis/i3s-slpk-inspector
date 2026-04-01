"""命令行入口：SLPK / I3S 质量诊断。"""

from __future__ import annotations

import argparse
import logging
import sys
import traceback
from pathlib import Path

from slpk_diagnoser.engine import run_diagnose
from slpk_diagnoser.logger import configure_logging, get_logger, log_error_context


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="slpk-diagnose",
        description="SLPK / I3S 场景层包质量诊断器（结构、资源、LOD、包围体）",
    )
    parser.add_argument("slpk", help="输入 .slpk 文件路径")
    parser.add_argument(
        "-o",
        "--json-out",
        metavar="FILE",
        help="可选：将完整诊断结果写入 JSON 文件",
    )
    loud = parser.add_mutually_exclusive_group()
    loud.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="启用详细日志（DEBUG）",
    )
    loud.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="仅将警告以上日志输出到 stderr（报告正文仍输出到 stdout）",
    )

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.ERROR if args.quiet else logging.INFO
    configure_logging(level=level, verbose=args.verbose)
    logger = get_logger(__name__)

    try:
        slpk_path = Path(args.slpk)
        if not slpk_path.exists():
            logger.critical(f"输入文件不存在: {args.slpk}")
            sys.exit(2)
        if not slpk_path.is_file():
            logger.critical(f"输入路径不是文件: {args.slpk}")
            sys.exit(2)

        text = run_diagnose(args.slpk, json_out=args.json_out)
        print(text)
        sys.exit(0)

    except FileNotFoundError as e:
        log_error_context(logger, e, "文件未找到")
        sys.exit(2)
    except PermissionError as e:
        log_error_context(logger, e, "权限错误")
        sys.exit(3)
    except IsADirectoryError as e:
        log_error_context(logger, e, "期望文件但得到目录")
        sys.exit(2)
    except OSError as e:
        log_error_context(logger, e, "操作系统错误")
        sys.exit(2)
    except KeyboardInterrupt:
        logger.warning("用户中断操作")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"未预期的错误: {type(e).__name__}: {e}")
        if args.verbose:
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
