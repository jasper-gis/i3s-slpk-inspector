"""命令行入口：SLPK / I3S 质量诊断。"""

from __future__ import annotations

import argparse
import sys

from slpk_diagnoser.engine import run_diagnose


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
    args = parser.parse_args()
    try:
        text = run_diagnose(args.slpk, json_out=args.json_out)
    except FileNotFoundError as e:
        print(f"错误：找不到文件 — {e}", file=sys.stderr)
        sys.exit(2)
    except OSError as e:
        print(f"错误：无法读取包 — {e}", file=sys.stderr)
        sys.exit(2)
    print(text)


if __name__ == "__main__":
    main()
