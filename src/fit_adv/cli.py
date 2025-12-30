from __future__ import annotations

import argparse

from fit_adv.pipeline import build_daily_from_latest_raw


def main() -> int:
    parser = argparse.ArgumentParser(prog="fit-adv")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("build-daily", help="Build daily_full.csv and daily_v1.csv from latest raw JSON")

    args = parser.parse_args()

    if args.cmd == "build-daily":
        result = build_daily_from_latest_raw()
        print("Wrote:", result.outputs.daily_full_csv)
        print("Wrote:", result.outputs.daily_v1_csv)
        print("Rows:", result.df_daily.shape[0])
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())

