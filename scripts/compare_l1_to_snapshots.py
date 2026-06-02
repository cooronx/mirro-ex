#!/usr/bin/env python3

import argparse
import bisect
import csv
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo


SH_TZ = ZoneInfo("Asia/Shanghai")
MIDDAY_BREAK_START = time(11, 30, 0)
MIDDAY_BREAK_END = time(13, 0, 0)


@dataclass
class BookRow:
    ts_ms: int
    code: str
    bids: list[str]
    asks: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare L1 report rows against nearby order_book_snapshot rows."
    )
    parser.add_argument(
        "--l1",
        default="data/l1_report_stock_SH600410_2026-05-14_093000_103000.csv",
        help="Path to L1 CSV.",
    )
    parser.add_argument(
        "--snapshots",
        default="data/order_book_snapshot.csv",
        help="Path to replay snapshot CSV.",
    )
    parser.add_argument(
        "--window-ms",
        type=int,
        default=3000,
        help="Nearby search window in milliseconds on each side.",
    )
    parser.add_argument(
        "--unmatched-output",
        default="data/l1_unmatched.csv",
        help="Output CSV for L1 rows that have no exact snapshot match within the search window.",
    )
    return parser.parse_args()


def l1_time_to_ms(raw: str) -> int:
    dt = datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=SH_TZ)
    return int(dt.timestamp() * 1000)


def is_midday_break_l1_time(raw: str) -> bool:
    dt = datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S")
    current_time = dt.time()
    return MIDDAY_BREAK_START < current_time < MIDDAY_BREAK_END


def normalize_code(raw: str) -> str:
    code = raw.strip()
    if not code:
        return code
    if code.endswith(".XSHG") or code.endswith(".XSHE"):
        return code
    if code.startswith("SH") and len(code) > 2:
        return f"{code[2:]}.XSHG"
    if code.startswith("SZ") and len(code) > 2:
        return f"{code[2:]}.XSHE"
    return code


def parse_snapshot_cell(raw: str) -> tuple[str, str]:
    raw = raw.strip()
    if not raw:
        return ("", "")
    price, qty = raw.split(":", 1)
    return (price, qty)


def normalize_price(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    try:
        return f"{Decimal(raw):.4f}"
    except InvalidOperation:
        return raw


def normalize_qty(raw: str) -> str:
    return raw.strip()


def normalize_level(price: str, qty: str) -> str:
    price = normalize_price(price)
    qty = normalize_qty(qty)
    return f"{price}:{qty}" if price and qty else ""


def snapshot_fields(row: dict[str, str], prefix: str) -> list[str]:
    return [
        normalize_level(*parse_snapshot_cell(row[f"{prefix}{index}"]))
        for index in range(1, 6)
    ]


def l1_fields(row: dict[str, str], price_prefix: str, vol_prefix: str) -> list[str]:
    fields: list[str] = []
    for index in range(1, 6):
        price = row.get(f"{price_prefix}{index}", "").strip()
        vol = row.get(f"{vol_prefix}{index}", "").strip()
        fields.append(normalize_level(price, vol))
    return fields


def load_snapshots(path: Path) -> list[BookRow]:
    rows: list[BookRow] = []
    with path.open("r", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(
                BookRow(
                    ts_ms=int(row["ts"]),
                    code=normalize_code(row["code"]),
                    bids=snapshot_fields(row, "bid"),
                    asks=snapshot_fields(row, "ask"),
                )
            )
    return rows


def index_snapshots_by_code(
    snapshots: list[BookRow],
) -> dict[str, tuple[list[int], list[BookRow]]]:
    grouped: dict[str, list[BookRow]] = {}
    for snapshot in snapshots:
        grouped.setdefault(snapshot.code, []).append(snapshot)

    indexed: dict[str, tuple[list[int], list[BookRow]]] = {}
    for code, rows in grouped.items():
        rows.sort(key=lambda row: row.ts_ms)
        indexed[code] = ([row.ts_ms for row in rows], rows)
    return indexed


def score_rows(l1_bids: list[str], l1_asks: list[str], snapshot: BookRow) -> int:
    score = 0
    for lhs, rhs in zip(l1_bids, snapshot.bids):
        if lhs != rhs:
            score += 1
    for lhs, rhs in zip(l1_asks, snapshot.asks):
        if lhs != rhs:
            score += 1
    return score


def format_unmatched_row(
    l1_row: dict[str, str],
    l1_ts_ms: int,
    l1_bids: list[str],
    l1_asks: list[str],
) -> dict[str, str]:
    result: dict[str, str] = {
        "l1_time": l1_row["time"],
        "l1_ts_ms": str(l1_ts_ms),
        "code": normalize_code(l1_row["code"]),
    }

    for index in range(5):
        result[f"l1_bid{index + 1}"] = l1_bids[index]
        result[f"l1_ask{index + 1}"] = l1_asks[index]

    return result


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "l1_time",
                    "l1_ts_ms",
                    "code",
                    "l1_bid1",
                    "l1_bid2",
                    "l1_bid3",
                    "l1_bid4",
                    "l1_bid5",
                    "l1_ask1",
                    "l1_ask2",
                    "l1_ask3",
                    "l1_ask4",
                    "l1_ask5",
                ],
            )
            writer.writeheader()
        return

    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    l1_path = Path(args.l1)
    snapshot_path = Path(args.snapshots)
    unmatched_output_path = Path(args.unmatched_output)

    snapshots = load_snapshots(snapshot_path)
    snapshot_index = index_snapshots_by_code(snapshots)
    unmatched_rows_output: list[dict[str, str]] = []
    exact_match_rows = 0
    total_rows = 0
    skipped_midday_rows = 0

    with l1_path.open("r", newline="") as fh:
        reader = csv.DictReader(fh)
        for l1_row in reader:
            if is_midday_break_l1_time(l1_row["time"]):
                skipped_midday_rows += 1
                continue

            total_rows += 1
            l1_ts_ms = l1_time_to_ms(l1_row["time"])
            l1_bids = l1_fields(l1_row, "BuyPrice", "BuyVol")
            l1_asks = l1_fields(l1_row, "SelPrice", "SelVol")
            code = normalize_code(l1_row["code"])

            indexed = snapshot_index.get(code)
            if indexed is None:
                unmatched_rows_output.append(
                    format_unmatched_row(
                        l1_row,
                        l1_ts_ms,
                        l1_bids,
                        l1_asks,
                    )
                )
                continue

            snapshot_ts, snapshot_rows = indexed

            candidates: list[tuple[int, int, BookRow]] = []
            left = bisect.bisect_left(snapshot_ts, l1_ts_ms - args.window_ms)
            right = bisect.bisect_right(snapshot_ts, l1_ts_ms + args.window_ms)
            for snapshot in snapshot_rows[left:right]:
                delta_ms = snapshot.ts_ms - l1_ts_ms
                score = score_rows(l1_bids, l1_asks, snapshot)
                candidates.append((score, abs(delta_ms), snapshot))

            candidates.sort(key=lambda item: (item[0], item[1], item[2].ts_ms))

            if not candidates:
                unmatched_rows_output.append(
                    format_unmatched_row(
                        l1_row,
                        l1_ts_ms,
                        l1_bids,
                        l1_asks,
                    )
                )
                continue

            best_score, _, best_snapshot = candidates[0]
            if best_score == 0:
                exact_match_rows += 1
            else:
                unmatched_rows_output.append(
                    format_unmatched_row(
                        l1_row,
                        l1_ts_ms,
                        l1_bids,
                        l1_asks,
                    )
                )

    write_csv(unmatched_output_path, unmatched_rows_output)

    unmatched_rows = total_rows - exact_match_rows
    print(f"total_l1_rows={total_rows}")
    print(f"exact_match_rows={exact_match_rows}")
    print(f"unmatched_l1_rows={unmatched_rows}")
    print(f"skipped_midday_l1_rows={skipped_midday_rows}")
    print(f"unmatched_output={unmatched_output_path}")


if __name__ == "__main__":
    main()
