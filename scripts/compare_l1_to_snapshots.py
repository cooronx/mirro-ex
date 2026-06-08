#!/usr/bin/env python3

from __future__ import annotations

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
        help="Path to replay snapshot CSV or Parquet file.",
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
    parser.add_argument(
        "--time-mismatch-output",
        default="data/l1_snapshot_time_mismatch_samples.csv",
        help="Output CSV containing nearby best-match snapshots whose timestamps differ.",
    )
    parser.add_argument(
        "--time-mismatch-limit",
        type=int,
        default=10,
        help="Maximum number of timestamp mismatch samples to write.",
    )
    return parser.parse_args()


def l1_time_to_ms(raw: str) -> int:
    dt = datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=SH_TZ)
    return int(dt.timestamp() * 1000)


def timestamp_ms_to_time(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=SH_TZ).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )[:-3]


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


def normalize_price(raw: object) -> str:
    if raw is None:
        return ""
    raw = str(raw).strip()
    if not raw:
        return ""
    try:
        return f"{Decimal(raw):.4f}"
    except InvalidOperation:
        return raw


def normalize_qty(raw: object) -> str:
    if raw is None:
        return ""
    raw = str(raw).strip()
    if not raw:
        return ""
    try:
        value = Decimal(raw)
        if value == value.to_integral_value():
            return str(int(value))
    except InvalidOperation:
        pass
    return raw


def normalize_level(price: object, qty: object) -> str:
    price = normalize_price(price)
    qty = normalize_qty(qty)
    return f"{price}:{qty}" if price and qty else ""


def csv_snapshot_fields(row: dict[str, str], prefix: str) -> list[str]:
    return [
        normalize_level(*parse_snapshot_cell(row[f"{prefix}{index}"]))
        for index in range(1, 6)
    ]


def parquet_snapshot_fields(row: dict[str, object], prefix: str) -> list[str]:
    return [
        normalize_level(
            row.get(f"{prefix}{index}_price"),
            row.get(f"{prefix}{index}_size"),
        )
        for index in range(1, 6)
    ]


def l1_fields(row: dict[str, str], price_prefix: str, vol_prefix: str) -> list[str]:
    fields: list[str] = []
    for index in range(1, 6):
        price = row.get(f"{price_prefix}{index}", "").strip()
        vol = row.get(f"{vol_prefix}{index}", "").strip()
        fields.append(normalize_level(price, vol))
    return fields


def load_csv_snapshots(path: Path) -> list[BookRow]:
    rows: list[BookRow] = []
    with path.open("r", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(
                BookRow(
                    ts_ms=int(row["ts"]),
                    code=normalize_code(row["code"]),
                    bids=csv_snapshot_fields(row, "bid"),
                    asks=csv_snapshot_fields(row, "ask"),
                )
            )
    return rows


def load_parquet_snapshots(path: Path) -> list[BookRow]:
    try:
        import pyarrow.parquet as parquet
    except ImportError as exc:
        raise SystemExit(
            "reading Parquet snapshots requires pyarrow; install it with: "
            "python3 -m pip install pyarrow"
        ) from exc

    columns = ["ts", "code"]
    for index in range(1, 6):
        columns.extend(
            [
                f"ask{index}_price",
                f"ask{index}_size",
                f"bid{index}_price",
                f"bid{index}_size",
            ]
        )

    rows: list[BookRow] = []
    parquet_file = parquet.ParquetFile(path)
    for batch in parquet_file.iter_batches(columns=columns):
        for row in batch.to_pylist():
            rows.append(
                BookRow(
                    ts_ms=int(row["ts"]),
                    code=normalize_code(row["code"]),
                    bids=parquet_snapshot_fields(row, "bid"),
                    asks=parquet_snapshot_fields(row, "ask"),
                )
            )
    return rows


def load_snapshots(path: Path) -> list[BookRow]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return load_csv_snapshots(path)
    if suffix == ".parquet":
        return load_parquet_snapshots(path)
    raise SystemExit(
        f"unsupported snapshot file type: {path}; expected .csv or .parquet"
    )


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


def format_time_mismatch_row(
    l1_row: dict[str, str],
    l1_ts_ms: int,
    l1_bids: list[str],
    l1_asks: list[str],
    snapshot: BookRow,
    mismatch_count: int,
    within_window: bool,
) -> dict[str, str]:
    result = {
        "code": normalize_code(l1_row["code"]),
        "l1_time": l1_row["time"],
        "l1_ts_ms": str(l1_ts_ms),
        "snapshot_time": timestamp_ms_to_time(snapshot.ts_ms),
        "snapshot_ts_ms": str(snapshot.ts_ms),
        "delta_ms": str(snapshot.ts_ms - l1_ts_ms),
        "book_mismatch_count": str(mismatch_count),
        "exact_book_match": str(mismatch_count == 0).lower(),
        "within_window": str(within_window).lower(),
    }

    for index in range(5):
        result[f"l1_bid{index + 1}"] = l1_bids[index]
        result[f"snapshot_bid{index + 1}"] = snapshot.bids[index]
        result[f"l1_ask{index + 1}"] = l1_asks[index]
        result[f"snapshot_ask{index + 1}"] = snapshot.asks[index]

    return result


def nearest_snapshot(
    snapshot_ts: list[int],
    snapshot_rows: list[BookRow],
    target_ts_ms: int,
) -> BookRow | None:
    if not snapshot_rows:
        return None

    position = bisect.bisect_left(snapshot_ts, target_ts_ms)
    candidate_indexes = [
        index
        for index in (position - 1, position)
        if 0 <= index < len(snapshot_rows)
    ]
    return min(
        (snapshot_rows[index] for index in candidate_indexes),
        key=lambda snapshot: (abs(snapshot.ts_ms - target_ts_ms), snapshot.ts_ms),
    )


def write_csv(
    path: Path,
    rows: list[dict[str, str]],
    empty_fieldnames: list[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=empty_fieldnames)
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
    time_mismatch_output_path = Path(args.time_mismatch_output)

    snapshots = load_snapshots(snapshot_path)
    snapshot_index = index_snapshots_by_code(snapshots)
    unmatched_rows_output: list[dict[str, str]] = []
    time_mismatch_rows_output: list[dict[str, str]] = []
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
                nearest = nearest_snapshot(snapshot_ts, snapshot_rows, l1_ts_ms)
                if (
                    nearest is not None
                    and nearest.ts_ms != l1_ts_ms
                    and len(time_mismatch_rows_output) < args.time_mismatch_limit
                ):
                    time_mismatch_rows_output.append(
                        format_time_mismatch_row(
                            l1_row,
                            l1_ts_ms,
                            l1_bids,
                            l1_asks,
                            nearest,
                            score_rows(l1_bids, l1_asks, nearest),
                            False,
                        )
                    )
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
            if (
                best_snapshot.ts_ms != l1_ts_ms
                and len(time_mismatch_rows_output) < args.time_mismatch_limit
            ):
                time_mismatch_rows_output.append(
                    format_time_mismatch_row(
                        l1_row,
                        l1_ts_ms,
                        l1_bids,
                        l1_asks,
                        best_snapshot,
                        best_score,
                        True,
                    )
                )

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

    unmatched_fieldnames = ["l1_time", "l1_ts_ms", "code"]
    for index in range(1, 6):
        unmatched_fieldnames.extend([f"l1_bid{index}", f"l1_ask{index}"])
    write_csv(unmatched_output_path, unmatched_rows_output, unmatched_fieldnames)

    time_mismatch_fieldnames = [
        "code",
        "l1_time",
        "l1_ts_ms",
        "snapshot_time",
        "snapshot_ts_ms",
        "delta_ms",
        "book_mismatch_count",
        "exact_book_match",
        "within_window",
    ]
    for index in range(1, 6):
        time_mismatch_fieldnames.extend(
            [
                f"l1_bid{index}",
                f"snapshot_bid{index}",
                f"l1_ask{index}",
                f"snapshot_ask{index}",
            ]
        )
    write_csv(
        time_mismatch_output_path,
        time_mismatch_rows_output,
        time_mismatch_fieldnames,
    )

    unmatched_rows = total_rows - exact_match_rows
    print(f"total_l1_rows={total_rows}")
    print(f"exact_match_rows={exact_match_rows}")
    print(f"unmatched_l1_rows={unmatched_rows}")
    print(f"skipped_midday_l1_rows={skipped_midday_rows}")
    print(f"unmatched_output={unmatched_output_path}")
    print(f"time_mismatch_samples={len(time_mismatch_rows_output)}")
    print(f"time_mismatch_output={time_mismatch_output_path}")


if __name__ == "__main__":
    main()
