#!/usr/bin/env python3

from __future__ import annotations

import argparse
import bisect
import csv
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as parquet


DEPTH = 5
SHANGHAI_OFFSET_MS = 8 * 60 * 60 * 1000
DAY_MS = 24 * 60 * 60 * 1000
OPENING_AUCTION_START_MS = (9 * 60 * 60 + 15 * 60) * 1000
OPENING_AUCTION_END_MS = (9 * 60 * 60 + 25 * 60) * 1000
PRE_CONTINUOUS_START_MS = OPENING_AUCTION_END_MS
PRE_CONTINUOUS_END_MS = (9 * 60 * 60 + 30 * 60) * 1000
CLOSING_AUCTION_START_MS = (14 * 60 * 60 + 57 * 60) * 1000
CLOSING_AUCTION_END_MS = 15 * 60 * 60 * 1000


@dataclass
class BookRow:
    ts: int
    bids: list[tuple[float | None, int | None]]
    asks: list[tuple[float | None, int | None]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare one L1 Parquet file with one replay snapshot Parquet file."
    )
    parser.add_argument("l1", type=Path, help="L1 Parquet file.")
    parser.add_argument("snapshots", type=Path, help="Replay snapshot Parquet file.")
    parser.add_argument(
        "--window-ms",
        type=int,
        default=2000,
        help="Search window before and after each L1 timestamp, default: %(default)s.",
    )
    parser.add_argument(
        "--mismatch-output",
        type=Path,
        help="Optional CSV output for rows without an exact five-level match.",
    )
    return parser.parse_args()


def required_columns() -> list[str]:
    columns = ["ts", "code"]
    for index in range(1, DEPTH + 1):
        columns.extend(
            [
                f"ask{index}_price",
                f"ask{index}_size",
                f"bid{index}_price",
                f"bid{index}_size",
            ]
        )
    return columns


def load_book(path: Path) -> tuple[str, list[BookRow]]:
    rows = parquet.read_table(path, columns=required_columns()).to_pylist()
    if not rows:
        raise ValueError(f"Parquet file contains no rows: {path}")

    codes = {str(row["code"]) for row in rows}
    if len(codes) != 1:
        raise ValueError(f"expected one code in {path}, found: {sorted(codes)}")

    book_rows = []
    for row in rows:
        book_rows.append(
            BookRow(
                ts=int(row["ts"]),
                bids=[
                    normalize_level(
                        row[f"bid{index}_price"],
                        row[f"bid{index}_size"],
                    )
                    for index in range(1, DEPTH + 1)
                ],
                asks=[
                    normalize_level(
                        row[f"ask{index}_price"],
                        row[f"ask{index}_size"],
                    )
                    for index in range(1, DEPTH + 1)
                ],
            )
        )

    book_rows.sort(key=lambda row: row.ts)
    return codes.pop(), book_rows


def normalize_level(
    price: object,
    size: object,
) -> tuple[float | None, int | None]:
    if price is None and size is None:
        return None, None

    normalized_price = None if price is None else round(float(price), 4)
    normalized_size = None if size is None else int(size)
    if normalized_price in (None, 0) and normalized_size in (None, 0):
        return None, None
    if normalized_price is None or normalized_size is None:
        return None, None
    return normalized_price, normalized_size


def local_ms_of_day(ts: int) -> int:
    return (ts + SHANGHAI_OFFSET_MS) % DAY_MS


def is_call_auction_time(ts: int) -> bool:
    local_ms = local_ms_of_day(ts)
    return (
        OPENING_AUCTION_START_MS <= local_ms < OPENING_AUCTION_END_MS
        or CLOSING_AUCTION_START_MS <= local_ms < CLOSING_AUCTION_END_MS
    )


def should_skip_row(ts: int) -> bool:
    local_ms = local_ms_of_day(ts)
    return PRE_CONTINUOUS_START_MS <= local_ms < PRE_CONTINUOUS_END_MS


def comparison_depth(ts: int) -> int:
    if is_call_auction_time(ts):
        return 1
    return DEPTH


def mismatch_count(left: BookRow, right: BookRow) -> int:
    return len(find_differences(left, right))


def find_differences(left: BookRow, right: BookRow) -> list[str]:
    differences = []
    depth = comparison_depth(left.ts)
    for side, left_levels, right_levels in (
        ("bid", left.bids, right.bids),
        ("ask", left.asks, right.asks),
    ):
        for index, (left_level, right_level) in enumerate(
            zip(left_levels[:depth], right_levels[:depth]),
            start=1,
        ):
            if left_level[0] != right_level[0]:
                differences.append(f"{side}{index}_price")
            if left_level[1] != right_level[1]:
                differences.append(f"{side}{index}_size")
    return differences


def best_snapshot(
    l1_row: BookRow,
    snapshots: list[BookRow],
    snapshot_times: list[int],
    window_ms: int,
) -> tuple[BookRow, int] | None:
    start = bisect.bisect_left(snapshot_times, l1_row.ts - window_ms)
    end = bisect.bisect_right(snapshot_times, l1_row.ts + window_ms)
    if start == end:
        return None

    candidates = snapshots[start:end]
    snapshot = min(
        candidates,
        key=lambda row: (
            mismatch_count(l1_row, row),
            abs(row.ts - l1_row.ts),
            row.ts,
        ),
    )
    return snapshot, mismatch_count(l1_row, snapshot)


def write_mismatches(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "l1_ts",
        "snapshot_ts",
        "delta_ms",
        "comparison_depth",
        "mismatch_count",
        "mismatch_fields",
    ]
    for side in ("bid", "ask"):
        for index in range(1, DEPTH + 1):
            fieldnames.extend(
                [
                    f"l1_{side}{index}_price",
                    f"snapshot_{side}{index}_price",
                    f"l1_{side}{index}_size",
                    f"snapshot_{side}{index}_size",
                ]
            )
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def mismatch_row(
    l1_row: BookRow,
    snapshot: BookRow | None,
) -> dict[str, object]:
    differences = find_differences(l1_row, snapshot) if snapshot else []
    row: dict[str, object] = {
        "l1_ts": l1_row.ts,
        "snapshot_ts": snapshot.ts if snapshot else "",
        "delta_ms": snapshot.ts - l1_row.ts if snapshot else "",
        "comparison_depth": comparison_depth(l1_row.ts),
        "mismatch_count": len(differences) if snapshot else "",
        "mismatch_fields": ",".join(differences) if snapshot else "no_snapshot_in_window",
    }

    for side in ("bid", "ask"):
        l1_levels = getattr(l1_row, f"{side}s")
        snapshot_levels = getattr(snapshot, f"{side}s") if snapshot else []
        for index, l1_level in enumerate(l1_levels, start=1):
            snapshot_level = (
                snapshot_levels[index - 1]
                if snapshot
                else (None, None)
            )
            row[f"l1_{side}{index}_price"] = l1_level[0]
            row[f"snapshot_{side}{index}_price"] = snapshot_level[0]
            row[f"l1_{side}{index}_size"] = l1_level[1]
            row[f"snapshot_{side}{index}_size"] = snapshot_level[1]
    return row


def main() -> int:
    args = parse_args()
    if args.window_ms < 0:
        raise ValueError("--window-ms must be greater than or equal to 0")

    l1_code, l1_rows = load_book(args.l1)
    snapshot_code, snapshots = load_book(args.snapshots)
    if l1_code != snapshot_code:
        raise ValueError(
            f"code mismatch: L1 contains {l1_code}, snapshots contain {snapshot_code}"
        )

    snapshot_times = [row.ts for row in snapshots]
    exact_matches = 0
    no_candidates = 0
    skipped_rows = 0
    mismatches: list[dict[str, object]] = []

    for l1_row in l1_rows:
        if should_skip_row(l1_row.ts):
            skipped_rows += 1
            continue

        result = best_snapshot(
            l1_row,
            snapshots,
            snapshot_times,
            args.window_ms,
        )
        if result is None:
            no_candidates += 1
            mismatches.append(mismatch_row(l1_row, None))
            continue

        snapshot, differences = result
        if differences == 0:
            exact_matches += 1
            continue

        mismatches.append(mismatch_row(l1_row, snapshot))

    print(f"code={l1_code}")
    print(f"total_l1_rows={len(l1_rows)}")
    print(f"skipped_rows={skipped_rows}")
    compared_rows = len(l1_rows) - skipped_rows
    print(f"compared_rows={compared_rows}")
    print(f"l1_time_range={l1_rows[0].ts}..{l1_rows[-1].ts}")
    print(f"snapshot_time_range={snapshots[0].ts}..{snapshots[-1].ts}")
    print(f"exact_match_rows={exact_matches}")
    print(f"mismatch_rows={len(mismatches)}")
    print(f"no_snapshot_in_window={no_candidates}")
    match_rate = exact_matches / compared_rows if compared_rows else 0
    print(f"match_rate={match_rate:.2%}")

    if args.mismatch_output:
        write_mismatches(args.mismatch_output, mismatches)
        print(f"mismatch_output={args.mismatch_output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
