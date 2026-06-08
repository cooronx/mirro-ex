#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from typing import Any


DEFAULT_BASE_URL = "http://127.0.0.1:5800"


def parse_codes(values: list[str]) -> list[str]:
    codes: list[str] = []
    for value in values:
        codes.extend(code.strip() for code in value.split(",") if code.strip())
    return codes


def request_json(base_url: str, method: str, path: str, payload: dict[str, Any] | None = None) -> tuple[int, Any]:
    url = f"{base_url.rstrip('/')}{path}"
    body = None
    headers = {"Accept": "application/json"}

    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response_body = response.read().decode("utf-8")
            return response.status, json.loads(response_body) if response_body else None
    except urllib.error.HTTPError as exc:
        response_body = exc.read().decode("utf-8")
        try:
            parsed_body: Any = json.loads(response_body) if response_body else None
        except json.JSONDecodeError:
            parsed_body = response_body
        return exc.code, parsed_body


def print_response(http_status: int, body: Any) -> int:
    print(json.dumps(body, ensure_ascii=False, indent=2))

    if http_status >= 400:
        return 1
    if isinstance(body, dict) and body.get("code") != 1:
        return 1
    return 0


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="Replay web service base URL, default: %(default)s",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Control Mirro replay service over HTTP.")
    add_common_args(parser)

    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser("start", help="Start a replay task.")
    start.add_argument("--start-date", required=True, help="Replay start date, e.g. 2026-05-14.")
    start.add_argument("--end-date", required=True, help="Replay end date, e.g. 2026-05-14.")
    start.add_argument("--start-time", required=True, help="Replay start time, e.g. 09:15:00.000.")
    start.add_argument("--end-time", required=True, help="Replay end time, e.g. 15:00:00.000.")
    start.add_argument(
        "--code",
        action="append",
        default=[],
        help="Replay code. Can be repeated or comma-separated. Omit to replay all codes.",
    )
    start.add_argument("--speed", type=float, required=True, help="Replay speed multiplier.")
    start.add_argument(
        "--skip-intraday-breaks",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip non-trading intraday breaks, default: true.",
    )

    for command in ("pause", "resume", "stop"):
        subparsers.add_parser(command, help=f"{command.capitalize()} the active replay task.")

    subparsers.add_parser("status", help="Get replay runtime status.")
    subparsers.add_parser("config", help="Get configured engine replay config and active task config.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "start":
        payload = {
            "replay_start_date": args.start_date,
            "replay_end_date": args.end_date,
            "replay_start_time": args.start_time,
            "replay_end_time": args.end_time,
            "replay_codes": parse_codes(args.code),
            "replay_speed": args.speed,
            "skip_intraday_breaks": args.skip_intraday_breaks,
        }
        http_status, body = request_json(args.base_url, "POST", "/replay/start", payload)
    elif args.command in {"pause", "resume", "stop"}:
        http_status, body = request_json(args.base_url, "POST", f"/replay/{args.command}")
    else:
        http_status, body = request_json(args.base_url, "GET", f"/replay/{args.command}")

    return print_response(http_status, body)


if __name__ == "__main__":
    sys.exit(main())
