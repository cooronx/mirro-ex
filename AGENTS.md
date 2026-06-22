# AGENTS.md

This file is for coding agents working on Mirro-Ex.

## Project Overview

Mirro-Ex is an early-stage Shanghai/Shenzhen market replay and simulated trading system.

Core goals:

- Read L2 order and transaction data from ClickHouse.
- Replay market data by date, time range, code list, and speed.
- Rebuild order books with multiple workers.
- Export order book snapshots to Parquet.
- Serve a Vue Web UI for replay control, market display, and simulated limit orders.
- Maintain simulated accounts, orders, fills, positions, and cancel flow in SQLite.

The project is still moving quickly. Prefer small, local changes that match existing style over broad redesigns.

## Repository Layout

Important Rust modules:

- `src/main.rs`: process entrypoint; loads config, initializes logging/db schema, starts web server.
- `src/config.rs`: TOML config model and defaults.
- `src/replay/`: replay engine, readers, coordinator, simulated clock, replay event types.
- `src/replay_manager.rs`: web-facing replay task manager; start/pause/resume/stop/speed state.
- `src/orderbook_worker.rs`: multi-worker order book rebuild, snapshot recording, simulated order matching hook.
- `src/matcher/`: order book implementation and matching-related market mechanics.
- `src/trading/`: simulated trading domain: accounts, orders, fills, positions, store, matching helpers.
- `src/db/`: ClickHouse and SQLite schema/query helpers.
- `src/web/`: HTTP handlers and routes using Salvo.
- `src/webdata/`: shared data state for Web interactions, currently SSE event bus and market display state.
- `src/publisher/`: NATS publishing code, not fully wired as a production data path yet.
- `src/snapshot_exporter.rs`: Parquet snapshot writer.
- `src/marketdata.rs`: generated protobuf module include.

Frontend:

- `webui/`: Vue 3 app.
- `webui/src/App.vue`: current main UI.
- `webui/src/api.ts`: HTTP/SSE client helpers.
- `webui/src/style.css`: app styling.
- `webui/vite.config.ts`: dev proxy to backend.

Scripts and data:

- `scripts/`: ClickHouse schema, comparison, and helper scripts.
- `helpers/`: local migration/export helper scripts.
- `config/conf.toml.example`: safe example config.
- `config/conf.toml`: local config; may contain local credentials.
- `data/trading.db`: local SQLite database; treat as local runtime state.
- `docs_for_agent/`: reference notes about source schemas/types.

## Module Boundaries

Keep these boundaries in mind:

- `web/` is HTTP routing and request/response handling only.
- `webdata/` is backend state and event models that exist to serve the Web UI. Core replay/orderbook code may depend on `webdata`, but should not depend on `web`.
- `replay/` should stay focused on reading, time coordination, and producing ordered replay events.
- `matcher/` should stay focused on exchange/order-book mechanics.
- `trading/` should stay focused on simulated trading state and matching/fill/account bookkeeping.
- `db/queries/` should contain SQL operations. Avoid hiding database writes in high-level business modules when a query helper is appropriate.

Do not move core logic into Vue or HTTP handlers. Handlers should validate input, call domain/store code, and render `ApiResponse`.

## Common Commands

Backend:

```bash
cargo fmt
cargo check
cargo test
cargo run
```

Frontend:

```bash
cd webui
npm run dev
npm run build
```

Useful runtime URLs with default config:

- Backend: `http://127.0.0.1:5800`
- Frontend dev server: `http://127.0.0.1:5173`

When testing LAN access, update `config/conf.toml` web host and/or the Vite dev host as needed. The default config binds to localhost.

## Configuration And Dependencies

- Rust edition is 2024.
- Salvo is used for HTTP and SSE.
- SQLite uses `rusqlite` with the `bundled` feature, so users generally do not need a system SQLite install just to build/run the Rust project.
- ClickHouse is required for market replay data.
- NATS is configured, but real-time snapshot publishing is still not the primary data path.
- Protobuf code generation uses `protoc-bin-vendored`.

Never commit local credentials or machine-specific config. Prefer updating `config/conf.toml.example` for shareable config changes.

## Web UI Data Flow

Current Web UI flow:

```text
ClickHouse L2 data
        -> replay coordinator / sim clock
        -> orderbook workers
        -> webdata::MarketState
        -> src/web/market.rs HTTP APIs
        -> src/web/events.rs SSE notifications
        -> webui Vue app
```

Market display state:

- `MarketState` stores latest order book display snapshot by code.
- Intraday chart points are aggregated by simulated market timestamp in 3-second buckets.
- SSE `market_changed` is notification-only; the frontend fetches latest `/market/snapshot` and `/market/intraday`.
- `replayStatus.sim_now_ms` is clock time. `marketSnapshot.timestamp_ms` is processed market data time. Do not confuse these.

## API Conventions

- Use `ApiResponse<T>` from `src/web/common.rs`.
- Success code is `1`.
- Keep error codes stable and grouped by handler/module.
- JSON body parsing and query parsing should go through common helpers.
- For replay speed requests, the backend expects `{"replay_speed": number}`.
- SSE events are named events, currently including `replay_changed`, `market_changed`, and `trading_changed`.

## Trading Notes

The simulated trading path currently focuses on limit orders:

- Account creation/query.
- Limit order creation.
- Cash/position freezing.
- Queue model based on reconstructed order book plus transaction volume.
- Order cancel.
- Fill/account/position settlement.

Be careful with transaction boundaries in `TradingStore`: publish events only after commits succeed.

## Testing Guidance

Before finishing Rust changes:

```bash
cargo fmt
cargo check
```

Run `cargo test` when changing replay, matcher, trading, db query, or shared state behavior.

Before finishing frontend changes:

```bash
cd webui
npm run build
```

Known warnings:

- There are existing Rust dead-code warnings because the project is still under active development.
- Vite may warn about large chunks due to UI dependencies.

## Coding Style

- Prefer existing patterns over new abstractions.
- Keep comments concise and only where they clarify non-obvious behavior.
- Keep structs and modules named after their actual responsibility.
- Avoid broad refactors mixed with feature work.
- Use integer prices scaled by `10000` unless the surrounding API explicitly expects human price units.
- Be explicit about simulated time vs processed market data time.

## File Editing Rules For Agents

- Do not revert user changes unless explicitly asked.
- Do not delete local data/config/log files unless explicitly asked.
- Prefer `rg` for search.
- Use `cargo fmt` after Rust edits.
- Use focused tests/builds that match the touched area.

