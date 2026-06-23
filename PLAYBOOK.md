# Hamid Bitunix Signal Agent Playbook

This Bitunix Futures panel now runs as a repeated analysis cycle instead of a one-shot buy/sell generator.

## Analysis loop
1. Load market, universe, ticker, funding and multi-timeframe candle data.
2. Detect market context and risk mode.
3. Score 5m, 15m and 1h trend/momentum/volume conditions.
4. Classify every symbol into a lifecycle state.
5. Validate mandatory checklist, risk/reward, liquidity, ADX/range and persistence controls.
6. Emit `SIGNAL` only when playbook confirmation passes; otherwise keep the symbol in the watchlist. Runtime hard filters are relaxed by default for iPad/HTML operation and can be re-enabled with `STRICT_FILTERS=1`.
7. Track paper-trade outcomes and stop-loss notes.
8. Accept manual feedback through `/api/feedback` for later review and calibration.

## Lifecycle states
- `NO_SETUP`: no useful structure.
- `WATCHING`: setup exists, but entry is not ready.
- `APPROACHING_ENTRY_ZONE`: price is moving toward the planned zone.
- `WAITING_CONFIRMATION`: zone is relevant, but 5m/15m confirmation is incomplete.
- `VALID_ENTRY`: playbook checks pass before final signal filters.
- `SIGNAL`: paper-first tradable signal after validation.
- `LOW_LIQUIDITY`: rejected by volume gate.
- `RANGE_FILTERED`: rejected by ADX/range gate.

## Mandatory checks before a signal
- Higher timeframe direction agrees.
- Setup timeframe direction agrees.
- Entry timeframe confirmation is present.
- Price is not overextended.
- Trend strength is acceptable.
- Volume expansion is present.

## iPad / HTML mode
- The backend serves the HTML panel at `/`, so an iPad can open the panel in Safari.
- The HTML panel now calls Bitunix Futures public market endpoints.
- If the iPad/browser blocks direct exchange calls, run the Python backend and open `http://<server-ip>:8765` from the iPad.

## Key endpoints
- `GET /api/status`
- `GET /api/signals`
- `GET /api/watchlist`
- `GET /api/universe`
- `GET /api/playbook`
- `GET /api/feedback`
- `POST /api/scan-now`
- `POST /api/start-paper`
- `POST /api/stop-paper`
- `POST /api/feedback` with JSON: `{ "symbol": "BTCUSDT", "verdict": "too_early", "note": "wait for candle close", "signal": {...} }`

## Run
```bash
python3 03_backend_signal_server_20260610_002822_IRST.py --host 0.0.0.0 --port 8765
```
Open `http://127.0.0.1:8765`.


## Data source
- Primary exchange: Bitunix USDT-M Futures public market API (`https://fapi.bitunix.com`).
- Klines: `/api/v1/futures/market/kline`.
- Tickers: `/api/v1/futures/market/tickers`.
- Trading pairs: `/api/v1/futures/market/trading_pairs`.
- Funding: `/api/v1/futures/market/funding_rate/batch`.
