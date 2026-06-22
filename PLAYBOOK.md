# Hamid Signal Agent Playbook

This panel now runs as a repeated analysis cycle instead of a one-shot buy/sell generator.

## Analysis loop
1. Load market, universe, ticker, funding and multi-timeframe candle data.
2. Detect market context and risk mode.
3. Score 5m, 15m and 1h trend/momentum/volume conditions.
4. Classify every symbol into a lifecycle state.
5. Validate mandatory checklist, risk/reward, liquidity, ADX/range and persistence controls.
6. Emit `SIGNAL` only when all hard filters pass; otherwise keep the symbol in the watchlist.
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
python3 03_backend_signal_server_20260610_002822_IRST.py --host 127.0.0.1 --port 8765
```
Open `http://127.0.0.1:8765`.
