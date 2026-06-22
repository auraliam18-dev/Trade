#!/usr/bin/env python3
"""
Hamid Crypto Futures Signal Panel v3.0

Production-oriented, paper-first futures signal scanner for the top 100 crypto
assets. It connects to Binance USD-M Futures public market data, uses
CoinMarketCap when an API key is provided, falls back to CoinGecko, and sends
Telegram alerts when Telegram environment variables are configured.

Important:
- This program does not place real orders by default.
- Paper trading starts with 3,000 USDT by default.
- Live trading is intentionally disabled unless you add real exchange-order
  code, API keys, and explicit operational controls.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import sqlite3
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - Python < 3.9 fallback
    ZoneInfo = None  # type: ignore


PROJECT_DIR = Path(__file__).resolve().parent
REPORTS_DIR = PROJECT_DIR / "reports"
STATE_DIR = PROJECT_DIR / "state"
LOGS_DIR = PROJECT_DIR / "logs"
DASHBOARD_FILE = PROJECT_DIR / "Future signal"
STATE_FILE = STATE_DIR / "hamid_signal_state.json"
DB_FILE = STATE_DIR / "hamid_paper_trading.sqlite3"

BINANCE_FAPI_BASE = os.getenv("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com").rstrip("/")
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
CMC_BASE = "https://pro-api.coinmarketcap.com"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat(timespec="seconds")


def local_timestamp() -> str:
    if ZoneInfo:
        local = now_utc().astimezone(ZoneInfo("Asia/Tehran"))
    else:
        local = now_utc()
    return local.strftime("%Y%m%d_%H%M%S_IRST")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b * 100.0


def round_price(price: float) -> float:
    if price <= 0:
        return price
    if price >= 1000:
        return round(price, 2)
    if price >= 100:
        return round(price, 3)
    if price >= 1:
        return round(price, 4)
    if price >= 0.01:
        return round(price, 6)
    return round(price, 8)


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


@dataclass
class Settings:
    http_host: str = os.getenv("HAMID_HTTP_HOST", "127.0.0.1")
    http_port: int = int(os.getenv("HAMID_HTTP_PORT", "8765"))
    scan_interval_seconds: int = int(os.getenv("SCAN_INTERVAL_SECONDS", "300"))
    kline_limit: int = int(os.getenv("KLINE_LIMIT", "180"))
    max_workers: int = int(os.getenv("MAX_WORKERS", "7"))
    initial_equity: float = float(os.getenv("INITIAL_EQUITY_USDT", "3000"))
    risk_per_trade_pct: float = float(os.getenv("RISK_PER_TRADE_PCT", "0.35"))
    max_daily_loss_pct: float = float(os.getenv("MAX_DAILY_LOSS_PCT", "3.0"))
    max_total_open_risk_pct: float = float(os.getenv("MAX_TOTAL_OPEN_RISK_PCT", "3.0"))
    max_open_positions: int = int(os.getenv("MAX_OPEN_POSITIONS", "8"))
    max_notional_per_trade_pct: float = float(os.getenv("MAX_NOTIONAL_PER_TRADE_PCT", "45"))
    max_leverage: float = float(os.getenv("MAX_LEVERAGE", "3"))
    min_signal_score: float = float(os.getenv("MIN_SIGNAL_SCORE", "74"))
    watch_score: float = float(os.getenv("WATCH_SCORE", "62"))
    ready_score: float = float(os.getenv("READY_SCORE", "70"))
    min_confirmation_score: float = float(os.getenv("MIN_CONFIRMATION_SCORE", "65"))
    strong_signal_score: float = float(os.getenv("STRONG_SIGNAL_SCORE", "82"))
    strong_flip_score: float = float(os.getenv("STRONG_FLIP_SCORE", "86"))
    flip_cooldown_minutes: int = int(os.getenv("FLIP_COOLDOWN_MINUTES", "30"))
    min_quote_volume_usd: float = float(os.getenv("MIN_QUOTE_VOLUME_USD", "25000000"))
    min_rr: float = float(os.getenv("MIN_RR", "1.8"))
    paper_target_hours: float = float(os.getenv("PAPER_TARGET_HOURS", "8"))
    cmc_api_key: str = os.getenv("CMC_API_KEY", "").strip()
    coingecko_api_key: str = os.getenv("COINGECKO_API_KEY", "").strip()
    crypto_bubbles_source_url: str = os.getenv("CRYPTO_BUBBLES_SOURCE_URL", "").strip()
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "").strip()


class StateStore:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.RLock()
        self.state = self._load()

    def _load(self) -> Dict[str, Any]:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {
            "created_at": now_iso(),
            "status": "BOOTING",
            "last_scan_at": None,
            "last_error": None,
            "last_report": None,
            "signals": [],
            "watchlist": [],
            "universe": [],
            "market_context": {},
            "paper": {
                "enabled": False,
                "started_at": None,
                "ends_at": None,
                "initial_equity": 3000.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "equity": 3000.0,
                "active_positions": [],
                "closed_trades": [],
                "stats": {},
            },
            "events": [],
            "feedback": [],
            "analysis_cycle": {},
            "active_signal_locks": {},
        }

    def save(self) -> None:
        with self.lock:
            atomic_write_json(self.path, self.state)

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return json.loads(json.dumps(self.state, ensure_ascii=False))

    def update(self, **kwargs: Any) -> None:
        with self.lock:
            self.state.update(kwargs)
            self.save()

    def event(self, level: str, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        with self.lock:
            events = self.state.setdefault("events", [])
            events.insert(
                0,
                {
                    "time": now_iso(),
                    "level": level,
                    "message": message,
                    "extra": extra or {},
                },
            )
            del events[500:]
            self.save()


class ApiClient:
    def __init__(self, settings: Settings, store: StateStore):
        self.settings = settings
        self.store = store
        self.exchange_info_cache: Optional[Dict[str, Any]] = None
        self.exchange_info_loaded_at = 0.0

    def request_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 18,
        retries: int = 3,
    ) -> Any:
        request_headers = {
            "Accept": "application/json",
            "User-Agent": "HamidSignalPanel/3.0 (+paper-trading)",
        }
        if headers:
            request_headers.update(headers)
        last_error: Optional[Exception] = None
        for attempt in range(retries):
            try:
                req = urllib.request.Request(url, headers=request_headers)
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    raw = resp.read()
                return json.loads(raw.decode("utf-8"))
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code in (418, 429, 500, 502, 503, 504):
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise
            except Exception as exc:
                last_error = exc
                time.sleep(1.2 * (attempt + 1))
        raise RuntimeError(f"request failed after retries: {url} :: {last_error}")

    def fetch_cmc_top100(self) -> List[Dict[str, Any]]:
        params = urllib.parse.urlencode(
            {"start": 1, "limit": 100, "convert": "USD", "sort": "market_cap"}
        )
        url = f"{CMC_BASE}/v1/cryptocurrency/listings/latest?{params}"
        payload = self.request_json(url, headers={"X-CMC_PRO_API_KEY": self.settings.cmc_api_key})
        result: List[Dict[str, Any]] = []
        for row in payload.get("data", []):
            quote = row.get("quote", {}).get("USD", {})
            result.append(
                {
                    "rank": int(row.get("cmc_rank") or len(result) + 1),
                    "symbol": str(row.get("symbol", "")).upper(),
                    "name": row.get("name", ""),
                    "market_cap": safe_float(quote.get("market_cap")),
                    "volume_24h": safe_float(quote.get("volume_24h")),
                    "percent_change_1h": safe_float(quote.get("percent_change_1h")),
                    "percent_change_24h": safe_float(quote.get("percent_change_24h")),
                    "percent_change_7d": safe_float(quote.get("percent_change_7d")),
                    "source": "CoinMarketCap",
                }
            )
        return result[:100]

    def fetch_coingecko_top100(self) -> List[Dict[str, Any]]:
        params = urllib.parse.urlencode(
            {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 100,
                "page": 1,
                "sparkline": "false",
                "price_change_percentage": "1h,24h,7d",
            }
        )
        headers: Dict[str, str] = {}
        if self.settings.coingecko_api_key:
            headers["x-cg-demo-api-key"] = self.settings.coingecko_api_key
        url = f"{COINGECKO_BASE}/coins/markets?{params}"
        payload = self.request_json(url, headers=headers)
        result = []
        for row in payload:
            result.append(
                {
                    "rank": int(row.get("market_cap_rank") or len(result) + 1),
                    "symbol": str(row.get("symbol", "")).upper(),
                    "name": row.get("name", ""),
                    "market_cap": safe_float(row.get("market_cap")),
                    "volume_24h": safe_float(row.get("total_volume")),
                    "percent_change_1h": safe_float(row.get("price_change_percentage_1h_in_currency")),
                    "percent_change_24h": safe_float(row.get("price_change_percentage_24h")),
                    "percent_change_7d": safe_float(row.get("price_change_percentage_7d_in_currency")),
                    "source": "CoinGecko fallback",
                }
            )
        return result[:100]

    def fetch_binance_volume_universe(self) -> List[Dict[str, Any]]:
        tickers = self.fetch_24h_tickers()
        usdt = [
            t
            for t in tickers.values()
            if str(t.get("symbol", "")).endswith("USDT") and safe_float(t.get("quoteVolume")) > 0
        ]
        usdt.sort(key=lambda x: safe_float(x.get("quoteVolume")), reverse=True)
        result = []
        for idx, row in enumerate(usdt[:100], 1):
            symbol = row["symbol"].replace("USDT", "")
            result.append(
                {
                    "rank": idx,
                    "symbol": symbol.replace("1000", "", 1) if symbol.startswith("1000") else symbol,
                    "name": symbol,
                    "market_cap": 0.0,
                    "volume_24h": safe_float(row.get("quoteVolume")),
                    "percent_change_1h": 0.0,
                    "percent_change_24h": safe_float(row.get("priceChangePercent")),
                    "percent_change_7d": 0.0,
                    "source": "Binance volume fallback",
                }
            )
        return result

    def fetch_top100_universe(self) -> List[Dict[str, Any]]:
        if self.settings.cmc_api_key:
            try:
                return self.fetch_cmc_top100()
            except Exception as exc:
                self.store.event("WARN", "CMC top100 failed; falling back to CoinGecko", {"error": str(exc)})
        try:
            return self.fetch_coingecko_top100()
        except Exception as exc:
            self.store.event("WARN", "CoinGecko top100 failed; falling back to Binance volume", {"error": str(exc)})
            return self.fetch_binance_volume_universe()

    def fetch_exchange_info(self) -> Dict[str, Any]:
        if self.exchange_info_cache and time.time() - self.exchange_info_loaded_at < 3600:
            return self.exchange_info_cache
        url = f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo"
        payload = self.request_json(url)
        self.exchange_info_cache = payload
        self.exchange_info_loaded_at = time.time()
        return payload

    def fetch_24h_tickers(self) -> Dict[str, Dict[str, Any]]:
        url = f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/24hr"
        payload = self.request_json(url, timeout=24)
        return {row.get("symbol"): row for row in payload if isinstance(row, dict)}

    def fetch_funding_map(self) -> Dict[str, float]:
        try:
            url = f"{BINANCE_FAPI_BASE}/fapi/v1/premiumIndex"
            payload = self.request_json(url, timeout=20)
            return {row.get("symbol"): safe_float(row.get("lastFundingRate")) for row in payload if isinstance(row, dict)}
        except Exception as exc:
            self.store.event("WARN", "Funding map failed", {"error": str(exc)})
            return {}

    def fetch_global_context(self) -> Dict[str, Any]:
        context = {
            "source": "CoinGecko global",
            "btc_dominance": None,
            "eth_dominance": None,
            "market_cap_change_24h": None,
            "risk_mode": "NEUTRAL",
        }
        try:
            payload = self.request_json(f"{COINGECKO_BASE}/global", timeout=15, retries=2)
            data = payload.get("data", {})
            mcp = data.get("market_cap_percentage", {})
            context["btc_dominance"] = safe_float(mcp.get("btc"), None)  # type: ignore[arg-type]
            context["eth_dominance"] = safe_float(mcp.get("eth"), None)  # type: ignore[arg-type]
            context["market_cap_change_24h"] = safe_float(data.get("market_cap_change_percentage_24h_usd"), None)  # type: ignore[arg-type]
            change = context["market_cap_change_24h"]
            if change is not None and change <= -2:
                context["risk_mode"] = "RISK_OFF"
            elif change is not None and change >= 2:
                context["risk_mode"] = "RISK_ON"
        except Exception as exc:
            context["error"] = str(exc)
        return context

    def fetch_crypto_bubbles_overlay(self) -> Dict[str, Dict[str, Any]]:
        if not self.settings.crypto_bubbles_source_url:
            return {}
        try:
            payload = self.request_json(self.settings.crypto_bubbles_source_url, timeout=15, retries=2)
            rows = payload if isinstance(payload, list) else payload.get("data", [])
            overlay: Dict[str, Dict[str, Any]] = {}
            for row in rows:
                symbol = str(row.get("symbol") or row.get("s") or "").upper()
                if symbol:
                    overlay[symbol] = row
            return overlay
        except Exception as exc:
            self.store.event("WARN", "Crypto Bubbles optional overlay failed", {"error": str(exc)})
            return {}

    def fetch_klines(self, symbol: str, interval: str, limit: int) -> List[Dict[str, float]]:
        params = urllib.parse.urlencode({"symbol": symbol, "interval": interval, "limit": limit})
        url = f"{BINANCE_FAPI_BASE}/fapi/v1/klines?{params}"
        payload = self.request_json(url, timeout=20)
        candles = []
        for row in payload:
            candles.append(
                {
                    "open_time": int(row[0]),
                    "open": safe_float(row[1]),
                    "high": safe_float(row[2]),
                    "low": safe_float(row[3]),
                    "close": safe_float(row[4]),
                    "volume": safe_float(row[5]),
                    "close_time": int(row[6]),
                    "quote_volume": safe_float(row[7]),
                    "trades": safe_float(row[8]),
                    "taker_buy_base": safe_float(row[9]),
                    "taker_buy_quote": safe_float(row[10]),
                }
            )
        return candles


def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for value in values[1:]:
        out.append(value * k + out[-1] * (1 - k))
    return out


def sma(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    sample = values[-period:]
    return sum(sample) / len(sample)


def stddev(values: List[float], period: int) -> float:
    sample = values[-period:]
    if len(sample) < 2:
        return 0.0
    mean = sum(sample) / len(sample)
    variance = sum((x - mean) ** 2 for x in sample) / (len(sample) - 1)
    return math.sqrt(max(variance, 0.0))


def rsi_wilder(values: List[float], period: int = 14) -> float:
    if len(values) <= period:
        return 50.0
    gains = []
    losses = []
    for i in range(1, period + 1):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(abs(min(change, 0.0)))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    for i in range(period + 1, len(values)):
        change = values[i] - values[i - 1]
        gain = max(change, 0.0)
        loss = abs(min(change, 0.0))
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(candles: List[Dict[str, float]], period: int = 14) -> float:
    if len(candles) < 2:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sma(trs, period)


def macd_hist(values: List[float]) -> Tuple[float, float, float]:
    if len(values) < 35:
        return 0.0, 0.0, 0.0
    fast = ema(values, 12)
    slow = ema(values, 26)
    line = [f - s for f, s in zip(fast, slow)]
    signal = ema(line, 9)
    hist = line[-1] - signal[-1]
    return line[-1], signal[-1], hist


def adx(candles: List[Dict[str, float]], period: int = 14) -> float:
    if len(candles) < period + 2:
        return 0.0
    plus_dm = []
    minus_dm = []
    tr = []
    for i in range(1, len(candles)):
        up = candles[i]["high"] - candles[i - 1]["high"]
        down = candles[i - 1]["low"] - candles[i]["low"]
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        tr.append(
            max(
                candles[i]["high"] - candles[i]["low"],
                abs(candles[i]["high"] - candles[i - 1]["close"]),
                abs(candles[i]["low"] - candles[i - 1]["close"]),
            )
        )
    if len(tr) < period:
        return 0.0
    atrs = sum(tr[-period:])
    if atrs == 0:
        return 0.0
    plus_di = 100 * sum(plus_dm[-period:]) / atrs
    minus_di = 100 * sum(minus_dm[-period:]) / atrs
    denom = plus_di + minus_di
    if denom == 0:
        return 0.0
    dx = abs(plus_di - minus_di) / denom * 100
    return dx


def vwap(candles: List[Dict[str, float]], period: int = 48) -> float:
    sample = candles[-period:]
    total_vol = sum(c["volume"] for c in sample)
    if total_vol == 0:
        return sample[-1]["close"] if sample else 0.0
    total = 0.0
    for c in sample:
        typical = (c["high"] + c["low"] + c["close"]) / 3
        total += typical * c["volume"]
    return total / total_vol


def volume_zscore(candles: List[Dict[str, float]], period: int = 40) -> float:
    volumes = [c["quote_volume"] or c["volume"] for c in candles]
    if len(volumes) < 10:
        return 0.0
    sample = volumes[-period - 1 : -1]
    if not sample:
        return 0.0
    mean = sum(sample) / len(sample)
    sd = stddev(sample, len(sample))
    if sd == 0:
        return 0.0
    return (volumes[-1] - mean) / sd


def timeframe_metrics(candles: List[Dict[str, float]], label: str) -> Dict[str, Any]:
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    if len(candles) < 60:
        raise ValueError(f"not enough candles for {label}")

    close = closes[-1]
    ema9 = ema(closes, 9)[-1]
    ema21 = ema(closes, 21)[-1]
    ema55 = ema(closes, 55)[-1]
    rsi = rsi_wilder(closes)
    macd_line, macd_signal, hist = macd_hist(closes)
    current_atr = atr(candles)
    current_adx = adx(candles)
    current_vwap = vwap(candles)
    vol_z = volume_zscore(candles)
    bb_mid = sma(closes, 20)
    bb_sd = stddev(closes, 20)
    bb_upper = bb_mid + 2 * bb_sd
    bb_lower = bb_mid - 2 * bb_sd
    recent_high = max(highs[-25:-1])
    recent_low = min(lows[-25:-1])
    prev_close = closes[-2]

    long_score = 0.0
    short_score = 0.0
    reasons: List[str] = []

    if close > ema9 > ema21 > ema55:
        long_score += 21
        reasons.append(f"{label}: EMA stack bullish")
    elif close < ema9 < ema21 < ema55:
        short_score += 21
        reasons.append(f"{label}: EMA stack bearish")
    else:
        if close > ema21:
            long_score += 7
        if close < ema21:
            short_score += 7

    if hist > 0 and macd_line > macd_signal:
        long_score += 13
        reasons.append(f"{label}: MACD bullish")
    elif hist < 0 and macd_line < macd_signal:
        short_score += 13
        reasons.append(f"{label}: MACD bearish")

    if 45 <= rsi <= 68:
        long_score += 10
    elif 32 <= rsi <= 55:
        short_score += 10
    if rsi > 76:
        long_score -= 9
        short_score += 4
    if rsi < 24:
        short_score -= 9
        long_score += 4

    if close > current_vwap:
        long_score += 9
    elif close < current_vwap:
        short_score += 9

    if current_adx >= 20:
        long_score += 7
        short_score += 7
    if current_adx >= 35:
        long_score += 4
        short_score += 4

    if close > recent_high and prev_close <= recent_high:
        long_score += 14
        reasons.append(f"{label}: BOS above recent high")
    if close < recent_low and prev_close >= recent_low:
        short_score += 14
        reasons.append(f"{label}: BOS below recent low")

    if vol_z >= 1.3:
        long_score += 6
        short_score += 6
        reasons.append(f"{label}: volume expansion z={vol_z:.2f}")

    bb_position = 50.0
    if bb_upper > bb_lower:
        bb_position = (close - bb_lower) / (bb_upper - bb_lower) * 100
    if bb_position > 92:
        long_score -= 6
        short_score += 3
    if bb_position < 8:
        short_score -= 6
        long_score += 3

    direction = "NEUTRAL"
    if long_score >= short_score + 8:
        direction = "LONG"
    elif short_score >= long_score + 8:
        direction = "SHORT"

    return {
        "label": label,
        "close": close,
        "ema9": ema9,
        "ema21": ema21,
        "ema55": ema55,
        "rsi": rsi,
        "macd_hist": hist,
        "atr": current_atr,
        "adx": current_adx,
        "vwap": current_vwap,
        "volume_z": vol_z,
        "recent_high": recent_high,
        "recent_low": recent_low,
        "bb_position": bb_position,
        "long_score": clamp(long_score, 0, 100),
        "short_score": clamp(short_score, 0, 100),
        "direction": direction,
        "reasons": reasons,
    }


class SignalEngine:
    ALIASES = {
        "SHIB": "1000SHIBUSDT",
        "PEPE": "1000PEPEUSDT",
        "BONK": "1000BONKUSDT",
        "FLOKI": "1000FLOKIUSDT",
        "LUNC": "1000LUNCUSDT",
        "XEC": "1000XECUSDT",
        "SATS": "1000SATSUSDT",
        "RATS": "1000RATSUSDT",
        "MOG": "1000000MOGUSDT",
        "WHY": "1000WHYUSDT",
    }

    def __init__(self, api: ApiClient, settings: Settings, store: StateStore):
        self.api = api
        self.settings = settings
        self.store = store

    def build_symbol_map(self, exchange_info: Dict[str, Any]) -> Dict[str, str]:
        direct: Dict[str, str] = {}
        for row in exchange_info.get("symbols", []):
            if row.get("contractType") != "PERPETUAL":
                continue
            if row.get("status") != "TRADING":
                continue
            if row.get("quoteAsset") != "USDT":
                continue
            symbol = row.get("symbol")
            base = row.get("baseAsset")
            if symbol and base:
                direct[str(base).upper()] = str(symbol).upper()
                if str(symbol).upper().endswith("USDT"):
                    raw_base = str(symbol).upper().replace("USDT", "")
                    direct.setdefault(raw_base, str(symbol).upper())
        return direct

    def resolve_symbol(self, asset_symbol: str, symbol_map: Dict[str, str]) -> Optional[str]:
        asset = asset_symbol.upper().strip()
        if asset in symbol_map:
            return symbol_map[asset]
        if asset in self.ALIASES:
            return self.ALIASES[asset]
        direct = f"{asset}USDT"
        if direct in symbol_map.values():
            return direct
        return None

    def setup_playbook(self, direction: str, m5: Dict[str, Any], m15: Dict[str, Any], m1h: Dict[str, Any], current: float) -> Dict[str, Any]:
        """Translate discretionary playbook ideas into explicit machine-checkable setup states."""
        pullback_ref = m15["ema21"]
        near_pullback = abs(current - pullback_ref) / current <= 0.008 if current else False
        breakout_long = current >= m15["recent_high"] * 0.998
        breakout_short = current <= m15["recent_low"] * 1.002
        volume_ok = max(m5["volume_z"], m15["volume_z"]) >= 0.8
        momentum_ok = (direction == "LONG" and m5["macd_hist"] > 0) or (direction == "SHORT" and m5["macd_hist"] < 0)
        confirmation_ok = m5["direction"] == direction and volume_ok and momentum_ok

        if direction == "LONG" and breakout_long:
            name = "BREAKOUT_CONTINUATION"
        elif direction == "SHORT" and breakout_short:
            name = "BREAKOUT_CONTINUATION"
        elif near_pullback:
            name = "EMA21_PULLBACK"
        else:
            name = "MOMENTUM_WATCH"

        mandatory_checks = {
            "higher_tf_direction": m1h["direction"] == direction,
            "setup_tf_direction": m15["direction"] == direction,
            "entry_tf_confirmation": confirmation_ok,
            "not_overextended": 10 < m15["bb_position"] < 90,
            "trend_strength": m15["adx"] >= 14 or m5["adx"] >= 18,
            "volume_expansion": volume_ok,
        }
        blockers = [k for k, v in mandatory_checks.items() if not v]
        if not blockers and name != "MOMENTUM_WATCH":
            lifecycle = "VALID_ENTRY"
        elif len(blockers) <= 2 and (near_pullback or breakout_long or breakout_short):
            lifecycle = "WAITING_CONFIRMATION"
        elif direction in ("LONG", "SHORT") and (m15["direction"] == direction or m1h["direction"] == direction):
            lifecycle = "APPROACHING_ENTRY_ZONE"
        else:
            lifecycle = "NO_SETUP"
        return {
            "name": name,
            "lifecycle": lifecycle,
            "entry_zone": {
                "low": round_price(min(current, pullback_ref) * 0.998),
                "high": round_price(max(current, pullback_ref) * 1.002),
                "reference": "15m EMA21 / breakout structure",
            },
            "mandatory_checks": mandatory_checks,
            "blockers": blockers,
        }

    def score_breakdown(
        self,
        direction: str,
        score: float,
        rr: float,
        tf_agreement: int,
        ticker: Dict[str, Any],
        funding_penalty: float,
        context_penalty: float,
        m5: Dict[str, Any],
        m15: Dict[str, Any],
        m1h: Dict[str, Any],
    ) -> Dict[str, Any]:
        trend = 20 if tf_agreement >= 3 else 14 if tf_agreement == 2 else 6
        entry = 20 if m5["direction"] == direction and m15["direction"] == direction else 12 if m15["direction"] == direction else 5
        momentum = 15 if ((direction == "LONG" and m15["macd_hist"] > 0) or (direction == "SHORT" and m15["macd_hist"] < 0)) else 8
        volume = 15 if max(m5["volume_z"], m15["volume_z"]) >= 1.3 else 9 if max(m5["volume_z"], m15["volume_z"]) >= 0.5 else 3
        risk = 15 if rr >= 2 else 10 if rr >= self.settings.min_rr else 4
        market = 10 if not context_penalty else 6
        timing = 5 if 15 < m15["bb_position"] < 85 else 2
        total = clamp(trend + entry + momentum + volume + risk + market + timing - funding_penalty, 0, 100)
        return {
            "trend_alignment": trend,
            "entry_zone_quality": entry,
            "momentum": momentum,
            "volume_confirmation": volume,
            "risk_reward": risk,
            "market_context": market,
            "timing": timing,
            "computed_total": round(total, 2),
            "engine_score": round(score, 2),
        }

    def analyze_symbol(
        self,
        asset: Dict[str, Any],
        binance_symbol: str,
        ticker: Dict[str, Any],
        funding_rate: float,
        market_context: Dict[str, Any],
        crypto_bubbles: Dict[str, Any],
    ) -> Dict[str, Any]:
        candles_5m = self.api.fetch_klines(binance_symbol, "5m", self.settings.kline_limit)
        time.sleep(0.05 + random.random() * 0.03)
        candles_15m = self.api.fetch_klines(binance_symbol, "15m", self.settings.kline_limit)
        time.sleep(0.05 + random.random() * 0.03)
        candles_1h = self.api.fetch_klines(binance_symbol, "1h", self.settings.kline_limit)

        m5 = timeframe_metrics(candles_5m, "5m")
        m15 = timeframe_metrics(candles_15m, "15m")
        m1h = timeframe_metrics(candles_1h, "1h")
        current = safe_float(ticker.get("lastPrice"), m5["close"])
        if current <= 0:
            current = m5["close"]

        long_total = 0.35 * m5["long_score"] + 0.40 * m15["long_score"] + 0.25 * m1h["long_score"]
        short_total = 0.35 * m5["short_score"] + 0.40 * m15["short_score"] + 0.25 * m1h["short_score"]
        direction = "NEUTRAL"
        directional_edge = abs(long_total - short_total)
        if long_total >= short_total + 7:
            direction = "LONG"
        elif short_total >= long_total + 7:
            direction = "SHORT"

        tf_agreement = sum(1 for m in (m5, m15, m1h) if m["direction"] == direction)
        base_score = 50 + directional_edge * 0.70 + tf_agreement * 4.5
        liquidity_bonus = 4 if safe_float(ticker.get("quoteVolume")) >= self.settings.min_quote_volume_usd * 3 else 0
        volume_bonus = clamp(max(m5["volume_z"], m15["volume_z"]) * 2.2, 0, 8)
        funding_penalty = 0.0
        if direction == "LONG" and funding_rate > 0.00035:
            funding_penalty = 6
        elif direction == "SHORT" and funding_rate < -0.00035:
            funding_penalty = 6

        context_penalty = 0.0
        if market_context.get("risk_mode") == "RISK_OFF" and direction == "LONG":
            context_penalty += 5
        if market_context.get("risk_mode") == "RISK_ON" and direction == "SHORT":
            context_penalty += 3

        score = clamp(base_score + liquidity_bonus + volume_bonus - funding_penalty - context_penalty, 0, 100)

        atr15 = max(m15["atr"], current * 0.0025)
        if direction == "LONG":
            structural_sl = min(m15["recent_low"], current - atr15 * 1.15)
            raw_sl = structural_sl
            min_sl = current * (1 - 0.0035)
            max_sl = current * (1 - 0.028)
            stop = clamp(raw_sl, max_sl, min_sl)
            stop_distance = current - stop
            tp1 = current + stop_distance * 1.15
            tp2 = current + stop_distance * 2.1
        elif direction == "SHORT":
            structural_sl = max(m15["recent_high"], current + atr15 * 1.15)
            raw_sl = structural_sl
            min_sl = current * (1 + 0.0035)
            max_sl = current * (1 + 0.028)
            stop = clamp(raw_sl, min_sl, max_sl)
            stop_distance = stop - current
            tp1 = current - stop_distance * 1.15
            tp2 = current - stop_distance * 2.1
        else:
            stop = current
            stop_distance = 0.0
            tp1 = current
            tp2 = current

        rr = 2.1 if stop_distance > 0 else 0.0
        playbook = self.setup_playbook(direction, m5, m15, m1h, current)
        scorecard = self.score_breakdown(
            direction, score, rr, tf_agreement, ticker, funding_penalty, context_penalty, m5, m15, m1h
        )
        all_reasons = []
        for m in (m5, m15, m1h):
            all_reasons.extend(m["reasons"])
        if funding_penalty:
            all_reasons.append(f"Funding penalty {funding_rate:.5f}")
        if context_penalty:
            all_reasons.append(f"Market context penalty {market_context.get('risk_mode')}")

        pre_pump_score = clamp(
            35
            + max(m5["volume_z"], 0) * 13
            + (12 if m5["bb_position"] > 60 and direction == "LONG" else 0)
            + (12 if m5["bb_position"] < 40 and direction == "SHORT" else 0)
            + (10 if tf_agreement >= 2 else 0),
            0,
            100,
        )

        status = playbook["lifecycle"]
        enough_confirmation = tf_agreement >= 3 or (tf_agreement >= 2 and score >= self.settings.strong_signal_score)
        trend_quality_ok = (m15["adx"] >= 16 or m5["adx"] >= 18) and m15["adx"] >= 12
        if (
            direction in ("LONG", "SHORT")
            and score >= self.settings.min_signal_score
            and enough_confirmation
            and trend_quality_ok
            and rr >= self.settings.min_rr
            and not playbook["blockers"]
        ):
            status = "SIGNAL"
        elif direction in ("LONG", "SHORT") and score >= self.settings.ready_score and rr >= self.settings.min_rr:
            status = "WAITING_CONFIRMATION"
        elif direction in ("LONG", "SHORT") and score >= self.settings.watch_score:
            status = "WATCHING"
        if safe_float(ticker.get("quoteVolume")) < self.settings.min_quote_volume_usd:
            status = "LOW_LIQUIDITY"
        if m15["adx"] < 14 and m5["adx"] < 14:
            status = "RANGE_FILTERED"

        result = {
            "scan_time": now_iso(),
            "rank": asset.get("rank"),
            "asset_symbol": asset.get("symbol"),
            "asset_name": asset.get("name"),
            "universe_source": asset.get("source"),
            "binance_symbol": binance_symbol,
            "status": status,
            "direction": direction,
            "score": round(score, 2),
            "grade": "HIGH" if score >= self.settings.strong_signal_score else "MEDIUM" if score >= self.settings.min_signal_score else "LOW",
            "entry": round_price(current),
            "stop_loss": round_price(stop),
            "tp1": round_price(tp1),
            "tp2": round_price(tp2),
            "rr": round(rr, 2),
            "tf_agreement": tf_agreement,
            "funding_rate": funding_rate,
            "quote_volume": safe_float(ticker.get("quoteVolume")),
            "change_24h_pct": safe_float(ticker.get("priceChangePercent")),
            "market_cap": safe_float(asset.get("market_cap")),
            "top100_volume_24h": safe_float(asset.get("volume_24h")),
            "pre_pump_score": round(pre_pump_score, 2),
            "setup": playbook["name"],
            "lifecycle": status,
            "entry_zone": playbook["entry_zone"],
            "mandatory_checks": playbook["mandatory_checks"],
            "blockers": playbook["blockers"],
            "scorecard": scorecard,
            "decision_trace": [
                "1 data_loaded",
                f"2 market_regime={market_context.get('risk_mode', 'UNKNOWN')}",
                f"3 setup={playbook['name']}",
                f"4 lifecycle={status}",
                f"5 blockers={','.join(playbook['blockers']) or 'none'}",
            ],
            "risk_mode": market_context.get("risk_mode", "UNKNOWN"),
            "crypto_bubbles_overlay": crypto_bubbles or {},
            "metrics": {"5m": m5, "15m": m15, "1h": m1h},
            "reasons": all_reasons[:12],
        }
        return self.apply_persistence(result)

    def apply_persistence(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        if candidate.get("status") != "SIGNAL":
            return candidate
        symbol = candidate["binance_symbol"]
        with self.store.lock:
            locks = self.store.state.setdefault("active_signal_locks", {})
            existing = locks.get(symbol)
            if not existing:
                locks[symbol] = {
                    "created_at": now_iso(),
                    "direction": candidate["direction"],
                    "entry": candidate["entry"],
                    "stop_loss": candidate["stop_loss"],
                    "tp1": candidate["tp1"],
                    "tp2": candidate["tp2"],
                    "score": candidate["score"],
                    "status": "ACTIVE",
                }
                candidate["persistence"] = "new_lock"
                self.store.save()
                return candidate

            created = datetime.fromisoformat(existing.get("created_at").replace("Z", "+00:00"))
            age_minutes = (now_utc() - created).total_seconds() / 60
            if existing.get("direction") == candidate.get("direction"):
                candidate["entry"] = existing["entry"]
                candidate["stop_loss"] = existing["stop_loss"]
                candidate["tp1"] = existing["tp1"]
                candidate["tp2"] = existing["tp2"]
                existing["score"] = candidate["score"]
                existing["last_seen_at"] = now_iso()
                candidate["persistence"] = "entry_sl_tp_preserved"
                self.store.save()
                return candidate

            strong_flip = candidate["score"] >= self.settings.strong_flip_score and candidate["tf_agreement"] >= 3
            cooldown_done = age_minutes >= self.settings.flip_cooldown_minutes
            if strong_flip or cooldown_done:
                locks[symbol] = {
                    "created_at": now_iso(),
                    "direction": candidate["direction"],
                    "entry": candidate["entry"],
                    "stop_loss": candidate["stop_loss"],
                    "tp1": candidate["tp1"],
                    "tp2": candidate["tp2"],
                    "score": candidate["score"],
                    "status": "ACTIVE",
                    "replaced_previous": existing,
                }
                candidate["persistence"] = "flip_allowed_strong_or_cooldown"
                self.store.event(
                    "WARN",
                    f"{symbol} signal flipped after controls",
                    {"old": existing.get("direction"), "new": candidate.get("direction"), "age_minutes": age_minutes},
                )
                self.store.save()
                return candidate

            candidate["status"] = "BLOCKED_FLIP_COOLDOWN"
            candidate["blocked_reason"] = (
                f"Existing {existing.get('direction')} lock is only {age_minutes:.1f} minutes old; "
                f"needs {self.settings.flip_cooldown_minutes}m or score >= {self.settings.strong_flip_score} with 3/3 TF."
            )
            candidate["entry"] = existing["entry"]
            candidate["stop_loss"] = existing["stop_loss"]
            candidate["tp1"] = existing["tp1"]
            candidate["tp2"] = existing["tp2"]
            self.store.save()
            return candidate


class PaperBroker:
    def __init__(self, settings: Settings, store: StateStore):
        self.settings = settings
        self.store = store
        self.db_lock = threading.RLock()
        self._init_db()

    def _init_db(self) -> None:
        DB_FILE.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_FILE) as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS closed_trades (
                    id TEXT PRIMARY KEY,
                    opened_at TEXT,
                    closed_at TEXT,
                    symbol TEXT,
                    direction TEXT,
                    entry REAL,
                    exit_price REAL,
                    qty REAL,
                    pnl REAL,
                    pnl_pct REAL,
                    reason TEXT,
                    stop_analysis TEXT
                )
                """
            )

    def enable(self, duration_hours: float) -> None:
        with self.store.lock:
            paper = self.store.state.setdefault("paper", {})
            paper["enabled"] = True
            paper["started_at"] = now_iso()
            paper["ends_at"] = datetime.fromtimestamp(time.time() + duration_hours * 3600, timezone.utc).isoformat(timespec="seconds")
            paper["initial_equity"] = self.settings.initial_equity
            paper.setdefault("realized_pnl", 0.0)
            paper.setdefault("unrealized_pnl", 0.0)
            paper["equity"] = self.settings.initial_equity + paper.get("realized_pnl", 0.0)
            paper.setdefault("active_positions", [])
            paper.setdefault("closed_trades", [])
            paper["stats"] = self.stats(paper.get("closed_trades", []), paper.get("active_positions", []))
            self.store.save()
        self.store.event("INFO", f"Paper trading enabled for {duration_hours} hours")

    def disable(self) -> None:
        with self.store.lock:
            self.store.state.setdefault("paper", {})["enabled"] = False
            self.store.save()
        self.store.event("INFO", "Paper trading stopped")

    def is_enabled(self) -> bool:
        with self.store.lock:
            paper = self.store.state.setdefault("paper", {})
            if not paper.get("enabled"):
                return False
            ends_at = paper.get("ends_at")
            if ends_at and now_utc() >= datetime.fromisoformat(ends_at.replace("Z", "+00:00")):
                paper["enabled"] = False
                self.store.save()
                return False
            return True

    def _active_risk(self, active_positions: List[Dict[str, Any]]) -> float:
        total = 0.0
        for pos in active_positions:
            total += safe_float(pos.get("risk_usdt"))
        return total

    def update_positions(self, ticker_map: Dict[str, Dict[str, Any]]) -> None:
        with self.store.lock:
            paper = self.store.state.setdefault("paper", {})
            active = paper.setdefault("active_positions", [])
            closed = paper.setdefault("closed_trades", [])
            realized = safe_float(paper.get("realized_pnl"))
            unrealized = 0.0
            still_active: List[Dict[str, Any]] = []
            for pos in active:
                symbol = pos["symbol"]
                ticker = ticker_map.get(symbol, {})
                current = safe_float(ticker.get("lastPrice"), pos["entry"])
                direction = pos["direction"]
                qty = safe_float(pos["qty"])
                entry = safe_float(pos["entry"])
                stop = safe_float(pos["stop_loss"])
                tp1 = safe_float(pos["tp1"])
                tp2 = safe_float(pos["tp2"])
                if direction == "LONG":
                    pnl = (current - entry) * qty
                    stop_hit = current <= stop
                    tp1_hit = current >= tp1
                    tp2_hit = current >= tp2
                else:
                    pnl = (entry - current) * qty
                    stop_hit = current >= stop
                    tp1_hit = current <= tp1
                    tp2_hit = current <= tp2

                close_reason = None
                exit_price = current
                if stop_hit:
                    close_reason = "SL_HIT"
                    exit_price = stop
                elif tp2_hit:
                    close_reason = "TP2_HIT"
                    exit_price = tp2
                elif tp1_hit and not pos.get("tp1_taken"):
                    partial_qty = qty * 0.5
                    partial_pnl = ((tp1 - entry) if direction == "LONG" else (entry - tp1)) * partial_qty
                    realized += partial_pnl
                    pos["qty"] = qty - partial_qty
                    pos["tp1_taken"] = True
                    pos["realized_partial_pnl"] = round(safe_float(pos.get("realized_partial_pnl")) + partial_pnl, 4)
                    pos["stop_loss"] = entry
                    pos["events"].append({"time": now_iso(), "type": "TP1_PARTIAL", "pnl": partial_pnl})
                    still_active.append(pos)
                    unrealized += pnl * 0.5
                    continue

                if close_reason:
                    final_qty = safe_float(pos.get("qty"))
                    final_pnl = ((exit_price - entry) if direction == "LONG" else (entry - exit_price)) * final_qty
                    final_pnl += safe_float(pos.get("realized_partial_pnl"))
                    realized += final_pnl
                    closed_trade = {
                        "id": pos["id"],
                        "opened_at": pos["opened_at"],
                        "closed_at": now_iso(),
                        "symbol": symbol,
                        "direction": direction,
                        "entry": entry,
                        "exit_price": round_price(exit_price),
                        "qty": final_qty,
                        "pnl": round(final_pnl, 4),
                        "pnl_pct": round(final_pnl / self.settings.initial_equity * 100, 4),
                        "reason": close_reason,
                        "stop_analysis": self.stop_analysis(pos, close_reason, ticker),
                    }
                    closed.insert(0, closed_trade)
                    self._record_closed_trade(closed_trade)
                    self.store.event(
                        "WARN" if close_reason == "SL_HIT" else "INFO",
                        f"{symbol} {direction} closed: {close_reason} pnl={final_pnl:.2f}",
                        closed_trade,
                    )
                else:
                    pos["current_price"] = round_price(current)
                    pos["unrealized_pnl"] = round(pnl, 4)
                    still_active.append(pos)
                    unrealized += pnl

            del closed[250:]
            paper["active_positions"] = still_active
            paper["closed_trades"] = closed
            paper["realized_pnl"] = round(realized, 4)
            paper["unrealized_pnl"] = round(unrealized, 4)
            paper["equity"] = round(self.settings.initial_equity + realized + unrealized, 4)
            paper["stats"] = self.stats(closed, still_active)
            self.store.save()

    def _record_closed_trade(self, trade: Dict[str, Any]) -> None:
        with self.db_lock, sqlite3.connect(DB_FILE) as con:
            con.execute(
                """
                INSERT OR REPLACE INTO closed_trades
                (id, opened_at, closed_at, symbol, direction, entry, exit_price, qty, pnl, pnl_pct, reason, stop_analysis)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade["id"],
                    trade["opened_at"],
                    trade["closed_at"],
                    trade["symbol"],
                    trade["direction"],
                    trade["entry"],
                    trade["exit_price"],
                    trade["qty"],
                    trade["pnl"],
                    trade["pnl_pct"],
                    trade["reason"],
                    trade["stop_analysis"],
                ),
            )

    def stop_analysis(self, pos: Dict[str, Any], close_reason: str, ticker: Dict[str, Any]) -> str:
        if close_reason != "SL_HIT":
            return "Target hit or planned exit; no stop-loss failure to diagnose."
        change_24h = safe_float(ticker.get("priceChangePercent"))
        quote_volume = safe_float(ticker.get("quoteVolume"))
        notes = []
        if abs(change_24h) > 8:
            notes.append("large 24h move increased liquidation/mean-reversion risk")
        if quote_volume < self.settings.min_quote_volume_usd * 1.5:
            notes.append("liquidity was weak relative to filter threshold")
        if pos.get("score", 0) < self.settings.strong_signal_score:
            notes.append("signal was medium score, not high conviction")
        if not notes:
            notes.append("normal invalidation: price reached the predefined structural stop")
        return "; ".join(notes)

    def can_open(self, signal: Dict[str, Any]) -> Tuple[bool, str]:
        if not self.is_enabled():
            return False, "paper trading disabled"
        with self.store.lock:
            paper = self.store.state.setdefault("paper", {})
            active = paper.setdefault("active_positions", [])
            closed = paper.setdefault("closed_trades", [])
            equity = safe_float(paper.get("equity"), self.settings.initial_equity)
            realized = safe_float(paper.get("realized_pnl"))
            daily_loss_limit = -self.settings.initial_equity * self.settings.max_daily_loss_pct / 100
            if realized <= daily_loss_limit:
                return False, "max daily loss reached"
            if len(active) >= self.settings.max_open_positions:
                return False, "max open positions reached"
            if any(p["symbol"] == signal["binance_symbol"] for p in active):
                return False, "symbol already active"
            if signal.get("status") != "SIGNAL":
                return False, f"status {signal.get('status')}"
            if signal.get("score", 0) < self.settings.min_signal_score:
                return False, "score below threshold"
            open_risk = self._active_risk(active)
            max_open_risk = self.settings.initial_equity * self.settings.max_total_open_risk_pct / 100
            if open_risk >= max_open_risk:
                return False, "max total open risk reached"
            recently_closed_sl = [
                t
                for t in closed[:30]
                if t.get("symbol") == signal["binance_symbol"] and t.get("reason") == "SL_HIT"
            ]
            if recently_closed_sl:
                return False, "cooldown after recent SL"
            if equity <= self.settings.initial_equity * 0.94:
                return False, "equity drawdown safety lock"
        return True, "ok"

    def open_from_signals(self, signals: List[Dict[str, Any]]) -> None:
        if not self.is_enabled():
            return
        candidates = [s for s in signals if s.get("status") == "SIGNAL"]
        candidates.sort(key=lambda s: (s.get("score", 0), s.get("tf_agreement", 0), s.get("quote_volume", 0)), reverse=True)
        for signal in candidates:
            allowed, reason = self.can_open(signal)
            if not allowed:
                signal["paper_decision"] = reason
                continue
            self._open(signal)

    def _open(self, signal: Dict[str, Any]) -> None:
        with self.store.lock:
            paper = self.store.state.setdefault("paper", {})
            active = paper.setdefault("active_positions", [])
            equity = safe_float(paper.get("equity"), self.settings.initial_equity)
            entry = safe_float(signal["entry"])
            stop = safe_float(signal["stop_loss"])
            distance = abs(entry - stop)
            if entry <= 0 or distance <= 0:
                return
            risk_usdt = min(
                equity * self.settings.risk_per_trade_pct / 100,
                self.settings.initial_equity * 0.006,
            )
            max_notional = equity * self.settings.max_leverage * self.settings.max_notional_per_trade_pct / 100
            qty = risk_usdt / distance
            notional = qty * entry
            if notional > max_notional:
                qty = max_notional / entry
                notional = qty * entry
                risk_usdt = qty * distance
            if risk_usdt < 2:
                return
            position = {
                "id": f"{signal['binance_symbol']}_{local_timestamp()}",
                "opened_at": now_iso(),
                "symbol": signal["binance_symbol"],
                "direction": signal["direction"],
                "entry": signal["entry"],
                "stop_loss": signal["stop_loss"],
                "tp1": signal["tp1"],
                "tp2": signal["tp2"],
                "qty": round(qty, 8),
                "notional": round(notional, 4),
                "risk_usdt": round(risk_usdt, 4),
                "score": signal["score"],
                "tf_agreement": signal["tf_agreement"],
                "reasons": signal.get("reasons", []),
                "events": [{"time": now_iso(), "type": "OPEN"}],
            }
            active.append(position)
            paper["stats"] = self.stats(paper.get("closed_trades", []), active)
            self.store.event("INFO", f"Paper opened {position['symbol']} {position['direction']}", position)
            self.store.save()

    def stats(self, closed: List[Dict[str, Any]], active: List[Dict[str, Any]]) -> Dict[str, Any]:
        wins = [t for t in closed if safe_float(t.get("pnl")) > 0]
        losses = [t for t in closed if safe_float(t.get("pnl")) <= 0]
        total = len(closed)
        realized = sum(safe_float(t.get("pnl")) for t in closed)
        return {
            "closed_trades": total,
            "wins": len(wins),
            "losses": len(losses),
            "win_rate_pct": round(len(wins) / total * 100, 2) if total else 0.0,
            "realized_pnl": round(realized, 4),
            "open_positions": len(active),
            "unique_symbols_traded": len({t.get("symbol") for t in closed} | {p.get("symbol") for p in active}),
        }


class TelegramNotifier:
    def __init__(self, settings: Settings, store: StateStore):
        self.settings = settings
        self.store = store

    def send(self, text: str) -> None:
        if not self.settings.telegram_bot_token or not self.settings.telegram_chat_id:
            return
        try:
            data = urllib.parse.urlencode(
                {
                    "chat_id": self.settings.telegram_chat_id,
                    "text": text[:3900],
                    "parse_mode": "HTML",
                    "disable_web_page_preview": "true",
                }
            ).encode("utf-8")
            url = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/sendMessage"
            req = urllib.request.Request(url, data=data, method="POST")
            with urllib.request.urlopen(req, timeout=12) as resp:
                resp.read()
        except Exception as exc:
            self.store.event("WARN", "Telegram send failed", {"error": str(exc)})


class SignalService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = StateStore(STATE_FILE)
        self.api = ApiClient(settings, self.store)
        self.engine = SignalEngine(self.api, settings, self.store)
        self.broker = PaperBroker(settings, self.store)
        self.telegram = TelegramNotifier(settings, self.store)
        self.scan_lock = threading.RLock()
        self.background_thread: Optional[threading.Thread] = None
        self.background_stop = threading.Event()

    def start_background_scanner(self) -> None:
        if self.background_thread and self.background_thread.is_alive():
            return
        self.background_stop.clear()
        self.background_thread = threading.Thread(target=self._scanner_loop, daemon=True)
        self.background_thread.start()

    def _scanner_loop(self) -> None:
        while not self.background_stop.is_set():
            try:
                self.run_scan("background")
            except Exception as exc:
                self.store.update(status="ERROR", last_error=str(exc))
                self.store.event("ERROR", "Background scan failed", {"error": str(exc), "trace": traceback.format_exc()})
            self.background_stop.wait(self.settings.scan_interval_seconds)

    def stop_background_scanner(self) -> None:
        self.background_stop.set()

    def start_paper(self, hours: Optional[float] = None) -> Dict[str, Any]:
        self.broker.enable(hours or self.settings.paper_target_hours)
        self.telegram.send(
            f"Paper trading started\nEquity: {self.settings.initial_equity} USDT\nDuration: {hours or self.settings.paper_target_hours}h"
        )
        return self.store.snapshot()["paper"]

    def stop_paper(self) -> Dict[str, Any]:
        self.broker.disable()
        self.telegram.send("Paper trading stopped")
        return self.store.snapshot()["paper"]

    def record_feedback(self, body: Dict[str, Any]) -> Dict[str, Any]:
        item = {
            "time": now_iso(),
            "symbol": str(body.get("symbol", "")).upper(),
            "verdict": str(body.get("verdict", "review")),
            "note": str(body.get("note", ""))[:1000],
            "signal": body.get("signal", {}),
        }
        with self.store.lock:
            feedback = self.store.state.setdefault("feedback", [])
            feedback.insert(0, item)
            del feedback[500:]
            self.store.save()
        self.store.event("INFO", "Signal feedback recorded", {"symbol": item["symbol"], "verdict": item["verdict"]})
        return {"ok": True, "feedback": item}

    def playbook(self) -> Dict[str, Any]:
        return {
            "cycle": [
                "load_data",
                "detect_market_regime",
                "score_multi_timeframe_setup",
                "classify_lifecycle",
                "validate_risk_reward",
                "emit_signal_or_wait",
                "track_outcome",
                "record_feedback",
            ],
            "statuses": {
                "NO_SETUP": "No actionable structure.",
                "WATCHING": "Setup exists but not near/confirmed enough.",
                "APPROACHING_ENTRY_ZONE": "Price is moving toward a planned zone.",
                "WAITING_CONFIRMATION": "Zone is relevant; wait for 5m/15m confirmation.",
                "VALID_ENTRY": "All playbook checks passed before final signal filters.",
                "SIGNAL": "Tradable paper-first signal after risk and persistence filters.",
                "LOW_LIQUIDITY": "Rejected by volume gate.",
                "RANGE_FILTERED": "Rejected by ADX/range gate.",
            },
            "mandatory_checks": [
                "higher_tf_direction",
                "setup_tf_direction",
                "entry_tf_confirmation",
                "not_overextended",
                "trend_strength",
                "volume_expansion",
            ],
        }

    def run_scan(self, source: str = "manual") -> Dict[str, Any]:
        if not self.scan_lock.acquire(blocking=False):
            return {"status": "BUSY", "message": "scan already running"}
        started = time.time()
        try:
            self.store.update(status="SCANNING", last_error=None)
            market_context = self.api.fetch_global_context()
            top100 = self.api.fetch_top100_universe()
            try:
                exchange_info = self.api.fetch_exchange_info()
                symbol_map = self.engine.build_symbol_map(exchange_info)
                tickers = self.api.fetch_24h_tickers()
                funding = self.api.fetch_funding_map()
                bubbles = self.api.fetch_crypto_bubbles_overlay()
            except Exception as exc:
                rows = []
                for asset in top100:
                    rows.append(
                        {
                            "scan_time": now_iso(),
                            "rank": asset.get("rank"),
                            "asset_symbol": asset.get("symbol"),
                            "asset_name": asset.get("name"),
                            "universe_source": asset.get("source"),
                            "binance_symbol": "",
                            "status": "BINANCE_UNREACHABLE",
                            "direction": "NONE",
                            "score": 0,
                            "grade": "NONE",
                            "entry": 0,
                            "stop_loss": 0,
                            "tp1": 0,
                            "tp2": 0,
                            "rr": 0,
                            "tf_agreement": 0,
                            "funding_rate": 0,
                            "quote_volume": 0,
                            "change_24h_pct": safe_float(asset.get("percent_change_24h")),
                            "market_cap": safe_float(asset.get("market_cap")),
                            "top100_volume_24h": safe_float(asset.get("volume_24h")),
                            "pre_pump_score": 0,
                            "risk_mode": market_context.get("risk_mode", "UNKNOWN"),
                            "error": str(exc),
                            "reasons": ["Top 100 source is real, but Binance Futures endpoint is unreachable from this machine/network."],
                        }
                    )
                report = self.write_reports(rows, market_context, source, started)
                with self.store.lock:
                    self.store.state["status"] = "DATA_SOURCE_LIMITED"
                    self.store.state["last_scan_at"] = now_iso()
                    self.store.state["last_error"] = str(exc)
                    self.store.state["last_report"] = report
                    self.store.state["signals"] = []
                    self.store.state["universe"] = rows
                    self.store.state["market_context"] = market_context
                    self.store.save()
                self.store.event("ERROR", "Binance Futures unreachable; limited Top 100 report written", {"error": str(exc), "report": report})
                return {"status": "DATA_SOURCE_LIMITED", "signals": 0, "report": report, "elapsed_seconds": round(time.time() - started, 2), "error": str(exc)}
            self.broker.update_positions(tickers)

            rows: List[Dict[str, Any]] = []
            futures: Dict[Any, Dict[str, Any]] = {}
            with ThreadPoolExecutor(max_workers=self.settings.max_workers) as executor:
                for asset in top100:
                    symbol = self.engine.resolve_symbol(str(asset.get("symbol", "")), symbol_map)
                    base_row = {
                        "scan_time": now_iso(),
                        "rank": asset.get("rank"),
                        "asset_symbol": asset.get("symbol"),
                        "asset_name": asset.get("name"),
                        "universe_source": asset.get("source"),
                        "binance_symbol": symbol or "",
                        "status": "NOT_ON_BINANCE_USDM_FUTURES",
                        "direction": "NONE",
                        "score": 0,
                        "grade": "NONE",
                        "entry": 0,
                        "stop_loss": 0,
                        "tp1": 0,
                        "tp2": 0,
                        "rr": 0,
                        "tf_agreement": 0,
                        "funding_rate": 0,
                        "quote_volume": 0,
                        "change_24h_pct": safe_float(asset.get("percent_change_24h")),
                        "market_cap": safe_float(asset.get("market_cap")),
                        "top100_volume_24h": safe_float(asset.get("volume_24h")),
                        "pre_pump_score": 0,
                        "risk_mode": market_context.get("risk_mode", "UNKNOWN"),
                        "reasons": [],
                    }
                    if not symbol or symbol not in tickers:
                        rows.append(base_row)
                        continue
                    ticker = tickers[symbol]
                    if safe_float(ticker.get("quoteVolume")) < self.settings.min_quote_volume_usd:
                        base_row.update(
                            {
                                "status": "LOW_LIQUIDITY",
                                "quote_volume": safe_float(ticker.get("quoteVolume")),
                                "change_24h_pct": safe_float(ticker.get("priceChangePercent")),
                            }
                        )
                        rows.append(base_row)
                        continue
                    bubble = bubbles.get(str(asset.get("symbol", "")).upper(), {})
                    fut = executor.submit(
                        self.engine.analyze_symbol,
                        asset,
                        symbol,
                        ticker,
                        funding.get(symbol, 0.0),
                        market_context,
                        bubble,
                    )
                    futures[fut] = base_row

                for fut in as_completed(futures):
                    base = futures[fut]
                    try:
                        rows.append(fut.result())
                    except Exception as exc:
                        base["status"] = "ANALYSIS_ERROR"
                        base["error"] = str(exc)
                        rows.append(base)

            rows.sort(key=lambda r: (int(r.get("rank") or 999), str(r.get("asset_symbol"))))
            signals = [r for r in rows if r.get("status") == "SIGNAL"]
            watchlist = [
                r
                for r in rows
                if r.get("status") in ("WATCHING", "APPROACHING_ENTRY_ZONE", "WAITING_CONFIRMATION", "VALID_ENTRY")
            ]
            watchlist.sort(key=lambda r: (safe_float(r.get("score")), safe_float(r.get("quote_volume"))), reverse=True)
            signals.sort(key=lambda r: (safe_float(r.get("score")), safe_float(r.get("quote_volume"))), reverse=True)
            self.broker.open_from_signals(signals)

            report = self.write_reports(rows, market_context, source, started)
            paper = self.store.snapshot()["paper"]
            with self.store.lock:
                self.store.state["status"] = "ONLINE"
                self.store.state["last_scan_at"] = now_iso()
                self.store.state["last_error"] = None
                self.store.state["last_report"] = report
                self.store.state["signals"] = signals[:40]
                self.store.state["watchlist"] = watchlist[:80]
                self.store.state["universe"] = rows
                self.store.state["market_context"] = market_context
                self.store.state["analysis_cycle"] = {
                    "last_completed_at": now_iso(),
                    "source": source,
                    "rows": len(rows),
                    "signals": len(signals),
                    "watchlist": len(watchlist),
                    "loop_seconds": self.settings.scan_interval_seconds,
                    "steps": self.playbook()["cycle"],
                }
                self.store.state["paper"] = paper
                self.store.save()

            high = [s for s in signals if s.get("grade") == "HIGH"]
            if high:
                msg = "\n".join(
                    [
                        "High signal scan:",
                        *[
                            f"{s['binance_symbol']} {s['direction']} score={s['score']} entry={s['entry']} SL={s['stop_loss']} TP2={s['tp2']}"
                            for s in high[:5]
                        ],
                    ]
                )
                self.telegram.send(msg)
            return {"status": "OK", "signals": len(signals), "report": report, "elapsed_seconds": round(time.time() - started, 2)}
        except Exception as exc:
            self.store.update(status="ERROR", last_error=str(exc))
            self.store.event("ERROR", "Scan failed", {"error": str(exc), "trace": traceback.format_exc()})
            raise
        finally:
            self.scan_lock.release()

    def write_reports(
        self,
        rows: List[Dict[str, Any]],
        market_context: Dict[str, Any],
        source: str,
        started: float,
    ) -> Dict[str, Any]:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        ts = local_timestamp()
        csv_path = REPORTS_DIR / f"{ts}_top100_real_scan.csv"
        json_path = REPORTS_DIR / f"{ts}_top100_real_scan.json"
        final_path = REPORTS_DIR / f"{ts}_paper_status.json"
        fieldnames = [
            "scan_time",
            "rank",
            "asset_symbol",
            "asset_name",
            "universe_source",
            "binance_symbol",
            "status",
            "direction",
            "score",
            "grade",
            "entry",
            "stop_loss",
            "tp1",
            "tp2",
            "rr",
            "tf_agreement",
            "funding_rate",
            "quote_volume",
            "change_24h_pct",
            "market_cap",
            "top100_volume_24h",
            "pre_pump_score",
            "setup",
            "lifecycle",
            "entry_zone",
            "blockers",
            "scorecard",
            "risk_mode",
            "persistence",
            "blocked_reason",
            "error",
            "reasons",
        ]
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                item = dict(row)
                item["reasons"] = " | ".join(row.get("reasons") or [])
                item["blockers"] = " | ".join(row.get("blockers") or [])
                item["entry_zone"] = json.dumps(row.get("entry_zone") or {}, ensure_ascii=False)
                item["scorecard"] = json.dumps(row.get("scorecard") or {}, ensure_ascii=False)
                writer.writerow(item)
        payload = {
            "generated_at": now_iso(),
            "source": source,
            "elapsed_seconds": round(time.time() - started, 2),
            "settings": {
                "initial_equity": self.settings.initial_equity,
                "risk_per_trade_pct": self.settings.risk_per_trade_pct,
                "max_daily_loss_pct": self.settings.max_daily_loss_pct,
                "min_signal_score": self.settings.min_signal_score,
                "min_quote_volume_usd": self.settings.min_quote_volume_usd,
            },
            "market_context": market_context,
            "rows": rows,
            "paper": self.store.snapshot().get("paper", {}),
        }
        atomic_write_json(json_path, payload)
        atomic_write_json(final_path, payload["paper"])
        return {
            "csv": str(csv_path),
            "json": str(json_path),
            "paper": str(final_path),
            "generated_at": payload["generated_at"],
            "row_count": len(rows),
        }


SERVICE = SignalService(Settings())


class AppHandler(BaseHTTPRequestHandler):
    server_version = "HamidSignalPanel/3.0"

    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_json({"ok": True})

    def do_GET(self) -> None:  # noqa: N802
        if self.path in ("/", "/index.html"):
            body = DASHBOARD_FILE.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == "/api/status":
            snap = SERVICE.store.snapshot()
            self._send_json(
                {
                    "status": snap.get("status"),
                    "last_scan_at": snap.get("last_scan_at"),
                    "last_error": snap.get("last_error"),
                    "last_report": snap.get("last_report"),
                    "market_context": snap.get("market_context"),
                    "paper": snap.get("paper"),
                    "settings": {
                        "scan_interval_seconds": SERVICE.settings.scan_interval_seconds,
                        "initial_equity": SERVICE.settings.initial_equity,
                        "risk_per_trade_pct": SERVICE.settings.risk_per_trade_pct,
                        "max_daily_loss_pct": SERVICE.settings.max_daily_loss_pct,
                        "max_open_positions": SERVICE.settings.max_open_positions,
                    },
                }
            )
            return
        if self.path == "/api/signals":
            self._send_json(SERVICE.store.snapshot().get("signals", []))
            return
        if self.path == "/api/watchlist":
            self._send_json(SERVICE.store.snapshot().get("watchlist", []))
            return
        if self.path == "/api/universe":
            self._send_json(SERVICE.store.snapshot().get("universe", []))
            return
        if self.path == "/api/playbook":
            self._send_json(SERVICE.playbook())
            return
        if self.path == "/api/feedback":
            self._send_json(SERVICE.store.snapshot().get("feedback", []))
            return
        if self.path == "/api/events":
            self._send_json(SERVICE.store.snapshot().get("events", []))
            return
        if self.path == "/api/report":
            snap = SERVICE.store.snapshot()
            self._send_json({"last_report": snap.get("last_report"), "paper": snap.get("paper")})
            return
        self._send_json({"error": "not found"}, status=404)

    def do_POST(self) -> None:  # noqa: N802
        body = self._read_json()
        if self.path == "/api/scan-now":
            try:
                self._send_json(SERVICE.run_scan("manual_api"))
            except Exception as exc:
                self._send_json({"status": "ERROR", "error": str(exc)}, status=500)
            return
        if self.path == "/api/start-paper":
            hours = safe_float(body.get("hours"), SERVICE.settings.paper_target_hours)
            self._send_json(SERVICE.start_paper(hours))
            return
        if self.path == "/api/stop-paper":
            self._send_json(SERVICE.stop_paper())
            return
        if self.path == "/api/feedback":
            self._send_json(SERVICE.record_feedback(body))
            return
        self._send_json({"error": "not found"}, status=404)

    def log_message(self, fmt: str, *args: Any) -> None:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        with (LOGS_DIR / "http_access.log").open("a", encoding="utf-8") as f:
            f.write(f"{now_iso()} {self.address_string()} {fmt % args}\n")


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hamid Crypto Futures Signal Panel v3.0")
    parser.add_argument("--host", default=SERVICE.settings.http_host)
    parser.add_argument("--port", type=int, default=SERVICE.settings.http_port)
    parser.add_argument("--once", action="store_true", help="Run one real scan and exit")
    parser.add_argument("--no-auto-scan", action="store_true", help="Do not run background repeated scans in server mode")
    parser.add_argument("--auto-start-paper", action="store_true", help="Start paper trading when server starts")
    parser.add_argument("--paper-hours", type=float, default=SERVICE.settings.paper_target_hours)
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    if args.once:
        result = SERVICE.run_scan("cli_once")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    if args.auto_start_paper:
        SERVICE.start_paper(args.paper_hours)
    if not args.no_auto_scan:
        SERVICE.start_background_scanner()

    server = ThreadingHTTPServer((args.host, args.port), AppHandler)
    url = f"http://{args.host}:{args.port}"
    print(f"Hamid Signal Panel v3.0 running at {url}")
    print("Press Ctrl+C to stop. Reports are written under:", REPORTS_DIR)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        SERVICE.stop_background_scanner()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
