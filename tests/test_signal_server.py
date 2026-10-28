import importlib.util
import tempfile
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("signal_server", ROOT / "03_backend_signal_server_20260610_002822_IRST.py")
server = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = server
SPEC.loader.exec_module(server)


class ApiClientTests(unittest.TestCase):
    def test_fetch_klines_excludes_open_candle(self):
        store = server.StateStore(Path(tempfile.mkdtemp()) / "state.json")
        client = server.ApiClient(server.Settings(), store)
        client.request_json = lambda *_args, **_kwargs: [
            [0, "1", "2", ".5", "1.5", "10", 900_000, "15", 2, "5", "7"],
            [900_001, "1.5", "3", "1", "2", "12", 1_100_000, "20", 3, "6", "9"],
        ]
        with patch.object(server.time, "time", return_value=1000):
            candles = client.fetch_klines("BTCUSDT", "5m", 2)
        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0]["close"], 1.5)


class PaperBrokerTests(unittest.TestCase):
    def test_partial_profit_is_not_counted_twice_on_final_close(self):
        with tempfile.TemporaryDirectory() as tmp:
            server.DB_FILE = Path(tmp) / "paper.sqlite3"
            store = server.StateStore(Path(tmp) / "state.json")
            broker = server.PaperBroker(server.Settings(initial_equity=3000), store)
            store.state["paper"].update({
                "realized_pnl": 5.0,
                "active_positions": [{
                    "id": "test", "opened_at": server.now_iso(), "symbol": "BTCUSDT",
                    "direction": "LONG", "entry": 100.0, "stop_loss": 100.0,
                    "tp1": 105.0, "tp2": 110.0, "qty": 1.0, "risk_usdt": 5.0,
                    "score": 90, "tp1_taken": True, "realized_partial_pnl": 5.0,
                    "events": [],
                }],
                "closed_trades": [],
            })
            broker.update_positions({"BTCUSDT": {"lastPrice": "110", "quoteVolume": "100000000"}})
            paper = store.snapshot()["paper"]
            self.assertEqual(paper["realized_pnl"], 15.0)
            self.assertEqual(paper["closed_trades"][0]["pnl"], 15.0)


if __name__ == "__main__":
    unittest.main()
