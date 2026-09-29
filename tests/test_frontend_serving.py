"""Regression checks for dashboard serving and the Pages upload layout."""

import contextlib
import http.client
import importlib.util
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "03_backend_signal_server_20260610_002822_IRST.py"
PANEL = ROOT / "04_dashboard_panel_20260610_002822_IRST.html"


class FrontendServingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("signal_server_frontend_test", BACKEND)
        module = importlib.util.module_from_spec(spec)
        import sys
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), module.AppHandler)
        cls.worker = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.worker.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.worker.join(timeout=2)

    def request(self, path):
        conn = http.client.HTTPConnection("127.0.0.1", self.httpd.server_port, timeout=3)
        try:
            conn.request("GET", path)
            response = conn.getresponse()
            return response.status, response.getheader("Content-Type"), response.read()
        finally:
            conn.close()

    def test_dashboard_routes_and_assets(self):
        expected = PANEL.read_bytes()
        for path in ("/", "/index.html"):
            with self.subTest(path=path):
                status, content_type, body = self.request(path)
                self.assertEqual(status, 200)
                self.assertIn("text/html", content_type)
                self.assertEqual(body, expected)
        for path, mime in (("/static/dashboard.css", "text/css"),
                           ("/static/dashboard.js", "text/javascript")):
            with self.subTest(path=path):
                status, content_type, body = self.request(path)
                self.assertEqual(status, 200)
                self.assertIn(mime, content_type)
                self.assertEqual(body, (ROOT / path.lstrip("/")).read_bytes())

    def test_static_path_allowlist(self):
        for path in ("/static/../Future%20signal", "/static/unknown.js"):
            with self.subTest(path=path):
                status, _, body = self.request(path)
                self.assertEqual(status, 404)
                self.assertNotIn(b"Future signal", body)

    def test_pages_artifact_entry_point(self):
        workflow = (ROOT / ".github/workflows/static.yml").read_text()
        self.assertIn("path: '.'", workflow)
        self.assertEqual((ROOT / "index.html").read_bytes(), PANEL.read_bytes())
        html = PANEL.read_text()
        for asset in ("static/dashboard.css", "static/dashboard.js"):
            self.assertIn(f'"{asset}"', html)
            self.assertTrue((ROOT / asset).is_file())


if __name__ == "__main__":
    unittest.main()
