import requests
from requests.auth import HTTPBasicAuth
from typing import Any, Dict, Optional

class AirflowClient:
    """Airflow 3.1 (API v2, /monitor/health) con fallback a v1. Soporta Bearer o Basic."""
    def __init__(self, base_url: str, username: str = "", password: str = "", token: str = "", timeout: int = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.token = token.strip()
        self.auth = None if self.token else (HTTPBasicAuth(username, password) if (username or password) else None)

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        if extra:
            h.update(extra)
        return h

    def _get(self, url: str) -> requests.Response:
        kw = {"headers": self._headers(), "timeout": self.timeout}
        if self.auth:
            kw["auth"] = self.auth
        return requests.get(url, **kw)

    def health(self) -> Dict[str, Any]:
        v2 = f"{self.base_url}/api/v2/monitor/health"
        v1 = f"{self.base_url}/api/v1/monitor/health"
        last = {"success": False, "status": 0, "error": "no response"}
        for url in (v2, v1):
            try:
                r = self._get(url)
                if r.status_code < 500:
                    data = {}
                    if "application/json" in r.headers.get("Content-Type",""):
                        data = r.json()
                    return {"success": r.ok, "status": r.status_code, "data": data, "url": url}
                last = {"success": False, "status": r.status_code, "error": r.text, "url": url}
            except Exception as e:
                last = {"success": False, "status": 0, "error": str(e), "url": url}
        return last

    def list_dags(self, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        v2 = f"{self.base_url}/api/v2/dags?limit={limit}&offset={offset}"
        v1 = f"{self.base_url}/api/v1/dags?limit={limit}&offset={offset}"
        last = {"success": False, "status": 0, "error": "no response"}
        for url in (v2, v1):
            try:
                r = self._get(url)
                if r.status_code < 500:
                    data = {}
                    if "application/json" in r.headers.get("Content-Type",""):
                        data = r.json()
                    return {"success": r.ok, "status": r.status_code, "data": data, "url": url}
                last = {"success": False, "status": r.status_code, "error": r.text, "url": url}
            except Exception as e:
                last = {"success": False, "status": 0, "error": str(e), "url": url}
        return last

    def list_dag_runs(self, dag_id: str, limit: int = 25, order_by: str = "-start_date") -> Dict[str, Any]:
        v2 = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns?limit={limit}&order_by={order_by}"
        v1 = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns?limit={limit}&order_by={order_by}"
        last = {"success": False, "status": 0, "error": "no response"}
        for url in (v2, v1):
            try:
                r = self._get(url)
                if r.status_code < 500:
                    data = {}
                    if "application/json" in r.headers.get("Content-Type",""):
                        data = r.json()
                    return {"success": r.ok, "status": r.status_code, "data": data, "url": url}
                last = {"success": False, "status": r.status_code, "error": r.text, "url": url}
            except Exception as e:
                last = {"success": False, "status": 0, "error": str(e), "url": url}
        return last
