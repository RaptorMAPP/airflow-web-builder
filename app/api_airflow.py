import os
from flask import Blueprint, jsonify, request
from .services.airflow_client import AirflowClient

api_airflow_bp = Blueprint("api_airflow", __name__)

def _client():
    url = os.getenv("AIRFLOW_URL", "http://127.0.0.1:8080")
    token = os.getenv("AIRFLOW_TOKEN", "").strip()
    user = os.getenv("AIRFLOW_USERNAME", "")
    pwd = os.getenv("AIRFLOW_PASSWORD", "")
    return AirflowClient(url, username=user, password=pwd, token=token, timeout=12)

@api_airflow_bp.get("/health")
def health():
    r = _client().health()
    return jsonify(r), (200 if r.get("success") else (r.get("status") or 502))

@api_airflow_bp.get("/dags")
def dags():
    limit = int(request.args.get("limit", 100))
    r = _client().list_dags(limit=limit)
    return jsonify(r), (200 if r.get("success") else (r.get("status") or 502))

@api_airflow_bp.get("/dagruns")
def dagruns():
    dag_id = request.args.get("dag_id")
    if not dag_id:
        return jsonify({"success": False, "error": "dag_id requerido"}), 400
    r = _client().list_dag_runs(dag_id=dag_id, limit=int(request.args.get("limit", 25)))
    return jsonify(r), (200 if r.get("success") else (r.get("status") or 502))
