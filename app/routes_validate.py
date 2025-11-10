# routes_validate.py
from flask import Blueprint, request, jsonify
from validator import validate_dag_code

validate_bp = Blueprint("validate_bp", __name__)

@validate_bp.post("/api/airflow/validate")
def api_validate():
    payload = request.get_json(silent=True) or {}
    code = payload.get("code") or ""
    if not code.strip():
        return jsonify(success=False, error="Falta 'code'"), 400
    try:
        res = validate_dag_code(code)
        return jsonify(res), 200
    except Exception as e:
        # garantizamos JSON
        return jsonify(success=False, error=type(e).__name__, detail=str(e)), 500
