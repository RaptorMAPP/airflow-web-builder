from flask import Blueprint, render_template, request, jsonify
from .services.dag_validator import validate_with_fallback

validator_bp = Blueprint("validator", __name__)

@validator_bp.get("/validate")
def validate_view():
    return render_template("validate.html")

@validator_bp.post("/api/validate")
def validate_api():
    content = request.get_json(silent=True) or {}
    code = content.get("content") or ""
    if not code.strip():
        return jsonify({"valid": False, "errors": ["Contenido vacío."]}), 400
    res = validate_with_fallback(code)
    return jsonify(res), (200 if res.get("valid") else 422)
