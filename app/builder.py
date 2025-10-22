from flask import Blueprint, render_template, request, jsonify, send_file
from io import BytesIO
import datetime as dt
from .services.dag_generator import build_dag_code
from .services.dag_validator import validate_with_fallback

builder_bp = Blueprint("builder", __name__)

@builder_bp.get("/builder")
def builder_view():
    # valores por defecto del formulario
    today = dt.date.today().isoformat()
    return render_template("builder.html", today=today)

@builder_bp.post("/api/builder/preview")
def builder_preview():
    data = request.get_json(silent=True) or {}
    code = build_dag_code(data)
    # validación inmediata
    val = validate_with_fallback(code)
    return jsonify({"code": code, "validation": val})

@builder_bp.post("/api/builder/download")
def builder_download():
    data = request.get_json(silent=True) or {}
    code = build_dag_code(data)
    dag_id = (data.get("dag_id") or "dag_generated").strip()
    filename = f"{dag_id}.py"
    bio = BytesIO(code.encode("utf-8"))
    return send_file(
        bio,
        mimetype="text/x-python",
        as_attachment=True,
        download_name=filename
    )
