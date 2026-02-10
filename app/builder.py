from flask import Blueprint, render_template, request, jsonify, send_file
from io import BytesIO
import datetime as dt
import re
from .services.dag_generator import build_dag_code
from .services.dag_validator import validate_with_fallback

builder_bp = Blueprint("builder", __name__)

def _slug(s: str) -> str:
    s = (s or "").strip().lower().replace(" ", "_")
    s = re.sub(r"[^a-z0-9_]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "na"

def _get_tag_value(tags, prefix: str) -> str | None:
    """
    tags: ["app:ventas", "subapp:stgo"]
    prefix: "app" -> "ventas"
    """
    if not isinstance(tags, list):
        return None
    p = prefix.lower() + ":"
    for t in tags:
        if isinstance(t, str) and t.lower().startswith(p):
            return t.split(":", 1)[1].strip()

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

    dag_id = _slug(data.get("dag_id") or "dag_generated")
    tags = data.get("tags") or []

    app = _slug(_get_tag_value(tags, "app"))
    filename = f"dag_{dag_id}_{app}.py"

    bio = BytesIO(code.encode("utf-8"))
    bio.seek(0)

    # Flask nuevo
    try:
        resp = send_file(
            bio,
            mimetype="text/x-python",
            as_attachment=True,
            download_name=filename
        )
    except TypeError:
        # Flask antiguo
        resp = send_file(
            bio,
            mimetype="text/x-python",
            as_attachment=True,
            attachment_filename=filename
        )

    return resp