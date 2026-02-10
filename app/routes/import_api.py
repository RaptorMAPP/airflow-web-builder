from flask import Blueprint, request, jsonify
import base64
import json
import re
import ast

bp_import = Blueprint("bp_import", __name__)

_SPEC_B64_RE = re.compile(r'__AIRFLOW_WEB_BUILDER_SPEC_B64__\s*=\s*[\'"]([^\'"]+)[\'"]')


@bp_import.post("/api/import-dag")
def import_dag():
    """
    Recibe un .py (multipart file) y devuelve una spec JSON compatible con el builder.

    Estrategia:
      1) Si el .py trae __AIRFLOW_WEB_BUILDER_SPEC_B64__, retorna eso (full fidelity).
      2) Si no, fallback AST best-effort (parcial).
    """
    f = request.files.get("file")
    if not f:
        return "missing file", 400

    raw = f.read()
    try:
        text = raw.decode("utf-8")
    except Exception:
        text = raw.decode("latin-1", errors="replace")

    # 1) Full fidelity: metadata embebida
    m = _SPEC_B64_RE.search(text)
    if m:
        try:
            spec_json = base64.b64decode(m.group(1)).decode("utf-8")
            spec = json.loads(spec_json)
            return jsonify(spec)
        except Exception as e:
            return f"spec metadata decode error: {e}", 400

    # 2) Fallback parcial (para dags antiguos sin metadata)
    try:
        spec = _parse_ast_best_effort(text)
        return jsonify(spec)
    except Exception as e:
        return f"ast parse error: {e}", 400


def _parse_ast_best_effort(py_text: str) -> dict:
    tree = ast.parse(py_text)

    spec = {
        "dag_id": "",
        "description": "",
        "owner": "owner",
        "app": "",
        "subapp": "",
        "retries": 1,
        "retry_minutes": 5,
        "start_date": "",
        "schedule": "@daily",
        "timezone": "UTC",
        "params": {},
        "argsets": {},
        "start_time": "",
        "catchup": False,
        "confirm": False,
        "assets_enabled": False,
        "asset_logic": "OR",
        "asset_inlets": [],
        "tags": [],
        "tasks": [],
        "dependencies": [],
    }

    tasks = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            cls = _call_name(node.value.func)
            if not cls:
                continue
            if not (cls.endswith("Operator") or cls.endswith("Sensor") or cls == "ExternalTaskSensor"):
                continue

            kw = {k.arg: _lit(k.value) for k in (node.value.keywords or []) if k.arg}
            task_id = kw.get("task_id") or _lit(node.targets[0]) or ""
            t = {"type": cls, "task_id": task_id}
            for k in ("bash_command", "command", "sql", "queue", "pool", "pool_slots",
                      "priority_weight", "depends_on_past", "task_concurrency", "params"):
                if k in kw:
                    t[k] = kw[k]
            tasks.append(t)

    spec["tasks"] = tasks
    return spec


def _call_name(func):
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _lit(node):
    try:
        return ast.literal_eval(node)
    except Exception:
        return ""
