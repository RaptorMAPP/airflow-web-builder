from __future__ import annotations

from typing import Any, Dict, List, Tuple
import re
import ast
import json
import pprint


def json_to_obj(raw: Any, default: Any) -> Any:
    """Parsea raw (str/dict/list) a objeto Python. Si falla, retorna default."""
    if raw is None:
        return default
    if isinstance(raw, (dict, list, int, float, bool)):
        return raw
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return default
        try:
            return json.loads(s)
        except Exception:
            try:
                return ast.literal_eval(s)
            except Exception:
                return default
    return default


def to_py_literal(raw: Any, default: str = "{}") -> str:
    """Convierte raw a literal Python. Útil para dict/list en kwargs."""
    try:
        obj = json_to_obj(raw, None)
        if obj is None:
            return default
        return pprint.pformat(obj, width=100)
    except Exception:
        return default


def ensure_dict(raw: Any) -> Dict[str, Any]:
    obj = json_to_obj(raw, {})
    return obj if isinstance(obj, dict) else {}


def ensure_list(raw: Any) -> List[Any]:
    obj = json_to_obj(raw, [])
    return obj if isinstance(obj, list) else []


def parse_kwargs_json(raw: Any) -> str:
    """Devuelve un literal dict de Python para **kwargs. Si viene vacío, {}."""
    if raw is None or str(raw).strip() == "":
        return "{}"
    try:
        obj = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(obj, dict):
            return "{}"
        return pprint.pformat(obj, width=100)
    except Exception:
        return "{}"


def sanitize_id(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_]", "_", s)
    if not re.match(r"^[A-Za-z_]", s):
        s = f"t_{s}"
    return s or "t_task"


def fmt_bool(val: Any) -> str:
    return "True" if str(val).strip().lower() in ("1", "true", "yes", "y") else "False"


def quote(s: str) -> str:
    return (s or "").replace('"', '\\"')


def shell_quote_token_if_needed(tok: str) -> str:
    """Quote simple para shell cuando hay espacios. No cita wildcards sin necesidad."""
    t = str(tok)
    if t == "":
        return "''"
    if any(ch.isspace() for ch in t) or '"' in t:
        # usamos comillas dobles y escapamos
        return '"' + t.replace('"', '\\"') + '"'
    return t


def argset_to_tokens(argset: Any) -> List[str]:
    """Acepta lista ['-r','-c',...] o dict {'PARM1':'-r',...} y devuelve tokens ordenados."""
    if argset is None:
        return []
    if isinstance(argset, list):
        return [str(x).strip() for x in argset if str(x).strip() != ""]
    if isinstance(argset, dict):
        # ordenar por sufijo numérico si existe (PARM1..)
        def keyfn(k: str) -> Tuple[int, str]:
            m = re.search(r"(\d+)$", str(k))
            return (int(m.group(1)) if m else 10**9, str(k))
        items = sorted(argset.items(), key=lambda kv: keyfn(kv[0]))
        toks: List[str] = []
        for _, v in items:
            sv = str(v).strip()
            if sv != "":
                toks.append(sv)
        return toks
    # si viene como string, tratamos de parsear JSON/literal
    if isinstance(argset, str):
        obj = json_to_obj(argset, None)
        return argset_to_tokens(obj)
    return []


def build_arg_string(tokens: List[str]) -> str:
    return " ".join(shell_quote_token_if_needed(t) for t in (tokens or []))


def apply_args_to_command(base_cmd: str, tokens: List[str], append_raw: str | None = None) -> str:
    base = (base_cmd or "").strip()
    arg_str = build_arg_string(tokens)
    extra = (append_raw or "").strip()
    if arg_str:
        base = f"{base} {arg_str}" if base else arg_str
    if extra:
        base = f"{base} {extra}" if base else extra
    return base


def merge_params(global_params: Any, task_params: Any) -> Dict[str, Any]:
    g = ensure_dict(global_params)
    t = ensure_dict(task_params)
    out = dict(g)
    out.update(t)
    return out


def params_literal(params_dict: Dict[str, Any]) -> str:
    if not params_dict:
        return ""
    return pprint.pformat(params_dict, width=100)


def base_operator_kwargs(t: Dict[str, Any], params_dict: Dict[str, Any] | None = None) -> str:
    """Kwargs comunes BaseOperator: queue, pool, pool_slots, priority_weight, depends_on_past, task_concurrency, params."""
    parts: List[str] = []

    if t.get("queue"):
        parts.append(f'queue="{t["queue"]}"')

    if t.get("pool"):
        parts.append(f'pool="{t["pool"]}"')
        ps = t.get("pool_slots")
        if ps not in (None, "", "0", 0, False):
            try:
                ps_int = int(ps)
                if ps_int > 0:
                    parts.append(f"pool_slots={ps_int}")
            except Exception:
                pass

    if t.get("priority_weight") not in (None, "", "0", 0, False):
        try:
            parts.append(f"priority_weight={int(t.get('priority_weight'))}")
        except Exception:
            pass

    if str(t.get("depends_on_past")).lower() in ("true", "1"):
        parts.append("depends_on_past=True")

    if t.get("task_concurrency") not in (None, "", "0", 0, False):
        try:
            parts.append(f"task_concurrency={int(t.get('task_concurrency'))}")
        except Exception:
            pass

    if params_dict:
        parts.append(f"params={pprint.pformat(params_dict, width=100)}")

    # Notificaciones (callbacks) - se inyectan si el DAG definió las funciones
    if str(t.get("notify_on_success")).lower() in ("true", "1") or t.get("notify_on_success") is True:
        parts.append("on_success_callback=_notify_task_success")
    if str(t.get("notify_on_failure")).lower() in ("true", "1") or t.get("notify_on_failure") is True:
        parts.append("on_failure_callback=_notify_task_failure")

    # Duración
    try:
        sla_min = int(t.get("sla_minutes") or 0)
    except Exception:
        sla_min = 0
    if sla_min > 0:
        parts.append(f"sla=timedelta(minutes={sla_min})")

    try:
        et_min = int(t.get("execution_timeout_minutes") or 0)
    except Exception:
        et_min = 0
    if et_min > 0:
        parts.append(f"execution_timeout=timedelta(minutes={et_min})")


    # Assets lineage (Airflow 3): inlets/outlets (list o string con \n o ,)
    def _asset_list(v: Any) -> List[str]:
        if v is None or v is False:
            return []
        if isinstance(v, list):
            raw = v
        else:
            raw = re.split(r"[\n,]+", str(v))
        out: List[str] = []
        for x in raw:
            x = str(x).strip()
            if x:
                out.append(x)
        # dedupe preserving order
        seen: set[str] = set()
        ded: List[str] = []
        for u in out:
            if u in seen:
                continue
            seen.add(u)
            ded.append(u)
        return ded

    inlets = _asset_list(t.get("inlets"))
    if inlets:
        parts.append("inlets=[" + ", ".join([f"Asset({json.dumps(u)})" for u in inlets]) + "]")

    outlets = _asset_list(t.get("outlets"))
    if outlets:
        parts.append("outlets=[" + ", ".join([f"Asset({json.dumps(u)})" for u in outlets]) + "]")

    return (", " + ", ".join(parts)) if parts else ""


def py_str(val: Any, default: str = "") -> str:
    """Devuelve un literal Python seguro para strings (soporta saltos de linea)."""
    if val is None:
        return repr(default)
    return repr(str(val))
