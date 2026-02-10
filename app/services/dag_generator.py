from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional
import json
import re
import base64
from .operator_specs import ARGS_TARGET_FIELD, imports_for_tasks, render_task_line
from .render_utils import (
    argset_to_tokens,
    apply_args_to_command,
    ensure_dict,
    fmt_bool,
    merge_params,
    sanitize_id,
)


_HEADER_IMPORTS = """
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
{extra_imports}{operator_imports}
"""

def _embed_spec_b64_line(spec_in: Dict[str, Any]) -> str:
    """Embebe la spec del builder en el DAG .py para permitir re-import 1:1.

    Se usa base64(JSON) para mantenerlo como una sola línea, fácil de extraer sin ejecutar el código.
    """
    try:
        raw = json.dumps(spec_in or {}, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        raw = "{}"
    b64 = base64.b64encode(raw.encode("utf-8")).decode("ascii")
    # IMPORTANTE: una sola línea para que el importador pueda leerlo con regex.
    return f'__AIRFLOW_WEB_BUILDER_SPEC_B64__ = "{b64}"\n'



def _render_python_callables(tasks: List[Dict[str, Any]]) -> str:
    """Genera stubs para PythonOperator si el builder incluyó body."""
    pieces: List[str] = []
    for t in tasks or []:
        if t.get("type") != "PythonOperator":
            continue
        fn = sanitize_id(t.get("python_callable_name") or f"fn_{t.get('task_id','task')}")
        body = (t.get("python_callable_body") or "print('Hello from PythonOperator')").rstrip()
        # indent seguro
        lines = body.splitlines() or ["pass"]
        indented = "\n".join(["    " + ln for ln in lines])
        pieces.append(f"def {fn}(**context):\n{indented}\n")
    return ("\n".join(pieces) + ("\n" if pieces else ""))


def _render_dependencies(edges: List[Dict[str, Any]], valid_ids: set[str]) -> Tuple[str, List[str]]:
    out: List[str] = []
    warns: List[str] = []
    for e in edges or []:
        u = sanitize_id(e.get("upstream") or "")
        d = sanitize_id(e.get("downstream") or "")
        if not u or not d:
            continue
        if u in valid_ids and d in valid_ids:
            out.append(f"{u} >> {d}")
        else:
            warns.append(f"dependencia ignorada: {u} >> {d} (task inexistente)")
    return ("\n    ".join(out), warns)


def _dedupe_task_ids(tasks: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
    out: List[Dict[str, Any]] = []
    seen: Dict[str, int] = {}
    for t in tasks or []:
        base = sanitize_id(t.get("task_id") or "") or "t_task"
        if base in seen:
            seen[base] += 1
            new_id = f"{base}_{seen[base]}"
        else:
            seen[base] = 1
            new_id = base
        out.append(dict(t, task_id=new_id))
    warnings = [f"task_id duplicado ajustado: {k} -> {k}_2..{v}" for k, v in seen.items() if v > 1]
    return out, warnings


def _normalize_spec(spec: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    spec = dict(spec or {})
    spec["dag_id"] = sanitize_id(spec.get("dag_id") or "dag_generated")
    spec["schedule"] = (spec.get("schedule") or "@daily").strip()
    tz = (spec.get("timezone") or "UTC").strip()
    spec["timezone"] = tz or "UTC"
    tasks, w = _dedupe_task_ids(spec.get("tasks") or [])
    spec["tasks"] = tasks
    # params/argsets
    spec["params"] = ensure_dict(spec.get("params"))
    spec["argsets"] = ensure_dict(spec.get("argsets"))
    spec["task_overrides"] = ensure_dict(spec.get("task_overrides"))

    # tags: acepta lista o string
    tags = spec.get("tags")
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",") if t.strip()]
    elif isinstance(tags, list):
        tags = [str(t).strip() for t in tags if str(t).strip()]
    else:
        tags = []
    spec["tags"] = tags

    # confirm: bool o string truthy
    spec["confirm"] = str(spec.get("confirm", "")).strip().lower() in ("1", "true", "yes", "y")

    return spec, w


def _parse_start_date(spec: Dict[str, Any]) -> Tuple[int, int, int, int, int]:
    """Retorna y,m,d,hh,mm. Usa start_time solo si schedule=@once."""
    y, m, d = 2025, 1, 1
    hh, mm = 0, 0
    sd = (spec.get("start_date") or "2025-01-01").strip()
    try:
        # acepta YYYY-MM-DD o YYYY-MM-DDTHH:MM
        if "T" in sd:
            date_part, time_part = sd.split("T", 1)
            y, m, d = [int(x) for x in date_part.split("-")]
            hh, mm = [int(x) for x in time_part.split(":")[:2]]
        else:
            y, m, d = [int(x) for x in sd.split("-")]
    except Exception:
        y, m, d, hh, mm = 2025, 1, 1, 0, 0

    # si schedule=@once y viene start_time explícito, se usa
    if (spec.get("schedule") or "").strip() == "@once" and spec.get("start_time"):
        try:
            th, tm = str(spec.get("start_time")).split(":")[:2]
            hh, mm = int(th), int(tm)
        except Exception:
            hh, mm = 0, 0

    return y, m, d, hh, mm


def _is_truthy(v: Any) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "y")


def _parse_multi_schedule(schedule: str) -> List[str]:
    # MULTI|cron1||cron2...
    if not isinstance(schedule, str):
        return []
    s = schedule.strip()
    if not s.startswith("MULTI|"):
        return []
    body = s[len("MULTI|") :]
    crons = [c.strip() for c in body.split("||") if c.strip()]
    return crons


def _parse_calendar_schedule(schedule: str) -> Optional[Dict[str, Any]]:
    """Parses calendario custom.

    - CAL|<calendar_name>|HH:MM
    - CALC|<calendar_name>|unit|N|from|to|at_mm
    """
    if not isinstance(schedule, str):
        return None
    s = schedule.strip()

    if s.startswith("CAL|"):
        parts = s.split("|")
        if len(parts) < 3:
            return None
        cal_name = (parts[1] or "").strip()
        at = (parts[2] or "00:00").strip()
        if not cal_name:
            return None
        if not re.match(r"^\d{2}:\d{2}$", at):
            at = "00:00"
        return {"kind": "CAL", "calendar": cal_name, "at": at}

    if s.startswith("CALC|"):
        parts = s.split("|")
        if len(parts) < 7:
            return None
        cal_name = (parts[1] or "").strip()
        if not cal_name:
            return None
        unit = (parts[2] or "minutes").strip()
        if unit not in ("minutes", "hours"):
            unit = "minutes"

        def _to_int(x: Any, default: int) -> int:
            try:
                return int(x)
            except Exception:
                return default

        every = _to_int(parts[3], 15)
        window_from = _to_int(parts[4], 0)
        window_to = _to_int(parts[5], 23)
        at_minute = _to_int(parts[6], 0)

        return {
            "kind": "CALC",
            "calendar": cal_name,
            "unit": unit,
            "every": every,
            "window_from": window_from,
            "window_to": window_to,
            "at_minute": at_minute,
        }

    return None





def _parse_asset_list(val: Any) -> List[str]:
    if val is None:
        return []
    if isinstance(val, list):
        raw = val
    else:
        raw = re.split(r"[\n,]+", str(val))
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


def _assets_expr(uris: List[str], logic: str = "OR") -> str:
    logic = (logic or "OR").upper().strip()
    if not uris:
        return ""
    parts = [f"Asset({json.dumps(u)})" for u in uris]
    if len(parts) == 1:
        return parts[0]
    op = " & " if logic == "AND" else " | "
    return "(" + op.join(parts) + ")"



def build_dag_code(spec: Dict[str, Any]) -> str:
    """Genera un archivo .py de DAG a partir de la spec del builder."""

    spec_meta_line = _embed_spec_b64_line(spec)

    spec, warn_dedupe = _normalize_spec(spec)

    dag_id = spec["dag_id"]
    description = (spec.get("description") or "").replace('"', '\\"')
    retries = int(spec.get("retries") or 1)
    retry_minutes = int(spec.get("retry_minutes") or 5)
    catchup = _is_truthy(spec.get("catchup", False))
    confirm = _is_truthy(spec.get("confirm", False))

    tags = list(spec.get("tags") or [])
    app_tag = (spec.get("app") or "").strip()
    subapp_tag = (spec.get("subapp") or "").strip()
    if app_tag:
        t = f"app:{app_tag}"
        if t not in tags:
            tags.append(t)
    if subapp_tag:
        t = f"subapp:{subapp_tag}"
        if t not in tags:
            tags.append(t)

    timezone = (spec.get("timezone") or "UTC")

    y, m, d, hh, mm = _parse_start_date(spec)

    schedule_raw = (spec.get("schedule") or "@daily").strip()
    if schedule_raw.lower() in ("@none", "none", "null", "nil", ""):
        schedule_raw = "None"

    multi_crons = _parse_multi_schedule(schedule_raw)
    cal = _parse_calendar_schedule(schedule_raw)

    # Assets (condiciones)
    assets_enabled = _is_truthy(spec.get("assets_enabled", False))
    asset_logic = (spec.get("asset_logic") or "OR").upper().strip()
    asset_inlets = _parse_asset_list(spec.get("asset_inlets"))
    assets_expr = _assets_expr(asset_inlets, asset_logic) if (assets_enabled and asset_inlets) else ""

    needs_asset_import = bool(assets_expr)
    for _t in spec.get("tasks") or []:
        if _t.get("inlets") or _t.get("outlets"):
            needs_asset_import = True
            break

    extra_imports = ""
    if needs_asset_import:
        extra_imports += "from airflow.sdk import Asset\n\n"

    timetable_expr = None
    schedule_render = None

    if cal:
        extra_imports += "from ctm_calendar.timetable import CalendarTimetable\n\n"
        if cal.get("kind") == "CAL":
            timetable_expr = (
                f"CalendarTimetable(calendar={json.dumps(cal['calendar'])}, "
                f"at={json.dumps(cal.get('at') or '00:00')}, "
                f"timezone={json.dumps(timezone)})"
            )
        else:
            timetable_expr = (
                "CalendarTimetable("
                f"calendar={json.dumps(cal['calendar'])}, "
                f"timezone={json.dumps(timezone)}, "
                "cyclic=True, "
                f"unit={json.dumps(cal.get('unit') or 'minutes')}, "
                f"every={int(cal.get('every') or 15)}, "
                f"window_from={int(cal.get('window_from') or 0)}, "
                f"window_to={int(cal.get('window_to') or 23)}, "
                f"at_minute={int(cal.get('at_minute') or 0)}"
                ")"
            )

    elif multi_crons:
        # timetable oficial Airflow 3.1
        extra_imports += "from airflow.timetables.trigger import MultipleCronTriggerTimetable\n\n"
        cron_args = ", ".join([json.dumps(c) for c in multi_crons])
        timetable_expr = f"MultipleCronTriggerTimetable({cron_args}, timezone=local_tz)"

    elif schedule_raw == "None":
        timetable_expr = None

    elif schedule_raw == "@once":
        # sin assets => schedule="@once" directo. con assets => usamos OnceTimetable()
        if assets_expr:
            extra_imports += "from airflow.timetables.simple import OnceTimetable\n\n"
            timetable_expr = "OnceTimetable()"
        else:
            schedule_render = json.dumps(schedule_raw)

    else:
        # presets (@daily, etc) o cron string
        if assets_expr:
            extra_imports += "from airflow.timetables.trigger import CronTriggerTimetable\n\n"
            timetable_expr = f"CronTriggerTimetable({json.dumps(schedule_raw)}, timezone=local_tz)"
        else:
            schedule_render = json.dumps(schedule_raw)

    # aplica assets en schedule si corresponde
    if assets_expr:
        if timetable_expr is None:
            schedule_render = assets_expr
        else:
            extra_imports += "from airflow.timetables.assets import AssetOrTimeSchedule\n\n"
            schedule_render = f"AssetOrTimeSchedule(timetable={timetable_expr}, assets={assets_expr})"
    else:
        if schedule_render is None:
            schedule_render = timetable_expr if timetable_expr is not None else "None"
# imports extra (confirm gate)
    if confirm:
        extra_imports += "from airflow.sensors.python import PythonSensor\nfrom airflow.models import Variable\n\n"

    # Imports de operadores
    operator_imports = imports_for_tasks(spec.get("tasks") or [])
    header = _HEADER_IMPORTS.format(
        extra_imports=extra_imports,
        operator_imports=("\n".join(operator_imports) + ("\n" if operator_imports else "")),
    )

    # callables PythonOperator
    pydefs = _render_python_callables(spec.get("tasks") or [])

    if confirm:
        pydefs += """def _confirm_gate(**context):
    dag = context.get(\"dag\")
    dag_id = dag.dag_id if dag else \"\"
    dag_run = context.get(\"dag_run\")
    run_id = getattr(dag_run, \"run_id\", \"\") if dag_run else \"\"
    key = f\"confirm__{dag_id}__{run_id}\"
    v = Variable.get(key, default_var=\"0\")
    return str(v).strip().lower() in (\"1\", \"true\", \"yes\", \"y\")


"""

    # apply params/argsets per task
    global_params = spec.get("params") or {}
    argsets = spec.get("argsets") or {}
    overrides = spec.get("task_overrides") or {}

    rendered_tasks: List[str] = []
    task_ids: set[str] = set()

    for raw in spec.get("tasks") or []:
        if not raw.get("type") or not raw.get("task_id"):
            continue
        t = dict(raw)
        t["task_id"] = sanitize_id(t["task_id"])
        task_ids.add(t["task_id"])

        ov = overrides.get(t["task_id"], {}) if isinstance(overrides, dict) else {}

        # params: merge global + override/task
        task_params = ov.get("params", None)
        if task_params is None:
            task_params = t.get("params")
        params_merged = merge_params(global_params, task_params)

        # argset: apply only for command-like operators
        argset_ref = (ov.get("argset_ref") or t.get("argset_ref") or "").strip()
        append_args = (ov.get("append_args") or t.get("append_args") or "").strip()

        if argset_ref and t.get("type") in ARGS_TARGET_FIELD:
            field = ARGS_TARGET_FIELD[t["type"]]
            base_cmd = t.get(field) or ""
            tokens = argset_to_tokens(argsets.get(argset_ref))
            t[field] = apply_args_to_command(base_cmd, tokens, append_raw=append_args)
        elif append_args and t.get("type") in ARGS_TARGET_FIELD:
            field = ARGS_TARGET_FIELD[t["type"]]
            base_cmd = t.get(field) or ""
            t[field] = apply_args_to_command(base_cmd, [], append_raw=append_args)

        line = render_task_line(t, params=params_merged if params_merged else None)
        if line:
            rendered_tasks.append(line)

    # confirm gate: agrega una tarea sensor inicial y la conecta a los "roots"
    deps_edges = list(spec.get("dependencies") or [])
    if confirm:
        base_id = "confirm"
        confirm_task_id = base_id
        i = 1
        while confirm_task_id in task_ids:
            i += 1
            confirm_task_id = f"{base_id}_{i}"

        rendered_tasks.insert(
            0,
            f'{confirm_task_id} = PythonSensor(task_id={json.dumps(confirm_task_id)}, python_callable=_confirm_gate, mode="reschedule", poke_interval=60)'
        )
        task_ids.add(confirm_task_id)

        # calcula roots según dependencias originales (antes de inyectar confirm)
        downstream: set[str] = set()
        for e in deps_edges:
            d = sanitize_id(e.get("downstream") or "")
            if d:
                downstream.add(d)

        roots = [tid for tid in task_ids if tid != confirm_task_id and tid not in downstream]
        if not roots and task_ids:
            roots = [tid for tid in task_ids if tid != confirm_task_id]

        for r in roots:
            deps_edges.append({"upstream": confirm_task_id, "downstream": r})

    tasks_section = "\n    ".join(rendered_tasks) if rendered_tasks else "pass"
    deps_section, warn_deps = _render_dependencies(deps_edges, task_ids)

    tags_list = ", ".join([json.dumps(str(t)) for t in tags])

    # warnings
    warnings: List[str] = []
    warnings.extend(warn_dedupe or [])
    warnings.extend(warn_deps or [])
    warnings_block = ""
    if warnings:
        warnings_block = "# WARNINGS:\n# " + "\n# ".join(warnings) + "\n\n"

    # start_date render
    if schedule_raw == "@once":
        start_date_render = f"datetime({y}, {m}, {d}, {hh}, {mm}, tzinfo=local_tz)"
    else:
        start_date_render = f"datetime({y}, {m}, {d}, tzinfo=local_tz)"

    max_active_runs_line = "    max_active_runs=1,\n" if assets_expr else ""

    # build code
    code = f"""{warnings_block}{header}# === airflow-web-builder import metadata (do not remove) ===
    {spec_meta_line}# === end metadata ===

local_tz = pendulum.timezone({json.dumps(timezone)})

default_args = {{
    \"owner\": {json.dumps(spec.get('owner') or 'owner')},
    \"retries\": {retries},
    \"retry_delay\": timedelta(minutes={retry_minutes})
}}

{pydefs}with DAG(
    dag_id={json.dumps(dag_id)},
    description={json.dumps(description)},
    start_date={start_date_render},
    schedule={schedule_render},
    catchup={fmt_bool(catchup)},
{max_active_runs_line}    default_args=default_args,
    tags=[{tags_list}]
) as dag:
    {tasks_section}
"""

    if deps_section:
        code += f"\n    {deps_section}\n"

    return code
