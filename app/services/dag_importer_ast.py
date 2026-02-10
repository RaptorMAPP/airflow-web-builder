# services/dag_importer_ast.py
from __future__ import annotations

import ast
from typing import Any, Dict, List, Optional, Tuple

def _get_name(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None

def _const(node: ast.AST) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    return None

def _as_str(node: ast.AST) -> Optional[str]:
    v = _const(node)
    if v is None:
        return None
    return str(v)

def _kw(call: ast.Call) -> Dict[str, ast.AST]:
    out = {}
    for k in call.keywords:
        if k.arg:
            out[k.arg] = k.value
    return out

def _call_name(call: ast.Call) -> Optional[str]:
    # SSHOperator / PythonSensor / DAG / AssetOrTimeSchedule / CronTriggerTimetable ...
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None

def import_dag_py_to_spec(py_text: str) -> Dict[str, Any]:
    tree = ast.parse(py_text)

    # defaults
    spec: Dict[str, Any] = {
        "dag_id": "",
        "description": "",
        "owner": "",
        "retries": 1,
        "retry_minutes": 5,
        "catchup": False,
        "confirm": False,
        "timezone": "UTC",
        "start_date": "",
        "start_time": "",
        "schedule": "@daily",
        "tags": [],
        "params": {},
        "argsets": {},
        "assets_enabled": False,
        "asset_logic": "OR",
        "asset_prefix": "asset://cond/",
        "tasks": [],
        "dependencies": [],
    }

    # capturas auxiliares
    default_args: Dict[str, Any] = {}
    local_tz_name: Optional[str] = None
    var_to_taskid: Dict[str, str] = {}  # nombre variable -> task_id

    # 1) recoger asignaciones top-level (default_args, local_tz)
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id

            # default_args = { ... }
            if name == "default_args" and isinstance(node.value, ast.Dict):
                for k, v in zip(node.value.keys, node.value.values):
                    kk = _as_str(k)
                    if kk is None:
                        continue
                    if kk == "owner":
                        default_args["owner"] = _as_str(v) or ""
                    elif kk == "retries":
                        default_args["retries"] = _const(v)
                    elif kk == "retry_delay":
                        # timedelta(minutes=5)
                        if isinstance(v, ast.Call) and _call_name(v) == "timedelta":
                            kw = _kw(v)
                            mins = _const(kw.get("minutes")) if kw.get("minutes") else None
                            if isinstance(mins, int):
                                default_args["retry_minutes"] = mins

            # local_tz = pendulum.timezone("America/Santiago")
            if name == "local_tz" and isinstance(node.value, ast.Call):
                if _call_name(node.value) == "timezone":
                    if node.value.args:
                        tz = _as_str(node.value.args[0])
                        if tz:
                            local_tz_name = tz

    if default_args.get("owner"):
        spec["owner"] = default_args["owner"]
    if isinstance(default_args.get("retries"), int):
        spec["retries"] = default_args["retries"]
    if isinstance(default_args.get("retry_minutes"), int):
        spec["retry_minutes"] = default_args["retry_minutes"]
    if local_tz_name:
        spec["timezone"] = local_tz_name

    # 2) encontrar el "with DAG(... ) as dag:" y parsear config
    dag_with: Optional[ast.With] = None
    for node in tree.body:
        if isinstance(node, ast.With):
            # with DAG(...) as dag:
            item = node.items[0] if node.items else None
            if item and isinstance(item.context_expr, ast.Call) and _call_name(item.context_expr) == "DAG":
                dag_with = node
                dag_call = item.context_expr
                dag_kw = _kw(dag_call)

                # dag_id
                if "dag_id" in dag_kw:
                    spec["dag_id"] = _as_str(dag_kw["dag_id"]) or ""
                elif dag_call.args:
                    spec["dag_id"] = _as_str(dag_call.args[0]) or ""

                # description
                if "description" in dag_kw:
                    spec["description"] = _as_str(dag_kw["description"]) or ""

                # start_date: datetime(YYYY, M, D, tzinfo=local_tz)
                if "start_date" in dag_kw and isinstance(dag_kw["start_date"], ast.Call):
                    c = dag_kw["start_date"]
                    if _call_name(c) == "datetime" and len(c.args) >= 3:
                        y = _const(c.args[0]); m = _const(c.args[1]); d = _const(c.args[2])
                        # si d no es int, lo dejamos vacío (tu caso)
                        if all(isinstance(x, int) for x in (y, m, d)):
                            spec["start_date"] = f"{y:04d}-{m:02d}-{d:02d}"

                # catchup
                if "catchup" in dag_kw:
                    v = _const(dag_kw["catchup"])
                    if isinstance(v, bool):
                        spec["catchup"] = v

                # tags (["app:..","subapp:.."])
                if "tags" in dag_kw and isinstance(dag_kw["tags"], (ast.List, ast.Tuple)):
                    tags = []
                    for el in dag_kw["tags"].elts:
                        s = _as_str(el)
                        if s:
                            tags.append(s)
                    spec["tags"] = tags

                # schedule: AssetOrTimeSchedule(timetable=CronTriggerTimetable("cron", timezone=local_tz), assets=Asset("..."))
                if "schedule" in dag_kw and isinstance(dag_kw["schedule"], ast.Call):
                    sch = dag_kw["schedule"]
                    if _call_name(sch) == "AssetOrTimeSchedule":
                        sch_kw = _kw(sch)
                        tt = sch_kw.get("timetable")
                        assets = sch_kw.get("assets")
                        # timetable
                        if isinstance(tt, ast.Call) and _call_name(tt) == "CronTriggerTimetable":
                            if tt.args:
                                cron = _as_str(tt.args[0])
                                if cron:
                                    spec["schedule"] = cron
                            ttkw = _kw(tt)
                            tzv = ttkw.get("timezone")
                            # timezone=local_tz
                            if isinstance(tzv, ast.Name) and tzv.id == "local_tz" and local_tz_name:
                                spec["timezone"] = local_tz_name
                        # assets (uno solo en tu ejemplo)
                        if isinstance(assets, ast.Call) and _call_name(assets) == "Asset":
                            if assets.args:
                                a = _as_str(assets.args[0])
                                if a:
                                    spec["assets_enabled"] = True
                                    spec["asset_logic"] = "OR"
                    else:
                        # schedule simple string o None
                        if sch.args:
                            s = _as_str(sch.args[0])
                            if s:
                                spec["schedule"] = s

                break

    if not dag_with:
        # no se encontró with DAG(...)
        return spec

    # 3) parsear tasks dentro del with
    tasks: List[Dict[str, Any]] = []
    deps: List[Dict[str, str]] = []

    def _parse_asset_list(node: ast.AST) -> List[str]:
        out = []
        if isinstance(node, ast.List):
            for el in node.elts:
                if isinstance(el, ast.Call) and _call_name(el) == "Asset" and el.args:
                    s = _as_str(el.args[0])
                    if s:
                        out.append(s)
        return out

    for st in dag_with.body:
        # asignación de tarea: var = SSHOperator(...)
        if isinstance(st, ast.Assign) and len(st.targets) == 1 and isinstance(st.targets[0], ast.Name) and isinstance(st.value, ast.Call):
            var = st.targets[0].id
            call = st.value
            op = _call_name(call)
            if not op:
                continue

            kw = _kw(call)
            task_id = _as_str(kw.get("task_id")) or var
            var_to_taskid[var] = task_id

            t: Dict[str, Any] = {"type": op, "task_id": task_id}

            # campos comunes que te importan en builder
            for k in ("ssh_conn_id","command","get_pty","mode","poke_interval","timeout"):
                if k in kw:
                    v = _const(kw[k])
                    if v is None:
                        v = _as_str(kw[k])
                    t[k] = v

            # environment dict
            if "environment" in kw and isinstance(kw["environment"], ast.Dict):
                env = {}
                for k, v in zip(kw["environment"].keys, kw["environment"].values):
                    kk = _as_str(k)
                    vv = _as_str(v) if not isinstance(v, ast.Constant) else str(v.value)
                    if kk is not None and vv is not None:
                        env[kk] = vv
                t["environment"] = env

            # inlets/outlets
            if "inlets" in kw:
                inl = _parse_asset_list(kw["inlets"])
                if inl:
                    t["inlets"] = inl
                    spec["assets_enabled"] = True
            if "outlets" in kw:
                outl = _parse_asset_list(kw["outlets"])
                if outl:
                    t["outlets"] = outl
                    spec["assets_enabled"] = True

            tasks.append(t)

            # detectar confirm gate (tu patrón)
            if op == "PythonSensor" and task_id == "confirm":
                spec["confirm"] = True

        # dependencias con >>
        if isinstance(st, ast.Expr) and isinstance(st.value, ast.BinOp) and isinstance(st.value.op, ast.RShift):
            left = st.value.left
            right = st.value.right
            if isinstance(left, ast.Name) and isinstance(right, ast.Name):
                u = var_to_taskid.get(left.id, left.id)
                v = var_to_taskid.get(right.id, right.id)
                deps.append({"upstream": u, "downstream": v})

    spec["tasks"] = tasks
    spec["dependencies"] = deps
    return spec
