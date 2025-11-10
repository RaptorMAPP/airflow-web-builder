# -*- coding: utf-8 -*-
from __future__ import annotations
from flask import Blueprint, request, jsonify
import ast
from typing import Any, Dict, List, Tuple, Set

# ---------- Analizador AST (estático, sin conectarse a Airflow) ----------
OP_SUFFIXES = ("Operator", "Sensor")

class DagAstVisitor(ast.NodeVisitor):
    def __init__(self):
        self.dag_args: Dict[str, Any] = {}
        self.tasks: Dict[str, str] = {}
        self.edges: List[Tuple[str, str]] = []
        self.errors: List[str] = []
        self.warnings: List[str] = []

    @staticmethod
    def _is_operator_name(name: str) -> bool:
        return name.endswith(OP_SUFFIXES)

    @staticmethod
    def _name_of_func(func: ast.AST) -> str:
        if isinstance(func, ast.Name):
            return func.id
        if isinstance(func, ast.Attribute):
            return func.attr
        return ""

    @staticmethod
    def _kw_value(kwargs: List[ast.keyword], key: str):
        for k in kwargs:
            if k.arg == key:
                return k.value
        return None

    @staticmethod
    def _const_str(node: ast.AST) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return None

    def _capture_dag_call(self, call: ast.Call):
        if self._name_of_func(call.func) != "DAG":
            return
        dag_id = self._kw_value(call.keywords, "dag_id")
        schedule = self._kw_value(call.keywords, "schedule")
        schedule_interval = self._kw_value(call.keywords, "schedule_interval")
        start_date = self._kw_value(call.keywords, "start_date")
        catchup = self._kw_value(call.keywords, "catchup")
        tags = self._kw_value(call.keywords, "tags")

        if dag_id:
            s = self._const_str(dag_id)
            if s: self.dag_args["dag_id"] = s
        if schedule:
            s = self._const_str(schedule)
            if s: self.dag_args["schedule"] = s
        if schedule_interval:
            self.warnings.append("Se encontró 'schedule_interval'. En Airflow 3.1 usa 'schedule'.")
            s = self._const_str(schedule_interval)
            if s and "schedule" not in self.dag_args:
                self.dag_args["schedule"] = s
        if catchup is not None and isinstance(catchup, ast.Constant) and isinstance(catchup.value, bool):
            self.dag_args["catchup"] = catchup.value
        if tags and isinstance(tags, (ast.List, ast.Tuple)):
            vals = []
            for el in tags.elts:
                v = self._const_str(el)
                if v: vals.append(v)
            self.dag_args["tags"] = vals
        if start_date is None:
            self.warnings.append("No se encontró 'start_date'.")

    def visit_Assign(self, node: ast.Assign):
        # t1 = ClassName(...)
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Call):
            cls = self._name_of_func(node.value.func)
            if cls and self._is_operator_name(cls):
                tid_node = self._kw_value(node.value.keywords, "task_id")
                task_id = self._const_str(tid_node) if tid_node else node.targets[0].id
                if task_id:
                    self.tasks[task_id] = cls
        return self.generic_visit(node)

    def visit_BinOp(self, node: ast.BinOp):
        # soporta a >> b y a << b (listas también)
        def collect(n: ast.AST) -> List[str]:
            if isinstance(n, ast.List):
                return [el.id for el in n.elts if isinstance(el, ast.Name)]
            if isinstance(n, ast.Name):
                return [n.id]
            return []
        if isinstance(node.op, ast.RShift):  # >>
            for u in collect(node.left):
                for v in collect(node.right):
                    self.edges.append((u, v))
        if isinstance(node.op, ast.LShift):  # <<
            for u in collect(node.right):
                for v in collect(node.left):
                    self.edges.append((u, v))
        return self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        self._capture_dag_call(node)
        # chain(t1, t2, [t3, t4])
        if self._name_of_func(node.func) == "chain":
            ids: List[str] = []
            for arg in node.args:
                if isinstance(arg, ast.Name):
                    ids.append(arg.id)
                elif isinstance(arg, ast.List):
                    ids.extend([el.id for el in arg.elts if isinstance(el, ast.Name)])
            for i in range(len(ids)-1):
                self.edges.append((ids[i], ids[i+1]))
        return self.generic_visit(node)

def _dedupe_edges(edges: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    seen: Set[Tuple[str, str]] = set()
    out: List[Tuple[str, str]] = []
    for u,v in edges:
        if (u,v) in seen: continue
        seen.add((u,v)); out.append((u,v))
    return out

def _detect_cycles(nodes: Set[str], edges: List[Tuple[str, str]]) -> List[List[str]]:
    adj: Dict[str, List[str]] = {n: [] for n in nodes}
    for u,v in edges:
        adj.setdefault(u, []).append(v)
        adj.setdefault(v, adj.get(v, []))
    visited: Set[str] = set(); stack: Set[str] = set(); cycles: List[List[str]] = []; path: List[str] = []
    def dfs(u: str):
        visited.add(u); stack.add(u); path.append(u)
        for w in adj.get(u, []):
            if w not in visited: dfs(w)
            elif w in stack and w in path:
                i = path.index(w); cycles.append(path[i:] + [w])
        stack.remove(u); path.pop()
    for n in nodes:
        if n not in visited: dfs(n)
    return cycles

def validate_dag_code(code: str) -> Dict[str, Any]:
    res = {
        "dag_id": None, "schedule": None, "valid": True,
        "summary": "Validación estática (AST)",
        "errors": [], "warnings": [],
        "nodes": [], "edges": [],
        "issues": []  # [{task_id, level: "error"|"warning", msg}]
    }
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        res["valid"] = False
        res["errors"].append(f"SyntaxError: {e.msg} (línea {e.lineno})")
        return res

    v = DagAstVisitor(); v.visit(tree)
    res["dag_id"]   = v.dag_args.get("dag_id")
    res["schedule"] = v.dag_args.get("schedule")

    if not res["dag_id"]:
        v.warnings.append("No se detectó 'dag_id' en la definición del DAG.")
    if not res["schedule"]:
        v.warnings.append("No se detectó 'schedule' o está vacío/@once. Verifica si es intencional.")

    nodes = [{"id": k, "cls": c} for k,c in v.tasks.items()]
    edges = _dedupe_edges(v.edges)

    known = set(v.tasks.keys())
    # dependencias con tasks inexistentes → warnings por task
    for u, w in edges:
        if u not in known:
            msg = f"Dependencia desde '{u}' (no definido) hacia '{w}'."
            v.warnings.append(msg)
            res["issues"].append({"task_id": u, "level": "warning", "msg": msg})
        if w not in known:
            msg = f"Dependencia hacia '{w}' (no definido)."
            v.warnings.append(msg)
            res["issues"].append({"task_id": w, "level": "warning", "msg": msg})

    # ciclos → errores sobre cada task del ciclo
    cyc = _detect_cycles(known, [(u,w) for u,w in edges if u in known and w in known])
    if cyc:
        res["valid"] = False
        for c in cyc:
            msg = "Ciclo detectado: " + " -> ".join(c)
            res["errors"].append(msg)
            for t in c:
                res["issues"].append({"task_id": t, "level": "error", "msg": "Participa en ciclo"})

    # clases que no parecen Operator/Sensor → warning por task
    for tid, cls in v.tasks.items():
        if not DagAstVisitor._is_operator_name(cls):
            msg = f"Tarea '{tid}' usa clase '{cls}' (no parece Operator/Sensor)."
            v.warnings.append(msg)
            res["issues"].append({"task_id": tid, "level": "warning", "msg": "Clase no típica de operador/sensor"})

    res["nodes"] = nodes
    res["edges"] = [[u,w] for (u,w) in edges]
    res["warnings"] = v.warnings
    res["valid"] = res["valid"] and not res["errors"]
    return res


# ---------- Blueprint HTTP ----------
validator_bp = Blueprint("validator_bp", __name__)

@validator_bp.post("/api/airflow/validate")
def api_validate():
    payload = request.get_json(silent=True) or {}
    code = payload.get("code") or ""
    if not code.strip():
        return jsonify(success=False, error="Falta 'code'"), 400
    try:
        return jsonify(validate_dag_code(code)), 200
    except Exception as e:
        return jsonify(success=False, error=type(e).__name__, detail=str(e)), 500
