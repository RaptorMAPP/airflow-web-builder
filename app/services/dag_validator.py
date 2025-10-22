import ast, tempfile, os, uuid, shutil
from typing import Dict, Any, List

def _ast_extract_dag_info(tree: ast.AST) -> Dict[str, Any]:
    info = {"found": False, "dag_id": None, "schedule": None, "schedule_interval": None}
    class V(ast.NodeVisitor):
        def visit_Call(self, node):
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
            if name == "DAG":
                info["found"] = True
                for kw in node.keywords or []:
                    if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant):
                        info["dag_id"] = kw.value.value
                    if kw.arg == "schedule" and isinstance(kw.value, ast.Constant):
                        info["schedule"] = kw.value.value
                    if kw.arg == "schedule_interval" and isinstance(kw.value, ast.Constant):
                        info["schedule_interval"] = kw.value.value
            self.generic_visit(node)
    V().visit(tree)
    return info

def validate_with_fallback(content: str) -> Dict[str, Any]:
    # 1) Intento con DagBag (si Airflow está instalado en este entorno)
    try:
        from airflow.models.dagbag import DagBag  # type: ignore
        tmpd = tempfile.mkdtemp(prefix="dagval_")
        fpath = os.path.join(tmpd, f"{uuid.uuid4().hex}.py")
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(content)
        bag = DagBag(dag_folder=tmpd, include_examples=False, safe_mode=True)
        errors = [f"{k}: {v}" for k, v in bag.import_errors.items()]
        if errors:
            return {"valid": False, "errors": errors, "warnings": [], "summary": "Errores de importación"}
        dags = list(bag.dags.values())
        if not dags:
            return {"valid": False, "errors": ["No se detectaron DAGs"], "warnings": [], "summary": "DagBag vacío"}
        dag = dags[0]
        warnings: List[str] = []
        schedule = getattr(dag, "schedule", None)
        if schedule in (None, "", "@once"):
            warnings.append("DAG sin 'schedule' (o '@once'). Verifica si es intencional.")
        default_args = getattr(dag, "default_args", None)
        if not default_args:
            warnings.append("Falta default_args (owner, retries, retry_delay recomendados).")
        else:
            if not default_args.get("owner"):
                warnings.append("default_args sin 'owner'.")
            if "retries" not in default_args:
                warnings.append("default_args sin 'retries'.")
        return {"valid": True, "errors": [], "warnings": warnings,
                "summary": f"DAG '{dag.dag_id}' importado", "dag_id": dag.dag_id, "schedule": str(schedule)}
    except Exception:
        pass
    finally:
        try:
            shutil.rmtree(locals().get("tmpd",""), ignore_errors=True)
        except Exception:
            pass

    # 2) Fallback AST (sin Airflow instalado en este venv)
    try:
        tree = ast.parse(content)
    except SyntaxError as e:
        return {"valid": False, "errors": [f"SyntaxError: {e}"], "warnings": [], "summary": "Código inválido"}

    info = _ast_extract_dag_info(tree)
    warnings: List[str] = []
    if info["schedule_interval"] is not None:
        warnings.append("Se detectó 'schedule_interval'. En Airflow 3.1 usa 'schedule'.")
    if info["schedule"] in (None, "", "@once"):
        warnings.append("No se detectó 'schedule=' o está vacío/@once. Verifica si es intencional.")

    return {
        "valid": True, "errors": [], "warnings": warnings,
        "summary": "Validación básica (AST)", "dag_id": info["dag_id"], "schedule": info["schedule"]
    }
