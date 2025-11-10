from __future__ import annotations
from typing import Dict, List, Tuple, Any
import re
import ast
import json
import pprint

# ────────────────────────────────────────────────────────────────────────────────
# Catálogo de operadores (Airflow 3.1)
# Nota: DummyOperator se mapea a EmptyOperator para compatibilidad.
#       Asegúrate de tener instalados los providers correspondientes en Airflow.
# ────────────────────────────────────────────────────────────────────────────────
SUPPORTED_OPERATORS: Dict[str, Dict[str, Any]] = {
    # Básicos
    "EmptyOperator": {
        "import": "from airflow.operators.empty import EmptyOperator",
    },
    "DummyOperator": {  # alias → EmptyOperator
        "import": "from airflow.operators.empty import EmptyOperator",
    },
    "BashOperator": {
        "import": "from airflow.operators.bash import BashOperator",
    },
    "PythonOperator": {
        "import": "from airflow.operators.python import PythonOperator",
    },

    # Bases de datos
    "SqliteOperator": {
        "import": "from airflow.providers.sqlite.operators.sqlite import SqliteOperator",
    },
    "PostgresOperator": {
        "import": "from airflow.providers.postgres.operators.postgres import PostgresOperator",
    },
    "MySqlOperator": {
        "import": "from airflow.providers.mysql.operators.mysql import MySqlOperator",
    },
    "MsSqlOperator": {
        "import": "from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator",
    },
    "OracleOperator": {
        "import": "from airflow.providers.oracle.operators.oracle import OracleOperator",
    },
    "SnowflakeOperator": {
        "import": "from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator",
    },

    # Cloud / Storage / Compute
    "S3CreateObjectOperator": {
        "import": "from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator",
    },
    "LambdaInvokeFunctionOperator": {
        "import": "from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator",
    },
    "GlueJobOperator": {
        "import": "from airflow.providers.amazon.aws.operators.glue import GlueJobOperator",
    },
    "BigQueryExecuteQueryOperator": {
        "import": "from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator",
    },
    "DataflowCreateJavaJobOperator": {
        "import": "from airflow.providers.google.cloud.operators.dataflow import DataflowCreateJavaJobOperator",
    },
    "AzureDataFactoryRunPipelineOperator": {
        "import": "from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator",
    },
    "DockerOperator": {
        "import": "from airflow.providers.docker.operators.docker import DockerOperator",
    },
    "KubernetesPodOperator": {
        "import": "from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator",
    },
    "SparkSubmitOperator": {
        "import": "from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator",
    },
    "DatabricksSubmitRunOperator": {
        "import": "from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator",
    },

    # HTTP / Email
    "SimpleHttpOperator": {
        "import": "from airflow.providers.http.operators.http import SimpleHttpOperator",
    },
    "EmailOperator": {
        "import": "from airflow.operators.email import EmailOperator",
    },

    # Sensores / DAG ops
    "ExternalTaskSensor": {
        "import": "from airflow.sensors.external_task import ExternalTaskSensor",
    },
    "TriggerDagRunOperator": {
        "import": "from airflow.operators.trigger_dagrun import TriggerDagRunOperator",
    },

    # Remotos
    "SSHOperator": {
        "import": "from airflow.providers.ssh.operators.ssh import SSHOperator",
    },
    "WinRMOperator": {
        "import": "from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator",
    },
}

# Presets de schedule para la UI (informativo; el backend acepta cualquier string)
SCHEDULE_PRESETS = [
    "@once", "@hourly", "@daily", "@weekly", "@monthly", "@yearly",
    "*/5 * * * *", "0 * * * *", "0 3 * * *", "0 0 * * 1", "0 0 1 * *"
]

_HEADER_IMPORTS = """\
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
{operator_imports}
"""

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _json_to_py_literal(raw: Any, default: str = "{}") -> str:
    """
    Convierte un JSON (str/dict/list/…) a un literal de Python válido.
    Si falla, devuelve `default` (por defecto "{}").
    """
    if raw is None:
        return default
    try:
        if isinstance(raw, (dict, list, int, float, bool)):
            obj = raw
        elif isinstance(raw, str):
            s = raw.strip()
            if not s:
                return default
            # Primero intentamos JSON
            try:
                obj = json.loads(s)
            except Exception:
                # Intento como literal Python seguro
                obj = ast.literal_eval(s)
        else:
            return default
        return pprint.pformat(obj, width=100)
    except Exception:
        return default

def _bo_kwargs(t: Dict[str, Any]) -> str:
    """
    Retorna kwargs comunes de BaseOperator si están presentes en la spec:
    queue (Celery), pool, priority_weight, depends_on_past, task_concurrency.
    """
    parts = []
    if t.get("queue"): parts.append(f'queue="{t["queue"]}"')
    if t.get("pool"): parts.append(f'pool="{t["pool"]}"')
    if t.get("priority_weight"): parts.append(f'priority_weight={int(t["priority_weight"])}')
    if str(t.get("depends_on_past")).lower() in ("true","1"): parts.append("depends_on_past=True")
    if t.get("task_concurrency"): parts.append(f'task_concurrency={int(t["task_concurrency"])}')
    return (", " + ", ".join(parts)) if parts else ""

def _parse_kwargs_json(raw: Any) -> str:
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

def _resolve_operator_key(ttype: str) -> str:
    # DummyOperator se resoluciona como EmptyOperator
    return "EmptyOperator" if ttype == "DummyOperator" else ttype

def _imports_for_tasks(tasks: List[Dict[str, Any]]) -> List[str]:
    used: set[str] = set()
    for t in tasks or []:
        typ = _resolve_operator_key(str(t.get("type") or ""))
        imp = SUPPORTED_OPERATORS.get(typ, {}).get("import")
        if imp:
            used.add(imp)
        # soporta imports ad-hoc de CustomOperator
        cimp = t.get("_custom_import")
        if cimp:
            used.add(cimp)
    return sorted(used)

def _sanitize_id(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_]", "_", s)
    if not re.match(r"^[A-Za-z_]", s):
        s = f"t_{s}"
    return s or "t_task"

def _render_python_callables(tasks: List[Dict[str, Any]]) -> str:
    """Genera stubs para PythonOperator."""
    pieces: List[str] = []
    for t in tasks or []:
        if t.get("type") == "PythonOperator":
            fn = _sanitize_id(t.get("python_callable_name") or f'fn_{t.get("task_id","task")}')
            body = t.get("python_callable_body") or "print('Hello from PythonOperator')"
            pieces.append(f"def {fn}(**context):\n    {body}\n")
    return ("\n".join(pieces) + ("\n" if pieces else ""))

def _fmt_bool(val: Any) -> str:
    return "True" if str(val).strip().lower() in ("1", "true", "yes", "y") else "False"

def _quote(s: str) -> str:
    return (s or "").replace('"', '\\"')

def _render_task_line(t: Dict[str, Any]) -> str:
    ttype = t["type"]
    task_id = _sanitize_id(t["task_id"])
    if ttype == "DummyOperator":
        ttype = "EmptyOperator"

    if ttype == "EmptyOperator":
        return f'{task_id} = EmptyOperator(task_id="{task_id}"{_bo_kwargs(t)})'

    if ttype == "BashOperator":
        cmd = _quote(t.get("bash_command") or 'echo "hello"')
        return f'{task_id} = BashOperator(task_id="{task_id}", bash_command="{cmd}"{_bo_kwargs(t)})'

    if ttype == "PythonOperator":
        fn = _sanitize_id(t.get("python_callable_name") or f"fn_{task_id}")
        return f'{task_id} = PythonOperator(task_id="{task_id}", python_callable={fn}{_bo_kwargs(t)})'

    if ttype == "SqliteOperator":
        return f'{task_id} = SqliteOperator(task_id="{task_id}", sqlite_conn_id="{t.get("sqlite_conn_id","sqlite_default")}", sql="{_quote(t.get("sql","SELECT 1;"))}"{_bo_kwargs(t)})'

    if ttype == "PostgresOperator":
        return f'{task_id} = PostgresOperator(task_id="{task_id}", postgres_conn_id="{t.get("postgres_conn_id","postgres_default")}", sql="{_quote(t.get("sql","SELECT 1;"))}"{_bo_kwargs(t)})'

    if ttype == "MySqlOperator":
        return f'{task_id} = MySqlOperator(task_id="{task_id}", mysql_conn_id="{t.get("mysql_conn_id","mysql_default")}", sql="{_quote(t.get("sql","SELECT 1;"))}"{_bo_kwargs(t)})'

    if ttype == "MsSqlOperator":
        return f'{task_id} = MsSqlOperator(task_id="{task_id}", mssql_conn_id="{t.get("mssql_conn_id","mssql_default")}", sql="{_quote(t.get("sql","SELECT 1;"))}"{_bo_kwargs(t)})'

    if ttype == "OracleOperator":
        return f'{task_id} = OracleOperator(task_id="{task_id}", oracle_conn_id="{t.get("oracle_conn_id","oracle_default")}", sql="{_quote(t.get("sql","SELECT 1 FROM dual"))}"{_bo_kwargs(t)})'

    if ttype == "SnowflakeOperator":
        return f'{task_id} = SnowflakeOperator(task_id="{task_id}", snowflake_conn_id="{t.get("snowflake_conn_id","snowflake_default")}", sql="{_quote(t.get("sql","SELECT 1;"))}"{_bo_kwargs(t)})'

    if ttype == "S3CreateObjectOperator":
        return (f'{task_id} = S3CreateObjectOperator(task_id="{task_id}", aws_conn_id="{t.get("aws_conn_id","aws_default")}", '
                f's3_bucket="{t.get("s3_bucket","my-bucket")}", s3_key="{t.get("s3_key","path/file.txt")}", data="{_quote(t.get("data","hello world"))}"{_bo_kwargs(t)})')

    if ttype == "LambdaInvokeFunctionOperator":
        return (f'{task_id} = LambdaInvokeFunctionOperator(task_id="{task_id}", aws_conn_id="{t.get("aws_conn_id","aws_default")}", '
                f'function_name="{t.get("function_name","my-fn")}", payload="{_quote(t.get("payload","{}"))}"{_bo_kwargs(t)})')

    if ttype == "GlueJobOperator":
        return (f'{task_id} = GlueJobOperator(task_id="{task_id}", aws_conn_id="{t.get("aws_conn_id","aws_default")}", '
                f'job_name="{t.get("job_name","my-job")}", script_location="{t.get("script_location","s3://bucket/script.py")}", '
                f'iam_role_name="{t.get("iam_role_name","glue-role")}", num_of_dpus={int(t.get("num_of_dpus") or 10)}{_bo_kwargs(t)})')

    if ttype == "BigQueryExecuteQueryOperator":
        return (f'{task_id} = BigQueryExecuteQueryOperator(task_id="{task_id}", gcp_conn_id="{t.get("gcp_conn_id","google_cloud_default")}", '
                f'sql="{_quote(t.get("sql","SELECT 1;"))}", use_legacy_sql={_fmt_bool(t.get("use_legacy_sql","false"))}{_bo_kwargs(t)})')

    if ttype == "DataflowCreateJavaJobOperator":
        options = _quote(t.get("options") or "{}")
        return (f'{task_id} = DataflowCreateJavaJobOperator(task_id="{task_id}", gcp_conn_id="{t.get("gcp_conn_id","google_cloud_default")}", '
                f'jar="{t.get("jar","/path/app.jar")}", job_name="{t.get("job_name","dataflow-job")}", options="{options}"{_bo_kwargs(t)})')

    if ttype == "AzureDataFactoryRunPipelineOperator":
        params = _quote(t.get("parameters") or "{}")
        return (f'{task_id} = AzureDataFactoryRunPipelineOperator(task_id="{task_id}", '
                f'azure_data_factory_conn_id="{t.get("azure_data_factory_conn_id","azure_data_factory_default")}", '
                f'pipeline_name="{t.get("pipeline_name","pipeline1")}", parameters="{params}"{_bo_kwargs(t)})')

    if ttype == "DockerOperator":
        return (f'{task_id} = DockerOperator(task_id="{task_id}", image="{t.get("image","python:3.11")}", '
                f'api_version="{t.get("api_version","auto")}", command="{_quote(t.get("command","echo hello"))}", '
                f'docker_url="{t.get("docker_url","unix://var/run/docker.sock")}", auto_remove={_fmt_bool(t.get("auto_remove","true"))}{_bo_kwargs(t)})')

    if ttype == "KubernetesPodOperator":
        # soporta env/cmds/args como JSON o literales python
        env_literal = _json_to_py_literal(t.get("env"), "{}")
        name = _quote(t.get("name", "pod-task"))
        namespace = _quote(t.get("namespace", "default"))
        image = _quote(t.get("image", "python:3.11"))
        cmds = _json_to_py_literal(t.get("cmds", '["python","-c"]'), "[]")
        args = _json_to_py_literal(t.get("arguments", '["print(\\"hi\\")"]'), "[]")
        return (f'{task_id} = KubernetesPodOperator(task_id="{task_id}", name="{name}", '
                f'namespace="{namespace}", image="{image}", cmds={cmds}, arguments={args}, '
                f'env_vars={env_literal}{_bo_kwargs(t)})')

    if ttype == "SparkSubmitOperator":
        args = _json_to_py_literal(t.get("application_args") or "[]", "[]")
        return (f'{task_id} = SparkSubmitOperator(task_id="{task_id}", application="{t.get("application","/path/app.py")}", '
                f'conn_id="{t.get("conn_id","spark_default")}", application_args={args}{_bo_kwargs(t)})')

    if ttype == "DatabricksSubmitRunOperator":
        payload = _json_to_py_literal(t.get("json") or "{}", "{}")
        return (f'{task_id} = DatabricksSubmitRunOperator(task_id="{task_id}", '
                f'databricks_conn_id="{t.get("databricks_conn_id","databricks_default")}", json={payload}{_bo_kwargs(t)})')

    if ttype == "SimpleHttpOperator":
        data = t.get("data") or ""
        headers = t.get("headers") or ""
        return (f'{task_id} = SimpleHttpOperator(task_id="{task_id}", http_conn_id="{t.get("http_conn_id","http_default")}", '
                f'endpoint="{t.get("endpoint","/")}", method="{(t.get("method") or "GET").upper()}", '
                f'data={repr(data)}, headers={repr(headers)}{_bo_kwargs(t)})')

    if ttype == "EmailOperator":
        to = t.get("to", "example@example.com")
        subj = _quote(t.get("subject") or "Airflow Notification")
        html = _quote(t.get("html_content") or "Job done.")
        return f'{task_id} = EmailOperator(task_id="{task_id}", to="{to}", subject="{subj}", html_content="{html}"{_bo_kwargs(t)})'

    if ttype == "ExternalTaskSensor":
        # espera a que termine una tarea (o todo el DAG) en OTRO DAG
        return (
            f'{task_id} = ExternalTaskSensor('
            f'task_id="{task_id}", '
            f'external_dag_id="{_quote(t.get("external_dag_id","other_dag"))}", '
            f'external_task_id={repr(t.get("external_task_id") or None)}, '  # None => espera al DAG completo
            f'mode="{t.get("mode","reschedule")}", '
            f'poke_interval={int(t.get("poke_interval") or 60)}, '
            f'timeout={int(t.get("timeout") or 3600)}'
            f'{_bo_kwargs(t)})'
        )

    if ttype == "TriggerDagRunOperator":
        # dispara otro DAG (no espera a task específica)
        return (
            f'{task_id} = TriggerDagRunOperator('
            f'task_id="{task_id}", '
            f'trigger_dag_id="{_quote(t.get("trigger_dag_id","other_dag"))}"'
            f'{_bo_kwargs(t)})'
        )

    if ttype == "CustomOperator":
        module = (t.get("import_path") or "").strip()     # ej: airflow.providers.amazon.aws.operators.s3
        cls     = (t.get("class_name") or "").strip()     # ej: S3CreateObjectOperator
        kwargs  = _parse_kwargs_json(t.get("kwargs"))     # ej: {"aws_conn_id":"...", "s3_bucket":"..."}
        if not module or not cls:
            # si falta algo esencial, genera Empty para no romper
            return f'{task_id} = EmptyOperator(task_id="{task_id}")  # CUSTOM MISSING IMPORT/CLASS'
        # guardamos el import en un campo especial para que _imports_for_tasks lo recoja
        t["_custom_import"] = f"from {module} import {cls}"
        return f'{task_id} = {cls}(task_id="{task_id}", **({kwargs}){_bo_kwargs(t)})'

    if ttype == "SSHOperator":
        cmd = _quote(t.get("command", "echo hello"))
        ssh_conn_id = _quote(t.get("ssh_conn_id", "ssh_default"))
        remote_host = t.get("remote_host")
        get_pty = str(t.get("get_pty", "true")).lower() in ("true", "1")
        env_literal = _json_to_py_literal(t.get("environment"), "{}")
        extra = f', remote_host="{remote_host}"' if remote_host else ""
        return (f'{task_id} = SSHOperator(task_id="{task_id}", ssh_conn_id="{ssh_conn_id}", '
                f'command="{cmd}", get_pty={get_pty}, environment={env_literal}{extra}{_bo_kwargs(t)})')

    if ttype == "WinRMOperator":
        winrm_conn_id = _quote(t.get("winrm_conn_id", "winrm_default"))
        cmd = _quote(t.get("command", "Write-Output \\Hello\\"))
        ps = str(t.get("powershell", "true")).lower() in ("true", "1")
        return (f'{task_id} = WinRMOperator(task_id="{task_id}", winrm_conn_id="{winrm_conn_id}", '
                f'command="{cmd}", powershell={ps}{_bo_kwargs(t)})')

    return ""  # tipo no soportado

def _render_tasks(tasks: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for raw in tasks or []:
        if not raw.get("type") or not raw.get("task_id"):
            continue
        raw["task_id"] = _sanitize_id(raw["task_id"])
        line = _render_task_line(raw)
        if line:
            lines.append(line)
    return "\n    ".join(lines)

def _render_dependencies(edges: List[Dict[str, Any]], valid_ids: set[str]) -> Tuple[str, List[str]]:
    out: List[str] = []
    warns: List[str] = []
    for e in edges or []:
        u = _sanitize_id(e.get("upstream") or "")
        d = _sanitize_id(e.get("downstream") or "")
        if not u or not d:
            continue
        if u in valid_ids and d in valid_ids:
            out.append(f"{u} >> {d}")
        else:
            warns.append(f"dependencia ignorada: {u} >> {d} (task inexistente)")
    return ("\n    ".join(out), warns)

# ────────────────────────────────────────────────────────────────────────────────
# Normalización y dedupe (evita “task_id duplicado”)
# ────────────────────────────────────────────────────────────────────────────────
def _dedupe_task_ids(tasks: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
    out: List[Dict[str, Any]] = []
    seen: Dict[str, int] = {}
    for t in tasks or []:
        base = _sanitize_id(t.get("task_id") or "")
        if not base:
            base = "t_task"
        if base in seen:
            seen[base] += 1
            new_id = f"{base}_{seen[base]}"
        else:
            seen[base] = 1
            new_id = base
        t2 = dict(t, task_id=new_id)
        out.append(t2)
    warnings = [f"task_id duplicado ajustado: {k} -> {k}_2..{v}" for k, v in seen.items() if v > 1]
    return out, warnings

def _normalize_spec(spec: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    spec = dict(spec or {})
    spec["dag_id"] = _sanitize_id(spec.get("dag_id") or "dag_generated")
    spec["schedule"] = spec.get("schedule") or "@daily"
    # timezone opcional (para start_date tz-aware)
    tz = (spec.get("timezone") or "").strip()
    spec["timezone"] = tz or "UTC"
    tasks, w = _dedupe_task_ids(spec.get("tasks") or [])
    spec["tasks"] = tasks
    return spec, w

# ────────────────────────────────────────────────────────────────────────────────
# Generación final del DAG
# ────────────────────────────────────────────────────────────────────────────────
def build_dag_code(spec: Dict[str, Any]) -> str:
    spec, warn_dedupe = _normalize_spec(spec)

    # Campos DAG
    dag_id = spec["dag_id"]
    description = _quote(spec.get("description") or "")
    retries = int(spec.get("retries") or 1)
    retry_minutes = int(spec.get("retry_minutes") or 5)

    # start_date
    try:
        y, m, d = [int(x) for x in (spec.get("start_date") or "2025-01-01").split("-")]
    except Exception:
        y, m, d = 2025, 1, 1

    schedule = spec.get("schedule") or "@daily"
    catchup = bool(spec.get("catchup", False))
    tags = spec.get("tags") or []
    timezone = (spec.get("timezone") or "UTC").replace('"', '\\"')

    # Imports por operador
    operator_imports = _imports_for_tasks(spec.get("tasks") or [])
    header = _HEADER_IMPORTS.format(operator_imports="\n".join(operator_imports))

    # callables PythonOperator
    pydefs = _render_python_callables(spec.get("tasks") or [])

    # Tareas + dependencias
    task_ids: set[str] = {_sanitize_id(t.get("task_id") or "") for t in (spec.get("tasks") or []) if t.get("task_id")}
    tasks_section = _render_tasks(spec.get("tasks") or [])
    deps_section, warn_deps = _render_dependencies(spec.get("dependencies") or [], task_ids)
    tags_list = ", ".join([f'"{t}"' for t in tags])

    # Warnings de generación (comentados al inicio del archivo generado)
    warnings_block = ""
    all_warns = []
    if warn_dedupe:
        all_warns.extend(warn_dedupe)
    if 'warn_deps' in locals() and warn_deps:
        all_warns.extend(warn_deps)
    if all_warns:
        warnings_block = "# WARNINGS:\n# " + "\n# ".join(all_warns) + "\n\n"

    # Código final
    code = f'''{warnings_block}{header}
local_tz = pendulum.timezone("{timezone}")

default_args = {{
    "owner": "{spec.get("owner","owner")}",
    "retries": {retries},
    "retry_delay": timedelta(minutes={retry_minutes})
}}

{pydefs}with DAG(
    dag_id="{dag_id}",
    description="{description}",
    start_date=datetime({y}, {m}, {d}, tzinfo=local_tz),
    schedule="{schedule}",
    catchup={str(catchup)},
    default_args=default_args,
    tags=[{tags_list}]
) as dag:
    {tasks_section}
    {deps_section}
'''
    return code
