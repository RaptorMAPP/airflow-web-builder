from __future__ import annotations
from typing import Dict, List, Tuple, Any
import re

# ────────────────────────────────────────────────────────────────────────────────
# Catálogo de operadores (Airflow 3.1)
# Nota: DummyOperator se mapea a EmptyOperator para compatibilidad.
#       Para que funcionen, instala los providers correspondientes en Airflow.
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
    "ExternalTaskSensor": {
        "import": "from airflow.sensors.external_task import ExternalTaskSensor",
    },
    "TriggerDagRunOperator": {
        "import": "from airflow.operators.trigger_dagrun import TriggerDagRunOperator",
    },

}

# Presets de schedule para la UI (informativo; el backend acepta cualquier string)
SCHEDULE_PRESETS = [
    "@once", "@hourly", "@daily", "@weekly", "@monthly", "@yearly",
    "*/5 * * * *", "0 * * * *", "0 3 * * *", "0 0 * * 1", "0 0 1 * *"
]

_HEADER_IMPORTS = """\
from datetime import datetime, timedelta
from airflow import DAG
{operator_imports}
"""

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

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
    return sorted(used)


def _sanitize_id(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_]", "_", s)
    if not re.match(r"^[A-Za-z_]", s):
        s = f"t_{s}"
    return s or "t_task"

def _format_default_args(owner: str, retries: int, retry_minutes: int) -> str:
    return (
        "default_args = {\n"
        f'    "owner": "{owner or "owner"}",\n'
        f'    "retries": {int(retries)},\n'
        f'    "retry_delay": timedelta(minutes={int(retry_minutes)})\n'
        "}\n"
    )

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

# ────────────────────────────────────────────────────────────────────────────────
# Render de tareas por tipo
# ────────────────────────────────────────────────────────────────────────────────
def _render_task_line(t: Dict[str, Any]) -> str:
    ttype = t["type"]
    task_id = _sanitize_id(t["task_id"])
    if ttype == "DummyOperator":
        ttype = "EmptyOperator"

    if ttype == "EmptyOperator":
        return f'{task_id} = EmptyOperator(task_id="{task_id}")'

    if ttype == "BashOperator":
        cmd = _quote(t.get("bash_command") or 'echo "hello"')
        return f'{task_id} = BashOperator(task_id="{task_id}", bash_command="{cmd}")'

    if ttype == "PythonOperator":
        fn = _sanitize_id(t.get("python_callable_name") or f"fn_{task_id}")
        return f'{task_id} = PythonOperator(task_id="{task_id}", python_callable={fn})'

    if ttype == "SqliteOperator":
        return f'{task_id} = SqliteOperator(task_id="{task_id}", sqlite_conn_id="{t.get("sqlite_conn_id","sqlite_default")}", sql="{_quote(t.get("sql","SELECT 1;"))}")'

    if ttype == "PostgresOperator":
        return f'{task_id} = PostgresOperator(task_id="{task_id}", postgres_conn_id="{t.get("postgres_conn_id","postgres_default")}", sql="{_quote(t.get("sql","SELECT 1;"))}")'

    if ttype == "MySqlOperator":
        return f'{task_id} = MySqlOperator(task_id="{task_id}", mysql_conn_id="{t.get("mysql_conn_id","mysql_default")}", sql="{_quote(t.get("sql","SELECT 1;"))}")'

    if ttype == "MsSqlOperator":
        return f'{task_id} = MsSqlOperator(task_id="{task_id}", mssql_conn_id="{t.get("mssql_conn_id","mssql_default")}", sql="{_quote(t.get("sql","SELECT 1;"))}")'

    if ttype == "OracleOperator":
        return f'{task_id} = OracleOperator(task_id="{task_id}", oracle_conn_id="{t.get("oracle_conn_id","oracle_default")}", sql="{_quote(t.get("sql","SELECT 1 FROM dual"))}")'

    if ttype == "SnowflakeOperator":
        return f'{task_id} = SnowflakeOperator(task_id="{task_id}", snowflake_conn_id="{t.get("snowflake_conn_id","snowflake_default")}", sql="{_quote(t.get("sql","SELECT 1;"))}")'

    if ttype == "S3CreateObjectOperator":
        return (f'{task_id} = S3CreateObjectOperator(task_id="{task_id}", aws_conn_id="{t.get("aws_conn_id","aws_default")}", '
                f's3_bucket="{t.get("s3_bucket","my-bucket")}", s3_key="{t.get("s3_key","path/file.txt")}", data="{_quote(t.get("data","hello world"))}")')

    if ttype == "LambdaInvokeFunctionOperator":
        return (f'{task_id} = LambdaInvokeFunctionOperator(task_id="{task_id}", aws_conn_id="{t.get("aws_conn_id","aws_default")}", '
                f'function_name="{t.get("function_name","my-fn")}", payload="{_quote(t.get("payload","{}"))}")')

    if ttype == "GlueJobOperator":
        return (f'{task_id} = GlueJobOperator(task_id="{task_id}", aws_conn_id="{t.get("aws_conn_id","aws_default")}", '
                f'job_name="{t.get("job_name","my-job")}", script_location="{t.get("script_location","s3://bucket/script.py")}", '
                f'iam_role_name="{t.get("iam_role_name","glue-role")}", num_of_dpus={int(t.get("num_of_dpus") or 10)})')

    if ttype == "BigQueryExecuteQueryOperator":
        return (f'{task_id} = BigQueryExecuteQueryOperator(task_id="{task_id}", gcp_conn_id="{t.get("gcp_conn_id","google_cloud_default")}", '
                f'sql="{_quote(t.get("sql","SELECT 1;"))}", use_legacy_sql={_fmt_bool(t.get("use_legacy_sql","false"))})')

    if ttype == "DataflowCreateJavaJobOperator":
        options = _quote(t.get("options") or "{}")
        return (f'{task_id} = DataflowCreateJavaJobOperator(task_id="{task_id}", gcp_conn_id="{t.get("gcp_conn_id","google_cloud_default")}", '
                f'jar="{t.get("jar","/path/app.jar")}", job_name="{t.get("job_name","dataflow-job")}", options="{options}")')

    if ttype == "AzureDataFactoryRunPipelineOperator":
        params = _quote(t.get("parameters") or "{}")
        return (f'{task_id} = AzureDataFactoryRunPipelineOperator(task_id="{task_id}", '
                f'azure_data_factory_conn_id="{t.get("azure_data_factory_conn_id","azure_data_factory_default")}", '
                f'pipeline_name="{t.get("pipeline_name","pipeline1")}", parameters="{params}")')

    if ttype == "DockerOperator":
        return (f'{task_id} = DockerOperator(task_id="{task_id}", image="{t.get("image","python:3.11")}", '
                f'api_version="{t.get("api_version","auto")}", command="{_quote(t.get("command","echo hello"))}", '
                f'docker_url="{t.get("docker_url","unix://var/run/docker.sock")}", auto_remove={_fmt_bool(t.get("auto_remove","true"))})')

    if ttype == "KubernetesPodOperator":
        cmds = t.get("cmds") or "[]"
        args = t.get("arguments") or "[]"
        return (f'{task_id} = KubernetesPodOperator(task_id="{task_id}", name="{t.get("name","pod-task")}", '
                f'namespace="{t.get("namespace","default")}", image="{t.get("image","python:3.11")}", '
                f'cmds={cmds}, arguments={args})')

    if ttype == "SparkSubmitOperator":
        args = t.get("application_args") or "[]"
        return (f'{task_id} = SparkSubmitOperator(task_id="{task_id}", application="{t.get("application","/path/app.py")}", '
                f'conn_id="{t.get("conn_id","spark_default")}", application_args={args})')

    if ttype == "DatabricksSubmitRunOperator":
        payload = t.get("json") or "{}"
        return (f'{task_id} = DatabricksSubmitRunOperator(task_id="{task_id}", '
                f'databricks_conn_id="{t.get("databricks_conn_id","databricks_default")}", json={payload})')

    if ttype == "SimpleHttpOperator":
        data = t.get("data") or ""
        headers = t.get("headers") or ""
        return (f'{task_id} = SimpleHttpOperator(task_id="{task_id}", http_conn_id="{t.get("http_conn_id","http_default")}", '
                f'endpoint="{t.get("endpoint","/")}", method="{(t.get("method") or "GET").upper()}", data={repr(data)}, headers={repr(headers)})')

    if ttype == "EmailOperator":
        to = t.get("to", "example@example.com")
        subj = _quote(t.get("subject") or "Airflow Notification")
        html = _quote(t.get("html_content") or "Job done.")
        return f'{task_id} = EmailOperator(task_id="{task_id}", to="{to}", subject="{subj}", html_content="{html}")'

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
            f')'
        )

    if ttype == "TriggerDagRunOperator":
        # dispara otro DAG (no espera a task específica)
        return (
            f'{task_id} = TriggerDagRunOperator('
            f'task_id="{task_id}", '
            f'trigger_dag_id="{_quote(t.get("trigger_dag_id","other_dag"))}"'
            f')'
        )


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
    owner = spec.get("owner") or "owner"
    retries = int(spec.get("retries") or 1)
    retry_minutes = int(spec.get("retry_minutes") or 5)
    try:
        y, m, d = [int(x) for x in (spec.get("start_date") or "2025-01-01").split("-")]
    except Exception:
        y, m, d = 2025, 1, 1
    schedule = spec.get("schedule") or "@daily"
    catchup = bool(spec.get("catchup", False))
    tags = spec.get("tags") or []

    # Imports por operador
    operator_imports = _imports_for_tasks(spec.get("tasks") or [])
    header = _HEADER_IMPORTS.format(operator_imports="\n".join(operator_imports))

    # default_args + callables
    default_args = _format_default_args(owner, retries, retry_minutes)
    pydefs = _render_python_callables(spec.get("tasks") or [])

    # Tareas + dependencias
    task_ids: set[str] = {_sanitize_id(t.get("task_id") or "") for t in (spec.get("tasks") or []) if t.get("task_id")}
    tasks_section = _render_tasks(spec.get("tasks") or [])
    deps_section, warn_deps = _render_dependencies(spec.get("dependencies") or [], task_ids)
    tags_list = ", ".join([f'"{t}"' for t in tags])

    warnings_block = ""
    all_warns = []
    if warn_dedupe:
        all_warns.extend(warn_dedupe)
    if 'warn_deps' in locals() and warn_deps:
        all_warns.extend(warn_deps)
    if all_warns:
        warnings_block = "# WARNINGS:\n# " + "\n# ".join(all_warns) + "\n\n"

    code = f'''{warnings_block}{header}
{default_args}
{pydefs}with DAG(
    dag_id="{dag_id}",
    description="{description}",
    start_date=datetime({y}, {m}, {d}),
    schedule="{schedule}",
    catchup={str(catchup)},
    default_args=default_args,
    tags=[{tags_list}]
) as dag:
    {tasks_section}
    {deps_section}
'''
    return code
