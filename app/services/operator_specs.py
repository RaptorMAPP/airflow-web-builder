from __future__ import annotations

from typing import Any, Dict, List

from .render_utils import (
    base_operator_kwargs,
    fmt_bool,
    parse_kwargs_json,
    quote,
    py_str,
    sanitize_id,
    to_py_literal,
)

# ────────────────────────────────────────────────────────────────────────────────
# Catálogo de operadores (Airflow 3.1)
# DummyOperator se mapea a EmptyOperator.
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

# Para argsets globales (solo command-like por ahora)
ARGS_TARGET_FIELD: Dict[str, str] = {
    "SSHOperator": "command",
    "BashOperator": "bash_command",
    "WinRMOperator": "command",
    "DockerOperator": "command",
}


def resolve_operator_key(ttype: str) -> str:
    return "EmptyOperator" if ttype == "DummyOperator" else ttype


def imports_for_tasks(tasks: List[Dict[str, Any]]) -> List[str]:
    used: set[str] = set()
    for t in tasks or []:
        typ = resolve_operator_key(str(t.get("type") or ""))
        imp = SUPPORTED_OPERATORS.get(typ, {}).get("import")
        if imp:
            used.add(imp)
        cimp = t.get("_custom_import")
        if cimp:
            used.add(cimp)
    return sorted(used)


def render_task_line(t: Dict[str, Any], params: Dict[str, Any] | None = None) -> str:
    """Renderiza una línea de instanciación del operador según type."""
    ttype = t["type"]
    task_id = sanitize_id(t["task_id"])

    # kwargs comunes
    bo = base_operator_kwargs(t, params_dict=params)

    if ttype == "DummyOperator":
        ttype = "EmptyOperator"

    if ttype == "EmptyOperator":
        return f'{task_id} = EmptyOperator(task_id="{task_id}"{bo})'

    if ttype == "BashOperator":
        cmd = py_str(t.get("bash_command") or 'echo "hello"')
        return f'{task_id} = BashOperator(task_id="{task_id}", bash_command={cmd}{bo})'

    if ttype == "PythonOperator":
        fn = sanitize_id(t.get("python_callable_name") or f"fn_{task_id}")
        return f'{task_id} = PythonOperator(task_id="{task_id}", python_callable={fn}{bo})'

    if ttype == "SqliteOperator":
        return f'{task_id} = SqliteOperator(task_id="{task_id}", sqlite_conn_id="{t.get("sqlite_conn_id", "sqlite_default")}", sql={py_str(t.get("sql", "SELECT 1;"))}{bo})'

    if ttype == "PostgresOperator":
        return f'{task_id} = PostgresOperator(task_id="{task_id}", postgres_conn_id="{t.get("postgres_conn_id", "postgres_default")}", sql={py_str(t.get("sql", "SELECT 1;"))}{bo})'

    if ttype == "MySqlOperator":
        return f'{task_id} = MySqlOperator(task_id="{task_id}", mysql_conn_id="{t.get("mysql_conn_id", "mysql_default")}", sql={py_str(t.get("sql", "SELECT 1;"))}{bo})'

    if ttype == "MsSqlOperator":
        return f'{task_id} = MsSqlOperator(task_id="{task_id}", mssql_conn_id="{t.get("mssql_conn_id", "mssql_default")}", sql={py_str(t.get("sql", "SELECT 1;"))}{bo})'

    if ttype == "OracleOperator":
        return f'{task_id} = OracleOperator(task_id="{task_id}", oracle_conn_id="{t.get("oracle_conn_id", "oracle_default")}", sql={py_str(t.get("sql", "SELECT 1 FROM dual"))}{bo})'

    if ttype == "SnowflakeOperator":
        return f'{task_id} = SnowflakeOperator(task_id="{task_id}", snowflake_conn_id="{t.get("snowflake_conn_id", "snowflake_default")}", sql={py_str(t.get("sql", "SELECT 1;"))}{bo})'

    if ttype == "S3CreateObjectOperator":
        return (
            f'{task_id} = S3CreateObjectOperator(task_id="{task_id}", aws_conn_id="{t.get("aws_conn_id", "aws_default")}", '
            f's3_bucket="{t.get("s3_bucket", "my-bucket")}", s3_key="{t.get("s3_key", "path/file.txt")}", data={py_str(t.get("data", "hello world"))}{bo})'
        )

    if ttype == "LambdaInvokeFunctionOperator":
        return (
            f'{task_id} = LambdaInvokeFunctionOperator(task_id="{task_id}", aws_conn_id="{t.get("aws_conn_id", "aws_default")}", '
            f'function_name="{t.get("function_name", "my-fn")}", payload={py_str(t.get("payload", "{}"))}{bo})'
        )

    if ttype == "GlueJobOperator":
        return (
            f'{task_id} = GlueJobOperator(task_id="{task_id}", aws_conn_id="{t.get("aws_conn_id", "aws_default")}", '
            f'job_name="{t.get("job_name", "my-job")}", script_location="{t.get("script_location", "s3://bucket/script.py")}", '
            f'iam_role_name="{t.get("iam_role_name", "glue-role")}", num_of_dpus={int(t.get("num_of_dpus") or 10)}{bo})'
        )

    if ttype == "BigQueryExecuteQueryOperator":
        return (
            f'{task_id} = BigQueryExecuteQueryOperator(task_id="{task_id}", gcp_conn_id="{t.get("gcp_conn_id", "google_cloud_default")}", '
            f'sql={py_str(t.get("sql", "SELECT 1;"))}, use_legacy_sql={fmt_bool(t.get("use_legacy_sql", "false"))}{bo})'
        )

    if ttype == "DataflowCreateJavaJobOperator":
        options = quote(t.get("options") or "{}")
        return (
            f'{task_id} = DataflowCreateJavaJobOperator(task_id="{task_id}", gcp_conn_id="{t.get("gcp_conn_id", "google_cloud_default")}", '
            f'jar="{t.get("jar", "/path/app.jar")}", job_name="{t.get("job_name", "dataflow-job")}", options="{options}"{bo})'
        )

    if ttype == "AzureDataFactoryRunPipelineOperator":
        params_s = quote(t.get("parameters") or "{}")
        return (
            f'{task_id} = AzureDataFactoryRunPipelineOperator(task_id="{task_id}", '
            f'azure_data_factory_conn_id="{t.get("azure_data_factory_conn_id", "azure_data_factory_default")}", '
            f'pipeline_name="{t.get("pipeline_name", "pipeline1")}", parameters="{params_s}"{bo})'
        )

    if ttype == "DockerOperator":
        return (
            f'{task_id} = DockerOperator(task_id="{task_id}", image="{t.get("image", "python:3.11")}", '
            f'api_version="{t.get("api_version", "auto")}", command="{quote(t.get("command", "echo hello"))}", '
            f'docker_url="{t.get("docker_url", "unix://var/run/docker.sock")}", auto_remove={fmt_bool(t.get("auto_remove", "true"))}{bo})'
        )

    if ttype == "KubernetesPodOperator":
        env_literal = to_py_literal(t.get("env"), "{}")
        name = quote(t.get("name", "pod-task"))
        namespace = quote(t.get("namespace", "default"))
        image = quote(t.get("image", "python:3.11"))
        cmds = to_py_literal(t.get("cmds", '["python","-c"]'), "[]")
        args = to_py_literal(t.get("arguments", '["print(\\"hi\\")"]'), "[]")
        return (
            f'{task_id} = KubernetesPodOperator(task_id="{task_id}", name="{name}", '
            f'namespace="{namespace}", image="{image}", cmds={cmds}, arguments={args}, '
            f'env_vars={env_literal}{bo})'
        )

    if ttype == "SparkSubmitOperator":
        args = to_py_literal(t.get("application_args") or "[]", "[]")
        return (
            f'{task_id} = SparkSubmitOperator(task_id="{task_id}", application="{t.get("application", "/path/app.py")}", '
            f'conn_id="{t.get("conn_id", "spark_default")}", application_args={args}{bo})'
        )

    if ttype == "DatabricksSubmitRunOperator":
        payload = to_py_literal(t.get("json") or "{}", "{}")
        return (
            f'{task_id} = DatabricksSubmitRunOperator(task_id="{task_id}", '
            f'databricks_conn_id="{t.get("databricks_conn_id", "databricks_default")}", json={payload}{bo})'
        )

    if ttype == "SimpleHttpOperator":
        data = t.get("data") or ""
        headers = t.get("headers") or ""
        return (
            f'{task_id} = SimpleHttpOperator(task_id="{task_id}", http_conn_id="{t.get("http_conn_id", "http_default")}", '
            f'endpoint="{t.get("endpoint", "/")}", method="{(t.get("method") or "GET").upper()}", '
            f'data={repr(data)}, headers={repr(headers)}{bo})'
        )

    if ttype == "EmailOperator":
        to = t.get("to", "example@example.com")
        subj = py_str(t.get("subject") or "Airflow Notification")
        html = py_str(t.get("html_content") or "Job done.")
        return f'{task_id} = EmailOperator(task_id="{task_id}", to="{to}", subject={subj}, html_content={html}{bo})'

    if ttype == "ExternalTaskSensor":
        return (
            f'{task_id} = ExternalTaskSensor('
            f'task_id="{task_id}", '
            f'external_dag_id="{quote(t.get("external_dag_id", "other_dag"))}", '
            f'external_task_id={repr(t.get("external_task_id") or None)}, '
            f'mode="{t.get("mode", "reschedule")}", '
            f'poke_interval={int(t.get("poke_interval") or 60)}, '
            f'timeout={int(t.get("timeout") or 3600)}'
            f'{bo})'
        )

    if ttype == "TriggerDagRunOperator":
        return (
            f'{task_id} = TriggerDagRunOperator('
            f'task_id="{task_id}", '
            f'trigger_dag_id="{quote(t.get("trigger_dag_id", "other_dag"))}"'
            f'{bo})'
        )

    if ttype == "CustomOperator":
        module = (t.get("import_path") or "").strip()
        cls = (t.get("class_name") or "").strip()
        kwargs = parse_kwargs_json(t.get("kwargs"))
        if not module or not cls:
            return f'{task_id} = EmptyOperator(task_id="{task_id}")  # CUSTOM MISSING IMPORT/CLASS'
        t["_custom_import"] = f"from {module} import {cls}"
        return f'{task_id} = {cls}(task_id="{task_id}", **({kwargs}){bo})'

    if ttype == "SSHOperator":
        cmd = py_str(t.get("command", "echo hello"))
        ssh_conn_id = quote(t.get("ssh_conn_id", "ssh_default"))
        remote_host = t.get("remote_host")
        get_pty = str(t.get("get_pty", "true")).lower() in ("true", "1")
        env_literal = to_py_literal(t.get("environment"), "{}")
        extra = f', remote_host="{remote_host}"' if remote_host else ""
        return (
            f'{task_id} = SSHOperator(task_id="{task_id}", ssh_conn_id="{ssh_conn_id}", '
            f'command={cmd}, get_pty={get_pty}, environment={env_literal}{extra}{bo})'
        )

    if ttype == "WinRMOperator":
        winrm_conn_id = quote(t.get("winrm_conn_id", "winrm_default"))
        cmd = py_str(t.get("command") or 'Write-Output "Hello"')
        ps = str(t.get("powershell", "true")).lower() in ("true", "1")
        return (
            f'{task_id} = WinRMOperator(task_id="{task_id}", winrm_conn_id="{winrm_conn_id}", '
            f'command={cmd}, powershell={ps}{bo})'
        )

    return ""  # tipo no soportado
