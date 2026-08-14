"""
Beverage Analytics Pipeline DAG.

Uses BashOperator + papermill CLI — no external provider needed.
Each task executes a notebook and writes the executed copy to /executed_notebooks/.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

NB  = "/opt/airflow/notebooks"
OUT = "/opt/airflow/executed_notebooks"

_pm = "papermill --no-progress-bar"


def nb_task(task_id: str, notebook: str, dag):
    """Return a BashOperator that runs one notebook via papermill."""
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"mkdir -p {OUT} && "
            f"{_pm} {NB}/{notebook} {OUT}/{task_id}_{{{{ ds }}}}.ipynb"
        ),
        dag=dag,
    )


with DAG(
    dag_id="demonstration_pipeline",
    start_date=datetime(2026, 8, 14),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["beverage", "medallion", "delta"],
) as dag:

    t_bronze      = nb_task("bronze_ingest",       "bronze/ingest_raw.ipynb",                dag)
    t_silver      = nb_task("silver_enrich",        "silver/enrich_and_cleanse.ipynb",        dag)
    t_dim_date    = nb_task("dim_date",             "gold/dim_date.ipynb",                    dag)
    t_dim_product = nb_task("dim_product",          "gold/dim_product.ipynb",                 dag)
    t_dim_channel = nb_task("dim_channel",          "gold/dim_channel.ipynb",                 dag)
    t_dim_geo     = nb_task("dim_geography",        "gold/dim_geography.ipynb",               dag)
    t_fact        = nb_task("fact_sales",           "gold/fact_sales.ipynb",                  dag)
    t_summary     = nb_task("summary_tables",       "gold/summary_tables.ipynb",              dag)
    t_dq          = nb_task("dq_report",            "observability/dq_report.ipynb",          dag)

    # ── DAG topology ─────────────────────────────────────────────────
    # Dimensions run sequentially to stay within the 1 GB driver memory budget per JVM
    t_bronze >> t_silver
    t_silver >> t_dim_date >> t_dim_product >> t_dim_channel >> t_dim_geo
    t_dim_geo >> t_fact >> t_summary >> t_dq
