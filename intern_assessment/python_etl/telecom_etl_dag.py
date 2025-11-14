from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# -----------------------------
# DAG DEFAULT ARGUMENTS
# -----------------------------
default_args = {
    "owner": "krish",
    "depends_on_past": False,
    "email": ["admin@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# -----------------------------
# DAG DEFINITION
# -----------------------------
with DAG(
    dag_id="telecom_etl_dag",
    default_args=default_args,
    description="Daily Telecom ETL Pipeline",
    schedule_interval="0 0 * * *",      # runs every day at midnight
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["telecom", "etl", "python"],
) as dag:

    # -----------------------------
    # TASK 1: Validate Input Files
    # -----------------------------
    validate_files = BashOperator(
        task_id="validate_input_files",
        bash_command="python3 /home/krish/etl_assessment/intern_assessment/python_etl/telecom_etl.py validate"
    )

    # -----------------------------
    # TASK 2: Clean Customer Data
    # -----------------------------
    clean_customer = BashOperator(
        task_id="clean_customer_data",
        bash_command="python3 /home/krish/etl_assessment/intern_assessment/python_etl/telecom_etl.py clean_customers"
    )

    # -----------------------------
    # TASK 3: Deduplicate Usage Data
    # -----------------------------
    dedupe_usage = BashOperator(
        task_id="deduplicate_usage_data",
        bash_command="python3 /home/krish/etl_assessment/intern_assessment/python_etl/telecom_etl.py dedupe_usage"
    )

    # -----------------------------
    # TASK 4: Aggregate Billing Data
    # -----------------------------
    aggregate_billing = BashOperator(
        task_id="aggregate_billing_data",
        bash_command="python3 /home/krish/etl_assessment/intern_assessment/python_etl/telecom_etl.py aggregate_billing"
    )

    # -----------------------------
    # TASK 5: Create Final Report
    # -----------------------------
    final_report = BashOperator(
        task_id="create_final_report",
        bash_command="python3 /home/krish/etl_assessment/intern_assessment/python_etl/telecom_etl.py final_report"
    )

    # -----------------------------
    # TASK DEPENDENCIES
    # -----------------------------
    validate_files >> clean_customer >> dedupe_usage >> aggregate_billing >> final_report
