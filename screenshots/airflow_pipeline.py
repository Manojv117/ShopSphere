
import pendulum
import logging

from airflow.sdk import dag, task
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="shopsphere_databricks_dag",
    description="Complete ShopSphere pipeline with failure monitoring",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    tags=["shopsphere", "databricks", "monitoring"],
)
def shopsphere_databricks_pipeline():

    bronze = DatabricksRunNowOperator(
        task_id="trigger_bronze_ingestion",
        databricks_conn_id="databricks_default",
        job_id=119894852359715,
	retries=2,
	retry_delay=pendulum.duration(minutes=1),
    )

    silver = DatabricksRunNowOperator(
        task_id="trigger_silver_transformation",
        databricks_conn_id="databricks_default",
        job_id=392702898224180,
	retries=2,
	retry_delay=pendulum.duration(minutes=1),
    )

    gold = DatabricksRunNowOperator(
        task_id="trigger_gold_analytics",
        databricks_conn_id="databricks_default",
        job_id=1002336653876322,
	retries=2,
	retry_delay=pendulum.duration(minutes=1),
    )

    quality = DatabricksRunNowOperator(
        task_id="trigger_data_quality",
        databricks_conn_id="databricks_default",
        job_id=533329640869178,
	retries=2,
	retry_delay=pendulum.duration(minutes=1),
    )

    @task(trigger_rule="all_done")
    def pipeline_status(**context):

        ti = context["ti"]

        task_ids = [
            "trigger_bronze_ingestion",
            "trigger_silver_transformation",
            "trigger_gold_analytics",
            "trigger_data_quality",
        ]

        states_by_run = ti.get_task_states(
            dag_id=ti.dag_id,
            run_ids=[ti.run_id],
            task_ids=task_ids,
        )

        task_states = states_by_run.get(ti.run_id, {})

        failed_tasks = []

        for task_id in task_ids:
            state = task_states.get(task_id, "unknown")
            state = getattr(state, "value", state)

            logger.info("Task: %s | Status: %s", task_id, state)

            if state in ["failed", "upstream_failed"]:
                failed_tasks.append(task_id)

        if failed_tasks:
            logger.error("SHOPSPHERE PIPELINE FAILED!")
            logger.error("Failed tasks: %s", failed_tasks)
        else:
            logger.info("SHOPSPHERE PIPELINE COMPLETED SUCCESSFULLY!")

    status = pipeline_status()

    bronze >> silver >> gold >> quality >> status


shopsphere_databricks_pipeline()
