# import library/dependencies
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

# DAG decorator
@dag(
    # DAG parameters
    dag_id="dag_assignment_22",
    description="DAG etl untuk assignment 22",
    tags=["assignment"],
    default_args={
        "owner": "Imam",
        "retry_delay": timedelta(minutes=5)
    },
    owner_links={
        "Imam": "https://www.linkedin.com/in/mimamwahid/"
    },
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    start_date=days_ago(1)
)
# Main function
def main():
    # task 1
    start = EmptyOperator(task_id="start")
    # task 2
    etl = SparkSubmitOperator(
        application="/spark-scripts/spark_assignment_22.py",
        conn_id="spark_main",
        task_id="spark_submit_task",
        packages="org.postgresql:postgresql:42.2.18"
)
    # task 3
    end = EmptyOperator(task_id="end")   
    # Control flow
    start >> etl >> end
# Call the DAG main function
main()