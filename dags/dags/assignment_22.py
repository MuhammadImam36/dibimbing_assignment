from airflow.decorators import dag
from datetime import timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

# DAG decorator
@dag(
    # DAG parameters
    dag_id="dag_etl_retail_table",
    description="DAG extract_load untuk assignment Dibimbing",
    tags=["assignment"],
    default_args={
        "owner": "Imam",
    },
    owner_links={
        "Imam": "https://www.linkedin.com/in/mimamwahid/"
    },
    schedule_interval=None,
    start_date=days_ago(1)
)

# Main function
def main():
    # Task 1
    start = EmptyOperator(task_id="start")
    # Task 2
    etl = SparkSubmitOperator(
        application="/spark-scripts/spark_assignment22.py",
        conn_id="spark_main",
        task_id="spark_submit_task",
        packages="org.postgresql:postgresql:42.2.18"
    )
    # Task 3
    end = EmptyOperator(task_id="end")
    
    # Control flow
    start >> etl >> end
    
# Call the DAG main function
main()