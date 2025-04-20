# Import Libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, dayofweek, hour, quarter
import os

# Load PostgreSQL connection details from environment variables
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

# Connection Setup between Spark and RDBMS
jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver'
}

# Helper Function
def create_spark_session(app_name: str, log_level: str, partitions: str ) -> SparkSession:
    """
    Create and return a SparkSession object.
    
    Parameters:
        app_name (str): The name of the Spark application.
        log_level (str): Logging level ("ALL", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF").
        partitions (str): Number of partitions to be used by the SparkSession.
    
    Returns:
        SparkSession object.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", partitions) \
        .getOrCreate()

    spark.sparkContext.setLogLevel(log_level)
    
    return spark

# Instantiate SparkSession
spark = create_spark_session("Assignment_D22", "WARN", "8")

# Helper function
def create_df_from_db(url: str, properties: dict, table_name: str) -> DataFrame:
    """
    Loads a PostgreSQL table into a Spark DataFrame using JDBC.

    Parameters:
        url (str): JDBC URL to connect to the PostgreSQL database (e.g., 'jdbc:postgresql://host:port/database').
        properties (dict): A dictionary of JDBC connection properties such as user, password, driver, and optional settings.
        table_name (str): The name of the table to read. Can be in the form 'schema.table' or a subquery alias.

    Returns:
        DataFrame.
    """
    return spark.read.jdbc(
        url=url,
        table=table_name,
        properties=properties
    )

retail_df = create_df_from_db(jdbc_url, jdbc_properties, 'public.retail')
retail_df.cache()

# Helper function
def clean_and_transform_retail_df(retail_df: DataFrame) -> DataFrame:
    """
    Cleans and transforms the retail dataset by applying the following steps:
    - Filters out non-product records based on StockCode patterns
    - Removes transactions with non-positive Quantity or UnitPrice
    - Casts InvoiceNo to integer
    - Parses InvoiceDate into timestamp format
    - Fills missing values in InvoiceNo, CustomerID, and Description
    - Extracts temporal features: Year, Month, Day, DayOfWeek, Hour, and Quarter

    Args:
        retail_df (DataFrame): The raw input retail DataFrame

    Returns:
        DataFrame: A cleaned and enriched retail DataFrame containing only valid product sales
    """
    retail_df = retail_df.filter(col("StockCode").rlike("^[0-9]+[A-Z]?$"))
    retail_df = retail_df.filter((col("Quantity") > 0) & (col("UnitPrice") > 0))
    retail_df = retail_df.withColumn("InvoiceNo", col("InvoiceNo").cast("int"))
    retail_df = retail_df.withColumn("InvoiceDate", to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))
    retail_df = retail_df.fillna({
        "InvoiceNo": 0,         
        "CustomerID": 0,
        "Description": "None temp"
    })
    retail_df = retail_df \
        .withColumn("Year", year("InvoiceDate")) \
        .withColumn("Month", month("InvoiceDate")) \
        .withColumn("Day", dayofmonth("InvoiceDate")) \
        .withColumn("DayOfWeek", dayofweek("InvoiceDate")) \
        .withColumn("Hour", hour("InvoiceDate")) \
        .withColumn("Quarter", quarter("InvoiceDate"))

    return retail_df

retail_df = clean_and_transform_retail_df(retail_df)

# Helper function
def run_sql_query(df: DataFrame, temp_view_name: str, sql_query: str) -> DataFrame:
    """
    Registers a Spark DataFrame as a temporary view and executes a SQL query on it.

    Args:
        df (DataFrame): The Spark DataFrame to register as a temporary view.
        temp_view_name (str): The name of the temporary view to create.
        sql_query (str): The SQL query to run against the temporary view.

    Returns:
        DataFrame: The resulting Spark DataFrame after executing the SQL query.
    """
    df.createOrReplaceTempView(temp_view_name)
    result_df = spark.sql(sql_query)
    
    return result_df

rfm_df = run_sql_query(
    df = retail_df,
    temp_view_name = "retail",
    sql_query = """
    WITH max_date AS (
    SELECT MAX(InvoiceDate) AS max_date FROM retail
    ),
    rfm AS (
    SELECT
        CustomerID,
        DATEDIFF((SELECT max_date FROM max_date), MAX(InvoiceDate)) AS Recency,
        COUNT(DISTINCT InvoiceNo) AS Frequency,
        ROUND(SUM(Quantity * UnitPrice), 2) AS Monetary
    FROM retail
    GROUP BY CustomerID
    ),
    scored_rfm AS (
    SELECT *,
        CASE
        WHEN Recency <= 30 THEN 3
        WHEN Recency <= 90 THEN 2
        ELSE 1
        END AS R_Score,
        CASE
        WHEN Frequency >= 10 THEN 3
        WHEN Frequency >= 5 THEN 2
        ELSE 1
        END AS F_Score,
        CASE
        WHEN Monetary >= 1000 THEN 3
        WHEN Monetary >= 500 THEN 2
        ELSE 1
        END AS M_Score
    FROM rfm
    )
    SELECT *,
        R_Score + F_Score + M_Score AS RFM_Score,
        CASE
            WHEN (R_Score + F_Score + M_Score) >= 8 THEN 'High Value'
            WHEN (R_Score + F_Score + M_Score) >= 5 THEN 'Mid Value'
            ELSE 'Low Value'
        END AS Segment
    FROM scored_rfm
    """
)
rfm_df.show()
retail_df.unpersist()