# Buckets and table need to already exist for glue 3.0: https://github.com/awslabs/aws-glue-libs/issues/64
import sys # Need for sys.argv argument, parameter in getResolvedOptions method
from pyspark.sql import DataFrame, SparkSession

from pyspark.sql.functions import size, col, explode, to_date, date_format, round, udf, max, monotonically_increasing_id, coalesce
from pyspark.sql.types import FloatType, StringType, LongType, DateType, TimestampType, DoubleType

# https://spark.apache.org/docs/2.3.0/configuration.html
# https://stackoverflow.com/questions/45704156/what-is-the-difference-between-spark-sql-shuffle-partitions-and-spark-default-pa
# https://spark.apache.org/docs/latest/configuration.html
# https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
# https://www.geeksforgeeks.org/overview-of-dynamic-partition-in-hive/
# https://stackoverflow.com/questions/54116332/what-is-the-difference-between-dynamic-partition-true-and-dynamic-partition-mode
# https://stackoverflow.com/questions/40200389/how-to-execute-spark-programs-with-dynamic-resource-allocation
# https://spark.apache.org/docs/latest/configuration.html
# https://spark.apache.org/docs/latest/sql-performance-tuning.html

spark = SparkSession.builder \
    .appName('airflow-weather-project-emr-clean-to-analytic-minutely') \
    .config('spark.memory.useLegacyMode', 'false') \
    .config('spark.default.parallelism', '2') \
    .config('spark.sql.shuffle.partitions', '1') \
    .config('spark.port.maxretries', '32') \
    .config('spark.rdd.compress', 'true') \
    .config('hive.exec.dynamic.partition', 'true') \
    .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
    .config('spark.shuffle.service.enabled', 'true') \
    .config('spark.dynamicAllocation.enabled', 'true') \
    .config('spark.dynamicAllocation.minExecutors', '1') \
    .config('spark.dynamicAllocation.maxExecutors', '10') \
    .config('spark.dynamicAllocation.initialExecutors', '5') \
    .config('spark.dynamicAllocation.executorAllocationRatio', '1') \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.speculation', 'false') \
    .config('spark.sql.caseSensitive', 'false') \
    .config('spark.sql.broadcastTimeout', '120') \
    .config('spark.network.timeout', '120') \
    .config('spark.driver.memoryOverhead', '1024') \
    .config('spark.executor.memoryOverhead', '1024') \
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .enableHiveSupport() \
    .getOrCreate()

# Step 1: Grab latest_processed_data_date from minutely data clean layer
spark.catalog.setCurrentDatabase('airflow-weather-project-analytic')
df = spark.sql('''select case when max(current_weather_time) is null then cast('1970-01-01' as timestamp) else max(current_weather_time) end as max_current_weather_time from airflow_weather_project_analytic_minutely_weather''')
latest_processed_data_date = df.select('max_current_weather_time').collect()[0][0]

# Step 2: Read into df data from raw layer that is new based on the latest_processed_data_date
# spark.catalog.setCurrentDatabase('airflow-weather-project-clean')
df_spark_2 = spark.read.parquet('/user/hadoop/processed/clean/minutely')
df_spark_2.createOrReplaceTempView('airflow_weather_project_clean_minutely_weather')
df_spark_3 = spark.sql(f'''select * from airflow_weather_project_clean_minutely_weather where current_weather_time > cast('{latest_processed_data_date}' as string)''')

if df_spark_3.take(1):
    # Step 3: Drop cols
    df_drop_3 = df_spark_3.drop('current_dt')

    # Step 4: Reorder cols
    dr_reorder_4 = df_drop_3.select(
        'lat',
        'lon',
        'timezone',
        'timezone_offset',
        'current_weather_time',
        'current_weather_date',
        'current_weather_date_partition',
        'time_precipitation_amt_dt',
        'time_precipitation_amt_precipitation'
    )

    # Step 5: Write to S3
    dr_reorder_4.write.mode('overwrite').partitionBy('current_weather_date_partition') \
        .parquet('/user/hadoop/processed/analytic/minutely')