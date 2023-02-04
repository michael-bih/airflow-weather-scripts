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
    .appName('airflow-weather-project-emr-clean-to-analytic-current') \
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

# Step 1: Grab latest_processed_data_date from current data clean layer
spark.catalog.setCurrentDatabase('airflow-weather-project-analytic')
df = spark.sql('''select case when max(current_weather_time) is null then cast('1970-01-01' as timestamp) else max(current_weather_time) end as max_current_weather_time from airflow_weather_project_analytic_current_weather''')
latest_processed_data_date = df.select('max_current_weather_time').collect()[0][0]

# Step 2: Read into df data from raw layer that is new based on the latest_processed_data_date
# https://stackoverflow.com/questions/27069537/sparksql-timestamp-query-failure
df_spark_2 = spark.read.parquet('/user/hadoop/processed/clean/current')
df_spark_2.createOrReplaceTempView('airflow_weather_project_clean_current_weather')
# spark.catalog.setCurrentDatabase('airflow-weather-project-clean')
df_spark_3 = spark.sql(f'''select * from airflow_weather_project_clean_current_weather where current_weather_time > cast('{latest_processed_data_date}' as string)''')

if df_spark_3.take(1):
    # Step 3: Adjust cols
    df_rename_4 = df_spark_3 \
        .withColumnRenamed('current_weather[0]_main', 'current_weather_state') \
        .withColumnRenamed('current_weather[0]_description', 'current_weather_state_desc') \
        .drop('current_weather[0]_id', 'current_weather[0]_icon', 'current_dt')

    # Step 4: Adjust sunrise and sunset dts
    df_ss_4 = df_rename_4 \
        .withColumn('current_sunrise_time', col('current_sunrise').cast(TimestampType())) \
        .withColumn('current_sunset_time', col('current_sunset').cast(TimestampType())) \
        .drop('current_sunrise', 'current_sunset')

    # Step 5: Reorder cols
    cols_to_order = [
        'lat',
        'lon',
        'timezone',
        'timezone_offset',
        'current_weather_time',
        'current_weather_date',
        'current_weather_date_partition',
        'current_sunrise_time',
        'current_sunset_time'
    ]

    cols_other = sorted(list(set(df_ss_4.columns) - set(cols_to_order)))

    df_reorder_5 = df_ss_4.select(*cols_to_order, *cols_other)

    df_repart_6 = df_reorder_5.repartition(1)

    # Step 6: Cast cols to double
    # df_final_7 = df_repart_6 \
    #     .withColumn('current_uvi', col('current_uvi').cast(DoubleType())) \
    #     .withColumn('current_dew_point', col('current_dew_point').cast(DoubleType())) \
    #     .withColumn('current_feels_like', col('current_feels_like').cast(DoubleType())) \
    #     .withColumn('current_temp', col('current_temp').cast(DoubleType())) \
    #     .withColumn('current_wind_gust', col('current_wind_gust').cast(DoubleType())) \
    #     .withColumn('current_wind_speed', col('current_wind_speed').cast(DoubleType()))

    df_repart_6.write.mode('overwrite').partitionBy('current_weather_date_partition') \
        .parquet('/user/hadoop/processed/analytic/current')