# Buckets and table need to already exist for glue 3.0: https://github.com/awslabs/aws-glue-libs/issues/64
import sys # Need for sys.argv argument, parameter in getResolvedOptions method
from pyspark.sql import DataFrame, SparkSession

from pyspark.sql.functions import size, col, explode, to_date, date_format, round, udf, max, monotonically_increasing_id, coalesce
from pyspark.sql.types import FloatType, StringType, LongType, DateType, TimestampType, DoubleType, IntegerType

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
    .appName('airflow-weather-project-emr-raw-to-clean-minutely') \
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

# Names all exploded cols with prefix to distinguish them from other cols
def flatten_struct_prefix(df: DataFrame, column: str) -> DataFrame:
    original_position = df.columns.index(column)

    new_columns = df.select(column + ".*").columns
    flattened_columns = [df[(column + "." + c)].alias(column + "_" + c) for c in new_columns]

    col_list = [df[c] for c in df.columns]
    col_list.pop(original_position)
    col_list[original_position:original_position] = flattened_columns

    return df.select(col_list)

# https://stackoverflow.com/questions/51648313/split-large-array-columns-into-multiple-columns-pyspark
def split_array_prefix(df: DataFrame, array_to_split: str) -> DataFrame:
    original_position = df.columns.index(array_to_split)
    
    columns = df.select(array_to_split).columns

    new_columns_size = df.select(*[size(col).alias(col) for col in columns])
    new_columns_max = new_columns_size.agg(*[max(col).alias(col) for col in columns])
        
    max_dict = new_columns_max.collect()[0].asDict()
    
    df_yes_array = df.select(*[df[col][i] for col in columns for i in range(max_dict[col])]).withColumn('id2', monotonically_increasing_id())
    
    col_list = [df[c] for c in df.columns]
    col_list.pop(original_position)
    
    df_no_array = df.select(col_list).withColumn('id1', monotonically_increasing_id())
    df = df_no_array.join(df_yes_array, col('id1') == col('id2'), 'inner').drop('id1', 'id2')
    
    return df

def flatten_struct_prefix_multiple(df: DataFrame, columns: str) -> DataFrame:
    col_list = [df[c] for c in df.columns]
    
    for index, each_col in enumerate(columns):
        original_position = df.columns.index(each_col) + index

        new_columns = df.select(each_col + ".*").columns
        flattened_columns = [df[(each_col + "." + c)].alias(each_col + "_" + c) for c in new_columns]
        
        col_list.pop(original_position)
        col_list[original_position:original_position] = flattened_columns

    return df.select(col_list)

# Step 1: Grab latest_processed_data_date from current data clean layer
spark.catalog.setCurrentDatabase('airflow-weather-project-clean')
df = spark.sql('select case when max(current_dt) is null then 0 else max(current_dt) end as max_current_dt from airflow_weather_project_clean_minutely_weather')
latest_processed_data_date = df.select('max_current_dt').collect()[0][0]

# Step 2: Read into df data from raw layer that is new based on the latest_processed_data_date
# spark.catalog.setCurrentDatabase('airflow-weather-project-raw')
df_spark_1 = spark.read.json('/user/hadoop/unprocessed/')
df_spark_1.createOrReplaceTempView('raw_weather_data')
df_spark_2 = spark.sql(f'''select * from raw_weather_data where current.dt > {latest_processed_data_date}''')

if df_spark_2.take(1):
    # Step 3: Get minutely weather into separate dataframe
    df_minutely_3 = df_spark_2.select('lat', 'lon', 'timezone', 'current.dt', 'timezone_offset', 'minutely')

    # Step 4: Split "minutely weather" array values into separate cols
    df_minutely_explode_4 = df_minutely_3.select('*', explode(col('minutely'))) \
        .drop('minutely') \
        .withColumnRenamed('dt', 'current_dt') \
        .withColumnRenamed('col', 'time_precipitation_amt')

    # Step 5: Flatten struct into dt and precipitation amt
    df_minutely_split_5 = flatten_struct_prefix(df_minutely_explode_4, 'time_precipitation_amt')

    # Step 6: Add date and timestamp cols
    df_final_6 = df_minutely_split_5 \
        .withColumn('current_weather_time', col('current_dt').cast(TimestampType())) \
        .withColumn('current_weather_date', to_date(col('current_weather_time')).cast(DateType())) \
        .withColumn('current_weather_date_partition', to_date(col('current_weather_time')).cast(DateType()))

    d_schema = {
        'lat': 'double',
        'lon': 'double',
        'timezone': 'string',
        'timezone_offset': 'integer',
        'current_dt': 'integer',
        'time_precipitation_amt_dt': 'integer',
        'time_precipitation_amt_precipitation': 'integer',
        # 'current_weather_time': 'timestamp',
        # 'current_weather_date': 'date',
        # 'current_weather_date_partition': 'date',
    } 

    df_temp_1 = df_final_6
    df_temp_1.cache()

    # https://stackoverflow.com/questions/70109882/how-to-check-if-a-pandas-column-value-appears-as-a-key-in-a-dictionary
    for each_col_official in d_schema.keys():
        if each_col_official in df_final_6.columns:
            df_temp_2 = df_temp_1.withColumn(each_col_official, col(each_col_official).cast(d_schema[each_col_official]))

            df_temp_1.unpersist()
            df_temp_1 = df_temp_2
 
    df_final_7 = df_temp_1

    # Step 7: Write to S3 bucket
    # https://stackoverflow.com/questions/63398078/write-new-data-into-the-existing-parquet-file-with-append-write-mode
    df_final_7 \
        .write \
        .partitionBy('current_weather_date_partition') \
        .mode('overwrite') \
        .parquet('/user/hadoop/processed/clean/minutely')

    # this is a test