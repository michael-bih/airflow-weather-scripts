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
    .appName('airflow-weather-project-emr-raw-to-clean-daily') \
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

def flatten_struct_prefix(df: DataFrame, column: str) -> DataFrame:
    original_position = df.columns.index(column)

    new_columns = df.select(column + ".*").columns
    flattened_columns = [df[(column + "." + c)].alias(column + "_" + c) for c in new_columns]

    col_list = [df[c] for c in df.columns]
    col_list.pop(original_position)
    col_list[original_position:original_position] = flattened_columns

    return df.select(col_list)

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
df = spark.sql('select case when max(current_dt) is null then 0 else max(current_dt) end as max_current_dt from airflow_weather_project_clean_daily_weather')
latest_processed_data_date = df.select('max_current_dt').collect()[0][0]

# Step 2: Read into df data from raw layer that is new based on the latest_processed_data_date
# spark.catalog.setCurrentDatabase('airflow-weather-project-raw')
df_spark_1 = spark.read.json('/user/hadoop/unprocessed/')
df_spark_1.createOrReplaceTempView('raw_weather_data')
df_spark_2 = spark.sql(f'''select * from raw_weather_data where current.dt > {latest_processed_data_date}''')

if df_spark_2.take(1):
    # Step 3: Get hourly weather into separate dataframe
    df_daily_3 = df_spark_2.select('lat', 'lon', 'timezone', 'current.dt', 'timezone_offset', 'daily')

    # Step 4: Expand daily array
    df_daily_array_expand_4 = df_daily_3.select('*', explode(col('daily'))) \
        .drop('daily') \
        .withColumnRenamed('dt', 'current_dt') \
        .withColumnRenamed('col', 'daily_weather_forecast')

    # Step 5: Flatten daily structs
    df_daily_flatten_5 = flatten_struct_prefix(df_daily_array_expand_4, 'daily_weather_forecast')

    struct_cols_to_flatten = []
    iterations = 1
    df_temp_1 = df_daily_flatten_5
    df_temp_1.cache()

    schema = {col: col_type for col, col_type in df_temp_1.dtypes}
    struct_cols_to_flatten = [col for col, col_type in schema.items() if 'struct' in col_type and 'array' not in col_type]

    while len(struct_cols_to_flatten) > 0:
        print(f'Starting with iteration {iterations}')

        df_temp_2 = flatten_struct_prefix_multiple(df_temp_1, struct_cols_to_flatten)
        df_temp_2 = df_temp_2.drop(*struct_cols_to_flatten)

        schema = {col: col_type for col, col_type in df_temp_2.dtypes}
        struct_cols_to_flatten = [col for col, col_type in schema.items() if 'struct' in col_type and 'array' not in col_type]

        df_temp_1.unpersist()
        df_temp_1 = df_temp_2
        iterations += 1

    df_daily_flatten_6 = df_temp_1

    # Step 6: Expand daily_weather_forecast_weather array
    df_daily_expand_7 = split_array_prefix(df_daily_flatten_6, 'daily_weather_forecast_weather')
    df_daily_flatten_8 = flatten_struct_prefix(df_daily_expand_7, 'daily_weather_forecast_weather[0]')

    # Step 7: Coalesce duplicate cols
    cols_to_coalesce = [col for col, col_type in schema.items() if '_int' in col or '_double' in col]
    final_coalesce_cols = []

    for each_col in cols_to_coalesce:
        if '_int' in each_col:
            final_coalesce_cols.append(each_col.rsplit('_int', 1)[0])
        else:
            final_coalesce_cols.append(each_col.rsplit('_double', 1)[0])

    final_coalesce_cols = list(set(final_coalesce_cols))
    d = {}

    for each_col in final_coalesce_cols:
        d[each_col] = [col for col in cols_to_coalesce if each_col in col]

    iterations = 1
    df_temp_1 = df_daily_flatten_8
    df_temp_1.cache()

    for each_col in final_coalesce_cols:
        print(f'Starting with iteration {iterations}')

        df_temp_2 = df_temp_1.withColumn(each_col, coalesce(df_temp_1[d[each_col][0]], df_temp_1[d[each_col][1]]).cast(FloatType())) \
            .drop(d[each_col][0], d[each_col][1])

        df_temp_1.unpersist()
        df_temp_1 = df_temp_2
        iterations += 1

    df_daily_coalesce_9 = df_temp_1

    # Step 8: Add date and timestamp cols
    df_final_10 = df_daily_coalesce_9 \
        .withColumn('current_weather_time', col('current_dt').cast(TimestampType())) \
        .withColumn('current_weather_date', to_date(col('current_weather_time')).cast(DateType())) \
        .withColumn('current_weather_date_partition', to_date(col('current_weather_time')).cast(DateType()))

    d_schema = {
        'lat': 'double',
        'lon': 'double',
        'timezone': 'string',
        'timezone_offset': 'integer',
        'current_dt': 'integer',
        'daily_weather_forecast_dt': 'integer',
        'daily_weather_forecast_sunrise': 'integer',
        'daily_weather_forecast_sunset': 'integer',
        'daily_weather_forecast_moonrise': 'integer',
        'daily_weather_forecast_moonset': 'integer',
        'daily_weather_forecast_moon_phase': 'double',
        'daily_weather_forecast_temp_day': 'double',
        'daily_weather_forecast_temp_min': 'double',
        'daily_weather_forecast_feels_like_day': 'double',
        'daily_weather_forecast_feels_like_night': 'double',
        'daily_weather_forecast_feels_like_morn': 'double',
        'daily_weather_forecast_temp_morn': 'double',
        'daily_weather_forecast_pressure': 'integer',
        'daily_weather_forecast_humidity': 'integer',
        'daily_weather_forecast_wind_deg': 'integer',
        'daily_weather_forecast_wind_gust': 'double',
        'daily_weather_forecast_clouds': 'integer',
        'daily_weather_forecast_rain': 'double',
        'daily_weather_forecast_weather[0]_id': 'integer',
        'daily_weather_forecast_weather[0]_main': 'string',
        'daily_weather_forecast_weather[0]_description': 'string',
        'daily_weather_forecast_weather[0]_icon': 'string',
        'daily_weather_forecast_uvi': 'float',
        'daily_weather_forecast_temp_eve': 'float',
        'daily_weather_forecast_feels_like_eve': 'double',
        'daily_weather_forecast_pop': 'float',
        'daily_weather_forecast_temp_night': 'float',
        'daily_weather_forecast_dew_point': 'float',
        # 'current_weather_time': 'timestamp',
        # 'current_weather_date': 'date',
        # 'current_weather_date_partition': 'date',
    } 

    df_temp_1 = df_final_10
    df_temp_1.cache()

    # https://stackoverflow.com/questions/70109882/how-to-check-if-a-pandas-column-value-appears-as-a-key-in-a-dictionary
    for each_col_official in d_schema.keys():
        if each_col_official in df_final_10.columns:
            df_temp_2 = df_temp_1.withColumn(each_col_official, col(each_col_official).cast(d_schema[each_col_official]))

            df_temp_1.unpersist()
            df_temp_1 = df_temp_2
 
    df_final_11 = df_temp_1

    # Step 9: Write to S3 bucket
    # https://stackoverflow.com/questions/63398078/write-new-data-into-the-existing-parquet-file-with-append-write-mode
    df_final_11.write.mode('overwrite').partitionBy('current_weather_date_partition') \
        .parquet('/user/hadoop/processed/clean/daily')