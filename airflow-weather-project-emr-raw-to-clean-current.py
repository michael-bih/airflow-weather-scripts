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
    .appName('airflow-weather-project-emr-clean-to-analytic-hourly') \
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
# df = spark.read.option('mergeSchema', 'true').parquet('s3a://airflow-weather-project-clean/current-weather')
# df.createOrReplaceTempView('clean_weather_data')
df_1 = spark.sql('select case when max(current_dt) is null then 0 else max(current_dt) end as max_current_dt from airflow_weather_project_clean_current_weather')
latest_processed_data_date = df_1.select('max_current_dt').collect()[0][0]

# Step 2: Read into df data from raw layer that is new based on the latest_processed_data_date
# spark.catalog.setCurrentDatabase('airflow-weather-project-raw')
df_spark_1 = spark.read.json('/user/hadoop/unprocessed/')
df_spark_1.createOrReplaceTempView('raw_weather_data')
# df_spark_2 = spark.sql(f'''select * from raw_weather_data''')
df_spark_2 = spark.sql(f'''select * from raw_weather_data where current.dt > {latest_processed_data_date}''')

if df_spark_2.take(1):
    # Step 3: Get current weather into separate dataframe
    df_current_3 = df_spark_2.select('lat', 'lon', 'timezone', 'timezone_offset', 'current')

     # Step 4: Flatten "current" struct values into separate cols   
    df_current_flatten_4 = flatten_struct_prefix(df_current_3, 'current')
    
    # Step 5: Split "current weather" array values into separate cols   
    df_current_split_5 = split_array_prefix(df_current_flatten_4, 'current_weather')
    
    # Step 6: Flatten current weather struct
    df_flatten_6 = flatten_struct_prefix(df_current_split_5, 'current_weather[0]')
    
    # Step 7: Flatten all other structs and coalesce
    struct_cols_to_flatten = []
    iterations = 1
    df_temp_1 = df_flatten_6
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
    
    df_current_flatten_7 = df_temp_1

    # Step 8: Coalesce duplicate cols
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
    df_temp_1 = df_current_flatten_7
    df_temp_1.cache()
    
    for each_col in final_coalesce_cols:
        print(f'Starting with iteration {iterations}')
        
        df_temp_2 = df_temp_1.withColumn(each_col, coalesce(df_temp_1[d[each_col][0]], df_temp_1[d[each_col][1]]).cast(FloatType())) \
            .drop(d[each_col][0], d[each_col][1])
        
        df_temp_1.unpersist()
        df_temp_1 = df_temp_2
        iterations += 1
    
    df_daily_coalesce_8 = df_temp_1

    # Step 9: Add date and timestamp cols
    df_final_9 = df_daily_coalesce_8 \
        .withColumn('current_weather_time', col('current_dt').cast(TimestampType())) \
        .withColumn('current_weather_date', to_date(col('current_weather_time')).cast(DateType())) \
        .withColumn('current_weather_date_partition', to_date(col('current_weather_time')).cast(DateType()))

    d_schema = {
        'lat': 'double',
        'lon': 'double',
        'timezone': 'string',
        'timezone_offset': 'integer',
        'current_dt': 'integer',
        'current_sunrise': 'integer',
        'current_sunset': 'integer',
        'current_temp': 'double',
        'current_feels_like': 'double',
        'current_pressure': 'integer',
        'current_humidity': 'integer',
        'current_dew_point': 'double',
        'current_clouds': 'integer',
        'current_visibility': 'integer',
        'current_wind_speed': 'double',
        'current_wind_deg': 'integer',
        'current_wind_gust': 'double',
        'current_weather[0]_id': 'integer',
        'current_weather[0]_main': 'string',
        'current_weather[0]_description': 'string',
        'current_weather[0]_icon': 'string',
        'current_uvi': 'double',
        # 'current_weather_time': 'timestamp',
        # 'current_weather_date': 'date',
        # 'current_weather_date_partition': 'date',
    } 

    df_temp_1 = df_final_9
    df_temp_1.cache()

    # https://stackoverflow.com/questions/70109882/how-to-check-if-a-pandas-column-value-appears-as-a-key-in-a-dictionary
    for each_col_official in d_schema.keys():
        if each_col_official in df_daily_coalesce_8.columns:
            df_temp_2 = df_temp_1.withColumn(each_col_official, col(each_col_official).cast(d_schema[each_col_official]))

            df_temp_1.unpersist()
            df_temp_1 = df_temp_2
 
    df_final_10 = df_temp_1

    # Step 10: Write to S3 bucket
    # https://stackoverflow.com/questions/63398078/write-new-data-into-the-existing-parquet-file-with-append-write-mode
    # https://aws.amazon.com/blogs/big-data/seven-tips-for-using-s3distcp-on-amazon-emr-to-move-data-efficiently-between-hdfs-and-amazon-s3/
    df_final_10 \
        .write \
        .partitionBy('current_weather_date_partition') \
        .mode('overwrite') \
        .parquet('/user/hadoop/processed/clean/current')