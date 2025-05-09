from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, date_format, avg, unix_timestamp, udf, coalesce, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

spark = SparkSession.builder \
    .appName("check intervalos") \
    .config("spark.executor.memory", "10G") \
    .config("spark.driver.memory", "4G") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

_schema = StructType([
    StructField("bus_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("city_code", IntegerType(), True),
    StructField("adapted", BooleanType(), True), 
    StructField("agency", StringType(), True),
    StructField("line_url", StringType(), True),
    StructField("line_fare", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("bus_speed", DoubleType(), True),
    StructField("is_eletric", BooleanType(), True),
    StructField("bus_type", StringType(), True),
    StructField("bus_direction", IntegerType(), True),
    StructField("updated_at", TimestampType(), True)
])

df = spark.read.csv("/home/diego/dataset-df/dataset/df_per_line_code/line_code=*/*.csv", header=False, schema=_schema)

df = df.repartition(8)

window_spec = Window.partitionBy("bus_id").orderBy("updated_at")
df = df.withColumn("time_diff", 
                  unix_timestamp(col("updated_at")) - 
                  unix_timestamp(lag("updated_at").over(window_spec)))

df = df.filter(col("time_diff").isNotNull())


avg_per_bus = df.groupBy("bus_id").agg(avg("time_diff").alias("avg_seconds"))


overall_avg = df.agg(avg("time_diff").alias("overall_avg_seconds"))

def seconds_to_hhmmss(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

seconds_to_hhmmss_udf = udf(seconds_to_hhmmss, StringType())


avg_per_bus = avg_per_bus.withColumn("avg_interval", 
                                    seconds_to_hhmmss_udf(col("avg_seconds")))\
                                    .select("bus_id", "avg_interval")

overall_avg = overall_avg.withColumn("overall_avg", 
                                    seconds_to_hhmmss_udf(col("overall_avg_seconds")))\
                                    .select("overall_avg")

(avg_per_bus.coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .option("delimiter", ",")
 .csv("per_bus_id_avg"))

(overall_avg.coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .option("delimiter", ",")
 .csv("overall_avg"))

spark.stop()