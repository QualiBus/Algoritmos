from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, regexp_extract, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

spark = SparkSession.builder \
    .appName("Identificar Linhas com Velocidades Zeradas") \
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

# Reading
df = spark.read.csv("path/*.csv", header=False, schema=_schema)

df = df.withColumn("file_path", input_file_name()) \
       .withColumn("line_code", regexp_extract(col("file_path"), r"line_code=([\d.]+)", 1).cast(DoubleType())) \
       .drop("file_path")

# Velocidades zeradas
df_grouped = df.groupBy("line_code").agg(
    when(count(when(col("bus_speed") == 0, True)) > 0, 1).otherwise(0).alias("has_zero_speed"),
    (count(when(col("bus_speed") == 0, True)) / count("*") * 100).alias("percent_zero_speed")
)

df_grouped.write.csv("path", header=True, mode="overwrite")

total_zero_speed_percent = df.select(
    (count(when(col("bus_speed") == 0, True)) / count("*") * 100
).alias("percent_zero_speed")
).collect()[0]["percent_zero_speed"]

# Writing
with open("out.txt", "w") as f:
    f.write(f"Percentual Geral de Velocidades Zeradas: {total_zero_speed_percent:.2f}%\n")

spark.stop()