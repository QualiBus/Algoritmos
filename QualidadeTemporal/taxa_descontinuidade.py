from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, unix_timestamp, when, stddev, mean, sum as _sum, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Taxa Descontinuidade Temporal") \
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

# Intervalos temporais por bus_id
window_spec = Window.partitionBy("bus_id").orderBy("updated_at")
df = df.withColumn("prev_time", lag("updated_at").over(window_spec))

df = df.withColumn("time_diff", 
                  unix_timestamp(col("updated_at")) - 
                  unix_timestamp(col("prev_time")))

df = df.filter(col("time_diff").isNotNull())

stats = df.groupBy("bus_id").agg(
    mean("time_diff").alias("avg_interval"),
    stddev("time_diff").alias("stddev_interval")
)

# Limiar para buracos
LIMIAR_MULTIPLIER = 3 
stats = stats.withColumn("limiar_buraco", 
                        col("avg_interval") + (LIMIAR_MULTIPLIER * col("stddev_interval")))

df = df.join(stats, "bus_id")

df = df.withColumn("buraco", 
                  when(col("time_diff") > col("limiar_buraco"), 1).otherwise(0))

# Métricas por veículo
metricas_por_bus = df.groupBy("bus_id").agg(
    count("*").alias("total_registros"),
    _sum("buraco").alias("total_buracos"),
    _sum(when(col("buraco") == 1, col("time_diff")).otherwise(0)).alias("tempo_total_buracos")
)

# Métricas globais
metricas_globais = df.agg(
    count("*").alias("total_registros_analisados"),
    _sum("buraco").alias("total_buracos_global"),
    _sum(when(col("buraco") == 1, col("time_diff")).otherwise(0)).alias("tempo_total_buracos_global")
)

metricas_por_bus = metricas_por_bus.withColumn(
    "taxa_buracos", 
    col("total_buracos")/col("total_registros")
)

metricas_globais = metricas_globais.withColumn(
    "taxa_buracos_global",
    col("total_buracos_global")/col("total_registros_analisados")
)

print("=== Métricas por Veículo ===")
metricas_por_bus.show()

print("\n=== Métricas Globais ===")
metricas_globais.show()

# Writing
metricas_por_bus.coalesce(1).write.csv("TaxaDescontinuidade/per_bus_out", header=True)
metricas_globais.coalesce(1).write.csv("TaxaDescontinuidade/global_out", header=True)

spark.stop()